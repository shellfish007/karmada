/*
Copyright 2025 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package elasticworkload

import (
	"context"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter"
	"github.com/karmada-io/karmada/pkg/sharedcli/ratelimiterflag"
	"github.com/karmada-io/karmada/pkg/util/restmapper"
)

// ControllerName is the controller name that will be used when reporting events and metrics.
const ControllerName = "elastic-workload-controller"

// Controller watches Work status changes and re-evaluates component demand from
// the status-enriched resource template for elastic workload types.
// Only GVKs listed in ElasticWorkloadGVKs are processed.
type Controller struct {
	client.Client
	DynamicClient        dynamic.Interface
	RESTMapper           meta.RESTMapper
	ResourceInterpreter  resourceinterpreter.ResourceInterpreter
	RateLimiterOptions   ratelimiterflag.Options
	// ElasticWorkloadGVKs is the set of GVKs that support runtime elastic scaling.
	// Populated from the --elastic-workload-gvks flag at controller startup.
	// Format: "group/version/kind", e.g. "sparkoperator.k8s.io/v1beta2/SparkApplication".
	ElasticWorkloadGVKs sets.Set[schema.GroupVersionKind]
}

// Reconcile re-evaluates component demand from the status-enriched resource template.
func (c *Controller) Reconcile(ctx context.Context, req controllerruntime.Request) (controllerruntime.Result, error) {
	if !features.FeatureGate.Enabled(features.ElasticWorkloadSchedulingGate) {
		return controllerruntime.Result{}, nil
	}

	klog.V(4).InfoS("Reconciling elastic workload components", "binding", req.NamespacedName)

	binding := &workv1alpha2.ResourceBinding{}
	if err := c.Client.Get(ctx, req.NamespacedName, binding); err != nil {
		if apierrors.IsNotFound(err) {
			return controllerruntime.Result{}, nil
		}
		return controllerruntime.Result{}, err
	}

	if !binding.DeletionTimestamp.IsZero() {
		return controllerruntime.Result{}, nil
	}

	if err := c.syncComponents(ctx, binding); err != nil {
		return controllerruntime.Result{}, err
	}
	return controllerruntime.Result{}, nil
}

func (c *Controller) syncComponents(ctx context.Context, binding *workv1alpha2.ResourceBinding) error {
	gvk := schema.FromAPIVersionAndKind(binding.Spec.Resource.APIVersion, binding.Spec.Resource.Kind)

	// Only process GVKs explicitly listed as elastic workload types.
	if c.ElasticWorkloadGVKs.Len() > 0 && !c.ElasticWorkloadGVKs.Has(gvk) {
		return nil
	}

	gvr, err := restmapper.GetGroupVersionResource(c.RESTMapper, gvk)
	if err != nil {
		return err
	}

	resource, err := c.DynamicClient.Resource(gvr).Namespace(binding.Spec.Resource.Namespace).Get(
		ctx, binding.Spec.Resource.Name, metav1.GetOptions{},
	)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// Multi-component workload: update binding.Spec.Components.
	if c.ResourceInterpreter.HookEnabled(gvk, configv1alpha1.InterpreterOperationInterpretComponent) {
		components, err := c.ResourceInterpreter.GetComponents(resource)
		if err != nil {
			return err
		}
		if reflect.DeepEqual(components, binding.Spec.Components) {
			return nil
		}
		patch := client.MergeFrom(binding.DeepCopy())
		binding.Spec.Components = components
		return c.Client.Patch(ctx, binding, patch)
	}

	// Single-component workload: update binding.Spec.Replicas.
	if c.ResourceInterpreter.HookEnabled(gvk, configv1alpha1.InterpreterOperationInterpretReplica) {
		replicas, _, err := c.ResourceInterpreter.GetReplicas(resource)
		if err != nil {
			return err
		}
		if replicas == binding.Spec.Replicas {
			return nil
		}
		patch := client.MergeFrom(binding.DeepCopy())
		binding.Spec.Replicas = replicas
		return c.Client.Patch(ctx, binding, patch)
	}

	return nil
}

// SetupWithManager creates the controller and registers it with the manager.
func (c *Controller) SetupWithManager(mgr controllerruntime.Manager) error {
	workMapFunc := handler.MapFunc(
		func(_ context.Context, workObj client.Object) []reconcile.Request {
			annotations := workObj.GetAnnotations()
			namespace, nsExist := annotations[workv1alpha2.ResourceBindingNamespaceAnnotationKey]
			name, nameExist := annotations[workv1alpha2.ResourceBindingNameAnnotationKey]
			if !nsExist || !nameExist {
				return nil
			}
			return []reconcile.Request{{
				NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
			}}
		})

	workStatusPredicate := builder.WithPredicates(predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldWork, ok1 := e.ObjectOld.(*workv1alpha1.Work)
			newWork, ok2 := e.ObjectNew.(*workv1alpha1.Work)
			if !ok1 || !ok2 {
				return false
			}
			return !reflect.DeepEqual(oldWork.Status, newWork.Status)
		},
	})

	return controllerruntime.NewControllerManagedBy(mgr).
		Named(ControllerName).
		Watches(&workv1alpha1.Work{}, handler.EnqueueRequestsFromMapFunc(workMapFunc), workStatusPredicate).
		WithOptions(controller.Options{RateLimiter: ratelimiterflag.DefaultControllerRateLimiter[controllerruntime.Request](c.RateLimiterOptions)}).
		Complete(c)
}
