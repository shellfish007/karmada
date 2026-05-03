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
	"fmt"
	"reflect"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/sharedcli/ratelimiterflag"
)

// ControllerName is the controller name that will be used when reporting events and metrics.
const ControllerName = "elastic-workload-controller"

// Controller watches a configured status field of resource templates for listed elastic GVKs.
// When the watched field changes (written by RBStatusController via updateResourceStatus),
// the controller bumps a karmada annotation on the resource template. This passes
// SpecificationChanged, re-enqueues the resource template in the detector, and
// GetComponents runs through the normal detection pipeline to update binding.Spec.Components.
//
// The annotation is stripped from Work manifests in ensureWork() so it never reaches
// member clusters.
type Controller struct {
	client.Client
	RESTMapper         meta.RESTMapper
	RateLimiterOptions ratelimiterflag.Options
	// ElasticWorkloadGVKs maps each elastic GVK to the dot-separated status field path
	// to watch for changes (e.g. "status.executors"). Only changes to that specific field
	// trigger annotation bumping. Populated from the --elastic-workload-gvks flag:
	//   --elastic-workload-gvks=sparkoperator.k8s.io/v1beta2/SparkApplication:status.executors
	ElasticWorkloadGVKs map[schema.GroupVersionKind]string
}

// Reconcile bumps the elastic-demand annotation on the resource template to trigger
// the detector. The request NamespacedName refers to the resource template itself.
func (c *Controller) Reconcile(ctx context.Context, req controllerruntime.Request) (controllerruntime.Result, error) {
	if !features.FeatureGate.Enabled(features.ElasticWorkloadSchedulingGate) {
		return controllerruntime.Result{}, nil
	}

	klog.V(4).InfoS("Triggering elastic workload re-detection", "resource", req.NamespacedName)

	resource := &unstructured.Unstructured{}
	// controller-runtime populates GVK on the returned object when the cache knows the type.
	if err := c.Client.Get(ctx, req.NamespacedName, resource); err != nil {
		if apierrors.IsNotFound(err) {
			return controllerruntime.Result{}, nil
		}
		return controllerruntime.Result{}, err
	}

	annotations := resource.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[workv1alpha2.ElasticDemandAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
	resource.SetAnnotations(annotations)

	if err := c.Client.Update(ctx, resource); err != nil {
		if apierrors.IsConflict(err) {
			return controllerruntime.Result{Requeue: true}, nil
		}
		return controllerruntime.Result{}, fmt.Errorf("failed to bump elastic-demand annotation: %w", err)
	}
	return controllerruntime.Result{}, nil
}

// SetupWithManager creates the controller and registers it with the manager.
// For each GVK in ElasticWorkloadGVKs, Watches() is called with a predicate that
// closes over the configured field path and fires only when that specific field changes.
func (c *Controller) SetupWithManager(mgr controllerruntime.Manager) error {
	bldr := controllerruntime.NewControllerManagedBy(mgr).Named(ControllerName)

	for gvk, fieldPath := range c.ElasticWorkloadGVKs {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)

		// Split once at setup time; the predicate closure captures the parts slice.
		parts := strings.Split(fieldPath, ".")

		fieldChangedPredicate := builder.WithPredicates(predicate.Funcs{
			CreateFunc:  func(event.CreateEvent) bool { return false },
			DeleteFunc:  func(event.DeleteEvent) bool { return false },
			GenericFunc: func(event.GenericEvent) bool { return false },
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldU, ok1 := e.ObjectOld.(*unstructured.Unstructured)
				newU, ok2 := e.ObjectNew.(*unstructured.Unstructured)
				if !ok1 || !ok2 {
					return false
				}
				oldVal, _, _ := unstructured.NestedFieldNoCopy(oldU.Object, parts...)
				newVal, _, _ := unstructured.NestedFieldNoCopy(newU.Object, parts...)
				return !reflect.DeepEqual(oldVal, newVal)
			},
		})

		bldr = bldr.Watches(u, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, obj client.Object) []controllerruntime.Request {
				return []controllerruntime.Request{{
					NamespacedName: client.ObjectKeyFromObject(obj),
				}}
			}),
			fieldChangedPredicate,
		)
	}

	return bldr.
		WithOptions(controller.Options{RateLimiter: ratelimiterflag.DefaultControllerRateLimiter[controllerruntime.Request](c.RateLimiterOptions)}).
		Complete(c)
}
