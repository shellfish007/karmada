---
title: Elastic Workload Scheduling Gate
authors:
- "@hzheng182"
reviewers:
- TBD
approvers:
- TBD

creation-date: 2026-04-22
---

# Elastic Workload Scheduling Gate

## Summary

Elastic workloads (e.g., SparkApplication with dynamic resource allocation) create pods at runtime. Today these pods are scheduled immediately in the member cluster without control-plane approval. With `FederatedResourceQuota` (FRQ), this bypasses centralized quota enforcement — member clusters may lack local `ResourceQuota` objects, allowing workloads to exceed federated limits.

This proposal holds dynamically created pods via Kubernetes scheduling gates until the control plane approves the scale change. Two approaches are described with different trade-offs.

## Motivation

FRQ enforces aggregate resource limits per namespace across clusters from the control plane. Member clusters may not have corresponding per-cluster `ResourceQuota` objects (unless `StaticAssignments` is configured), so there is no local enforcement to prevent dynamic scale-up from exceeding the federated quota.

Once a workload is running in a member cluster, it can create new pods freely without consulting Karmada. There is no mechanism for the control plane to approve or deny a scale-up before new pods consume resources.

FederatedHPA does not help here. Certain elastic workloads have a two-layer architecture: an operator creates the initial resource (e.g., a driver or master pod), and that resource then dynamically spawns worker pods based on its own internal metrics and logic. This runtime scaling happens entirely within the member cluster, driven by the workload itself — not by a Kubernetes-managed replica field. FederatedHPA can only adjust a declared replica field in the spec; it cannot intercept or govern pods that are created autonomously by a running workload.

### Goals

- Hold elastic workload pods via scheduling gates until the control plane approves the scale-up.
- Reflect runtime status changes back to the control plane for quota enforcement.

### Non-Goals

- Providing a built-in member cluster controller for gate release — deployment-specific.
- Prescribing the mutating webhook that adds scheduling gates to pods — deployment-specific.

## User Stories

**Scale-Up:** Team A's elastic job requests 3 more workers. The new pods are created with scheduling gates (set by a mutating webhook). Status propagation detects the increased demand. The control plane approves against FRQ and communicates the approved count to the member cluster. Gates are released and pods run.

**Scale-Down:** The elastic job releases 3 workers. Pods terminate, status propagates, the control plane updates the approved state, and freed quota returns to the FRQ pool for the workload to reclaim if it scales back up.

**Blocked Scale-Up:** The elastic job requests 6 more workers but namespace quota is exhausted by other workloads in the same namespace. The approved state is not updated — all 6 pods remain `SchedulingGated`. When quota becomes available (e.g., another workload in the namespace finishes), the control plane re-evaluates, approves, and gates are released.

## Design Details

### Prerequisites

1. **Mutating webhook in member cluster** — adds a scheduling gate to new pods created by elastic workloads (deployment-specific).
2. **Status propagation** — workload demand must propagate back via existing `InterpretStatus` / `AggregateStatus` hooks.
3. **Member cluster gate-release controller** — watches for the approved replica signal and releases scheduling gates (deployment-specific).

### Core Problem

When an elastic workload scales in a member cluster, status propagates back via `InterpretStatus` / `AggregateStatus`, but no controller acts on these changes to communicate approval back. Additionally, the detector's `SpecificationChanged` filter (`pkg/util/eventfilter/eventfilter.go`) strips `.status` before comparing, so status updates do not re-trigger the detector pipeline.

---

### Approach A: Dedicated `ElasticWorkloadController`

**Branch:** [`elastic-workloads-dedicated-controller`](https://github.com/shellfish007/karmada/compare/master...elastic-workloads-dedicated-controller)

A new controller watches a configured status field on resource templates for declared elastic GVKs and stamps `karmada.io/approved-replicas` with the field's value. The annotation is the recalculation signal, not the quota source: it makes the detector treat the resource template as changed, re-run component/replica extraction from the latest template status, and update the quota-relevant `ResourceBinding` fields. That `ResourceBinding` update then triggers FRQ validation in the control plane before Work sync.

```
Member Cluster                    Control Plane
─────────────                    ─────────────
Workload scales up
  │
  ▼
Work.Status updated ──────────► RBStatusController
                                  │
                                  └─ AggregateStatus → write .status to resource template
                                                         │
                                ElasticWorkloadController │ (watches configured status field)
                                  │                       │
                                  ◄───────────────────────┘
                                  │
                                  ├─ read configured field → "5"
                                  ├─ approved-replicas already "5"? → no-op
                                  └─ stamp karmada.io/approved-replicas = "5"
                                       │
                                       ▼
                                  Detector picks up annotation change
                                       │
                                       ▼
                                  ResourceBinding updated
                                  (components/replicas recalculated from status)
                                       │
                                       ▼
                                  Scheduler re-evaluates (checks FRQ)
                                       │
                                       ▼
                                  Work synced to member cluster
                                       │
◄──────────────────────────────────────┘
Member controller reads
annotation, releases gates
```

Configuration:

```
--elastic-workload-gvks=sparkoperator.k8s.io/v1beta2/SparkApplication:status.executors
```

The controller maps GVK to field path. `SetupWithManager` builds a per-GVK predicate that fires only when the configured field changes.

| File | Change |
|------|--------|
| `pkg/features/features.go` | Add `ElasticWorkloadSchedulingGate` feature gate |
| `pkg/apis/work/v1alpha2/well_known_constants.go` | Add `ApprovedReplicasAnnotation` constant |
| `pkg/controllers/elasticworkload/controller.go` | New controller — watches configured status field, stamps annotation |

**Pros:**
- Clean separation of concerns — separate controller, no modifications to existing controllers
- Explicit opt-in via flag; admins can also not start the controller entirely
- Smallest change surface (~100 lines, 3 files)
- No interpreter hooks or Lua scripts required
- No extra API server GET — reads from informer cache (Approach B and the Alternative run inside `RBStatusController` right after `updateResourceStatus`, before the cache reflects the write, so they need a fresh GET)

**Cons:**
- Requires controller restart to add/change elastic GVKs
- New controller to register and configure

---

### Approach B: New `PostAggregateStatus` Interpreter Hook

**Branch:** [`elastic-workloads-post-aggregate-hook`](https://github.com/shellfish007/karmada/compare/master...elastic-workloads-post-aggregate-hook)

Adds a `PostAggregateStatus` interpreter operation called by `RBStatusController` after writing aggregated status. The hook returns a modified resource template (e.g., bumps an annotation), which passes `SpecificationChanged` and re-triggers the detector pipeline. The detector then updates `binding.Spec.Components` through the normal path.

```
Member Cluster                    Control Plane
─────────────                    ─────────────
Workload scales up
  │
  ▼
Work.Status updated ──────────► RBStatusController
                                  │
                                  ├─ 1. AggregateStatus → write .status to resource template
                                  │
                                  ├─ 2. PostAggregateStatus hook (Lua)
                                  │      ├─ GET resource template (with fresh .status)
                                  │      ├─ hook returns modified resource template
                                  │      └─ PATCH resource template (e.g. bumps annotation)
                                  │
                                  ▼
                                SpecificationChanged fires
                                  │
                                  ▼
                                Detector re-enqueues resource template
                                  │
                                  ├─ updates binding
                                  │
                                  ▼
                                Scheduler re-evaluates (checks FRQ)
                                  │
                                  ▼
                                Execution controller syncs Work
                                  │
◄─────────────────────────────────┘
Member controller reads
hook-written signal, releases gates
```

| File | Change |
|------|--------|
| `pkg/apis/config/v1alpha1/resourceinterpretercustomization_types.go` | Add `PostAggregateStatus` field and type |
| `pkg/apis/config/v1alpha1/zz_generated.deepcopy.go` | DeepCopy for new type |
| `pkg/resourceinterpreter/customized/declarative/configmanager/accessor.go` | Accessor getter/setter |
| `pkg/resourceinterpreter/customized/declarative/luavm/lua.go` | Lua VM function |
| `pkg/resourceinterpreter/customized/declarative/configurable.go` | HookEnabled + method |
| `pkg/resourceinterpreter/interpreter.go` | Interface method + implementation |
| `pkg/controllers/status/common.go` | Add `applyPostAggregateStatus` |
| `pkg/controllers/status/rb_status_controller.go` | Call `applyPostAggregateStatus` (1 line) |
| `pkg/controllers/status/crb_status_controller.go` | Same for cluster-scoped resources |

**Pros:**
- Users control what triggers re-detection and what flows to member cluster
- Fits naturally into the interpreter framework

**Cons:**
- Larger change surface (~270 lines, 10 files)
- Users must write 2 Lua scripts (`GetComponents` + `PostAggregateStatus`)
- Extra API server GET/PATCH on every status reconcile where the hook fires
- Extra reconcile hop adds latency vs Approach A
- No feature gate — disabling requires removing hook configuration
- Communication protocol is fully user-defined, reducing interoperability

---

### Comparison

| Criterion | A: Dedicated Controller | B: PostAggregateStatus Hook |
|-----------|------------------------|----------------------------|
| Code change | ~100 lines, 3 files (1 new) | ~270 lines, 10 files |
| New abstractions | New controller | New interpreter operation |
| Separation of concerns | Clean (separate controller) | Clean (hook triggers existing pipeline) |
| Timing correctness | Correct (predicate fires only on configured field change) | Extra hop (status -> hook -> detector -> binding) |
| Opt-in mechanism | GVK + field path flag; admin can also not start the controller | HookEnabled (per-type hook config) |
| Member cluster protocol | Karmada-standard annotation (raw field value) | User-defined (hook controls what flows down) |
| Quota enforcement | Annotation change can trigger FRQ re-evaluation in control plane | Scheduler re-evaluates FRQ |
| User Lua scripts | 0 | 2 (`GetComponents` + `PostAggregateStatus`) |
| Feature gate | Yes (`ElasticWorkloadSchedulingGate`) | No (remove hook config to disable) |

### Edge Cases

**Partial approval:** Quota enforcement is all-or-nothing per scale event. If a workload requests 6 more workers but FRQ only has capacity for 2, the entire scale-up is blocked — all 6 pods remain `SchedulingGated`. When sufficient quota becomes available, the control plane re-evaluates and approves the full request.

**Concurrent scale-up:** When multiple elastic workloads in the same namespace scale simultaneously, quota contention is resolved through Kubernetes optimistic locking. Each workload's approval is processed independently; if a conflict occurs (e.g., two workloads compete for remaining quota), the losing update is retried with exponential backoff.

**Control plane unavailability:** If the control plane is unreachable, pods remain `SchedulingGated` indefinitely. Status cannot propagate back, the approval annotation is never stamped, and gates are never released. This is intentional — it prevents uncontrolled scale-up when centralized quota enforcement is unavailable. Recovery is automatic once connectivity is restored and status propagation resumes.

## Test Plan

- **Unit tests:** Controller logic for annotation stamping, predicate filtering, and conflict retry.
- **Integration tests:** End-to-end flow from status propagation through annotation stamping to FRQ validation webhook rejection/approval.
- **E2E tests:** Full cycle with a sample elastic workload: scale-up approved, scale-down quota release, and blocked scale-up when FRQ is exhausted.

## Alternatives

### Reuse `GetComponents` in `RBStatusController`

**Branch:** [`elastic-workloads-annotation-impl`](https://github.com/shellfish007/karmada/compare/master...elastic-workloads-annotation-impl)

After `updateResourceStatus`, `RBStatusController` calls `GetComponents` on the resource template and patches `binding.Spec.Components` if changed. The binding controller then stamps `karmada.io/approved-replicas` on the Work manifest via `setApprovedReplicasAnnotation` in `ensureWork()`.

```
Member Cluster                    Control Plane
─────────────                    ─────────────
Workload scales up
  │
  ▼
Work.Status updated ──────────► RBStatusController
                                  │
                                  ├─ AggregateStatus → write .status to resource template
                                  │
                                  ├─ GET resource template (extra API call)
                                  ├─ GetComponents (Lua VM, every reconcile)
                                  ├─ diff against current binding.Spec.Components
                                  └─ PATCH binding.Spec.Components if changed
                                       │
                                       ▼
                                  Binding controller → ensureWork()
                                  stamps karmada.io/approved-replicas on Work
                                       │
                                       ▼
                                  Work synced to member cluster
                                       │
◄──────────────────────────────────────┘
Member controller reads
annotation, releases gates
```

**Why not chosen:**
- Mixes status aggregation with spec mutation in `RBStatusController` — violates single-responsibility.
- `GetComponents` (potentially Lua VM) runs on every status reconcile even when nothing changed. Per-workload opt-in (e.g., annotation check) would add complexity to the hot path; the feature gate is too coarse (all-or-nothing for the entire control plane).
- Extra API server GET on every Work status change.
