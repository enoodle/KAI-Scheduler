# Deferred Session Action Commit

Related issue: [#2319](https://github.com/kai-scheduler/KAI-Scheduler/issues/2319).

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Expected behavior](#expected-behavior)
  - [Limitations and risks](#limitations-and-risks)
- [Design Details](#design-details)
  - [Session lifecycle](#session-lifecycle)
  - [Journal and normalization](#journal-and-normalization)
    - [Baseline capture](#baseline-capture)
  - [External effects and ordering](#external-effects-and-ordering)
  - [Failures, shutdown, and restart](#failures-shutdown-and-restart)
  - [Related scenario-validation work](#related-scenario-validation-work)
  - [Implementation scope](#implementation-scope)
  - [Monitoring](#monitoring)
  - [Test plan](#test-plan)
  - [Graduation criteria](#graduation-criteria)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

The scheduler will plan all actions against one mutable, in-memory session, then resolve and execute their surviving Kubernetes effects at the end of the session. A later eviction of a Pod newly allocated or pipelined in that session cancels that earlier placement before either operation reaches Kubernetes. Surviving BindRequests and evictions run through bounded workers, which publish accepted BindRequests to the informer store; the scheduler goroutine owns journal resolution and session-state finalization.

## Motivation

On main, Allocate creates each BindRequest synchronously before scheduling the next Pod. Dispatching surviving BindRequests in parallel removes that serial API wait; dropping operations superseded by later actions avoids unnecessary API work.

Deferring effects also gives the scheduler a complete view of action decisions before committing anything to Kubernetes. It can cancel or coalesce conflicting intents and control dispatch order, dependencies, and Events. Today actions commit as they finish, so later actions cannot revise effects already exposed to the cluster.

For example, fixing [issue #2274](https://github.com/kai-scheduler/KAI-Scheduler/issues/2274) can enable reclaim of a Pod placed only virtually by an earlier action. Deferred commit lets the placement and eviction cancel before either reaches Kubernetes.

### Goals

- Create surviving BindRequests in parallel instead of serially as on main, and dispatch independent evictions concurrently.
- Avoid Kubernetes writes for superseded operations; give the scheduler a session-wide commit plan that can cancel, coalesce, and order effects after all actions have run.
- Cancel a same-session pending Pod's allocation or pipeline when later evicted, without a BindRequest, deletion, scheduler Event, Pod condition, or PodGroup status effect from that pair; preserve evictions of Pods already active at session open.
- Let later actions inspect virtual placements and evictions while keeping `Session`, plugin, and `NodeInfo` mutations single-threaded; workers publish accepted BindRequests through the thread-safe informer store.
- Create BindRequests idempotently, publish successful requests to the scheduler cache, and require Binder to verify the target Pod UID.

### Non-Goals

- An atomic transaction across Kubernetes BindRequests, Pod deletions, Events, and statuses.
- Rebinding an already bound Pod to a different node or GPU placement under the same UID.
- Canceling API effects from a previous session, another scheduler, or a controller.
- Parallel placement or scenario simulation.
- Changing queue fairness or scenario-validation job selection in this change.

## Proposal

Actions still execute in configured order, and successful statements still update the session and plugin accounting immediately. Instead of performing external effects, they transfer immutable intents into a session journal. Every action and planning hook must use this path; there is no alternative way to commit scheduling effects. The journal is normalized after the last action and after plugins that can add planning work have finished. Only the resulting effects are sent to workers. Kubernetes-facing status and Events for surviving operations are emitted after resolution.

### Expected behavior

| Session start | Actions within session | End-of-session effect |
| --- | --- | --- |
| Pod Pending, no BindRequest | Allocate, then later eviction of same UID | Neither bind nor eviction; original Pod remains Pending; no scheduler-created placement/eviction Event |
| Pod Pending, no BindRequest | Pipeline, then later eviction of same UID | Neither pipeline Event nor deletion; original Pod remains Pending |
| Pod Pending, no BindRequest | Allocate or pipeline, no later eviction | Create BindRequest or emit pipeline Event, respectively |
| Pod already active | Later eviction | Delete original UID and report real eviction |
| Pod already active | Evict, then restore its same placement before dispatch | No delete; preserve original Pod |

Cancellation uses the Pod UID and the session-opening state, not namespace/name alone. It applies only to Pods that were Pending without a BindRequest at session open. Existing BindRequests remain under the current Binder and failed-request cleanup lifecycle. The final journal must contain at most one primary placement or deletion decision per Pod UID; labels and Events follow only a surviving decision.

### Limitations and risks

- All actions and pre-close planning hooks must finish before any planned Kubernetes effect is committed. An early placement therefore waits for the remaining planning, including potentially expensive reclaim or preemption simulations, before Binder can begin processing it. Evictions are delayed by the same boundary. This added latency and loss of overlap between planning and cluster execution are an accepted drawback; parallel dispatch may improve total throughput but cannot remove that initial wait.
- The final plan is not durable. A crash before dispatch loses planned changes; a crash during dispatch can leave a partial result. The next snapshot and existing stale cleanup reconstruct API truth.
- API acceptance does not mean a Pod has bound or a deletion has completed. Surviving pipelines continue to wait for capacity in later sessions.
- Concurrent API effects can complete out of order. The implementation must enforce only actual dependencies and must not report gang atomicity.
- A deferred queue can grow with the number of planned tasks. Measure peak entries and memory on scale workloads.
- Other controllers may produce Kubernetes Events for the same Pod. The no-event guarantee applies to scheduler-origin effects canceled before dispatch.

## Design Details

### Session lifecycle

```text
Open session and record authoritative starting identities before plugin hooks
  -> run configured actions in order; accept successful virtual statements
  -> finish pre-close planning hooks (including background-pod restoration)
  -> normalize journal and reconcile canceled virtual state
  -> freeze immutable external-effect payloads
  -> dispatch surviving operations; workers publish accepted BindRequests
  -> drain and join workers; apply results on scheduler goroutine
  -> close plugin state; record final PodGroup/Pod statuses; release session
```

`Statement.Discard` and rollback still reverse tentative simulation operations. Acceptance must make a statement's operations unavailable for a second accept. All action loops continue using virtual task/node/plugin state; no worker runs before normalization. A session-owned executor dispatches surviving effects after normalization.

The close path needs a planning phase before plugin teardown. `backgroundpods.OnSessionClose` currently restores background Pods and commits remaining evictions. Move that planning work into the pre-close phase so it can add intents while plugin accounting is live; `OnSessionClose` then releases state. Stale gang eviction must also use a virtual statement and journal acceptance instead of direct `Session.Evict`. Audit other direct cache writes and plugin hooks before the executor becomes authoritative.

### Journal and normalization

An accepted statement contributes ordered records containing action provenance, Pod namespace/name/UID, initial state identity, operation kind, immutable placement or eviction metadata, and a sequence number. The journal does not retain mutable `PodInfo` or a live `Statement` as worker input. Placement payloads must include GPU, NUMA, DRA, labels, annotations, and node selection after plugin mutation.

`Statement.Evict`, `Statement.Allocate`, `Statement.Pipeline`, and restoration operations validate journal-specific transitions against the current virtual state. They reject duplicate evictions, conflicting intents, and same-UID rebinding of an already active Pod before changing state or recording an intent. Existing scheduling paths remain responsible for resource-fit checks; statements do not repeat those checks. Actions handle journal errors while planning; normalization folds valid transitions rather than resolving invalid plans by dropping individual Pods or abandoning the session.

Single-threaded normalization folds the records for each UID against that Pod's session-opening state:

1. Ignore operations undone within their statement.
2. Preserve each surviving virtual transition for later action decisions, but collapse its external effect at the end.
3. Cancel an Allocate/Pipeline followed by Evict when the Pod was Pending at session open and no prior external placement existed.
4. Preserve an eviction of a Pod active at session open unless it was restored without changing its original placement. A changed placement of an already bound Pod cannot become a new BindRequest for that UID.

   Existing reclaim/consolidation simulations may pipeline an evicted active Pod elsewhere to model its future replacement. Preserve that simulation, but retain the original-UID eviction; neither a new BindRequest nor a pipeline Event is committed for that UID.
5. Restore canceled pending tasks to their original in-memory state before job-status recording: remove their virtual node charge, clear placement, and return them to Pending. The Allocate and Deallocate plugin callbacks have already offset each other; do not apply a second Deallocate callback. Assert the final node and queue accounting against the session-opening state plus surviving intents.

An eviction of a newly allocated/pipelined Pod is a cancellation, not a real capacity release. Existing placement logic continues to distinguish physically idle capacity, including capacity returned by a canceled virtual allocation, from capacity promised by real evictions. Work depending on a real eviction remains pipelined and never creates a BindRequest in this session.

An accepted eviction immediately marks the Pod Releasing, excluding it from later victim selection. There can be only one surviving eviction per UID, carrying its action's reason and preemptor. If an eviction is undone and a later action evicts the restored Pod, only the later eviction survives. A canceled pair emits neither operation's Events or status updates.

#### Baseline capture

Before plugin hooks, record only each Pod UID's opening status and external BindRequest presence from task references and raw snapshot-map entries, including failed requests. Preserve opening job-start timestamps separately. These records retain no Kubernetes objects; do not deep-copy every Pod at session opening.

Lazily capture one owned baseline per touched UID, independent of accepted effects and retained through replacement, tentative rollback, and accepted Unevict. Include status, node, virtual-status marker, fractional GPU groups, NUMA placement, DRA claim allocations, extended resource claim UID/allocation, accepted GPU requirement/resource vector, and received resource type. Deep-copy nested mutable placement values and clone again on restoration; the baseline is not an executable action.

Capture idempotently before pre-predicate/predicate callbacks, NUMA evaluation, fractional GPU selection, and direct Statement mutations, including opening background-pod eviction. Acceptance or `Statement.Allocate` alone is too late. Opening NUMA hydration reconstructs physical placement from persisted records; preserve this authoritative original placement separately from speculative mutations, without reordering hooks or changing accounting. Never reconstruct a baseline from later virtual state.

Per-operation undo records hold the immediately preceding state, separately from the session baseline and accepted-effect copies. Eviction undo needs only node, status, virtual marker, GPU groups, NUMA placement, and DRA claims. Worker payloads remain immutable; accepted allocation snapshots must preserve annotation callbacks' unrestricted `PodInfo` input contract until it is separately narrowed.

### External effects and ordering

After normalization, a bounded pool performs immutable API calls and publishes accepted BindRequests directly to the thread-safe informer store. Each worker inserts a deep copy immediately after a successful Create or matching `AlreadyExists` recovery, preserving `origin/main` publication behavior without a separate reservation overlay or routine refresh Gets. Workers do not call session methods or emit Events. Start with one `max(1, ceil(k8sClientQPS))` concurrency budget shared by BindRequest Creates and Pod Deletes; each client still applies its own rate limiter. The finite journal is the pending queue. Per-UID deduplication prevents concurrent effects on one Pod. Independent UIDs can dispatch in parallel. If a real allocation depends on an eviction's completion, it remains pipelined and is not bound at this boundary.

BindRequest workers use deterministic names, up to five attempts for transient API errors with a 20 ms initial backoff, and a direct Get plus intent comparison on `AlreadyExists`. Cancellation stops queued and in-flight attempts. Binder verifies that the fetched Pod UID matches the BindRequest's Pod owner-reference UID before binding or updating Pod status. Eviction workers must verify the target UID and use a Kubernetes Delete UID precondition; a same-name replacement must never be deleted. A surviving eviction's optional Pod-condition patch also needs an atomic UID check. Refactor `SchedulerCache.Evict` so the worker reports the Delete outcome rather than launching another untracked goroutine. Emit eviction status/Event only for a surviving, confirmed Delete request, not for a canceled intent or failed precondition.

After all workers join, the scheduler goroutine consumes results, updates task state, then emits surviving Events and status changes. `StatusUpdater.PreBind`, Pod-label patches, `Scheduled`, `Pipelined`, and eviction reporting move to this finalization path. Pod-label and status writes must be guarded by UID so they cannot affect a same-name replacement. `RecordJobStatusEvent` runs only after journal normalization and result handling; otherwise a canceled virtual transition can leak as a PodGroup status/Event. No worker may mutate plugin accounting. Direct store insertion retains the existing race with newer informer watch updates; this change does not add conditional publication semantics.

### Failures, shutdown, and restart

- A canceled intent has no API request and needs no compensation.
- A failed BindRequest Create retains its virtual gang slot for the remainder of this session; the next snapshot repairs truth.
- A failed or ambiguous Delete leaves no assumed physical capacity. Report failure and let the next session re-evaluate; dependent work remains pipelined.
- Retry transient Delete errors with the same five-attempt, 20 ms initial backoff. For ambiguous Delete, direct Get with UID comparison distinguishes a still-present original Pod from NotFound or a replacement. Never delete by name after a UID mismatch.
- Leadership loss cancels queued and in-flight work, then joins workers before session teardown. Successful API operations already accepted by Kubernetes are not rolled back.
- A canceled pending Pod must not receive delayed status-updater payloads from this session. Queued status work is reconciled by UID when the final plan is known.

### Related scenario-validation work

The separate scenario-validation fix can reduce simulation work by ordering only the preemptor and proposed victim jobs. It must preserve ordering semantics for participating jobs and fall back to the full queue where a plugin requires it. The session journal makes an earlier virtual placement cancellable if that Pod becomes a later victim. Integrate the filtering patch after session cancellation tests pass.

### Implementation scope

- `framework.Statement`: validate journal-specific transitions; transfer valid operations as immutable records; keep rollback semantics for unaccepted statements.
- `framework.Session` and scheduler run loop: own the journal, pre-close planning, normalization, bounded executor, and finalization barrier.
- Allocate/Reclaim/Preempt/Consolidation/StaleGangEviction: accept virtual statements without external commits; make stale gang eviction virtual.
- `backgroundpods` and session lifecycle: separate final planning from plugin teardown.
- `cache` and `status_updater`: split API calls, informer publication, status/Event emission, and UID-safe eviction; move `PreBind` to surviving bind finalization.
- No CRD or user-facing configuration change is proposed. BindRequest Creates and Pod Deletes share one QPS-derived worker budget.

### Monitoring

Record planned, canceled, surviving, dispatched, succeeded, and failed operations by kind and action; queue depth, dispatch and drain duration; API retry/throttle rates; BindRequest and eviction latency; informer publication failures; and unexpected same-UID conflicts. Do not use Pod UID as a metric label. Keep per-session structured logs with action sequence, Pod UID, resolution reason, and API outcome. Compare `Scheduled`, `Pipelined`, and `Evict` Event counts with surviving intents in tests and during rollout.

### Test plan

1. Unit tests for statement transfer, undo filtering, UID folding, plugin-state consistency, and final job-status projection. Verify invalid journal transitions, duplicate evictions, conflicting intents, and same-UID rebinding fail at the statement call without changing state or recording an intent. Cover capture before preparation and opening hooks, baseline retention through rollback/replacement/Unevict, nested GPU/NUMA/DRA ownership, and canceled job-start timestamps. Verify existing hook order and accounting remain unchanged.
2. Session tests for Allocate -> Reclaim/Preempt/Consolidation cancellation, pipeline -> eviction cancellation, active-Pod eviction, same-placement Unevict, stale gang eviction, and background-pod restoration.
3. Fake-client tests asserting zero Create/Delete/Event/status calls for canceled pairs and exactly one UID-safe effect for surviving intents, including same-name replacement and ambiguous API results.
4. Envtest with a real API server and Binder: no BindRequest or Pod deletion for a canceled pending Pod; surviving BindRequests are visible before the next session; interrupted or partially failed dispatch converges on the next session.
5. With the separate issue #2274 fix, run the reclaim regression and E2E workload: reclaim progresses without deleting or emitting scheduler Events for a canceled virtual Pod. Also test a real allocation canceled by later reclaim.
6. Matched baseline/candidate scale runs: compare session planning time, wait from placement decision to dispatch, time to first BindRequest, commit/drain time, overall fill time, API/Binder load, p50/p95/p99 Create/Delete latency, peak journal memory, and scheduling outcomes. Benchmark opening-only, sparse/all-touched, and repeated-effect replacement across Pod sizes, including GPU/NUMA/DRA; measure allocations and retained memory before claiming copy-related gains. Measure scenario-validation CPU separately when integrating the related fix. Run the same workload and configuration sequentially.

### Graduation criteria

1. **Internal MVP:** journal covers all default actions and background-pod planning; canceled pending Pods cause no Kubernetes effects; focused unit and envtests pass.
2. **Integrated correctness:** reclaim E2E passes with the separate scenario-validation fix; UID-safe retry and failure paths are exercised; no regressions in gang, elastic, GPU-sharing, NUMA, and DRA paths.
3. **Production readiness:** matched scale runs show an acceptable end-to-end cost, API/Binder health is stable, journal memory is bounded in observed workloads, and monitoring distinguishes canceled work from failed external work.

## Alternatives

- **Create BindRequests during Allocate:** could overlap API calls with later planning, but BindRequests and pipeline Events can escape before later actions cancel their Pod.
- **Delete a BindRequest after later eviction:** the Binder may already have bound the Pod; deletion cannot retract Events or other effects.
- **Reorder actions or ban pipelined victims:** may avoid the reproduced path but does not give a general cross-action cancellation boundary.
- **Serialize all end-of-session effects:** simplifies ordering but forfeits the parallel persistence goal.
