# Safe Settlement Queue Recovery Design

## Scope

This note defines what it should mean to "safely process the queue" for committed
settlement tasks after crash recovery or task reassignment.

The immediate target is:

- `SettleInternalMatchTask`
- `SettlePrivateMatchTask`

The broader goal is to make the recovery logic used after snapshot recovery the
same logic used any time a committed in-flight settlement task is resumed.

## Problem Statement

Today the system has three separate facts that do not line up:

1. The queue marks settlement tasks as committed before the on-chain leg runs.
2. The queue only persists a display string plus a committed bit for running tasks.
3. Reassignment/restart of a committed running task currently just reruns it.

That combination is unsafe for settlement tasks because the task may have crashed
in any of these states:

- before the transaction was sent
- after the transaction was sent but before local post-settlement state was written
- after some local post-settlement state was written but not all of it

The current code acknowledges this directly:

- `crates/state/src/applicator/task_queue.rs`
  - committed reassignment should be "smarter" and check on-chain state

## Current Behavior

### Queue persistence

`QueuedTaskState::Running` stores:

- `state: String`
- `committed: bool`

It does not store typed task state or task-specific recovery payload.

### Task reconstruction

The task driver reconstructs tasks from `TaskDescriptor` alone.

That means a restarted settlement task comes back with:

- `task_state = Pending`
- no persisted `updated_order*`
- no persisted `updated_*_balance*`
- no persisted transaction hash

### Local update semantics

Local post-settlement application is not uniformly idempotent:

- Ring 2 / private-settlement balance writes are overwrite-style and are close
  to idempotent.
- Ring 0 / Ring 1 EOA balance updates are decrement-style, not idempotent.
- Order updates are overwrite-style only if we can prove the stored order is
  still pre-settlement or already-equal-to-expected-post-settlement.

So "just rerun the second leg" is not safe.

## Safety Definition

"Safely process the queue" for a committed settlement task means:

1. Never apply local post-settlement state unless the on-chain settlement leg is
   known to have finalized.
2. Never apply the local post-settlement state more than once.
3. If the local post-settlement state is already fully applied, resume must be a
   no-op and complete successfully.
4. If the local post-settlement state is partially applied or otherwise does not
   match either the expected pre-state or expected post-state, do not guess.
   Escalate to explicit repair.
5. Snapshot recovery must not special-case this by blowing away queues. The same
   committed-task recovery policy should run for normal reassignment and for
   snapshot recovery.

## Required States

For a committed settlement task, there are only four valid recovery outcomes:

1. On-chain leg not finalized
   - local settlement application must be a no-op
   - task should fail cleanly and trigger account refresh / re-evaluation

2. On-chain leg finalized, local state still pre-settlement
   - apply deterministic local post-settlement state exactly once

3. On-chain leg finalized, local state already post-settlement
   - no-op and complete successfully

4. Local state neither fully pre-settlement nor fully post-settlement
   - enter explicit repair path
   - do not rerun normal settlement logic

Anything else is unsafe.

## What Must Be Persisted

The queue needs enough structured data to answer two questions after a crash:

1. What exact post-settlement state do we expect?
2. How do we determine whether the on-chain leg finalized?

The minimum payload for settlement recovery is:

- task id
- settlement kind
  - internal
  - private
- order ids and account ids
- expected post-settlement orders for both parties
- expected post-settlement balances for both parties where applicable
- expected post-settlement intent commitments for both parties
- transaction hash once available

Optional but useful:

- pre-settlement order snapshots
- pre-settlement balances used for validation

## Recommended Design

### Recommendation

Implement a structured persisted execution-state path for running tasks, then
use it to store settlement-specific recovery payloads.

This is preferable to a settlement-only side table because:

- the queue is already the source of truth for in-flight work
- reassignment logic already operates on queued running tasks
- other committed tasks may need the same recovery model later

### Concrete shape

Change `QueuedTaskState::Running` from:

- `state: String`
- `committed: bool`

to something conceptually like:

- `state: String`
- `committed: bool`
- `execution_state: Option<TaskStateWrapper>`

Notes:

- `state` should remain for API readability.
- `execution_state` is the typed serialized state used for recovery.
- `TaskStateWrapper` must become deserializable.

### Task construction

Introduce task restoration from queued task state, not descriptor alone.

Conceptually:

- `Task::new(...)` for fresh tasks
- `Task::restore(descriptor, persisted_execution_state, ctx)` for resumed tasks

For tasks that do not need structured restore, `restore` can delegate to `new`.

## Settlement State Model

The settlement tasks should stop treating `SubmittingTx` as payload-free.

Recommended recovery-capable state model:

### Internal settlement

- `Pending`
- `SubmittingTx { recovery }`
- `UpdatingState { recovery }`
- `UpdatingValidityProofs { recovery }`
- `Completed`

### Private settlement

- `Pending`
- `SubmittingTx { recovery }`
- `UpdatingState { recovery }`
- `UpdatingValidityProofs { recovery }`
- `Completed`

Where `recovery` contains:

- expected post-settlement orders
- expected post-settlement balances
- expected post-settlement commitments
- optional tx hash

## Commit Point Semantics

Do not move the logical commit point later than `SubmittingTx`.

Reason:

- once a settlement task starts the on-chain leg, the queue must treat it as
  non-preemptable
- a crash while the transaction is in-flight is exactly the case that needs
  recovery handling

Instead:

- keep the commit point before or at transaction submission
- persist structured execution state before submission
- update that execution state with the tx hash after receipt is available

## Recovery Algorithm

This algorithm should run whenever a committed running settlement task is
reassigned or resumed after startup.

### Step 1: Load structured recovery payload

If no structured execution state exists:

- treat the task as legacy / unrecoverable
- fail closed
- refresh affected accounts
- do not rerun normal settlement logic

### Step 2: Check finalization of the first leg

Preferred sources, in order:

1. If `tx_hash` exists, query the receipt and finalization status directly.
2. Otherwise, verify that the expected post-settlement intent commitment(s) are
   present on-chain.

For settlement tasks, "first leg finalized" means:

- the settlement transaction landed and
- the expected post-settlement commitments are actually present in the contract
  state / receipt trail

### Step 3: If first leg is not finalized

Do not apply any local settlement updates.

Safe behavior:

- mark the task failed
- pop it from the queue
- enqueue refresh tasks for affected accounts

This is the "no-op" branch with respect to the local second leg.

### Step 4: Compare local state to expected post-state

Read current local state and classify it:

- `PreState`
  - current order/balance rows still reflect pre-settlement values
- `PostState`
  - current order/balance rows equal the persisted expected post-state
- `Inconsistent`
  - mixed or unexpected values

### Step 5: Act on the classification

If `PostState`:

- no-op
- advance to validity-proof regeneration or complete if already done

If `PreState`:

- apply local settlement updates exactly once using the persisted expected
  values, not by recomputing from mutable current state

If `Inconsistent`:

- fail closed
- emit repair telemetry
- enqueue refresh / manual repair workflow

## Why Recompute Is Not Good Enough

Do not recompute the post-settlement state during recovery from the current
database rows.

Reasons:

- current rows may already be partially updated
- current rows may have been refreshed from chain after startup
- Ring 0 / Ring 1 EOA updates are decrement-based, so recovery-by-recompute can
  double-apply

Recovery must use persisted expected post-state values as the comparison target.

## Task-Specific Notes

### `SettleInternalMatchTask`

This is the highest-risk path because it mixes:

- Ring 0 / Ring 1 EOA decrement updates
- Ring 2 overwrite-style balance updates

The recovery path must never rerun the EOA decrement blindly.

### `SettlePrivateMatchTask`

This path is simpler because both parties use darkpool balances and overwrite
semantics for balances, but it still needs structured recovery because:

- the typed expected post-state is currently not persisted
- rerun-from-descriptor restarts at `Pending`
- partial local application is still possible

## Interaction With Snapshot Recovery

Snapshot recovery should no longer clear account task queues.

Instead:

1. restore snapshot
2. allow normal task reassignment / resume
3. run the committed-settlement recovery algorithm above
4. separately run account refresh for all accounts if `recovered_from_snapshot`
   is true

The refresh step is useful hygiene, but it is not the settlement recovery
mechanism.

## Minimal Implementation Plan

### Phase 1: Persistence plumbing

1. Make `TaskStateWrapper` deserializeable.
2. Persist structured `execution_state` alongside running task state.
3. Teach the task driver to restore from queued execution state.

### Phase 2: Settlement recovery payload

1. Introduce a `SettlementRecoveryPayload`.
2. Persist it in `SubmittingTx` before submission.
3. Record `tx_hash` after receipt is available.

### Phase 3: Recovery handler

1. Add a committed-task resume path for settlement tasks.
2. Query on-chain finalization from `tx_hash` or expected commitment.
3. Compare local state to expected post-state.
4. Choose:
   - no-op complete
   - apply local second leg once
   - fail closed and refresh

### Phase 4: Remove blind rerun behavior

Replace the current reassignment behavior for committed settlement tasks with the
recovery handler.

## Testing Matrix

The implementation should include deterministic tests for:

1. crash before tx submission
2. crash after tx submission but before tx hash persistence
3. crash after tx hash persistence but before local state updates
4. crash after one local update but before the second
5. crash after all local state updates but before task completion
6. snapshot recovery with committed settlement task still queued
7. peer failure and task reassignment for a committed settlement task

Per test, assert the recovered system chooses exactly one of:

- no-op and complete
- apply second leg once
- fail closed and refresh

and never double-applies an EOA decrement.

## Bottom Line

The correct invariant is:

- local post-settlement state is conditional on proven on-chain finalization
- once finalized, local application must be compare-and-apply, not blind replay
- queue recovery should use the same logic everywhere, not a special-case
  snapshot hack

That is the meaning of "safely process the queue" in this codebase.
