# Settlement Consistency and Recovery

This document consolidates the settlement-focused material from `reports/` into a single narrative about the PR #1420 failure class, the intended queue-based protection model, and the recovery work needed to make that model explicit and safe.

## Source Reports

- `reports/implicit-vs-explicit-atomicity.md`
- `reports/investigation-report.md`
- `reports/latency-vs-atomicity-tradeoff.md`
- `reports/matching-settlement-state-machine.md`
- `reports/pr1420-analysis.md`
- `reports/safe-settlement-queue-recovery-design.md`
- `reports/settlement-atomicity-analysis.md`

## Executive Summary

The recurring settlement issue is not that Renegade has no concurrency control. It does. The system uses the account serial task queue as a mutex-like guard that prevents other account work from running while settlement is in flight.

The real problem is that settlement correctness is enforced mostly through implicit coordination:

- the task queue blocks concurrent work
- the matching engine updates state with low-latency assumptions
- post-failure checks try to detect whether the world is still safe
- recovery paths assume queued work can be replayed or cleared safely

That works in the happy path, but it leaves a dangerous gap when a node crashes or resumes between the first and second local settlement writes. PR #1420 improves post-failure validation, but it still treats the symptom more than the root cause.

## Core Model

### What settlement is trying to do

For darkpool/internal settlement, the relayer-side state update logically contains one settlement intent with several effects:

1. finalize the on-chain leg
2. debit the input balance
3. credit the output balance
4. refresh order and matching-engine state
5. clear the running task and unlock the account

### How the current implementation does it

The current path performs the local balance updates with two separate `update_account_balance(...)` Raft proposals in `crates/workers/task-driver/src/tasks/settlement/helpers/mod.rs`.

That means the account can temporarily exist in a state where one balance reflects settlement and the other does not.

### Why the system was built this way

The split is not obviously accidental. The source reports consistently point to an intentional latency tradeoff:

- single-balance update APIs are simpler
- the matching-engine cache can be updated before full consensus completion
- the queue serializes access so intermediate states are supposed to stay unobservable to unrelated work

In short: the design trades strict atomicity for lower matching latency.

## Why the design mostly works

The strongest insight across the reports is that the serial task queue already acts as the missing lock.

While a settlement task is first in the account's serial queue:

- concurrent tasks for that account are blocked
- other serial tasks are blocked
- the matching engine should not be able to perform conflicting work against the same account

This is why the system often survives two-phase settlement in practice. The queue turns "two writes" into a de facto critical section.

## Where the design breaks down

### Implicit atomicity is not explicit safety

The queue prevents concurrent account work, but it does not by itself prove:

- which phase of settlement was reached
- whether the on-chain leg finalized
- whether zero, one, or both local balance writes applied
- whether order state is still pre-settlement or post-settlement
- whether replaying the task is safe

That missing explicit state is the real source of fragility.

### The vulnerability window

The source state-machine and PR analyses all converge on the same bad window:

1. settlement acquires exclusive queue ownership
2. on-chain settlement succeeds
3. local balance proposal A applies
4. local balance proposal B has not yet applied
5. node crashes, restarts, or reassigns the task

At that point the account is protected from concurrent work only if the queue state, recovery logic, and task reconstruction all preserve the intended semantics. Today they do not fully do that.

## What PR #1420 fixes and what it does not

### What it improves

PR #1420 adds a validity re-check after settlement failure so the matching engine can avoid blindly continuing when the underlying order/account state no longer looks usable.

That is a good defensive improvement because recent failures showed that missing metadata or partially updated state can otherwise trigger crashes or bad follow-on behavior.

### Why it is still incomplete

The consolidated reports make the same critique in different words:

- it validates after corruption-prone states are already possible
- it re-reads multiple pieces of state rather than validating a single persisted settlement intent
- it does not encode "only two legal next steps: finish or roll back"
- it does not make crash recovery idempotent

So PR #1420 is useful, but it is not yet a full settlement consistency design.

## Recovery Problem

### Why replay is unsafe

The queue-recovery design note is especially important here. On restart or reassignment, the system can reconstruct the task descriptor, but not the full typed settlement progress.

That means a resumed settlement task may not know:

- whether the transaction was ever sent
- which transaction hash belongs to it
- which expected post-settlement balances were computed
- whether local order updates already ran
- whether the current stored state matches pre-state, post-state, or a partial hybrid

Blind replay is therefore unsafe. Some local operations are overwrite-like and close to idempotent, but others are decrement-style or otherwise unsafe to rerun without classification.

### Safety definition for recovery

A safe recovery path must guarantee all of the following:

1. never apply local post-settlement state unless the on-chain leg finalized
2. never apply local post-settlement state more than once
3. treat fully applied post-state as a successful no-op
4. treat mixed or ambiguous state as a repair case, not a guess

## Recommended Explicit Model

The source reports collectively point toward the same target design.

### 1. Persist typed settlement recovery state

Persist enough structured task data to reconstruct settlement progress, including:

- task type
- on-chain transaction identity
- whether the on-chain leg finalized
- expected pre-state and expected post-state
- updated order metadata and balances

### 2. Make settlement phase boundaries explicit

The queue should not only say "running" and "committed." It should be possible to tell whether the task is:

- pending before on-chain submission
- submitted but not finalized
- finalized on-chain but not applied locally
- partially applied locally
- fully applied locally

### 3. Recover by classification, not replay

On restart, recovery should:

1. load the persisted settlement payload
2. verify whether the on-chain leg finalized
3. compare current local state to expected pre-state and post-state
4. classify the task as not-started, safe-to-apply, already-applied, or ambiguous
5. only continue automatically in the safe cases

### 4. Validate invariants at the boundary

The system should explicitly check that the account/order state sits in one of the few allowed recovery configurations instead of inferring safety from queue ownership alone.

## Atomicity Options

### Option A: keep the current two-step local write model

This is viable if the queue/recovery semantics are made explicit and verifiable. It preserves the current low-latency orientation and requires less storage/API change.

### Option B: introduce atomic multi-balance updates

This would reduce the inconsistent intermediate state window by introducing a batch transition such as `UpdateAccountBalances`. It is architecturally cleaner, but it likely comes with more implementation and performance tradeoffs.

### Practical conclusion

The reports do not show that Renegade must immediately switch to batched atomic writes. They do show that if the current two-step model remains, recovery semantics have to become first-class and typed.

## Recommended Work Plan

### Immediate

- keep the PR #1420 defensive checks
- add explicit settlement invariant checks around recovery/resume
- stop blindly rerunning committed settlement tasks after restart

### Short term

- persist typed recovery payload for settlement tasks
- classify resumed tasks against expected pre-state and post-state
- make "already fully applied" a clean no-op success path

### Medium term

- decide whether to keep queue-based atomicity or add a batched balance update transition
- unify snapshot-recovery settlement handling with normal committed-task resume handling
- add tests for crash-at-each-phase and partial-apply recovery

## Bottom Line

Renegade's settlement pipeline already contains the right core idea: serialize account mutation with the task queue and allow a fast path for matching.

What is missing is not another band-aid validity check. It is an explicit settlement state model that survives crash recovery, proves which phase the task reached, and constrains resume behavior to a small set of safe, testable transitions.
