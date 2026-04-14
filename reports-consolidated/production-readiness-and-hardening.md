# Production Readiness and Hardening

This document consolidates the production-readiness notes, the readiness report, and the hardening roadmap into one engineering-facing summary of what looks healthy, what still looks risky, and where hardening effort should go next.

## Source Reports

- `reports/production-hardening-roadmap.md`
- `reports/renegade-production-readiness-notes.md`
- `reports/renegade-production-readiness-report.md`

## Executive Summary

Renegade looks like a serious system that is already being operated under meaningful load and failure pressure. The strongest signal from the 2026 trail is not "the architecture is fundamentally unsound." It is that the team is actively hardening a fast system whose correctness and recovery edges have already been tested by real incidents.

The recurring themes are:

- replicated-state inconsistencies can still escape containment
- recovery and repair are present, but not yet systematic
- observability exists, but is still thinner than the failure surface demands
- some components still favor warn-and-skip or panic-style behavior where repair-grade workflows would be stronger

## What looks good

### The core architecture is not random

The source material consistently credits several design choices as sound:

- Raft is a reasonable fit for the trusted-relayer federation model
- the serial task queue is an effective exclusivity mechanism for account-level work
- the low-latency matching tradeoff is understandable for a DEX
- the codebase already has telemetry plumbing, peer metrics, and worker supervision

This matters because the hardening work should mostly refine explicit guarantees, not replace the architecture wholesale.

## What appears to be hurting in production

### 1. Crash containment around replicated state

Recent fixes point to repeated incidents where missing or stale replicated state caused crash loops or unsafe behavior. The reports specifically call out:

- `MissingEntry` during Raft apply
- missing order metadata crash loops
- wallet updates referencing deleted or nonexistent metadata

This is the clearest sign that bad replicated state can still become an operational incident rather than a contained repair case.

### 2. Recovery is pragmatic, but still ad hoc

Recovery exists, but several signs suggest it is not yet a fully mature repair discipline:

- snapshot recovery clears queues rather than replaying a richer task lifecycle
- normal cleanup and recovery cleanup do not always follow the same semantics
- migration/repair infrastructure appears thin relative to the kinds of failures being patched

### 3. Matching and settlement remain an active hardening zone

The readiness notes and roadmap both treat settlement correctness as ongoing work, not solved history:

- local settlement still spans multiple writes
- defensive checks are still being added around stale or missing state
- correctness still depends on implicit guarantees becoming explicit

### 4. Some resilience paths are still "skip, clear, or panic"

The system sometimes repairs by dropping queue state, tolerating missing metadata, or panicking at worker boundaries and startup boundaries. Those choices may be reasonable in isolation, but they signal a system that still needs more explicit containment and repair paths.

### 5. Operational visibility is improving, not complete

The report finds real telemetry and metrics infrastructure, but also notes:

- metric coverage is still relatively sparse
- readiness/health surface appears limited
- event logging and startup instrumentation have recently needed incremental improvement

That is typical of a system still climbing the observability maturity curve.

### 6. External dependency handling is still somewhat reactive

The reports point to recent fixes around websocket handling, failover heuristics, and event-listener assumptions. That suggests the external-service boundary is functional, but not yet deeply hardened for backpressure, exactly-once semantics, or operator-friendly failure modes.

### 7. Worker recovery is uneven

The coordinator advertises worker recovery, but several workers are unrecoverable or rely on default/unimplemented recovery behavior. That gap matters because a system with real consensus, settlement, and event-ingestion complexity benefits from explicit restart contracts per worker.

## Hardening Priorities

### Priority 1: recovery verification

Move from "clear the bad thing and continue" toward "classify state, verify invariants, then continue." This is the highest leverage area because it reduces cluster-integrity incidents and makes later failures easier to reason about.

### Priority 2: post-settlement validation

Settlement and queue-resume logic should be able to prove whether local state is pre-state, post-state, or a partial hybrid.

### Priority 3: pre-consensus validation

Bad or incomplete state should be rejected before it becomes replicated state whenever possible.

### Priority 4: observability expansion

Add the instrumentation needed to answer:

- which phase a task died in
- whether recovery classified the task as safe, already-applied, or ambiguous
- whether a node is merely alive or actually ready
- which upstream dependency is degraded when a worker begins to thrash

### Priority 5: worker recovery contracts

Each worker should either have a credible recovery path or be clearly treated as a fatal boundary with operator-visible diagnosis.

## Suggested roadmap

### Immediate

- add invariant checks around recovery and queue cleanup
- close obvious panic-based failure paths where warn-and-repair is safer
- improve readiness and startup visibility

### Short term

- standardize recovery semantics across normal cleanup, reassignment, and snapshot restore
- strengthen settlement and metadata repair flows
- add richer task lifecycle instrumentation

### Medium term

- build a first-class repair/migration discipline for inconsistent replicated state
- harden external event ingestion with clearer ownership, backpressure, and failure classification
- make worker recoverability explicit per subsystem

## Overall Assessment

The consolidated reports paint a pretty healthy engineering picture: the team is already discovering real failure modes, documenting them, and shipping pragmatic mitigations. The main gap is not competence. It is that explicit correctness and recovery semantics have not yet caught up to the complexity of the system.

That is exactly what production hardening looks like in a serious distributed Rust system. The next step is to convert the current implicit guarantees into durable, observable, testable contracts.
