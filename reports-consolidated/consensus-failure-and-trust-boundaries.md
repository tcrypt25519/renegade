# Consensus Failure and Trust Boundaries

This document consolidates the reports about crash failures, consensus behavior, and the places where Renegade's operational assumptions drift from pure crash-fault tolerance into trust-boundary risk.

## Source Reports

- `reports/byzantine-fault-audit.md`
- `reports/crash-failure-modes.md`

## Executive Summary

Renegade uses Raft, which is a crash-fault tolerant consensus algorithm. That is a reasonable choice for a trusted relayer federation, but it depends on a critical assumption: nodes replay and apply correct state.

The consolidated failure analysis shows two distinct classes of problems:

1. standard Raft availability scenarios, where crashes or leader loss temporarily stall progress but do not corrupt state
2. state-integrity failures, where nodes can restart with missing, partial, or invalid local state and then propagate that bad state through consensus

The second class is the one that matters most here. It behaves like a Byzantine boundary violation even if the formal threat model is "trusted relayers."

## What Raft does guarantee

The crash-failure report gives the normal taxonomy:

- single follower crash with quorum intact is safe
- leader crash with quorum intact is safe after reelection
- quorum loss mainly hurts availability
- uncommitted entries may be dropped, but committed entries remain durable

Those are standard Raft conditions and are not the core Renegade concern.

## What Raft does not guarantee

Raft assumes the state machine on each node is correct enough to apply the committed log safely.

It does not protect against:

- corrupted snapshots
- partial local state after crash recovery
- invalid data accepted before consensus
- apply-time handlers that skip or soften invariant violations in inconsistent ways
- nodes rejoining with bad local assumptions and then helping replicate or accept further transitions

Once those things happen, the system is no longer living purely in a crash-fault world.

## The Key Boundary Problem

The strongest cross-report theme is that Renegade's real incidents cluster around "bad state becomes durable enough to poison the cluster."

Examples called out across the source material include:

- crash during Raft apply
- snapshot recovery with incomplete state
- cluster-wide crash loops from persisted poison-pill entries
- missing metadata or missing wallet/order entries after recovery or pruning

These are not just availability events. They are integrity failures at the consensus boundary.

## Why the reports call this Byzantine-like

The Byzantine-audit report uses strong language, but the core point is sound:

- Raft assumes a trusted leader and trusted participants
- if a relayer can emit, recover, or propagate semantically bad state
- and other relayers accept it because the log shape is valid
- then the system experiences a failure mode outside ordinary crash tolerance

That does not necessarily mean Renegade needs a full Byzantine consensus algorithm. It does mean the system must aggressively validate state before and during consensus application if it wants Raft to remain a sound fit.

## Main Failure Classes

### 1. Non-atomic multi-step transitions

Some high-value workflows, especially settlement-related ones, span multiple proposals. If a crash or resume occurs between those phases, recovery may expose or preserve an invalid intermediate state.

### 2. Recovery that clears work without proving consistency

Snapshot or restart recovery can remove queue context or task context without proving that the underlying state is safe for normal operation.

### 3. Pre-consensus validation gaps

If bad or incomplete state is admitted into the replicated flow before validation, consensus can faithfully replicate a mistake.

### 4. Apply-time invariant softening

Warn-and-skip behavior improves uptime, but it can also normalize bad state instead of forcing repair. That is a tradeoff, not a free win.

### 5. Multi-read race windows

When safety checks are built from several independent state reads, the system can validate an impossible combination of facts rather than a single coherent snapshot.

## Crash Taxonomy That Matters Most

The ten-scenario crash taxonomy can be collapsed into three engineering buckets.

### Ordinary Raft recovery

Includes:

- single node crashes
- leader replacement
- quorum loss and restoration
- pending proposal interruption

Primary effect: availability stalls, then catch-up.

### Corruption-sensitive recovery

Includes:

- crash during apply
- snapshot recovery with incomplete or mismatched state
- disk corruption or bad restored state

Primary effect: state may no longer satisfy the assumptions the Raft state machine expects.

### Cluster-integrity hazards

Includes:

- persisted poison-pill entries
- split-brain or concurrent leader bugs
- nodes rejoining with semantically invalid but syntactically accepted state

Primary effect: one bad state transition can become a cluster-wide problem.

## Practical Trust Model

The reports also sharpen the real trust model:

- users are not assumed to control relayer internals
- relayers are treated as mutually trusted federation members
- therefore full BFT may be out of scope

But even in that model, a compromised or buggy relayer can still behave "Byzantine enough" for operational purposes if it emits or recovers invalid state that peers trust.

So the right response is not necessarily to replace Raft. It is to harden the safety envelope around Raft.

## Recommended Guardrails

### 1. Validate before proposing

State transitions should be rejected before entering consensus when required preconditions are missing or incoherent.

### 2. Validate during apply

Apply-time logic should check strong invariants and route ambiguous cases into repair paths instead of silently skipping them.

### 3. Validate after recovery

Recovery should prove that restored state matches expected invariants before the node fully rejoins normal consensus participation.

### 4. Treat poison-pill states as first-class incidents

If a persisted entry or snapshot can crash-loop the cluster, the system needs explicit quarantine and repair workflows, not only best-effort logging.

### 5. Prefer explicit correctness to implicit trust

The less the system relies on informal assumptions such as "that queue probably serialized it" or "that metadata is usually there," the more appropriate Raft remains for the workload.

## Bottom Line

Renegade's main consensus risk is not ordinary leader election or quorum loss. It is the gap between "the log replicated correctly" and "the replicated state is semantically safe."

If state validation, recovery validation, and apply-time invariants are strengthened, Raft still looks compatible with the trusted-relayer model. If those guardrails remain implicit, the system will keep drifting into failure modes that look Byzantine even if the architecture officially claims only crash-fault tolerance.
