# 09 — ADR: general 2PC recovery model for arbitrary pipelines ("Mechanism B done right")

- **Status:** Proposed
- **Supersedes (conceptually):** the deleted Mechanism B (separate-transaction share-ack committer)
- **Complements:** A1 same-transaction writer (docs 01, 07, 08) — kept as the 1:1 optimization
- **Related:** the engine-agnostic SPI design (`README` §"Validation status"); the producer-independent
  Kafka primitive (deferred — see §9)

## Context

A1 (`SameTransactionShareAckKafkaWriter`) stages share acks **inside the sink's own Kafka producer
transaction**, so the *logical unit of work is one Kafka transaction*. That is correct and optimal for
**1:1 forwarding** pipelines, and we keep it for that case.

But for **arbitrary Flink pipelines — fanout, joins, drops/filters, and multiple sinks — one Kafka
transaction no longer represents the whole logical unit of work**:

- a single source record's effects may land in **many** sink transactions (different subtasks / sinks);
- records may be **dropped** (filter) or **merged** (aggregate/join), so the ack cannot ride the data;
- there may be **multiple sinks**, none of which alone owns the source record's effect.

In this regime the **checkpoint** — not the Kafka transaction — is the atomic unit. Flink's checkpoint
+ committer machinery is already a distributed-transaction coordinator (`CommitterOperator` durably
stores committables in `ListState`/`CommittableCollector` and re-commits unconfirmed ones idempotently
in `initializeState`; it commits **all committables of checkpoint N or fails the job and retries from
N**). The work is therefore to make the **share-ack a first-class participant** in that coordinator,
not to build a new one.

## Decision

Reintroduce the architecture of the deleted Mechanism B — a **separate, durable ack transaction
committed as a checkpoint committable** — but built correctly on Flink's committable-recovery model:

1. The share-ack becomes a **first-class, checkpoint-durable committable** (`ShareAckCommittable`),
   **not** staged into a sink output transaction.
2. Acks reach the sink/committer over a **record-drop-proof side-channel** (the A2 union of a source
   side-output), so filters/joins/aggregations cannot lose them.
3. Acks are committed by a **dedicated ack producer** via its **KIP-939 prepared handle**
   (`prepareTransaction()` → `PreparedTxnState` → `completeTransaction(handle)`), so a different /
   restarted process (the committer) can finalize them.
4. **Recovery is Flink's existing idempotent committable re-commit** — no in-memory phase markers.
5. A1 remains available and is selected automatically for 1:1 forwarding (true single-marker
   atomicity, no caveat).

### Why this is *not* a repeat of the deleted Mechanism B's bug

The deleted Mechanism B was architecturally right but **implemented wrongly**: its `CommitPhase`
(`READY → SINK_COMMITTED`) lived only in the committer's in-memory retry state (`updateAndRetryLater`),
so a crash lost it and recovery re-committed already-committed sink transactions; it also had no
source-side ack state serializer. This ADR fixes that by **storing every participant's handle in
checkpoint state** (the `CommittableCollector`) and relying on Flink's **idempotent re-commit on
recovery** — never on in-memory bookkeeping.

## The unit of work, precisely

> For checkpoint N: the unit of work = **{all sink output transactions across all subtasks and all
> sinks for records consumed before barrier N}** ∪ **{the share-ack transaction(s) for those records}**.
> All of these are durable committables of N. Flink commits them all (idempotently, retrying on
> recovery) or restarts the job from N.

Barrier alignment guarantees that a record consumed before barrier N has its full effect captured in
checkpoint N (as committed output and/or operator state), and its ack belongs to N. So acking at N is
consistent regardless of fanout, drops, or multi-checkpoint emission (see doc on "R → txn A/B/C").

## Correctness argument

Let R be a source record consumed before barrier N, with effects spread across sink transactions and
its ack in a dedicated ack transaction, all committables of N.

- **N completes normally:** committer commits all of N's committables → R's effects visible, R acked
  → not redelivered. ✅
- **Crash before N completes:** all of N's transactions abort (output invisible under
  `read_committed`, ack aborted → R returns to `AVAILABLE`) → R redelivered → reprocessed. ✅
- **Crash mid commit-phase:** Flink restores N's committables from `CommittableCollector` state and
  **re-commits the unconfirmed ones idempotently** (`completeTransaction(handle)` is idempotent) →
  all of N eventually commits. ✅

**Residual hole (irreducible):** Kafka has no cross-producer atomic commit, so the participants do not
share one marker. If a participant's prepared transaction can *never* be completed (expired past
`transaction.max.timeout.ms` during a very long outage), you get the standard Flink-EOS loss window —
sharper here because a committed ack has already released R. Mitigations: 2PC timeout exemption
(KIP-939 prepared txns are not auto-aborted), sizing the timeout for worst-case recovery, and
committing acks **after** outputs where ordering is available (§ multi-sink).

## Multi-sink: the hardest sub-problem (explicit decision)

Flink runs each sink as an **independent** `Committer`; there is **no native ordering** across
committers — they all fire on `notifyCheckpointComplete(N)`. So "release the source records only after
*all* sinks durably committed N" is not expressible out of the box. Decision:

- **v1 decision:** rely on Flink's **checkpoint-level all-or-nothing** guarantee (every committable of
  N commits, or the job restarts from N and re-commits idempotently). Accept the transaction-timeout
  caveat. Commit the ack via a **global ack committer** (`StandardSinkTopologies.addGlobalCommitter`,
  parallelism 1) so there is a single, ordered ack-commit point per checkpoint.
- **Deferred:** strict "ack-after-all-sinks" ordering would require a custom coordination operator
  Flink doesn't provide; revisit only if the timeout window proves unacceptable for a concrete sink.

## Consequences

**Positive:** supports arbitrary pipelines (fanout/joins/drops/multi-sink); reuses Flink's proven
committer recovery; no in-memory phase bug; A1 preserved for the clean 1:1 case; maps directly onto
the engine-agnostic `CommitCoordinator` SPI (this is the Flink adapter).

**Negative / costs:** a dedicated ack producer + transactional.id management; an ack side-channel
operator; the transaction-timeout caveat (vs A1's none); multi-sink strict ordering deferred.

## Scope boundary (what this ADR does *not* cover)

The **producer-independent** Kafka ack lifecycle (making `txnOwnerId` a real coordinator-owned token
+ a standalone `CompleteShareAcknowledge` RPC) is **not needed for Flink** — a dedicated ack producer
with KIP-939 suffices. That decoupling is the **cross-engine** upgrade (Spark/Druid/XA sinks) and is
tracked separately. See doc 10 §"Out of scope" and the SPI design.
