# 08 — Implementation plan: incremental staging (#01) + acks-only / real-pipeline support (#07)

> **Status (v1 = A1, forwarding-only):** Workstreams **B, C, D done** — `FlinkKafkaInternalProducer`
> tracks staged acks (`markShareAcksStaged`/`hasWorkInTransaction`) so acks-only transactions can
> commit; `SameTransactionShareAckKafkaWriter` stages incrementally at `write()`; the all-or-nothing
> abort and dedup are unit-tested. **Not yet done:** Workstream A2 (union side-channel for
> filter/aggregate pipelines), Workstream E (config preconditions — lands with builder wiring), and
> the recovery IT matrix (doc 06 §B).

Goal: make the same-transaction (Mechanism A) model correct for **real** pipelines (filter / window /
aggregate), and shrink the lock-expiry (#01) and member-epoch (#03) windows — in one coherent change.

This plan is grounded in the current code:
- `SameTransactionShareAckKafkaWriter` — `write()` buffers; `prepareCommit()` stages then precommits.
- `ShareAckPayloadBuffer.stage()` — throws if acks present but no sink records.
- `ExactlyOnceKafkaWriter.prepareCommit():221-238` — emits committable only if `hasRecordsInTransaction()`.
- `FlinkKafkaInternalProducer` — `hasRecordsInTransaction()` true only after a data `send()`
  (`:135-137`); `precommitTransaction()` asserts it (`:144`); 2PC prepare gated on
  `twoPhaseCommitEnabled` (`:143-152`).

## The problem decomposes into two halves

**Half 1 — propagation (the hard half).** Acks must reach the sink writer even for records whose data
is dropped/absorbed by an upstream operator. Today acks are extracted from the data record at the
sink, so a dropped record drops its ack. Flink's `Sink` takes a single input stream, so "a side
channel of acks" is not free.

**Half 2 — sink commit (the mechanical half).** Once acks reach the sink, the sink must be able to
commit a transaction that carries acks even when it carries **zero** output records. The broker
supports this (validated: acks-only transactions commit); the connector currently forbids it.

Half 2 is worthless without Half 1, so Half 1's strategy is a prerequisite decision (see §Decision).

---

## Workstream A — Ack propagation (depends on the Decision)

Two viable strategies given the single-input `Sink` constraint:

### Strategy A1 — "forwarding-only" scope for v1 (smallest, safe)
Constrain supported topologies to those that carry every ack-bearing record to the sink (1:1
`map`/forwarding; no filtering/aggregation between share-source and sink). Enforce it: fail fast at
job build if an unsupported shape is detectable, and document the limitation loudly. No propagation
machinery needed. Half 2 still needed for the *idle-window* case (a checkpoint with no records at all
is fine — `prepareCommit` recycles; but a window that forwards records always has output, so acks-only
commit is rarely hit). **Effort: low. Capability: limited.**

### Strategy A2 — ack side-channel via a union record type (general, larger)
The source emits a union `ShareElement = DATA(record) | ACK(payload)`. User transforms operate on
`DATA` only; `ACK` markers flow through untouched (filters must not drop them — enforced by the
connector wrapping the stream so user code never sees `ACK`). The sink writer splits the union:
`DATA` → normal write; `ACK` → stage into the current transaction. Filtered `DATA` records still
deliver their `ACK` marker, so their acks reach the sink. **Effort: high** (touches source emission,
the stream type, and operator chaining). **Capability: full** (filter/window/aggregate supported).

Recommendation: ship **A1** first (document + fail-fast), design **A2** as the follow-up that lifts the
restriction. Both need Workstream B.

---

## Workstream B — Allow acks-only transactions at the sink (#07 Half 2)

Make "has staged acks" count as transaction-has-work, so a transaction with acks but no output records
precommits and commits. **Constrain all changes to the share path; the non-share EOS path must be
byte-for-byte unaffected.**

1. **Track staged acks on the producer.** In `FlinkKafkaInternalProducer`, add a field
   `acksStagedInTransaction` (reset in `beginTransaction`/`abortTransaction`/`commitTransaction`), set
   true when share acks are staged. Add `hasWorkInTransaction()` = `hasRecordsInTransaction() ||
   acksStagedInTransaction`. **Do not change** `hasRecordsInTransaction()` semantics (other code reads
   it).
2. **Stage marks work.** `ShareAckPayloadStager.stage()` currently calls
   `sendShareAcknowledgementsToTransaction` reflectively; after a successful call, flip the producer's
   `acksStagedInTransaction`. (Pass the typed `FlinkKafkaInternalProducer` so we can set the flag, or
   return a "staged" signal the writer applies.)
3. **Relax the gate in the share writer's commit path.** Introduce a share-aware precommit that uses
   `hasWorkInTransaction()`. Cleanest: give `ExactlyOnceKafkaWriter` a protected
   `hasWorkInTransaction()` (default = `hasRecordsInTransaction()`) and have the share path use it,
   leaving the base `prepareCommit()` gate intact for non-share sinks. The committable carries the
   prepared state exactly as today.
4. **Relax `ShareAckPayloadBuffer.stage()`** to drop the "no records" throw (the broker allows
   acks-only). Keep the empty-payloads early return.
5. **`precommitTransaction()`** must allow precommit when acks are staged: change its `checkState` from
   `hasRecordsInTransaction()` to `hasWorkInTransaction()` — but **only reachable on the share path**;
   verify the non-share writer never reaches it without records (it doesn't — base `prepareCommit`
   gates on records first).

Risk control: every change is additive (`hasWorkInTransaction`, a new flag) or share-path-only. Add a
unit test asserting the non-share `ExactlyOnceKafkaWriter` still recycles (no committable) on an
empty window.

## Workstream C — Incremental staging (#01)

Move staging from `prepareCommit()` to `write()` in `SameTransactionShareAckKafkaWriter`:

- In `write(element)`: after `delegate.write(element)`, immediately stage *that element's* payloads
  into `delegate.currentProducer()` (instead of only buffering). This cancels the broker lock timer as
  soon as the record reaches the sink, shrinking the `ACQUIRED` window to topology latency and the
  epoch-staleness window with it (#03).
- Keep a small buffer only for payloads that arrive before the transaction is open (shouldn't happen
  once a transaction is begun at writer init, but guard it).
- `prepareCommit()` then only precommits (no bulk stage). With Workstream B, it precommits whenever
  there is work (records or staged acks).
- Dedup: staging per-record removes the buffer's batch dedup; move the id-uniqueness guard
  (`payloadsById`, conflict check) to the incremental path so a duplicate id within a window is still
  caught.

Risk: more `sendShareAcknowledgementsToTransaction` RPCs (one per record/batch vs one per checkpoint).
Mitigate by batching per `write()` call's payload collection (already a `Collection`), and optionally
coalescing within a short window. Acceptable for correctness-first v1.

## Workstream D — All-or-nothing abort invariant (#01 safety)

- Ensure any stage failure poisons the transaction: a failed `stage()` must cause the checkpoint to
  fail (it throws today) and must not leave a committable that omits the failed ack. Add an explicit
  test: stub a producer whose stage throws for one payload; assert `prepareCommit()`/`write()`
  surfaces the failure and **no** `KafkaCommittable` is produced.

## Workstream E — Config preconditions (#05)

- At sink build / writer init: require `transaction.2pc.enable=true` for share-EOS (else the recovery
  story silently doesn't work) and `transaction.timeout.ms > checkpoint interval`. Fail fast with a
  clear message. (Lands wherever wiring lands; see doc 06 §C.)

---

## Sequencing

1. **Decision** on Workstream A (A1 vs A2) — gates everything.
2. **Workstream B** (acks-only commit) — independent of A1/A2; unit-testable now.
3. **Workstream C** (incremental staging) — builds on B.
4. **Workstream D** (abort invariant tests) — alongside C.
5. **Workstream E** (config preconditions) — with wiring.
6. Recovery IT matrix (doc 06 §B) — proves the whole thing.

## Decision needed (architectural, user's call)

Pick the propagation strategy. This changes the user-facing contract:

- **A1 (forwarding-only v1):** ship fast; only 1:1/forwarding pipelines supported; filtering/
  aggregation between share-source and sink is rejected/documented as unsupported.
- **A2 (union side-channel):** full pipeline support; larger change to source emission, stream type,
  and operator chaining.

Recommendation: **A1 now, A2 next.** Workstream B is needed either way.
