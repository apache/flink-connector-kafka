# 07 — Records that produce no sink output cannot be acknowledged 🔴 HIGH

> **Found during plan validation**, not in the original ranking. This is arguably the most important
> *functional* limitation of the same-transaction (Mechanism A) design, because it bites any pipeline
> that filters or aggregates — i.e. almost all of them.

## 1. Design background

Mechanism A acknowledges a source record by **staging its ack into the same producer transaction that
carries the sink output**, and that transaction is committed at checkpoint completion. The implicit
assumption is: *every source record co-produces sink output in the same checkpoint window.* That holds
for a pure 1:1 `map`/forwarding job. It does **not** hold for:

- a **filter** that drops records,
- a **window/aggregation** that emits only on triggers (most windows emit nothing in a given
  checkpoint),
- a **join** with no match,
- any window that consumes source records but emits zero sink records.

## 2. Current flow

Three independent gates all require sink output before anything commits:

1. `ShareAckPayloadBuffer.stage()` **throws** if there are acks but the transaction has no records —
   `ShareAckPayloadBuffer.java:56-59`: *"Cannot commit share acknowledgements without sink records in
   the same Kafka transaction."*
2. `ExactlyOnceKafkaWriter.prepareCommit()` only emits a committable
   `if (currentProducer.hasRecordsInTransaction())`; otherwise it **recycles the producer without
   committing** — `ExactlyOnceKafkaWriter.java:221-238`.
3. `FlinkKafkaInternalProducer.precommitTransaction()` hard-asserts `hasRecordsInTransaction()` —
   `FlinkKafkaInternalProducer.java:144`.

And `hasRecordsInTransaction()` is true only after a `send()` of a data record
(`FlinkKafkaInternalProducer.java:93-97, 135-137`) — staging acks does **not** set it.

## 3. The issue

A checkpoint window that consumes source records but emits no sink output has pending acks and no
transaction to carry them:

```
window W: source records R1..Rn consumed (acks pending), all filtered → 0 sink output
prepareCommit():
  buffer.stage(producer, transactionHasRecords=false, …)  → throws IOException
  → checkpoint fails → job restarts → R1..Rn redelivered → filtered again → throws again
  → permanent restart loop; meanwhile delivery count climbs → eventually ARCHIVED → DATA LOSS (doc 02)
```

Even if gate (1) were simply removed, the acks would just never be committed → records redelivered
until archived → the same data loss. So the same-transaction model, as written, **can only
acknowledge records that happen to co-produce output**. That is a severe functional restriction, not
an edge case.

## 4. Classes & modules

| Layer | Class | Role in the gap |
| --- | --- | --- |
| Flink sink | `ShareAckPayloadBuffer` (`sink/`) | Throws on acks-without-records. |
| Flink sink | `SameTransactionShareAckKafkaWriter` (`sink/`) | Stages then delegates to `prepareCommit`. |
| Flink sink | `ExactlyOnceKafkaWriter` (`sink/`) | Skips commit when no records. |
| Flink sink | `FlinkKafkaInternalProducer` (`sink/internal/`) | `precommitTransaction` asserts records present. |

## 5. Client/server involvement

- **Client (Flink):** imposes the "must have sink records" rule. This is a **self-imposed**
  restriction.
- **Server (broker):** does **not** require it. Validated separately (see README, "Claim C"): a
  producer transaction containing only share-acks and **no produced data records commits cleanly**
  (`sendShareAcknowledgementsToTransaction` sets `transactionStarted=true`; the EndTxn short-circuit
  only fires when nothing was started; share-state partitions are enrolled and resolved by the
  marker). So the broker can finalize an acks-only transaction.

The mismatch is the whole point: the broker supports exactly the operation we need; the connector
forbids it.

## 6. How it is solved

Allow an **acks-only transaction** to be precommitted and committed when a checkpoint window has
pending acks but no sink output:

1. **Treat "has staged acks" as transaction-has-work.** Track an `acksStagedInTransaction` flag on the
   producer/writer set when `sendShareAcknowledgementsToTransaction` is called, and make the commit
   decision `hasRecordsInTransaction() || acksStagedInTransaction`.
2. **Relax the three gates accordingly:** drop `ShareAckPayloadBuffer`'s throw; change
   `ExactlyOnceKafkaWriter.prepareCommit()` to emit a committable when there are staged acks even with
   zero records; relax `precommitTransaction()`'s assertion to "records OR acks present."
3. **Keep all-or-nothing intact (doc 01):** an acks-only transaction still aborts wholesale if any
   stage fails.
4. **Stage incrementally (doc 01 synergy):** with staging at `write()`, the producer naturally opens a
   transaction as soon as the first ack is staged, even when no output record is ever produced.

This is safe precisely because the broker supports acks-only transactions; we are removing an
artificial client restriction, not inventing new broker behavior.

> Interim alternative if the above is deferred: document that v1 supports **only forwarding/1:1
> pipelines** (no filtering/aggregation between share-source and sink), and fail fast / warn if a
> window produces acks without output — so the limitation is loud, not a silent restart loop.

## 7. How to verify

- **IT — filter:** share-source → `filter(false)` → sink (no output). Assert the source records are
  acknowledged (share-lag → 0) via committed acks-only transactions, the job makes progress, and
  nothing is archived.
- **IT — windowed aggregation:** many input records, sparse output. Assert every input record's ack is
  committed exactly once across the windows that emit nothing and the ones that do.
- **Unit:** with the relaxed gates, a writer that received acks but zero `write()`s still emits a
  committable; assert the committer commits an acks-only transaction.
