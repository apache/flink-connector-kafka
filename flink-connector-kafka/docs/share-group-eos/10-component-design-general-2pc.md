# 10 — Component design & build plan: general 2PC (the ack as its own sink)

Implements ADR doc 09. The central design move: **make the share-ack its own Flink `Sink`** (a
"consumption-commit sink") that is **fully decoupled from the output sink(s)**. The output sink stays
an ordinary, unmodified Flink `Sink` (Kafka today, any sink later); the ack-sink is the only
share-group-specific participant. Both are committables of the same checkpoint.

```
                                 user's pipeline (DATA only)
KafkaShareSource ─ShareElement─► [splitter] ─DATA(T)─► filter/join/agg ─► output Sink(s)  ─┐
   (emits DATA|ACK)                  │                                                      ├─ checkpoint N
                                     └─ACK(payload) side-output ───────────► ShareAckSink ──┘   commits all
```

## Component 1 — Source emits `ShareElement`, splitter creates the ack side-channel

- `KafkaShareSource` emits a single stream of `ShareElement<T> = Data(T) | Ack(ShareAckPayload)` (acks
  derived from the share session per `poll()`).
- The connector inserts a **splitter** `ProcessFunction` immediately after the source (before any user
  operator): `Data` → main output `DataStream<T>` handed to the user; `Ack` → a **side output**
  routed to the `ShareAckSink`.
- Barrier alignment guarantees acks emitted before barrier N reach the ShareAckSink's checkpoint-N
  transaction. Because the ack rides the side output, **filters/joins/aggregations on the DATA branch
  never see or drop it** (this is the A2 record-drop-proof channel).

The builder returns the DATA `DataStream<T>` to the user and **internally retains the ack side-output**,
wiring it to the ShareAckSink — so the user just writes their normal pipeline + their own output sink.

## Component 2 — `ShareAckSink` (a Flink `Sink` implementing `SupportsCommitter`)

A standalone two-phase-committing sink whose input is `ShareAckPayload`.

**`ShareAckWriter` (CommittingSinkWriter):**
- holds a **dedicated ack producer** (`FlinkKafkaInternalProducer`, `transactional.id` =
  `<ackPrefix>-<subtask>-…`, `transaction.2pc.enable=true`);
- `write(ackPayload)` → `ShareAckPayloadStager.stage(ackProducer, payload)` then
  `ackProducer.markShareAcksStaged()` (reuses the WS-B work);
- `prepareCommit()` → if `ackProducer.hasWorkInTransaction()`, `precommitTransaction()` →
  `PreparedTxnState`, emit a `ShareAckCommittable`; else recycle;
- `snapshotState()` starts the next checkpoint's ack transaction (same pattern as
  `ExactlyOnceKafkaWriter`).

**`ShareAckCommitter` (`Committer<ShareAckCommittable>`):**
- `commit(requests)` → for each, `ackProducer.completePreparedTransaction(c.preparedState())` (idempotent);
- maps Kafka errors onto `CommitRequest` exactly like `KafkaCommitter`: `RetriableException →
  retryLater()`; `ProducerFenced/InvalidTxnState/UnknownProducerId → signalFailedWithKnownReason`
  (already-resolved on a recovery re-commit is treated as success).

This is the existing `KafkaCommitter` shape applied to acks — no new recovery machinery.

## Component 3 — `ShareAckCommittable` + serializer (the durable handle)

```
final class ShareAckCommittable {
    long   checkpointId;
    String transactionalId;
    long   txnOwnerId;          // == producerId today (alias); future: coordinator token
    short  txnOwnerEpoch;
    String preparedTransactionState;   // KIP-939 "id:epoch" handle
    String groupId;
    int    sourceSubtaskId;
}
```
`ShareAckCommittableSerializer implements SimpleVersionedSerializer<ShareAckCommittable>` — versioned;
constructor null-guards `transactionalId`/`groupId` (avoid the `writeUTF(null)` NPE from doc 06). This
is the **same shape as the deleted committable** — the difference is purely that it now lives in
checkpoint state and is recovered idempotently (ADR §"not a repeat of the bug").

## Component 4 — durability & recovery (reuse, don't build)

Nothing custom. The `ShareAckSink`'s committer is a normal `CommitterOperator`:
- committables are durable in `CommittableCollector` `ListState`;
- on restart, `initializeState` → `commitAndEmitCheckpoints` re-commits unconfirmed ack committables
  via `completePreparedTransaction(handle)` (idempotent);
- unaligned checkpoints are force-disabled for the sink subtree (Flink guarantees all committables of
  N are present before commit).

## Component 5 — wiring / opt-in

A builder that ties the source's ack side-output to the ShareAckSink, e.g.:

```
DataStream<T> events = KafkaShareEos.source(env, shareSourceBuilder);   // returns DATA branch,
                                                                        // internally wires ShareAckSink
events.filter(...).keyBy(...).process(...)
      .sinkTo(anyOutputSink);   // unchanged; Kafka or any other sink
```
Builder-time **preconditions** (doc 05): ack producer requires `transaction.2pc.enable=true` and
`transaction.timeout.ms > checkpoint interval`; fail fast otherwise. Selection rule: if the topology is
detectably 1:1 forwarding to a single Kafka sink, prefer **A1** (`SameTransactionShareAckKafkaWriter`,
true single-marker atomicity); otherwise use this general path.

## Component 6 — multi-sink

Per ADR: commit the ack via a **global ack committer** (`StandardSinkTopologies.addGlobalCommitter`,
parallelism 1) so there is a single ordered ack-commit point per checkpoint. Multiple output sinks each
remain independent sinks; correctness rests on Flink's "all committables of N commit-or-retry-from-N"
guarantee (transaction-timeout caveat accepted). Strict ack-after-all-sinks ordering is deferred.

## Build plan (sequenced)

1. `ShareAckCommittable` + `ShareAckCommittableSerializer` (+ null guards) — unit-tested round-trip.
2. `ShareAckSink` = `ShareAckWriter` + `ShareAckCommitter` over a dedicated ack producer (reuse
   `FlinkKafkaInternalProducer` 2PC + `ShareAckPayloadStager`).
3. Source `ShareElement` emission + splitter side-output operator.
4. Builder wiring + preconditions + A1-vs-general selection.
5. Global ack committer for multi-sink.
6. **Recovery IT matrix (doc 06 §B)** — crash before/at/after commit, fanout, filter, multi-sink;
   assert input multiset == output multiset and no redelivery of committed records. *This is the gate
   for calling it EOS.*

## Tests (the proof)

| Scenario | Assert |
|---|---|
| Fanout (1→N outputs) + crash mid-commit | all N outputs + ack of N re-commit idempotently; R once |
| Filter drops record | dropped record's ack still committed via side-channel; share-lag→0 |
| Aggregate/window (N→1) across checkpoints | each input acked at its consuming checkpoint; output once |
| Multiple sinks | acks committed once both sinks' outputs for N committed (or job retries from N) |
| Crash after ack, before an output committable | recovery re-commits the output committable; no loss |

## Out of scope (cross-engine upgrade)

The **producer-independent** ack lifecycle (real `txnOwnerId` token + standalone
`CompleteShareAcknowledge` RPC) is **not** part of this plan — a dedicated ack producer + KIP-939
covers Flink. It is the separate cross-engine track (Spark/Druid/XA sinks). The `ShareAckCommittable`
already carries `txnOwnerId/Epoch`, so adopting the producer-independent primitive later is a
drop-in change to Components 2–3, not a redesign.
