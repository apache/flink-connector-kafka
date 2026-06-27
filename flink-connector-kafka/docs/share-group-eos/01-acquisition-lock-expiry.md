# 01 — Acquisition-lock expiry vs. checkpoint/topology dwell time 🔴 HIGH

## 1. Design background

A share group does not hand a consumer a sticky partition with an offset cursor. Instead, on each
`poll()` the broker **acquires** a set of in-flight records *for that member* and starts an
**acquisition-lock timer** per record. If the member does not acknowledge a record before the timer
fires, the broker assumes the member died or is stuck, returns the record to `AVAILABLE`, and
**redelivers** it (possibly to a different member of the group).

- Config: `group.share.record.lock.duration.ms` (default **30000**, bounded 15000–60000 by
  `group.share.min/max.record.lock.duration.ms`).
- This is fundamentally different from consumer groups, where an un-committed offset simply stays
  un-committed forever with no redelivery and no clock.

The KIP-1289 fork adds an important relief valve: when an ack is **staged** into a producer
transaction (`ACQUIRED → TX_PENDING`), the broker **cancels the acquisition-lock timer**
(`InFlightState.stageTxnAcknowledge`). From that moment the *producer-transaction timeout* governs
how long the record may be held, not the 30s record lock. So the danger window is **only**
`fetch → stage`, not `fetch → commit`.

## 2. Current flow

`SameTransactionShareAckKafkaWriter` buffers ack payloads as records arrive and stages them **all at
once at `prepareCommit()`** (i.e. at the checkpoint barrier):

- `write(element, ctx)` writes the sink record, then buffers the payload —
  `SameTransactionShareAckKafkaWriter.java:60-66`.
- `prepareCommit()` stages every buffered payload into the current producer transaction, then
  precommits — `SameTransactionShareAckKafkaWriter.java:73-83`.
- `ShareAckPayloadBuffer.stage()` loops payloads and calls the stager —
  `ShareAckPayloadBuffer.java:51-63`.

So a record acquired at the source remains `ACQUIRED` for **the whole time it travels the topology
plus the remainder of the checkpoint interval**, until the sink's `prepareCommit()` stages its ack.

## 3. The issue

If `dwell time > record.lock.duration.ms`, the broker redelivers the record **while a copy is still
in flight** inside Flink:

```
t0  source poll: record R ACQUIRED by member M (lock timer 30s starts)
t1  R flows through topology, buffered at sink (still ACQUIRED)
t2  30s elapse with no stage  → broker: R → AVAILABLE → redelivered as R'
t3  R' processed too; sink writes output for R' into a later transaction
t4  both transactions commit → R's output appears TWICE  (EOS violated by duplication)
```

The dwell can blow past 30s under **backpressure**, a **slow checkpoint**, or a large
**checkpoint interval**. Note the failure here is **duplication**, not loss.

### Why it is *recoverable* but only if one invariant holds

The broker's `stageTxnAcknowledge` is **state-matched**: it only moves a record `ACQUIRED → TX_PENDING`
if this member still holds it. If R's lock already expired, staging R's ack **fails**. As long as a
failed stage **aborts the whole transaction** (no partial commit), the in-flight copy's output is
never published and only the current owner commits — back to exactly-once. The invariant:

> **All-or-nothing:** if *any* share-ack stage fails, the entire sink transaction must abort. Never
> commit sink output while silently dropping a failed ack.

Today this holds *implicitly*: a stage failure throws `IOException` out of `prepareCommit()`, which
fails the checkpoint, which aborts the transaction on restart. It is not asserted or tested.

## 4. Classes & modules

| Layer | Class | Role |
| --- | --- | --- |
| Flink sink | `SameTransactionShareAckKafkaWriter` (`sink/`) | Buffers payloads; stages at `prepareCommit`. |
| Flink sink | `ShareAckPayloadBuffer` (`sink/`) | Holds payloads by id; `stage()` loops them. |
| Flink share | `ShareAckPayloadStager` (`share/`) | Reflectively calls `sendShareAcknowledgementsToTransaction`. |
| Kafka broker | `SharePartition` / `InFlightState.stageTxnAcknowledge` | `ACQUIRED → TX_PENDING`, cancels lock timer. |
| Kafka broker | `ShareGroupConfig` | Owns `record.lock.duration.ms`. |

## 5. Client/server involvement

- **Client (Flink):** controls dwell time via checkpoint interval, buffering, and *when* it stages.
  It decides all-or-nothing abort.
- **Server (broker):** owns the lock timer and redelivery; validates staging against current
  acquisition; cancels the timer on successful stage.

The lever the client owns is **dwell time**; the lever the server owns is **lock duration**. The fix
is to make the client's dwell small and the server's lock duration large enough to cover it.

## 6. How it is solved

Three layers, in priority order:

1. **Stage acks incrementally at `write()`, not in a batch at `prepareCommit()`.** As soon as a
   record reaches the sink, stage its ack into the already-open transaction. This shrinks the danger
   window from "topology latency + checkpoint interval" to "topology latency" only, and immediately
   cancels the broker lock timer. Implementation: in `SameTransactionShareAckKafkaWriter.write()`,
   after `delegate.write(...)`, stage that element's payloads into `delegate.currentProducer()`
   instead of buffering for `prepareCommit()`. The `prepareCommit()` path then only flushes/precommits.
   (Keep the buffer only for payloads that arrive before the first record opens the transaction.)

2. **Make the all-or-nothing invariant explicit.** Wrap the per-payload stage loop so that any stage
   failure marks the transaction poisoned and forces an abort on the next `prepareCommit()`/`flush`,
   with a clear log line ("share-ack stage failed for id=…, aborting transaction"). Add a unit test
   that asserts a stage failure prevents commit.

3. **Operational guardrails (config + docs).** Require `record.lock.duration.ms` ≫ worst-case
   dwell; recommend bounding the checkpoint interval; document the relationship. Expose a metric
   `shareAckStageLatency` (record-acquire → stage) and alert when it approaches the lock duration.

Layer 1 is the real fix; 2 guarantees safety even when 1's window is exceeded; 3 keeps you out of the
redelivery regime in the first place.

## 7. How to verify

- **Unit:** stub a producer whose `sendShareAcknowledgementsToTransaction` throws for one payload;
  assert `prepareCommit()` throws and no `KafkaCommittable` is emitted (proves all-or-nothing).
- **IT (the important one):** run the share→sink pipeline with `record.lock.duration.ms` set *below*
  the checkpoint interval and induce backpressure; assert output count == input count (no duplicates)
  because expired records' transactions abort and only re-acquired copies commit.
- **Metric check:** assert `shareAckStageLatency` stays well under `record.lock.duration.ms` in the
  happy path once incremental staging is in place.
