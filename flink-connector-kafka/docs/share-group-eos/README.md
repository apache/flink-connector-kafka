# Share-Group EOS — design notes and corner-case plan

These notes explain how exactly-once (EOS) works for a **Kafka share-group source → Flink → Kafka
sink** pipeline in this connector, and lay out a ranked plan for the distributed corner cases that
still threaten correctness.

They are written to be read top-to-bottom: this file gives the model and vocabulary; each numbered
file drills into one corner case with the same structure:

1. **Design background** — what the relevant Kafka/Flink mechanism is and why it exists.
2. **Current flow** — what the code does today, with `file:line` references.
3. **The issue** — the distributed failure mode, step by step.
4. **Classes & modules** — exactly where it lives.
5. **Client/server involvement** — who does what across Flink (client) and the Kafka broker (server).
6. **How it is solved** — the concrete plan, and why it restores correctness.
7. **How to verify** — the test or experiment that proves the fix.

## The model we committed to: one transaction (Mechanism A)

We deleted the separate-transaction 2PC ("Mechanism B") and kept the **same-transaction** design.
The single most important idea:

> The source's share-group **acknowledgements** and the sink's **output records** are written into
> **one Kafka producer transaction** and made durable by **one transaction-commit marker**, which is
> finalized exactly when one Flink checkpoint completes.

Because there is only one transaction and one marker, the broker's per-record share-group state can
never disagree with the sink's committed output:

- Checkpoint's transaction **commits** → output visible *and* records `ACKNOWLEDGED` → not redelivered.
- Checkpoint's transaction **aborts** (normal recovery of in-flight work) → output invisible
  (`read_committed`) *and* records returned to `AVAILABLE` → redelivered and reprocessed.

Either way, each source record is reflected in the sink output exactly once. This also means the
**source needs essentially no checkpoint state** — anything not acknowledged-in-a-committed-transaction
is redelivered by the broker, so the source is a near-stateless forwarder.

## End-to-end flow (happy path)

```
                        Flink job (the transaction coordinator)
   ┌─────────────────┐     ShareAckPayload      ┌──────────────────────────────┐
   │  share-group     │  carried with emitted    │ SameTransactionShareAckKafka │
   │  source reader   │  records                 │ Writer (sink)                │
   │  (KafkaShare     │ ───────────────────────► │  - initialize(): begin txn   │
   │   Consumer)      │                          │  - write(): send output and │
   └─────────────────┘                          │    stage share acks inline  │
          │ poll + explicit-ack mode             │  - prepareCommit(): prepare │
          │ acknowledgementsForTransaction()     └──────────────┬───────────────┘
          ▼                                                      │ KafkaCommittable
   Kafka broker (server)                                         ▼ (producerId, epoch,
   - share coordinator: ACQUIRED → TX_PENDING               KafkaCommitter.commit()
   - __share_group_state topic enrolled in producer txn     completeTransaction(prepared)
   - WriteTxnMarkers(COMMIT) → TX_PENDING → ACKNOWLEDGED
```

Key Kafka primitives (in the `apache_stream/kafka` fork, KIP-1289 + KIP-939):

- `producer.sendShareAcknowledgementsToTransaction(ShareAcknowledgements, ShareGroupMetadata)` —
  attaches share-ack intent to the open producer transaction. Broker enrolls the relevant
  `__share_group_state` partitions into the transaction (like `__consumer_offsets` for
  `sendOffsetsToTransaction`).
- `producer.prepareTransaction()` → `PreparedTxnState("producerId:epoch")` — KIP-939 2PC prepare.
- `producer.completeTransaction(PreparedTxnState)` — commit/abort the prepared transaction from any
  process (this is what the committer does on recovery).
- Broker record state machine adds `TX_PENDING`; staging an ack moves `ACQUIRED → TX_PENDING` and
  **cancels the acquisition-lock timer** so the producer-transaction timeout governs the hold.

## Component and thread map

| Step | Module / component | Thread that runs it | Notes |
| --- | --- | --- | --- |
| Poll share records | `KafkaShareConsumer` in the share source | Current IT: legacy source task thread. Future FLIP-27 source: split-fetcher I/O thread. | Records become `ACQUIRED` on the broker and the acquisition-lock timer starts. |
| Local ack | Share source | Same thread as `poll()` | `acknowledge(record, ACCEPT/REJECT)` updates client-local state only. |
| Build ack payload | `ShareAckPayload.fromKafkaObjects(...)` | Same source-side thread as local ack extraction | `acknowledgementsForTransaction()` drains all acknowledged in-flight records and requires explicit mode. |
| Emit to Flink | Share source / source record emitter | Current IT: legacy source task thread under checkpoint lock. Future FLIP-27 source: reader/operator main thread emits fetched records. | Payload routing must ensure a payload is staged by one sink transaction only. |
| Open sink transaction | `ExactlyOnceKafkaWriter.initialize()` and `snapshotState(...)` | Sink writer task thread | `startTransaction(...)` calls `producer.beginTransaction()` before any `write(...)`. |
| Write output | `KafkaWriter.write(...)` | Sink writer task thread | `producer.send(...)` buffers output records in the Kafka producer sender path. |
| Stage share ack | `SameTransactionShareAckKafkaWriter.write(...)` + `ShareAckPayloadStager` | Sink writer task thread blocks; Kafka producer sender performs the RPC | `sendShareAcknowledgementsToTransaction(...)` waits for the broker response; broker stages `ACQUIRED -> TX_PENDING` and cancels the lock timer. |
| Prepare checkpoint transaction | `ExactlyOnceKafkaWriter.prepareCommit()` | Sink writer task thread during checkpoint prepare | With Kafka 2PC enabled, `prepareTransaction()` flushes pending records and returns `PreparedTxnState`; no share-ack staging should happen here. |
| Start next transaction | `ExactlyOnceKafkaWriter.snapshotState(...)` | Sink writer task thread | After snapshotting transaction state, the writer opens the producer transaction for the next checkpoint window. |
| Complete transaction | `KafkaCommitter.commit(...)` | Sink committer operator thread after checkpoint completion | Calls `completePreparedTransaction(...)` for prepared 2PC or `commitTransaction()` otherwise. |
| Finalize broker state | Kafka transaction coordinator, output partition leaders, share coordinator | Kafka broker network/coordinator threads | EndTxn writes `PREPARE_COMMIT`; WriteTxnMarkers makes output visible and resolves `__share_group_state`. |

## Glossary

| Term | Meaning |
| --- | --- |
| Share group | KIP-932 consumption: per-record acquire + explicit ack, no sticky partition assignment, no engine-owned offset. |
| Acquisition lock | Per-record lease (`group.share.record.lock.duration.ms`, default 30s) after which an un-acked record is redelivered. |
| Delivery count | Times a record was delivered; at `group.share.delivery.count.limit` (default 5) it is archived. |
| `TX_PENDING` | Broker record state: staged into a producer transaction, awaiting the commit/abort marker. |
| Dwell time | Wall-clock a record spends `ACQUIRED` in the Flink pipeline before its ack is staged. |
| Member epoch | Share-group membership generation; bumped on rebalance; fences stale acks. |

## Ranked corner cases

| # | Severity | Corner case | File |
| --- | --- | --- | --- |
| 01 | 🔴 HIGH | Acquisition-lock expiry vs. checkpoint/topology dwell time | `01-acquisition-lock-expiry.md` |
| 02 | 🔴 HIGH | Delivery-count limit → silent data loss | `02-delivery-count-data-loss.md` |
| 07 | 🔴 HIGH | Record-carried acks do not cover records that produce no sink output | `07-no-output-records-cannot-ack.md` |
| 03 | 🟠 MED | Member-epoch staleness between fetch and stage | `03-member-epoch-staleness.md` |
| 04 | 🟠 MED | One `ShareGroupMetadata` per ack vs. topology shuffles | `04-multi-member-per-transaction.md` |
| 05 | 🟠 MED | Transaction timeout & prepared-transaction holding (2PC) | `05-transaction-timeout-holding.md` |
| 06 | ⚪ LOW | `groupId` NPE + missing crash-recovery test | `06-hardening-and-tests.md` |

## Validation status

The claims behind these docs were re-checked against the actual code (Kafka fork
`kip-1289-txn-ack-share-groups` and this connector). Summary:

- **CONFIRMED:** staging cancels the acquisition-lock timer and is state/owner-matched (01); lock
  duration 30s default and delivery-count limit 5 with archive-at-limit + REJECT→DLQ (02);
  **member epoch is validated at stage time, NOT at commit time** (03 — so once staged, recovery is
  safe even if membership changed); the broker accepts a transaction containing **only share-acks
  and no data records** ("Claim C" — relevant to 07 and to engine-agnostic Spark/Druid use).
- **SHARPENED:** multi-member-per-transaction is unconstrained by the broker (no per-txn member
  binding) but untested (04).
- **CORRECTED:** prepared **2PC** transactions are *exempt* from the timeout sweeper
  (`txnTimeoutMs = MAX`) and are recoverable only when `transaction.2pc.enable=true`; the risk is
  therefore *indefinite holding of abandoned transactions*, not premature abort (05, rewritten).
- **NEW:** the current record-carried A1 wiring cannot route acks for records that produce no sink
  output; the producer path itself supports ack-only transactions, so the general fix is a separate
  share-ack committable participant (07).

## Design records

- `08-implementation-plan-01-and-07.md` — incremental staging + acks-only commit (A1 hardening; B/C/D done).
- `09-adr-general-2pc-recovery-model.md` — **ADR**: arbitrary pipelines (fanout/joins/drops/multi-sink)
  use the checkpoint as the unit of work; the share-ack becomes a durable committable participant
  ("Mechanism B done right"). A1 kept as the 1:1 optimization.
- `10-component-design-general-2pc.md` — concrete components: the ack as its **own** `Sink`
  (`ShareAckSink` = `ShareAckWriter` + `ShareAckCommitter`), `ShareAckCommittable` + serializer, the
  source side-output ack channel, wiring, multi-sink, and the recovery IT matrix.

> Scope note: as of this branch, Mechanism A exists as library primitives + tests and is **not yet
> wired** into the `KafkaSink`/`KafkaSource` builders. Several fixes below are therefore "design +
> where to put it when wiring," not "edit an existing call site."
