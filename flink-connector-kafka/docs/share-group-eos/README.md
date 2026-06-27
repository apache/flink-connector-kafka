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
   │  share-group     │  rides on each record    │ SameTransactionShareAckKafka │
   │  source reader   │ ───────────────────────► │ Writer (sink)                │
   │  (KafkaShare     │                          │  - write(): buffer payloads  │
   │   Consumer)      │                          │  - prepareCommit(): stage    │
   └─────────────────┘                          │    acks INTO sink producer   │
          │ poll + explicit-ack mode             │    txn, then precommit       │
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
| 03 | 🟠 MED | Member-epoch staleness between fetch and stage | `03-member-epoch-staleness.md` |
| 04 | 🟠 MED | One `ShareGroupMetadata` per ack vs. topology shuffles | `04-multi-member-per-transaction.md` |
| 05 | 🟠 MED | Transaction timeout now governs record holding | `05-transaction-timeout-holding.md` |
| 06 | ⚪ LOW | `groupId` NPE + missing crash-recovery test | `06-hardening-and-tests.md` |

> Scope note: as of this branch, Mechanism A exists as library primitives + tests and is **not yet
> wired** into the `KafkaSink`/`KafkaSource` builders. Several fixes below are therefore "design +
> where to put it when wiring," not "edit an existing call site."
