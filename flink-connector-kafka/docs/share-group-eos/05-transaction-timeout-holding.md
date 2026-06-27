# 05 — Transaction timeout now governs record holding 🟠 MED

## 1. Design background

In the classic offset-based EOS sink, the producer-transaction timeout (`transaction.timeout.ms`)
only governs the **output** transaction: if Flink fails to commit within the window, the broker
aborts the transaction and the buffered output is discarded. The source's *position* is unaffected —
offsets just stay un-committed.

In the share-group design, staging an ack cancels the per-record acquisition-lock timer
(`InFlightState.stageTxnAcknowledge`), and the **producer-transaction timeout becomes the clock that
holds the staged records** in `TX_PENDING`. So one timeout now bounds **two** things at once:

- how long the sink may hold buffered output before committing, **and**
- how long the source's acquired records stay held (un-redeliverable) on the broker.

Relevant settings: client `transaction.timeout.ms`, and broker ceiling
`transaction.max.timeout.ms` (default 15 min). KIP-939 adds prepared transactions
(`initTransactions(keepPreparedTxn=true)` + `prepareTransaction()`/`completeTransaction()`), which is
what lets a *prepared* transaction be re-attached and completed by a different process after a crash.

## 2. Current flow

- The sink writer precommits via `prepareTransaction()` at the checkpoint barrier (inside
  `ExactlyOnceKafkaWriter.prepareCommit()`), producing a `KafkaCommittable` with the prepared state.
- `KafkaCommitter.commit()` finalizes the prepared transaction with `completeTransaction(...)` on
  checkpoint completion / recovery.
- Standard Flink EOS already requires `transaction.timeout.ms > checkpoint interval`. The new wrinkle
  is that this same timeout now also governs the **held share records**, and it must additionally
  cover the **recovery window** (time from crash to the committer re-attaching and completing the
  prepared transaction).

## 3. The issue

If recovery (or any stall between prepare and complete) outlasts the transaction timeout, the broker
aborts the prepared transaction:

```
t0  checkpoint N: sink prepareTransaction(); acks staged → TX_PENDING; output buffered
t1  job crashes before KafkaCommitter completes the transaction
t2  recovery takes longer than transaction.max.timeout.ms
t3  broker aborts the transaction → output discarded AND staged acks → AVAILABLE
t4  records redelivered → reprocessed on the new run
```

Because output and acks share the one transaction, t3 is **consistent**: the output never became
visible and the records come back for reprocessing — exactly-once is preserved, it is just
*reprocessing*, not loss. The risk is therefore mainly **operational surprise** (a long outage
silently rolls back a checkpoint's worth of work and replays it) plus a subtle dependency: the
timeout must be sized for the worst-case recovery, and the broker ceiling must allow it.

The sharper danger is **misconfiguration**: if `transaction.timeout.ms` is set below the checkpoint
interval (or below realistic recovery time), transactions abort routinely and the job thrashes; and
holding many records in `TX_PENDING` for a long timeout ties up share-partition capacity.

## 4. Classes & modules

| Layer | Class | Role |
| --- | --- | --- |
| Flink sink | `ExactlyOnceKafkaWriter` (`sink/`) | `prepareTransaction()` at barrier. |
| Flink sink | `KafkaCommitter` (`sink/internal/`) | `completeTransaction()` on commit/recovery. |
| Flink sink | `FlinkKafkaInternalProducer` (`sink/internal/`) | Wraps prepare/complete + `initTransactions(keepPreparedTxn)`. |
| Kafka broker | transaction coordinator | Enforces `transaction.max.timeout.ms`; aborts on expiry; supports prepared-txn recovery. |
| Kafka broker | `InFlightState` / share coordinator | Holds `TX_PENDING` until the marker; releases on abort. |

## 5. Client/server involvement

- **Client (Flink):** chooses `transaction.timeout.ms`; controls checkpoint interval and how fast the
  committer re-attaches on recovery.
- **Server (broker):** enforces the ceiling, runs the abort-on-timeout, and (KIP-939) preserves the
  prepared transaction so it can be completed post-restart rather than auto-aborted immediately.

## 6. How it is solved

1. **Size the timeout for the worst case and validate it at startup.** Require
   `transaction.timeout.ms ≥ checkpoint interval + max expected recovery time + margin`, and
   `transaction.max.timeout.ms` on the broker ≥ that. Add a builder-time precondition that rejects a
   configuration where the timeout is smaller than the checkpoint interval (fail fast instead of
   thrashing in production).

2. **Confirm prepared-transaction survival semantics.** Verify in the fork that a *prepared*
   transaction is held (not eagerly aborted) until the timeout, and that
   `initTransactions(keepPreparedTxn=true)` on recovery re-attaches it for completion. Document the
   exact survival window so operators know how long an outage may last before a checkpoint rolls back.

3. **Make rollback observable.** When the committer finds a prepared transaction already aborted by
   timeout on recovery, log it explicitly and emit a metric `expiredPreparedTxnCount` so a "long
   outage rolled back checkpoint N, replaying" event is visible rather than mysterious.

4. **Keep `TX_PENDING` dwell short in steady state.** Short checkpoint intervals mean records sit in
   `TX_PENDING` only briefly, limiting how much share-partition capacity is tied up by held records.

## 7. How to verify

- **IT — recovery within timeout:** crash the job after prepare, restart within the timeout; assert
  the committer completes the prepared transaction and output + acks commit exactly once.
- **IT — recovery beyond timeout:** crash after prepare, delay restart past `transaction.max.timeout.ms`;
  assert the transaction aborted, `expiredPreparedTxnCount` incremented, records were redelivered, and
  the final output is still exactly-once (reprocessed, not duplicated or lost).
- **Config precondition test:** assert the builder rejects `transaction.timeout.ms < checkpoint
  interval`.
