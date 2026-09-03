# 05 — Transaction timeout & prepared-transaction holding 🟠 MED

> **Corrected after code validation.** An earlier draft of this doc claimed the broker aborts a
> prepared transaction once `transaction.max.timeout.ms` elapses. That is **wrong for 2PC
> transactions**, which are the ones this design relies on. See §3.

## 1. Design background

In the classic offset-based EOS sink, `transaction.timeout.ms` only bounds the **output** transaction;
the source position is unaffected. In the share-group design, staging an ack cancels the per-record
acquisition-lock timer (`InFlightState.stageTxnAcknowledge`, verified), so the **transaction**, not
the record lock, holds the staged records in `TX_PENDING`. One transaction lifecycle now governs both
the buffered output and the held source records.

Two transaction modes exist in the producer, and they behave **very differently** under timeout:

| Mode | How entered | Broker timeout (`txnTimeoutMs`) | Recoverable after crash? |
| --- | --- | --- | --- |
| Ordinary txn | `transaction.2pc.enable=false` | the configured `transaction.timeout.ms` | **No** — see §3 |
| **2PC / distributed** | `transaction.2pc.enable=true` + `initTransactions(keepPreparedTxn=true)` | **`Integer.MAX_VALUE`** (effectively never) | **Yes** via `completeTransaction` |

Verified in the fork:
- `TransactionMetadata.isDistributedTwoPhaseCommitTxn()` is defined as `txnTimeoutMs == Integer.MAX_VALUE`
  (`TransactionMetadata.java:308-310`), set when `enableTwoPCFlag` is true
  (`TransactionCoordinator.scala:152`).
- The timeout sweeper **explicitly exempts** 2PC txns from abort:
  `TransactionStateManager.scala:138-140` — *"Do not apply timeout to distributed two phase commit
  transactions"*.
- `recoverPreparedTransaction` **refuses** to recover a txn that is not 2PC:
  `TransactionCoordinator.scala:243-244` → `INVALID_TXN_STATE`.

Verified on the Flink side: `FlinkKafkaInternalProducer.precommitTransaction()` only performs the real
KIP-939 prepare (and returns a `PreparedTxnState` string) when `twoPhaseCommitEnabled`
(`transaction.2pc.enable`) is set (`FlinkKafkaInternalProducer.java:143-152`). Otherwise it returns
`Optional.empty()` and falls back to legacy resume-by-id/epoch.

## 2. Current flow

- `ExactlyOnceKafkaWriter.prepareCommit()` precommits and emits the `KafkaCommittable`
  (`ExactlyOnceKafkaWriter.java:221-234`).
- `KafkaCommitter.commit()` finalizes via `completePreparedTransaction(...)` (2PC) or resume+commit.
- Standard Flink EOS already requires `transaction.timeout.ms > checkpoint interval`. The new wrinkle:
  with **2PC enabled**, the prepared transaction is parked at `txnTimeoutMs = MAX` and the held share
  records ride along with it.

## 3. The issue (corrected)

The danger is **not** "the broker aborts the prepared transaction after 15 minutes." It is the
opposite, and it splits by mode:

**If 2PC is NOT enabled (the misconfiguration trap):**

```
t0  checkpoint N: precommit (legacy) → producer in PRECOMMITTED, txn still ONGOING on broker
t1  crash before KafkaCommitter commits
t2  broker timeout sweeper finds the ONGOING txn past transaction.timeout.ms → ABORTS it
t3  recovery tries to re-attach via keepPreparedTxn → broker returns INVALID_TXN_STATE
result  the committable cannot be completed; output lost-or-aborted, acks aborted, records
        redelivered (exactly-once preserved by replay, but the committer errors are ugly)
```

So **the whole prepared-transaction recovery story only works if `transaction.2pc.enable=true`.**
Running this feature on ordinary transactions is a latent correctness/operability bug.

**If 2PC IS enabled (the correct config) — the real trade-off:**

The prepared transaction is exempt from timeout and is **held indefinitely** until a commit/abort
marker arrives. That makes recovery robust, but it means a job that **crashes and never recovers**
leaves its share records pinned in `TX_PENDING` **forever** — never redelivered, because (verified in
doc 01/06) the *only* thing that releases `TX_PENDING` is the transaction marker, and there is no
independent timeout on `TX_PENDING`. Those records become unavailable to every other member of the
share group until someone aborts the abandoned transaction.

```
t0  2PC prepare; share records in TX_PENDING; txnTimeoutMs = MAX
t1  job dies permanently (no restart)
t2  no marker ever arrives → records stay TX_PENDING indefinitely → share-partition capacity pinned
```

So the timeout concern **flips**: with 2PC you no longer fear premature abort; you fear *never*
releasing. This is a share-group availability problem, not a data-loss problem.

## 4. Classes & modules

| Layer | Class | Role |
| --- | --- | --- |
| Flink sink | `ExactlyOnceKafkaWriter` (`sink/`) | precommit at barrier. |
| Flink sink | `FlinkKafkaInternalProducer` (`sink/internal/`) | `twoPhaseCommitEnabled` gate; precommit/complete. |
| Flink sink | `KafkaCommitter` (`sink/internal/`) | completes prepared txn on commit/recovery. |
| Kafka broker | `TransactionCoordinator` / `TransactionStateManager` | 2PC exemption from timeout; `recoverPreparedTransaction`. |
| Kafka broker | `TransactionMetadata` | `isDistributedTwoPhaseCommitTxn` (`txnTimeoutMs == MAX`). |

## 5. Client/server involvement

- **Client (Flink):** must set `transaction.2pc.enable=true` and call
  `initTransactions(keepPreparedTxn=true)` for recovery to be possible; otherwise it silently runs in
  the non-recoverable mode.
- **Server (broker):** parks 2PC txns at MAX timeout, exempts them from the sweeper, and re-attaches
  them on `recoverPreparedTransaction`. It will **never** auto-release an abandoned 2PC txn's records.

## 6. How it is solved

1. **Require 2PC; fail fast otherwise.** Make `transaction.2pc.enable=true` mandatory for share-EOS
   and add a builder precondition that rejects the configuration if it is off (and likewise rejects
   `transaction.timeout.ms < checkpoint interval` for the non-2PC output bound). This closes the §3
   misconfiguration trap.

2. **Bound the abandoned-transaction blast radius.** Because 2PC removes the safety-net timeout,
   provide an operational cleanup path for permanently-dead jobs: the existing lingering-transaction
   abort on restart (`ExactlyOnceKafkaWriter.abortLingeringTransactions`,
   `ExactlyOnceKafkaWriter.java:310-337`) covers restart-with-same-prefix; for jobs that never come
   back, document a manual/admin abort of open transactions by `transactionalId` prefix so pinned
   share records are released. Surface a metric for open prepared transactions.

3. **Keep `TX_PENDING` dwell short in steady state.** Short checkpoint intervals mean records sit in
   `TX_PENDING` only briefly between prepare and commit, limiting pinned share-partition capacity.

4. **Make rollback/abandonment observable.** Log explicitly when recovery completes a prepared txn vs.
   when it finds one already gone, and expose `openPreparedTxnCount` / `expiredPreparedTxnCount`.

## 7. How to verify

- **Config precondition test:** builder rejects share-EOS with `transaction.2pc.enable=false`, and
  rejects `transaction.timeout.ms < checkpoint interval`.
- **IT — recovery (2PC on):** crash after prepare, restart; assert the committer completes the prepared
  txn and output + acks commit exactly once — even after a delay far exceeding a normal txn timeout
  (proves the MAX-timeout exemption).
- **IT — abandoned txn:** prepare, then never recover; assert the records remain `TX_PENDING` and that
  the documented admin abort releases them back to `AVAILABLE` for redelivery.
