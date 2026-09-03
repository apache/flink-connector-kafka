# 06 — Hardening & missing tests ⚪ LOW (but ship-blocking for the test gap)

This file collects the lower-severity correctness/robustness items. Individually small; together they
are the difference between "compiles and demos" and "trustworthy EOS."

## A. `writeUTF(null)` NPE on null identifiers

### Issue
Ack-carrying types serialize string fields with `DataOutputView.writeUTF`, which throws `NPE` on
`null`. `ShareAckPayload` does guard its own fields (`Objects.requireNonNull` for `id`/`groupId`/
`memberId`, `ShareAckPayload.java:48-50`), but any committable/state type that writes a `groupId` or
`transactionalId` without a null guard fails **at checkpoint serialization time** — the worst place to
discover it, because it surfaces as a checkpoint failure mid-run rather than at construction.

### How it is solved
Null-check identifier fields **in the constructor** of every share-EOS value type (fail at
construction, with a clear message), and add a serializer round-trip unit test that feeds the minimal
valid object and asserts no NPE. Mechanism A's surviving `ShareAckPayload` already does this; the rule
is "every new value type that gets serialized into checkpoint/committable state guards its strings in
its constructor."

### Verify
Unit: construct each value type with a `null` id/group/txn id and assert it throws `NullPointerException`
immediately (not later at serialize).

## B. No crash / checkpoint-restore integration test

### Issue — this is the real gap
The existing `KafkaShareEosPipelineITCase` runs with `RestartStrategy = none` and asserts only the
**happy path** (output count, unique offsets, share-lag → 0). There is **no test that kills a task
between prepare and commit and asserts no-loss / no-duplicate.** You cannot claim EOS from a
happy-path test — EOS is a statement about behavior *under failure*. Every corner case in docs 01–05
ultimately needs a recovery test to be considered closed.

### How it is solved
Add a recovery IT matrix that injects failure at each meaningful point and asserts exactly-once
end-to-end (input multiset == output multiset):

| Scenario | Inject failure… | Assert |
| --- | --- | --- |
| Pre-commit crash | after `prepareTransaction()`, before committer completes | committer re-attaches prepared txn; output+acks commit once |
| In-flight crash | mid checkpoint window, before barrier | transaction aborts; records redelivered; reprocessed once |
| Lock expiry | dwell > `record.lock.duration.ms` (doc 01) | expired copy's txn aborts; only re-acquired copy commits; no dup |
| Beyond txn timeout | restart delayed past `transaction.max.timeout.ms` (doc 05) | prepared txn aborted; redelivered; still exactly-once |
| Rebalance fence | add consumer mid-flight (doc 03) | checkpoint aborts; settles; exactly-once |

Use a restart strategy that actually restarts, a `MiniCluster` with a kill hook (or a failing map
that throws once at a controlled count), and compare the committed sink topic (read with
`read_committed`) against the input set.

### Verify
The matrix above *is* the verification. Gate the feature's "ready" state on this matrix passing, not
on the happy-path IT.

## C. Wiring is absent (context, tracked elsewhere)

Mechanism A is library primitives + tests; nothing in the `KafkaSink`/`KafkaSource` builders enables
it, and there is no user-facing option. Most fixes above ("add a builder precondition", "expose a
metric", "decide ACCEPT/REJECT in processing") only have a home **once wiring exists**. Sequence the
work as: (1) wire Mechanism A into the builders with an opt-in, (2) land the recovery IT matrix (B),
then (3) apply the corner-case fixes (docs 01–05) against real call sites.

## D. Metrics & observability (cross-cutting)

Docs 01, 03, 05 each call for a metric. Consolidate them into one share-EOS metric group so operators
have a single dashboard:

| Metric | Meaning | Sourced from doc |
| --- | --- | --- |
| `shareAckStageLatency` | acquire → stage latency vs. lock duration | 01 |
| `shareAckFencedCount` | stale-member-epoch fences | 03 |
| `expiredPreparedTxnCount` | prepared txns aborted by timeout on recovery | 05 |
| `deliveryCountHighWatermark` | max delivery count seen vs. limit | 02 |

### Verify
Assert each metric is registered and moves under its corresponding injected-failure IT.
