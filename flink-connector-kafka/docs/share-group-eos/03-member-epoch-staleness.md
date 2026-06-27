# 03 — Member-epoch staleness between fetch and stage 🟠 MED

## 1. Design background

Every share-group member has an identity `(groupId, memberId, memberEpoch)`. The **member epoch** is
a monotonically increasing generation number that the broker bumps on every share-group rebalance
(a consumer joining or leaving, a heartbeat-driven reconciliation, or the source restarting). The
epoch is the broker's fencing token: an operation carrying a **stale** epoch is rejected so that a
zombie/old member cannot mutate state after the group has moved on.

When the producer stages share acks, it passes the consumer's `ShareGroupMetadata(groupId, memberId,
memberEpoch)`. The broker validates this against the *current* membership and returns
`STALE_MEMBER_EPOCH` / a fencing error if the epoch is no longer current.

Crucial timing detail: this validation happens at **stage time**
(`KafkaApis.handleTxnShareAcknowledgeRequest` → `groupCoordinator.validateShareGroupMember`), **not**
at commit time. The `WriteTxnMarkers` commit path only resolves `TX_PENDING` records by producerId /
epoch; it does not re-check member epoch. So once an ack is successfully staged, recovery via
`completeTransaction(preparedState)` is safe even if the source member is long gone.

## 2. Current flow

The member identity is captured at the **source** and travels downstream inside each record:

- `ShareAckPayload` stores `groupId`, `memberId`, `memberEpoch` (`ShareAckPayload.java:36-56`),
  built from the consumer's `shareGroupMetadata()` (`fromKafkaObjects`, `:59-95`).
- The **sink** reconstructs `ShareGroupMetadata` from those fields and stages the ack —
  `ShareAckPayloadStager.groupMetadata()` (`:72-80`) and `stage()` (`:45-70`).

So the epoch that reaches the broker is the one captured **when the source polled**, but it is used
**when the sink stages** — one checkpoint interval (plus topology latency) later.

## 3. The issue

If the source's share-group membership rebalances in the window between fetch and stage, the captured
epoch is stale and the stage is fenced:

```
t0  source polls; captures memberEpoch = 7 into ShareAckPayload for record R
t1  another consumer joins the share group → broker bumps source member to epoch 8
t2  sink stages R's ack with epoch 7 → broker rejects: STALE_MEMBER_EPOCH
t3  prepareCommit() throws → checkpoint fails → job restarts → reprocess
```

This is **safe** (no loss, no duplicate — the transaction aborts and records redeliver) but it is a
**liveness** hazard: if the share group rebalances frequently (e.g. autoscaling consumers, flapping
membership), every checkpoint can fail the same way, producing a **restart loop** that makes no
progress. The exposure window is exactly `fetch → stage`; shrinking it (doc 01's incremental staging)
also shrinks this window.

## 4. Classes & modules

| Layer | Class | Role |
| --- | --- | --- |
| Flink share | `ShareAckPayload` (`share/`) | Carries the captured `memberEpoch`. |
| Flink share | `ShareAckPayloadStager` (`share/`) | Rebuilds `ShareGroupMetadata` at stage time. |
| Kafka broker | `KafkaApis.handleTxnShareAcknowledgeRequest` | Validates member epoch at stage time. |
| Kafka broker | group coordinator `validateShareGroupMember` | Source of `STALE_MEMBER_EPOCH`. |

## 5. Client/server involvement

- **Client (Flink):** the *member* lives in the source operator; the *stage* happens in the sink
  operator — a different task, often a different JVM, a checkpoint later. Flink is responsible for the
  gap between capture and use.
- **Server (broker):** owns epoch issuance and fencing. It will not accept an ack on behalf of a
  member at an epoch it has superseded.

The architectural tension: the actor that *owns* the membership (source) is not the actor that
*commits* the ack (sink), and time passes between them.

## 6. How it is solved

1. **Shrink the window (primary).** Incremental staging at `write()` (doc 01) collapses
   `fetch → stage` to roughly topology latency, making a rebalance landing inside the window rare.

2. **Keep the source member stable.** Treat the share consumer's membership as long-lived: tune
   session/heartbeat so routine operation does not rebalance, and avoid autoscaling the share-group
   consumer count under a running EOS job. Document that membership churn directly costs throughput
   via aborted checkpoints.

3. **Fail soft on fencing, don't hard-loop.** Detect `STALE_MEMBER_EPOCH` distinctly from other
   errors at stage time and surface it as a *retryable, throttled* condition with a clear log
   ("share-ack fenced by rebalance; aborting checkpoint and retrying"), plus a metric
   `shareAckFencedCount`. This turns an opaque restart loop into an observable, rate-limited retry so
   operators can react (and so the loop doesn't spin hot).

4. **Document the safety guarantee.** Make explicit in code comments and these docs that a fenced
   stage is *correctness-safe* (abort + redeliver) and only a liveness concern — so future
   maintainers don't "fix" it by committing partial work.

## 7. How to verify

- **IT — rebalance mid-flight:** start the pipeline, then add a second consumer to the share group
  between a poll and the next checkpoint; assert the affected checkpoint aborts, `shareAckFencedCount`
  increments, and after the membership settles the records commit exactly once (no loss/dup).
- **Unit:** stub the producer to throw the fencing exception on stage; assert it is classified as the
  retryable fenced condition (not a generic failure) and that no committable is emitted.
