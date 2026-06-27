# 04 — One `ShareGroupMetadata` per ack call vs. topology shuffles 🟠 MED

## 1. Design background

`sendShareAcknowledgementsToTransaction(ShareAcknowledgements, ShareGroupMetadata)` carries **exactly
one** member identity per call. The acks in that one call are attributed to that one
`(groupId, memberId, memberEpoch)`. A record can only be acknowledged by the member that currently
holds its acquisition — so the member identity used to ack a record must match the member that polled
it.

In a share group there is no sticky partition→member mapping: different source subtasks (each its own
share-group member) can hold records from the *same* topic-partition at the same time. So "which
member holds record R" is genuinely per-record information.

## 2. Current flow

This is actually handled reasonably today, and it's worth understanding why:

- Each `ShareAckPayload` carries its own `groupId`/`memberId`/`memberEpoch`
  (`ShareAckPayload.java:36-56`) — i.e. payloads are **per member** (per source poll).
- `ShareAckPayloadBuffer` keys payloads by `id` and `stage()` **loops payloads, issuing one stage
  call each** (`ShareAckPayloadBuffer.java:60-62`).
- `ShareAckPayloadStager.stage()` builds a fresh `ShareGroupMetadata` from *that payload's* fields
  per call (`ShareAckPayloadStager.java:72-80`).

So one sink transaction can already contain acks for **multiple members**, each sent as a separate
`sendShareAcknowledgementsToTransaction` call into the same transaction. The single-member-per-call
constraint is respected by sending N calls, not one.

## 3. The issue

Two residual risks remain:

1. **Unverified broker assumption.** We *assume* the broker accepts multiple
   `sendShareAcknowledgementsToTransaction` calls for **different members** within **one** producer
   transaction, all resolved by the same commit marker. If the broker instead binds a transaction to
   a single member, or rejects a second member's acks, then any topology that mixes members into one
   sink subtask's transaction breaks. This needs to be confirmed against the fork's broker code /
   tests, not presumed.

2. **Payload identity / dedup under shuffles.** When the source→sink edge is a shuffle
   (`keyBy`/rebalance/rescale), one sink subtask's transaction aggregates payloads from many source
   members. The buffer dedups by `payload.getId()` and **rejects conflicting payloads with the same
   id** (`ShareAckPayloadBuffer.java:39-45`). Correctness therefore depends on payload ids being
   **globally unique and stable** across the shuffle; if two source members ever mint the same id for
   different acks, the buffer throws and fails the checkpoint. The id scheme must be audited.

```
source subtask 0 (member A) ─┐
                             ├─shuffle─► sink subtask 0 ─► one txn must ack for A and B
source subtask 1 (member B) ─┘            → 2 stage calls (A-meta, B-meta), 1 marker
```

## 4. Classes & modules

| Layer | Class | Role |
| --- | --- | --- |
| Flink sink | `ShareAckPayloadBuffer` (`sink/`) | Dedups by id; loops one stage call per payload. |
| Flink share | `ShareAckPayload` (`share/`) | Per-member identity + the `id` used for dedup. |
| Flink share | `ShareAckPayloadStager` (`share/`) | One `ShareGroupMetadata` per call. |
| Kafka broker | txn coordinator + share coordinator | Must allow multi-member acks under one producer txn. |

## 5. Client/server involvement

- **Client (Flink):** must group acks by member (done implicitly: one payload = one member) and
  guarantee unique stable payload ids across the shuffle.
- **Server (broker):** must enrol each member's `__share_group_state` partitions into the *same*
  producer transaction and resolve them all on the single marker. This is the assumption to verify.

## 6. How it is solved

1. **Verify the broker contract (gating).** Before trusting multi-member transactions, confirm in the
   `apache_stream/kafka` fork that several `sendShareAcknowledgementsToTransaction` calls for distinct
   members in one producer transaction all reach `TX_PENDING` and are committed by one
   `WriteTxnMarkers`. If not supported, fall back to **per-member transactions** (which reintroduces
   the multi-transaction problem and argues for a forward-only topology).

2. **Constrain the topology, or embrace the shuffle deliberately.** Two viable shapes:
   - *Forward-only (simplest):* keep source→sink chained at equal parallelism with no shuffle, so each
     sink subtask's transaction only ever contains its co-located source member's acks. Document this
     as the supported topology for v1.
   - *Shuffle-tolerant:* explicitly support member-mixing, contingent on (1), and on a hardened id
     scheme.

3. **Harden the payload id.** Make `ShareAckPayload.id` provably globally unique (e.g.
   `groupId:memberId:memberEpoch:topicId:partition:firstOffset`) so the buffer's conflict check can
   never trip on legitimate distinct acks, and so a duplicate id reliably means a real bug.

## 7. How to verify

- **Broker contract test:** in the Kafka fork, one producer transaction, two members' acks staged,
  one commit marker — assert both members' records become `ACKNOWLEDGED`.
- **IT — shuffle:** run source parallelism ≠ sink parallelism with a `keyBy` between them; assert
  every input record's ack is committed exactly once and no `Conflicting share acknowledgement
  payload` error is thrown.
- **Unit:** feed the buffer two payloads with the same id but different content; assert it throws (the
  guard works), and two payloads with distinct ids from different members; assert both stage.
