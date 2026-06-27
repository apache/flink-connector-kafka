# 02 — Delivery-count limit → silent data loss 🔴 HIGH

## 1. Design background

Share groups bound how many times a record may be redelivered. Each redelivery increments the
record's **delivery count**; once it reaches `group.share.delivery.count.limit` (default **5**, range
2–10) the broker **archives** the record. An archived record is **never delivered again** —
permanently. This is the broker's poison-pill defense: a record that keeps failing must not block the
share-partition forever.

This mechanism has **no equivalent in the offset-based source**. With consumer groups, a record that
is never committed is simply re-read on restart forever; there is no silent drop. So this is a
share-group-specific data-loss vector that the EOS design must explicitly handle.

## 2. Current flow

The connector never inspects or sets acknowledge *types*. `ShareAckPayload.AcknowledgementBatch`
carries `acknowledgeTypes` as an opaque `List<Byte>` (`ShareAckPayload.java:252-272`); the only type
used anywhere in the codebase is `ACCEPT`, and only in tests. There is:

- no detection of climbing delivery counts,
- no `RELEASE`/`REJECT` path,
- no dead-letter routing.

Every abort→redeliver cycle (from a failed checkpoint, expired lock, or fenced ack — see docs 01,
03, 05) silently advances the delivery count toward the archive threshold.

## 3. The issue

A record that repeatedly fails to commit is **silently lost** after N attempts:

```
attempt 1  R delivered, checkpoint fails (or lock expires) → abort → R AVAILABLE (count=1)
attempt 2..4  same  → count climbs
attempt 5  delivery count == limit → broker ARCHIVES R → never delivered again
result    R is gone; no error surfaced to the Flink job; output is missing one record
```

Triggers in practice: a genuine **poison record** (deserialization error, schema mismatch, a sink
that always rejects it), or **sustained backpressure / flapping** that causes many abort cycles for
otherwise-healthy records. The latter is the nastier case: transient infrastructure trouble can
quietly drop good data.

## 4. Classes & modules

| Layer | Class | Role today / role in fix |
| --- | --- | --- |
| Flink share | `ShareAckPayload.AcknowledgementBatch` (`share/`) | Carries opaque `acknowledgeTypes`; would carry ACCEPT vs REJECT. |
| Flink source | (future) share reader | Must observe delivery count from poll and decide ACCEPT/REJECT/RELEASE. |
| Kafka broker | `SharePartition` release/archive handlers | Apply lock-expiry → AVAILABLE/ARCHIVED, enforce `delivery.count.limit`. |
| Kafka broker | `ShareGroupConfig` | Owns `delivery.count.limit`; DLQ-on-REJECT behavior (`ARCHIVING` vs `ARCHIVED`). |

## 5. Client/server involvement

- **Server (broker):** counts deliveries, archives at the limit, and (in the fork) routes
  `REJECT` to a DLQ-style `ARCHIVING` terminal state when DLQ is enabled.
- **Client (Flink):** the only actor that can *decide* a record is poison and should be `REJECT`ed
  (to DLQ) rather than retried into oblivion — and the only actor that can surface "we are nearing
  the archive limit" as a job-visible signal. The broker cannot know whether Flink will eventually
  succeed.

## 6. How it is solved

A policy decision plus three mechanisms:

1. **Pick an explicit delivery-count policy and document it.** For a streaming EOS job the natural
   default is: raise `delivery.count.limit` high enough that transient backpressure never archives
   healthy records, and rely on the abort/redeliver loop to self-heal. Make this an explicit,
   documented requirement, not a silent reliance on the 5 default.

2. **Dead-letter the genuine poison records.** When the pipeline classifies a record as
   unprocessable (deserialization failure, repeated sink rejection), emit a `REJECT` acknowledge type
   for it (routing it to the broker DLQ / `ARCHIVING` path) **inside the same transaction** as the
   rest of the batch, instead of letting it loop. This requires threading a per-record
   ACCEPT/REJECT decision from the processing function into the `ShareAckPayload`'s
   `acknowledgeTypes` (today hard-wired to ACCEPT). This is the structural change: the payload must
   be able to express REJECT.

3. **Observe and alarm before loss.** Surface the per-record delivery count (available from the
   share `poll`) as a metric and log/alert when any record exceeds a threshold (e.g. limit − 1), so
   operators see "about to archive" *before* data is lost. This converts silent loss into a loud
   signal.

The combination means: healthy records never archive (1), known-bad records go to a DLQ you can
inspect rather than vanishing (2), and you get warned before either failure mode bites (3).

## 7. How to verify

- **IT — poison record:** feed a record the sink always rejects; assert that after the policy kicks
  in it lands in the DLQ topic (REJECT path) and the job continues, rather than the record being
  silently archived and the job stalling/losing data.
- **IT — flapping:** induce N−1 abort cycles on a healthy record (N = limit) and assert it is still
  eventually committed exactly once (proves we don't archive healthy data under transient failure
  when the limit is configured correctly).
- **Metric check:** assert the delivery-count gauge rises during induced failures and an alert/log
  fires at the configured threshold.
