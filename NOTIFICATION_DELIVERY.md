# Telegram notification delivery

New schedules are deduplicated in `discovered_schedules`. Only schedule IDs
inserted for the first time are included in an alert. Each verified schedule is
formatted as its own message and outbox row in the same SQLite transaction.
The schedule ID is the notification key, so the unique index prevents a second
outbox record for that schedule. Schedules with a missing ID or a job ID that
does not match the parent job are rejected. Missing alert fields are labelled
"Not supplied"; the alert does not invent pay, hours, contract details, or
schedule times. Existing grouped outbox rows are preserved and remain
deliverable. The additive migration does not remove existing records.

The Telegram worker drains up to five due notifications per cycle, and a
single-flight guard prevents its timer from starting another cycle while one is
running. Each row is claimed before sending and marked `sent` only when the HTTP
response succeeds and Telegram returns JSON with `ok: true`. Network errors and
HTTP 5xx responses retry with capped exponential backoff and stay pending until
delivery succeeds. HTTP 429 uses `Retry-After` when present (including
Telegram's JSON retry hint), otherwise a 60-second delay. Other non-retryable
rejections are marked `failed` after three attempts. The attempt count and last
error remain recorded. A one-minute claim expiry recovers work left processing
by a stopped process.

Job-feed and schedule pagination are each bounded by their existing page
limits. If a `nextToken` remains at the limit, the scan is reported as
`PARTIAL`, not `COMPLETE`.

Delivery is at-least-once, not exactly-once. If Telegram accepts a message but
the response is lost, the queue cannot distinguish that from a failed send and
will retry after backoff. This can produce a duplicate alert. Telegram's
`sendMessage` API does not provide a general idempotency key that would remove
this risk.
