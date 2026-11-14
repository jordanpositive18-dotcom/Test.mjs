# Telegram notification delivery

New schedules are deduplicated in `discovered_schedules`. Only schedule IDs
inserted for the first time are included in an alert. All newly discovered
schedules from one job pass are formatted into one message and one outbox row,
inserted in the same SQLite transaction. A unique notification key prevents
the same group from being inserted twice. The outbox migration adds columns and
an index without dropping existing records.

The Telegram worker drains up to five due notifications per cycle, and a
single-flight guard prevents its timer from starting another cycle while one is
running. Each row is claimed before sending and marked `sent` only when the HTTP
response succeeds and Telegram returns JSON with `ok: true`. Failures retry
after five seconds and then ten seconds; after three total attempts the row is
marked `failed` and is not automatically retried. The attempt count and last
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
