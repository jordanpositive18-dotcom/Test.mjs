# Telegram notification delivery

New schedules are deduplicated in `seen_schedules` and their formatted alert is
inserted into `telegram_notifications` in the same SQLite transaction. The
notification queue is additive: existing scanner tables and rows are preserved.

The schedule-processing loop drains up to ten due notifications per run. Each
notification is atomically claimed in an immediate SQLite transaction before
the Telegram request, then marked `SENT` only when the HTTP response succeeds
and Telegram returns JSON with `ok: true`. Failures remain queued with their
attempt count and last error. Retry delay doubles from five seconds and is
capped at one hour; there is no maximum attempt count. A claim expires after
one minute so a later loop can recover work left claimed by a stopped process.
Telegram requests time out after 15 seconds.

Delivery is at-least-once, not exactly-once. If Telegram accepts a message but
the response is lost, the queue cannot distinguish that from a failed send and
will retry after backoff. This can produce a duplicate alert. Telegram's
`sendMessage` API does not provide a general idempotency key that would remove
this risk.
