# Amazon request protection and recovery

## Scope

The scanner runs **one Node.js process with the existing three asynchronous
loops**. Startup stagger, normal scan delays, GraphQL payloads, headers,
authorization resolution, proxy routing, and Telegram behavior are unchanged.

Each Amazon request checks protection state before sending and again before
accepting its response. Its abort controller remains registered through JSON body
reading. The 30-second deadline covers both headers and body. Aborted or blocked
responses do not reach job/schedule accumulation or notification enqueueing.

## Three-loop scan coordination

The existing three loops and their 0/5/10-second startup timing remain. A shared
scan generation fetches and paginates the nationwide job feed once. The complete
deduplicated result is shared among the loops; each loop processes a distinct
index-based subset of jobs for schedule discovery. Schedule requests for the
same job are coalesced while in flight. A new generation cannot start until all
three loops finish the current one, so the loops do not trigger duplicate feed
polls or accumulate a request backlog.

When a stored cooldown has just expired, only the loop that starts the first
recovery generation may issue Amazon requests in that generation. The other
loops' schedule attempts are dropped rather than queued and are retried in the
next shared generation. This avoids a synchronized three-loop request burst
without changing the normal 3–5-second loop delay.

## Durable recovery

The recovery files live beside the existing SQLite file:

- `<database filename>.amazon-protection-recovery.json`
- `<database filename>.amazon-protection-recovery.json.tmp`

For the existing database name these are `shift_tracker.db.amazon-protection-recovery.json`
and its `.tmp` counterpart. They contain a format version, reason, expiry, and
update timestamp—never credentials or response/job data.

Before attempting a cooldown update in SQLite, the process writes a recovery
record to the temporary file, fsyncs the file and directory, renames it atomically,
and fsyncs the directory again. SQLite is then updated inside a `BEGIN IMMEDIATE`
transaction. Only after the transaction commits and its result is verified are
the recovery files removed and the directory synced.

On startup, any remaining main or temporary marker is reconciled with SQLite.
The maximum expiry and strongest overlapping reason win. A crash after SQLite
commit but before marker removal is safe: repeating reconciliation is idempotent.
An incomplete, corrupt, or unreadable marker blocks requests; it is not silently
deleted or replaced with a clear state. SQLite read/write errors also block
requests while reconciliation is retried at the existing loop cadence.

A newly created protection table receives an explicit `none`/expired baseline
row. A missing row in a previously initialized table is **not** interpreted as
permission to scan. Without a valid recovery marker, it is recorded as
`protection_state_unknown` with an effectively indefinite expiry. This includes
legacy installations with an existing but empty protection table. Operator
review and explicit restoration of trusted state are required; do not delete
recovery files or the database merely to resume scanning.

## Monotonic updates

Every SQLite update reads and merges the current row inside the same immediate
transaction as the write. Expiry never decreases, including during recovery.
Concurrent SQLite writers cannot perform an unprotected read/overwrite sequence.
In-memory pending state and recovery records use the same maximum-expiry rule.

For overlapping active reasons the precedence is:

1. Unknown/untrusted protection state
2. HTTP 403
3. HTTP 401 authentication
4. HTTP 429
5. HTTP 5xx
6. Timeout/network failure

Expiry always remains the latest required expiry, independent of which reason is
strongest. An expired reason does not outrank a new active reason.

## Limits and operational requirements

- **Use one scanner process per database/marker directory.** The SQLite
  transaction is safe against competing writers, but recovery-file ownership and
  in-flight abort signaling are process-local, not a distributed worker protocol.
- The database **and its sidecar directory must be on durable storage** that
  supports fsync and atomic rename. Process-restart tests do not establish that a
  particular hosting volume survives redeployment. No production storage settings
  were inspected or changed by this implementation.
- If marker writing fails, the code still attempts to save the cooldown in
  SQLite and blocks until reconciliation is possible. If **both independent
  writes fail**, the running process remains blocked, but no software can
  guarantee recovery of an unrecorded event after power loss. A corrupt or
  unreadable marker that remains on disk continues to block after restart.
- Abort signals cannot undo requests that Amazon has already received.
- Cooldowns discard blocked attempts rather than queuing requests for simultaneous
  release. Normal loop cadence resumes only when trusted protection state permits.

## Offline verification

The suite uses temporary SQLite databases and mocked Amazon/Telegram responses.
Recovery tests start fresh OS child processes to demonstrate failed-write recovery
and the commit-before-marker-removal crash boundary. Separate competing child
processes exercise the atomic SQLite update independently of sidecar ownership.
Deterministic response-boundary and body-reading races verify that rejected
payloads create no schedules or alerts.
