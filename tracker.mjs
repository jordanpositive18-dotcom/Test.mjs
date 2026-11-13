import { ProxyAgent, fetch } from 'undici';
import Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const AMAZON_GRAPHQL_URL = 'https://www.jobsatamazon.co.uk/graphql';

export const UK_HUB_SEARCH_TERMS = Object.freeze([
  'Dartford',
  'Tilbury',
  'London',
  'Dunfermline',
  'Doncaster',
  'Coventry',
  'Manchester',
  'Bristol',
  'Birmingham',
  'Glasgow',
  'Edinburgh',
  'Cardiff',
  'Belfast',
  'Leeds',
  'Newcastle',
  'Sheffield',
  'Southampton',
  'Liverpool',
  'Nottingham',
  'Leicester',
  'Peterborough',
  'Northampton',
  'Milton Keynes',
  'Swindon',
]);

export function createScanner(options = {}) {
const {
  fetchImpl = fetch,
  databasePath = 'database.sqlite',
  database: suppliedDatabase,
  dispatcher,
  auth = '',
  telegramToken,
  telegramChatId,
  telegramTransport,
  telegramTimeoutMs = 15000,
  notificationClaimLeaseMs = 60000,
  notificationBaseBackoffMs = 5000,
  notificationMaxBackoffMs = 60 * 60 * 1000,
  amazonPageSize = 20,
  amazonMaxConcurrentRequests = 3,
  amazonMinimumRequestIntervalMs = 4000,
  amazonRequestTimeoutMs = 15000,
  amazonMaxRequestAttempts = 3,
  amazonRetryBaseMs = 1000,
  amazonForbiddenCooldownMs = 15 * 60 * 1000,
  loopFailureRetryBaseMs = 1000,
  loopFailureRetryMaxMs = 60 * 1000,
  jobSearchTerms = UK_HUB_SEARCH_TERMS,
  maxCyclesPerLoop = Infinity,
  clock = () => Date.now(),
  monotonicClock = () => performance.now(),
  sleep = (milliseconds) => new Promise(resolveSleep => setTimeout(resolveSleep, milliseconds)),
  logger = console,
} = options;
if (!Number.isInteger(amazonMaxConcurrentRequests) || amazonMaxConcurrentRequests < 1) {
  throw new RangeError('amazonMaxConcurrentRequests must be a positive integer');
}
if (!Number.isInteger(amazonMinimumRequestIntervalMs) || amazonMinimumRequestIntervalMs < 0) {
  throw new RangeError('amazonMinimumRequestIntervalMs must be a non-negative integer');
}
if (
  !Array.isArray(jobSearchTerms)
  || jobSearchTerms.length === 0
  || jobSearchTerms.some((term) => typeof term !== 'string' || !term.trim())
) {
  throw new TypeError('jobSearchTerms must be a non-empty array of strings');
}
if (maxCyclesPerLoop !== Infinity
  && (!Number.isInteger(maxCyclesPerLoop) || maxCyclesPerLoop < 1)) {
  throw new RangeError('maxCyclesPerLoop must be a positive integer or Infinity');
}
const url = options.url ?? AMAZON_GRAPHQL_URL;
const uniqueJobSearchTerms = [...new Set(jobSearchTerms.map((term) => term.trim()))];
const TG_TOKEN = telegramToken;
const TG_CHAT_ID = telegramChatId;
const db = suppliedDatabase ?? new Database(databasePath);
const wait = (milliseconds) => sleep(milliseconds);
const nowIso = () => new Date(clock()).toISOString();
let amazonCooldownUntil = 0;
let activeAmazonRequests = 0;
const amazonRequestWaiters = [];
let nextAmazonRequestAt = 0;
let amazonPacingQueue = Promise.resolve();

async function waitForAmazonPacing() {
  const previousRequest = amazonPacingQueue;
  let releaseRequest;
  amazonPacingQueue = new Promise((resolveRequest) => {
    releaseRequest = resolveRequest;
  });
  await previousRequest;

  try {
    const delay = Math.max(0, nextAmazonRequestAt - clock());
    if (delay > 0) await wait(delay);
    nextAmazonRequestAt = Math.max(clock(), nextAmazonRequestAt)
      + amazonMinimumRequestIntervalMs;
  } finally {
    releaseRequest();
  }
}

class AmazonCooldownError extends Error {
  constructor(until) {
    super(`Amazon requests are paused until ${new Date(until).toISOString()}`);
    this.name = 'AmazonCooldownError';
    this.code = 'AMAZON_COOLDOWN';
    this.retryAt = until;
  }
}

function getRetryAfterMs(response, fallbackMs = 60 * 1000) {
  const value = response.headers?.get?.('retry-after')
    ?? response.headers?.['retry-after'];
  if (value === undefined || value === null || value === '') return fallbackMs;

  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);

  const retryAt = Date.parse(value);
  return Number.isFinite(retryAt)
    ? Math.max(0, retryAt - clock())
    : fallbackMs;
}

function applyAmazonCooldown(response) {
  const delay = response.status === 403
    ? Math.max(amazonForbiddenCooldownMs, getRetryAfterMs(response, 0))
    : getRetryAfterMs(response);
  amazonCooldownUntil = Math.max(amazonCooldownUntil, clock() + delay);
  persistAmazonCooldownStmt.run(
    'cooldown_until',
    new Date(amazonCooldownUntil).toISOString(),
  );
}

function withAmazonRequestSlot(operation) {
  return new Promise((resolveRequest, rejectRequest) => {
    const run = () => {
      activeAmazonRequests += 1;
      Promise.resolve()
        .then(operation)
        .then(resolveRequest, rejectRequest)
        .finally(() => {
          activeAmazonRequests -= 1;
          const next = amazonRequestWaiters.shift();
          if (next) next();
        });
    };

    if (activeAmazonRequests < amazonMaxConcurrentRequests) run();
    else amazonRequestWaiters.push(run);
  });
}

function fetchAmazonOnce(requestUrl, requestOptions, metrics) {
  return withAmazonRequestSlot(async () => {
    if (clock() < amazonCooldownUntil) {
      throw new AmazonCooldownError(amazonCooldownUntil);
    }
    await waitForAmazonPacing();
    if (clock() < amazonCooldownUntil) {
      throw new AmazonCooldownError(amazonCooldownUntil);
    }

    if (metrics) metrics.requestCount += 1;
    const signal = requestOptions.signal ?? AbortSignal.timeout(amazonRequestTimeoutMs);
    const response = await fetchImpl(requestUrl, {
      ...requestOptions,
      ...(dispatcher ? { dispatcher } : {}),
      signal,
    });
    if (response.status === 403 || response.status === 429) {
      applyAmazonCooldown(response);
      logger.warn(
        `[Amazon] HTTP ${response.status}; pausing all scanner requests until `
        + `${new Date(amazonCooldownUntil).toISOString()}.`,
      );
      try {
        await response.body?.cancel?.();
      } catch {
        // The cooldown is already recorded; cancellation is only connection cleanup.
      }
    }
    return response;
  });
}

async function fetchAmazon(requestUrl, requestOptions, metrics) {
  for (let attempt = 1; attempt <= amazonMaxRequestAttempts; attempt += 1) {
    let response;
    try {
      response = await fetchAmazonOnce(requestUrl, requestOptions, metrics);
    } catch (error) {
      if (error?.code === 'AMAZON_COOLDOWN' || attempt === amazonMaxRequestAttempts) {
        throw error;
      }
      await wait(amazonRetryBaseMs * (2 ** (attempt - 1)));
      continue;
    }

    if (response.status === 403 || response.status === 429) return response;
    if (response.status >= 500 && attempt < amazonMaxRequestAttempts) {
      try {
        await response.body?.cancel?.();
      } catch {
        // Best-effort cleanup before retrying the idempotent GraphQL query.
      }
      await wait(amazonRetryBaseMs * (2 ** (attempt - 1)));
      continue;
    }
    return response;
  }
}

// 1. Initialize Shared SQLite Database & Tables for Nationwide 3-Loop Architecture
db.exec(`
  CREATE TABLE IF NOT EXISTS seen_schedules (
    id TEXT PRIMARY KEY,
    discovered_at TEXT
  );

  CREATE TABLE IF NOT EXISTS job_queue (
    job_id TEXT PRIMARY KEY,
    region TEXT,
    status TEXT DEFAULT 'PENDING',
    discovered_at TEXT
  );

  CREATE TABLE IF NOT EXISTS failed_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_type TEXT,
    payload TEXT,
    error TEXT,
    retry_count INTEGER DEFAULT 0,
    last_tried TEXT
  );

  CREATE TABLE IF NOT EXISTS telegram_notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id TEXT NOT NULL UNIQUE,
    message TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
      CHECK (status IN ('PENDING', 'CLAIMED', 'SENT')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_attempt_at TEXT,
    claim_token TEXT,
    claim_expires_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    sent_at TEXT
  );

  CREATE INDEX IF NOT EXISTS telegram_notifications_ready_idx
    ON telegram_notifications(status, next_attempt_at, claim_expires_at);

  CREATE TABLE IF NOT EXISTS amazon_jobs (
    job_id TEXT PRIMARY KEY,
    title TEXT,
    location TEXT,
    raw_json TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS amazon_schedules (
    schedule_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    discovered_at TEXT NOT NULL
  );

  CREATE INDEX IF NOT EXISTS amazon_schedules_job_idx
    ON amazon_schedules(job_id, discovered_at);

  CREATE TABLE IF NOT EXISTS scan_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    loop_name TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    pages_fetched INTEGER NOT NULL DEFAULT 0,
    schedule_pages_fetched INTEGER NOT NULL DEFAULT 0,
    jobs_found INTEGER NOT NULL DEFAULT 0,
    request_count INTEGER NOT NULL DEFAULT 0,
    pagination_complete INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    time_since_previous_ms INTEGER,
    error TEXT,
    retry_at TEXT
  );

  CREATE INDEX IF NOT EXISTS scan_runs_loop_started_idx
    ON scan_runs(loop_name, started_at);

  CREATE TABLE IF NOT EXISTS loop_health (
    loop_name TEXT PRIMARY KEY,
    last_started_at TEXT,
    last_finished_at TEXT,
    last_status TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_pages_fetched INTEGER NOT NULL DEFAULT 0,
    last_jobs_found INTEGER NOT NULL DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS amazon_control (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
`);

const scanRunColumns = new Set(
  db.prepare('PRAGMA table_info(scan_runs)').all().map((column) => column.name),
);
for (const [column, definition] of [
  ['schedule_pages_fetched', 'INTEGER NOT NULL DEFAULT 0'],
  ['request_count', 'INTEGER NOT NULL DEFAULT 0'],
  ['pagination_complete', 'INTEGER NOT NULL DEFAULT 0'],
  ['duration_ms', 'INTEGER NOT NULL DEFAULT 0'],
  ['time_since_previous_ms', 'INTEGER'],
]) {
  if (!scanRunColumns.has(column)) {
    try {
      db.exec(`ALTER TABLE scan_runs ADD COLUMN ${column} ${definition}`);
    } catch (error) {
      if (!/duplicate column name/i.test(error.message)) throw error;
    }
  }
}

const persistedCooldown = db.prepare(
  "SELECT value FROM amazon_control WHERE key = 'cooldown_until'",
).get();
if (persistedCooldown) {
  const parsedCooldown = Date.parse(persistedCooldown.value);
  if (Number.isFinite(parsedCooldown)) amazonCooldownUntil = parsedCooldown;
}
const persistAmazonCooldownStmt = db.prepare(`
  INSERT INTO amazon_control (key, value) VALUES (?, ?)
  ON CONFLICT(key) DO UPDATE SET value = excluded.value
`);

const checkSeenStmt = db.prepare('SELECT id FROM seen_schedules WHERE id = ?');
const insertSeenStmt = db.prepare('INSERT INTO seen_schedules (id, discovered_at) VALUES (?, ?)');
const upsertAmazonJobStmt = db.prepare(`
  INSERT INTO amazon_jobs
    (job_id, title, location, raw_json, first_seen_at, last_seen_at)
  VALUES (?, ?, ?, ?, ?, ?)
  ON CONFLICT(job_id) DO UPDATE SET
    title = COALESCE(excluded.title, amazon_jobs.title),
    location = COALESCE(excluded.location, amazon_jobs.location),
    raw_json = excluded.raw_json,
    last_seen_at = excluded.last_seen_at
`);

const upsertJobStmt = db.prepare(`
  INSERT INTO job_queue (job_id, region, status, discovered_at) 
  VALUES (?, ?, 'PENDING', ?) 
  ON CONFLICT(job_id) DO UPDATE SET
    status = CASE WHEN job_queue.status IN ('PROCESSED', 'FAILED') THEN job_queue.status ELSE 'PENDING' END
`);

const markJobProcessedStmt = db.prepare("UPDATE job_queue SET status = 'PROCESSED' WHERE job_id = ?");
const markJobFailedStmt = db.prepare(
  "UPDATE job_queue SET status = 'FAILED' WHERE job_id = ? AND status != 'PROCESSED'",
);
const markJobPendingStmt = db.prepare(
  "UPDATE job_queue SET status = 'PENDING' WHERE job_id = ? AND status != 'PROCESSED'",
);
let loop2Running = false;
let loop1Running = false;
let loop3Running = false;

const insertFailedStmt = db.prepare('INSERT INTO failed_requests (task_type, payload, error, retry_count, last_tried) VALUES (?, ?, ?, ?, ?)');
const findFailedScheduleStmt = db.prepare(
  "SELECT * FROM failed_requests WHERE task_type = 'LOOP2_SCHEDULE' AND payload = ? ORDER BY id DESC LIMIT 1",
);
const updateFailedErrorStmt = db.prepare(
  'UPDATE failed_requests SET retry_count = retry_count + 1, last_tried = ?, error = ? WHERE id = ?',
);
const deleteFailedScheduleStmt = db.prepare(
  "DELETE FROM failed_requests WHERE task_type = 'LOOP2_SCHEDULE' AND payload = ?",
);
const getJobQueueStatusStmt = db.prepare('SELECT status FROM job_queue WHERE job_id = ?');
const insertScheduleStmt = db.prepare(`
  INSERT INTO amazon_schedules (schedule_id, job_id, raw_json, discovered_at)
  VALUES (?, ?, ?, ?)
  ON CONFLICT(schedule_id) DO UPDATE SET
    job_id = excluded.job_id,
    raw_json = excluded.raw_json
`);
const insertNotificationStmt = db.prepare(`
  INSERT OR IGNORE INTO telegram_notifications
    (schedule_id, message, status, attempt_count, next_attempt_at, created_at)
  VALUES (?, ?, 'PENDING', 0, ?, ?)
`);
const findClaimableNotificationStmt = db.prepare(`
  SELECT *
  FROM telegram_notifications
  WHERE (status = 'PENDING' AND next_attempt_at <= ?)
     OR (status = 'CLAIMED' AND claim_expires_at <= ?)
  ORDER BY next_attempt_at, id
  LIMIT 1
`);
const hasClaimableNotificationStmt = db.prepare(`
  SELECT EXISTS (
    SELECT 1
    FROM telegram_notifications
    WHERE (status = 'PENDING' AND next_attempt_at <= ?)
       OR (status = 'CLAIMED' AND claim_expires_at <= ?)
  ) AS has_claimable
`);
const claimNotificationStmt = db.prepare(`
  UPDATE telegram_notifications
  SET status = 'CLAIMED',
      attempt_count = attempt_count + 1,
      last_attempt_at = ?,
      claim_token = ?,
      claim_expires_at = ?
  WHERE id = ?
    AND (
      (status = 'PENDING' AND next_attempt_at <= ?)
      OR (status = 'CLAIMED' AND claim_expires_at <= ?)
    )
`);
const markNotificationSentStmt = db.prepare(`
  UPDATE telegram_notifications
  SET status = 'SENT',
      sent_at = ?,
      last_error = NULL,
      claim_token = NULL,
      claim_expires_at = NULL
  WHERE id = ? AND status = 'CLAIMED' AND claim_token = ?
`);
const markNotificationPendingStmt = db.prepare(`
  UPDATE telegram_notifications
  SET status = 'PENDING',
      next_attempt_at = ?,
      last_error = ?,
      claim_token = NULL,
      claim_expires_at = NULL
  WHERE id = ? AND status = 'CLAIMED' AND claim_token = ?
`);
const saveDiscoveredJob = db.transaction((job, discoveredAt) => {
  if (!job?.jobId) return false;
  const serialized = JSON.stringify(job);
  upsertAmazonJobStmt.run(
    job.jobId,
    job.jobTitle ?? null,
    job.locationName ?? job.city ?? null,
    serialized,
    discoveredAt,
    discoveredAt,
  );
  upsertJobStmt.run(job.jobId, 'GB', discoveredAt);
  return true;
});
const saveScheduleAndNotification = db.transaction((schedule, jobId, message, discoveredAt) => {
  if (!schedule?.scheduleId) return false;
  insertScheduleStmt.run(
    schedule.scheduleId,
    jobId,
    JSON.stringify(schedule),
    discoveredAt,
  );

  if (checkSeenStmt.get(schedule.scheduleId)) return false;

  insertSeenStmt.run(schedule.scheduleId, discoveredAt);
  const inserted = insertNotificationStmt.run(
    schedule.scheduleId,
    message,
    discoveredAt,
    discoveredAt,
  );
  return inserted.changes === 1;
});
const beginScanRunStmt = db.prepare(`
  INSERT INTO scan_runs (loop_name, started_at, status)
  VALUES (?, ?, 'RUNNING')
`);
const markLoopStartedStmt = db.prepare(`
  INSERT INTO loop_health (loop_name, last_started_at, last_status)
  VALUES (?, ?, 'RUNNING')
  ON CONFLICT(loop_name) DO UPDATE SET
    last_started_at = excluded.last_started_at,
    last_status = 'RUNNING',
    last_error = NULL
`);
const finishScanRunStmt = db.prepare(`
  UPDATE scan_runs
  SET finished_at = ?, status = ?, pages_fetched = ?, schedule_pages_fetched = ?,
      jobs_found = ?, request_count = ?, pagination_complete = ?, duration_ms = ?,
      time_since_previous_ms = ?, error = ?, retry_at = ?
  WHERE id = ?
`);
const updateLoopHealthStmt = db.prepare(`
  UPDATE loop_health
  SET last_finished_at = ?, last_status = ?,
      consecutive_failures = CASE WHEN ? = 'COMPLETE' THEN 0 ELSE consecutive_failures + 1 END,
      last_error = ?, last_pages_fetched = ?, last_jobs_found = ?
  WHERE loop_name = ?
`);
const getLoopHealthStmt = db.prepare('SELECT * FROM loop_health ORDER BY loop_name');
const getScanRunsStmt = db.prepare('SELECT * FROM scan_runs ORDER BY id DESC LIMIT ?');
const getPreviousCompletedScanStmt = db.prepare(`
  SELECT finished_at
  FROM scan_runs
  WHERE loop_name = ? AND status = 'COMPLETE' AND finished_at IS NOT NULL
  ORDER BY id DESC
  LIMIT 1
`);
const claimNotificationTransaction = db.transaction((now, claimExpiresAt, claimToken) => {
  const notification = findClaimableNotificationStmt.get(now, now);
  if (!notification) return null;

  const claimed = claimNotificationStmt.run(
    now,
    claimToken,
    claimExpiresAt,
    notification.id,
    now,
    now,
  );
  if (claimed.changes !== 1) return null;

  return {
    ...notification,
    claim_token: claimToken,
    attempt_count: notification.attempt_count + 1,
  };
});

function claimNextNotification(now = nowIso()) {
  const claimToken = randomUUID();
  const claimExpiresAt = new Date(
    clock() + notificationClaimLeaseMs,
  ).toISOString();
  return claimNotificationTransaction.immediate(now, claimExpiresAt, claimToken);
}

// 3. Telegram Message Formatter matching exact user layout
function formatShiftAlert(shift) {
  return `1️⃣ 📍 ${shift.location}
📮 Postcode: ${shift.postcode}
💰 Pay: £${shift.pay} /hr
📅 Start Date: ${shift.startDate}
📆 Working Days: ${shift.workingDays}
⏰ Shift Time: ${shift.shiftTime}
🕐 Hours/Week: ${shift.hoursPerWeek}`;
}

async function requestTelegram(message, { timeout = true } = {}) {
  const signal = timeout ? AbortSignal.timeout(telegramTimeoutMs) : undefined;
  if (telegramTransport) {
    return telegramTransport(message, { signal });
  }
  if (!TG_TOKEN || !TG_CHAT_ID) {
    throw new Error('Telegram bot token and chat ID are not configured');
  }

  const tgUrl = `https://api.telegram.org/bot${TG_TOKEN}/sendMessage`;
  return fetchImpl(tgUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: TG_CHAT_ID, text: message }),
    ...(signal ? { signal } : {}),
  });
}

async function sendTelegramAlert(message) {
  if (!TG_TOKEN || !TG_CHAT_ID) {
    logger.log('📱 [Telegram Mock Alert]:\n', message);
    return;
  }
  try {
    const res = await requestTelegram(message, { timeout: false });
    if (!res.ok) {
      logger.error(`⚠️ Telegram API returned status ${res.status}`);
    }
  } catch (err) {
    logger.error('⚠️ Failed to send Telegram notification:', err.message);
  }
}

function getNotificationBackoffMs(attemptCount) {
  const exponent = Math.min(Math.max(attemptCount - 1, 0), 30);
  return Math.min(
    notificationBaseBackoffMs * (2 ** exponent),
    notificationMaxBackoffMs,
  );
}

async function deliverPendingNotifications() {
  let sentCount = 0;
  let failedCount = 0;
  const batchSize = 10;

  for (let index = 0; index < batchSize; index += 1) {
    const notification = claimNextNotification();
    if (!notification) break;

    try {
      const response = await requestTelegram(notification.message);
      let telegramResult;
      try {
        telegramResult = await response.json();
      } catch (error) {
        if (!response.ok) {
          throw new Error(`Telegram returned HTTP ${response.status}`);
        }
        throw new Error(`Telegram returned invalid JSON: ${error.message}`);
      }

      if (!response.ok) {
        throw new Error(`Telegram returned HTTP ${response.status}`);
      }
      if (telegramResult?.ok !== true) {
        throw new Error(
          `Telegram rejected the message: ${telegramResult?.description || 'ok was not true'}`,
        );
      }

      const sentAt = nowIso();
      const markedSent = markNotificationSentStmt.run(
        sentAt,
        notification.id,
        notification.claim_token,
      );
      if (markedSent.changes === 1) {
        sentCount += 1;
      } else {
        logger.warn(`⚠️ Notification ${notification.id} was delivered after its claim expired.`);
      }
    } catch (error) {
      const errorMessage = String(error?.message ?? error).slice(0, 2000);
      const retryAt = new Date(
        clock() + getNotificationBackoffMs(notification.attempt_count),
      ).toISOString();
      const markedPending = markNotificationPendingStmt.run(
        retryAt,
        errorMessage,
        notification.id,
        notification.claim_token,
      );
      if (markedPending.changes === 1) {
        failedCount += 1;
        logger.error(`⚠️ Notification ${notification.id} will retry: ${errorMessage}`);
      }
    }
  }

  return { sentCount, failedCount };
}

let notificationDeliveryRunning = false;
let notificationDeliveryPromise = Promise.resolve();

function kickNotificationDelivery() {
  if (notificationDeliveryRunning) return;
  notificationDeliveryRunning = true;
  notificationDeliveryPromise = deliverPendingNotifications()
    .catch((error) => logger.error('⚠️ Notification delivery pass failed:', error.message))
    .finally(() => {
      notificationDeliveryRunning = false;
      const now = nowIso();
      if (hasClaimableNotificationStmt.get(now, now).has_claimable) {
        kickNotificationDelivery();
      }
    });
}

async function waitForNotificationDelivery() {
  while (notificationDeliveryRunning) {
    await notificationDeliveryPromise;
  }
}

// Common headers for Amazon GraphQL
const amazonHeaders = {
  'Content-Type': 'application/json',
  'Authorization': auth,
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Origin': 'https://www.jobsatamazon.co.uk',
  'Referer': 'https://www.jobsatamazon.co.uk/',
};

// Input naming follows the supplied hub-search source. Its example has no
// paginated response: retain the existing connection selection, but verify this
// request/response contract against an authorized successful capture before release.
const JOB_SEARCH_QUERY = `
  query searchJobCardsByLocation($request: SearchJobRequest!) {
    searchJobCardsByLocation(request: $request) {
      nextToken
      jobCards {
        jobId
        jobTitle
        jobType
        employmentType
        city
        state
        postalCode
        locationName
        totalPayRateMin
        totalPayRateMax
        scheduleCount
      }
    }
  }
`;

const SCHEDULE_SEARCH_QUERY = `
  query searchScheduleCards($searchScheduleRequest: SearchScheduleRequest!) {
    searchScheduleCards(searchScheduleRequest: $searchScheduleRequest) {
      nextToken
      scheduleCards {
        scheduleId
        hireStartDate
        basePay
        totalPayRate
        hoursPerWeek
        address
        city
        state
        postalCode
        scheduleType
        scheduleText
        employmentType
        currencyCode
      }
    }
  }
`;

function getGraphQLErrorMessage(payload) {
  const errors = Array.isArray(payload?.errors) ? payload.errors : [];
  return errors
    .map((error) => {
      const details = [String(error?.message ?? 'Unknown GraphQL error').slice(0, 500)];
      const code = error?.extensions?.code ?? error?.extensions?.errorType;
      if (typeof code === 'string' && code.length <= 120) details.push(`code=${code}`);
      if (Array.isArray(error?.path)) {
        details.push(`path=${error.path.map((part) => String(part).slice(0, 80)).join('.')}`);
      }
      if (Array.isArray(error?.locations)) {
        const locations = error.locations
          .slice(0, 4)
          .map((location) => `${Number(location?.line) || '?'}:${Number(location?.column) || '?'}`)
          .join(',');
        if (locations) details.push(`locations=${locations}`);
      }
      return details.join(' ');
    })
    .join('; ');
}

function createAmazonHttpError(response, operation) {
  const error = new Error(`Amazon ${operation} returned HTTP ${response.status}`);
  error.status = response.status;
  error.retryAt = amazonCooldownUntil > clock() ? amazonCooldownUntil : null;
  return error;
}

function startScanRun(loopName) {
  const startedAt = nowIso();
  const result = beginScanRunStmt.run(loopName, startedAt);
  markLoopStartedStmt.run(loopName, startedAt);
  return result.lastInsertRowid;
}

function finishScanRun(runId, loopName, {
  jobPagesFetched,
  schedulePagesFetched,
  jobsFound,
  requestCount,
  paginationComplete,
  durationMs,
  error,
}) {
  const finishedAt = nowIso();
  const errorMessage = error
    ? String(error?.message ?? error).slice(0, 2000)
    : null;
  const blocked = error?.status === 403
    || error?.status === 429
    || error?.code === 'AMAZON_COOLDOWN';
  const status = !error && paginationComplete
    ? 'COMPLETE'
    : blocked && jobPagesFetched === 0 && schedulePagesFetched === 0
      ? 'BLOCKED'
      : jobPagesFetched > 0 || schedulePagesFetched > 0
        ? 'PARTIAL'
        : 'FAILED';
  const retryTimestamp = amazonCooldownUntil > clock()
    ? amazonCooldownUntil
    : Number.isFinite(error?.retryAt)
      ? error.retryAt
      : null;
  const retryAt = retryTimestamp === null
    ? null
    : new Date(retryTimestamp).toISOString();
  const previousCompletedScan = status === 'COMPLETE'
    ? getPreviousCompletedScanStmt.get(loopName)
    : null;
  const previousCompletedAt = Date.parse(previousCompletedScan?.finished_at ?? '');
  const timeSincePreviousMs = Number.isFinite(previousCompletedAt)
    ? Math.max(0, Date.parse(finishedAt) - previousCompletedAt)
    : null;

  const finish = db.transaction(() => {
    finishScanRunStmt.run(
      finishedAt,
      status,
      jobPagesFetched,
      schedulePagesFetched,
      jobsFound,
      requestCount,
      paginationComplete ? 1 : 0,
      durationMs,
      timeSincePreviousMs,
      errorMessage,
      retryAt,
      runId,
    );
    updateLoopHealthStmt.run(
      finishedAt,
      status,
      status,
      errorMessage,
      jobPagesFetched + schedulePagesFetched,
      jobsFound,
      loopName,
    );
  });
  finish.immediate();
  const result = {
    status,
    pagesFetched: jobPagesFetched,
    jobPagesFetched,
    schedulePagesFetched,
    jobsFound,
    requestCount,
    paginationComplete,
    durationMs,
    timeSincePreviousMs,
    error: errorMessage,
    retryAt,
  };
  logger.log(
    `[${loopName}] scan=${status} duration=${durationMs}ms requests=${requestCount} `
    + `pages=jobs:${jobPagesFetched},schedules:${schedulePagesFetched} `
    + `pagination=${paginationComplete ? 'complete' : 'incomplete'} jobs=${jobsFound} `
    + `sincePreviousComplete=${timeSincePreviousMs === null ? 'n/a' : `${timeSincePreviousMs}ms`}`,
  );
  return result;
}

async function runNationwideScan(loopName) {
  const startedAt = monotonicClock();
  const runId = startScanRun(loopName);
  const metrics = {
    requestCount: 0,
    jobPagesFetched: 0,
    schedulePagesFetched: 0,
  };
  const foundJobs = new Map();
  let error = null;
  let jobPaginationComplete = false;
  let schedulePaginationComplete = true;

  try {
    for (const searchTerm of uniqueJobSearchTerms) {
      const seenTokens = new Set();
      let nextToken = null;
      while (true) {
        const request = {
          country: 'GB',
          locale: 'en_GB',
          query: searchTerm,
          ...(nextToken ? { nextToken } : {}),
        };
        const response = await fetchAmazon(url, {
          method: 'POST',
          headers: amazonHeaders,
          body: JSON.stringify({
            operationName: 'searchJobCardsByLocation',
            query: JOB_SEARCH_QUERY,
            variables: { request },
          }),
        }, metrics);

        if (response.status !== 200) {
          throw createAmazonHttpError(response, `job search for "${searchTerm}"`);
        }
        const payload = await response.json();
        const graphQLError = getGraphQLErrorMessage(payload);
        if (graphQLError) {
          throw new Error(`Amazon job search GraphQL error for "${searchTerm}": ${graphQLError}`);
        }

        const result = payload?.data?.searchJobCardsByLocation;
        const jobCards = Array.isArray(result)
          ? result
          : result?.jobCards;
        if (!Array.isArray(jobCards)) {
          throw new Error(`Amazon job search for "${searchTerm}" did not contain a jobCards list`);
        }

        metrics.jobPagesFetched += 1;
        for (const job of jobCards) {
          if (!job?.jobId) continue;
          saveDiscoveredJob.immediate(job, nowIso());
          foundJobs.set(job.jobId, job);
        }

        const pageToken = Array.isArray(result) ? null : result?.nextToken;
        if (!pageToken) break;
        if (seenTokens.has(pageToken)) {
          throw new Error(`Amazon job search returned a repeated pagination token for "${searchTerm}"`);
        }
        seenTokens.add(pageToken);
        nextToken = pageToken;
      }
    }
    jobPaginationComplete = true;
  } catch (scanError) {
    error = scanError;
    logger.error(`❌ [${loopName}] Nationwide scan did not finish:`, scanError.message);
  }

  for (const job of foundJobs.values()) {
    try {
      await retrieveSchedulesForJob(job, metrics);
      markJobProcessedAndClearFailures(job.jobId);
    } catch (scheduleError) {
      schedulePaginationComplete = false;
      if (scheduleError?.code === 'SCHEDULE_RETRY_DEFERRED') {
        markJobPendingStmt.run(job.jobId);
      } else if (
        scheduleError?.code === 'AMAZON_COOLDOWN'
        || scheduleError?.status === 403
        || scheduleError?.status === 429
      ) {
        markJobPendingStmt.run(job.jobId);
      } else {
        recordScheduleFailure(job.jobId, scheduleError);
      }
      if (!error) error = scheduleError;
      logger.warn(`⚠️ [${loopName}] Schedule retrieval for ${job.jobId} did not finish: ${scheduleError.message}`);
      if (
        scheduleError?.code === 'AMAZON_COOLDOWN'
        || scheduleError?.status === 403
        || scheduleError?.status === 429
      ) break;
    }
  }

  const durationMs = Math.max(0, Math.round(monotonicClock() - startedAt));
  const paginationComplete = jobPaginationComplete && schedulePaginationComplete;
  return finishScanRun(runId, loopName, {
    jobPagesFetched: metrics.jobPagesFetched,
    schedulePagesFetched: metrics.schedulePagesFetched,
    jobsFound: foundJobs.size,
    requestCount: metrics.requestCount,
    paginationComplete,
    durationMs,
    error,
  });
}

async function runLoop1Discovery() {
  return runSingleLoopPass('loop1', () => loop1Running, (value) => { loop1Running = value; });
}

// ----------------------------------------------------
// LOOP 2: Schedule Processing & Detailed Retrieval
// ----------------------------------------------------
function recordScheduleFailure(jobId, failure) {
  const payload = JSON.stringify({ jobId });
  const errorMessage = String(failure?.message ?? failure).slice(0, 2000);
  const triedAt = nowIso();
  const saveFailure = db.transaction(() => {
    if (getJobQueueStatusStmt.get(jobId)?.status === 'PROCESSED') return false;
    markJobFailedStmt.run(jobId);
    const existing = findFailedScheduleStmt.get(payload);
    if (existing) {
      updateFailedErrorStmt.run(triedAt, errorMessage, existing.id);
    } else {
      insertFailedStmt.run('LOOP2_SCHEDULE', payload, errorMessage, 0, triedAt);
    }
    return true;
  });
  return saveFailure.immediate();
}

function markJobProcessedAndClearFailures(jobId) {
  const markComplete = db.transaction(() => {
    markJobProcessedStmt.run(jobId);
    deleteFailedScheduleStmt.run(JSON.stringify({ jobId }));
  });
  markComplete.immediate();
}

async function runSingleLoopPass(loopName, isRunning, setRunning) {
  if (isRunning()) {
    logger.log(`[${loopName}] Skipping this pass because its previous pass is still running.`);
    return { status: 'SKIPPED', pagesFetched: 0, jobsFound: 0 };
  }

  setRunning(true);
  try {
    logger.log(`[${loopName}] Starting a UK hub-term scan across ${uniqueJobSearchTerms.length} terms.`);
    const result = await runNationwideScan(loopName);
    kickNotificationDelivery();
    return result;
  } catch (error) {
    logger.error(`[${loopName}] Fatal scan error:`, error.message);
    return { status: 'FAILED', pagesFetched: 0, jobsFound: 0, error: error.message };
  } finally {
    setRunning(false);
  }
}

function checkScheduleRetryWindow(jobId) {
  const task = findFailedScheduleStmt.get(JSON.stringify({ jobId }));
  if (!task) return;
  const lastTriedAt = Date.parse(task.last_tried || '');
  const retryDelay = Math.min(
    1000 * (2 ** Math.min(task.retry_count, 10)),
    loopFailureRetryMaxMs,
  );
  if (!Number.isFinite(lastTriedAt) || clock() - lastTriedAt >= retryDelay) return;

  const retryAt = lastTriedAt + retryDelay;
  const error = new Error(`Schedule retry for job ${jobId} is deferred until ${new Date(retryAt).toISOString()}`);
  error.code = 'SCHEDULE_RETRY_DEFERRED';
  error.retryAt = retryAt;
  throw error;
}

async function retrieveSchedulesForJob(job, metrics) {
  const jobId = job.jobId ?? job.job_id;
  checkScheduleRetryWindow(jobId);
  let nextToken = null;
  const seenTokens = new Set();

  while (true) {
    const searchScheduleRequest = {
      country: 'GB',
      locale: 'en_GB',
      jobId,
      keyWords: '',
      pageSize: amazonPageSize,
      equalFilters: [],
      containFilters: [],
      rangeFilters: [],
      orFilters: [],
      dateFilters: [],
      excludeFilters: [],
      consolidateSchedule: true,
      ...(nextToken ? { nextToken } : {}),
    };
    const response = await fetchAmazon(url, {
      method: 'POST',
      headers: amazonHeaders,
      body: JSON.stringify({
        operationName: 'searchScheduleCards',
        query: SCHEDULE_SEARCH_QUERY,
        variables: { searchScheduleRequest },
      }),
    }, metrics);

    if (response.status !== 200) {
      throw createAmazonHttpError(response, `schedule search for job ${jobId}`);
    }
    const payload = await response.json();
    const graphQLError = getGraphQLErrorMessage(payload);
    if (graphQLError) {
      throw new Error(`Amazon schedule search GraphQL error: ${graphQLError}`);
    }

    const result = payload?.data?.searchScheduleCards;
    const scheduleCards = Array.isArray(result)
      ? result
      : result?.scheduleCards;
    if (!Array.isArray(scheduleCards)) {
      throw new Error(`Amazon schedule response for job ${jobId} did not contain a scheduleCards list`);
    }

    metrics.schedulePagesFetched += 1;
    for (const schedule of scheduleCards) {
      if (!schedule?.scheduleId) {
        throw new Error(`Amazon returned a schedule without scheduleId for job ${jobId}`);
      }

      const shiftInfo = {
        location: schedule.location
          || schedule.locationName
          || schedule.address
          || schedule.city
          || `UK Location (${job.region || 'GB'})`,
        postcode: schedule.postcode || schedule.postalCode || 'N/A',
        pay: schedule.basePay || schedule.totalPayRate || '16.10',
        startDate: schedule.hireStartDate || schedule.firstDayOnSite || 'Upcoming',
        workingDays: schedule.workingDays
          || schedule.scheduleType
          || schedule.scheduleText
          || 'Flexible',
        shiftTime: `${schedule.startTime || '07:00'} - ${schedule.finishTime || '15:30'}`,
        hoursPerWeek: schedule.hoursPerWeek || '30',
      };
      const enqueued = saveScheduleAndNotification.immediate(
        schedule,
        jobId,
        formatShiftAlert(shiftInfo),
        nowIso(),
      );
      if (enqueued) {
        logger.log(`🚨 [${jobId}] New schedule discovered & queued for alert! ID: ${schedule.scheduleId}`);
      }
    }

    const pageToken = Array.isArray(result) ? null : result?.nextToken;
    if (!pageToken) return;
    if (seenTokens.has(pageToken)) {
      throw new Error(`Amazon returned a repeated schedule pagination token for job ${jobId}`);
    }
    seenTokens.add(pageToken);
    nextToken = pageToken;
  }
}

async function runLoop2Schedules() {
  return runSingleLoopPass('loop2', () => loop2Running, (value) => { loop2Running = value; });
}

async function runLoop3Recovery() {
  return runSingleLoopPass('loop3', () => loop3Running, (value) => { loop3Running = value; });
}

let stopRequested = false;
let loopsStarted = false;
let continuousLoopPromises = [];
const loopFailureCounts = new Map();

async function runContinuousLoop(loopName, runPass) {
  let cycles = 0;
  while (!stopRequested && cycles < maxCyclesPerLoop) {
    const result = await runPass();
    cycles += 1;
    if (stopRequested || cycles >= maxCyclesPerLoop) break;

    if (result.status === 'COMPLETE') {
      loopFailureCounts.set(loopName, 0);
      continue;
    }

    if (clock() < amazonCooldownUntil) {
      await wait(Math.max(0, amazonCooldownUntil - clock()));
      continue;
    }

    const failureCount = (loopFailureCounts.get(loopName) ?? 0) + 1;
    loopFailureCounts.set(loopName, failureCount);
    const retryDelay = Math.min(
      loopFailureRetryBaseMs * (2 ** Math.min(failureCount - 1, 10)),
      loopFailureRetryMaxMs,
    );
    await wait(retryDelay);
  }
}

function start() {
  if (loopsStarted) return { status: 'ALREADY_RUNNING', loops: 3 };
  logger.log(
    `🚀 Starting exactly three simultaneous UK hub-term scan loops; `
    + `maximum Amazon request concurrency=${amazonMaxConcurrentRequests}; `
    + `minimum shared request spacing=${amazonMinimumRequestIntervalMs}ms.`,
  );
  stopRequested = false;
  loopsStarted = true;
  continuousLoopPromises = [
    ['loop1', runLoop1Discovery],
    ['loop2', runLoop2Schedules],
    ['loop3', runLoop3Recovery],
  ].map(([loopName, runPass]) => runContinuousLoop(loopName, runPass));

  void sendTelegramAlert(
    '🚀 *UK Amazon Shift Tracker Online!*\nThree simultaneous hub-term scan loops initiated.',
  ).catch((error) => logger.error('⚠️ Startup Telegram alert failed:', error.message));
  return { status: 'STARTED', loops: 3 };
}

async function stop() {
  stopRequested = true;
  await Promise.all(continuousLoopPromises);
  loopsStarted = false;
}

return {
  db,
  runLoop1Discovery,
  runLoop2Schedules,
  runLoop3Recovery,
  runNotificationDelivery: deliverPendingNotifications,
  getLoopHealth: () => getLoopHealthStmt.all(),
  getScanRuns: (limit = 50) => getScanRunsStmt.all(limit),
  getRequestConcurrency: () => amazonMaxConcurrentRequests,
  sendTelegramAlert,
  start,
  stop,
  waitForLoops: () => Promise.all(continuousLoopPromises),
  waitForNotificationDelivery,
  close: () => db.close(),
};
}

const isDirectExecution = process.argv[1]
  && import.meta.url === pathToFileURL(resolve(process.argv[1])).href;

if (isDirectExecution) {
  let auth = process.env.AMAZON_AUTHORIZATION;
  let proxyUrl = process.env.AMAZON_PROXY_URL;

  if (!auth || !proxyUrl) {
    console.error('❌ Missing required environment variables (AMAZON_AUTHORIZATION or AMAZON_PROXY_URL)');
    process.exit(1);
  }

  proxyUrl = proxyUrl.replace(/["']/g, '').trim();
  const spaceIdx = proxyUrl.indexOf(' ');
  if (spaceIdx !== -1) {
    proxyUrl = proxyUrl.substring(0, spaceIdx);
  }

  if (!auth.startsWith('Bearer ') && !auth.startsWith('Basic ')) {
    auth = `Bearer ${auth}`;
  }

  const scanner = createScanner({
    auth,
    dispatcher: new ProxyAgent(proxyUrl),
    telegramToken: process.env.TELEGRAM_BOT_TOKEN,
    telegramChatId: process.env.TELEGRAM_CHAT_ID,
  });
  await scanner.start();
}
