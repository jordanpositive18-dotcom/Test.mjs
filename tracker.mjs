import { ProxyAgent, fetch } from 'undici';
import Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const AMAZON_GRAPHQL_URL = 'https://www.jobsatamazon.co.uk/graphql';

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
  amazonRequestIntervalMs = 2000,
  amazonRequestTimeoutMs = 15000,
  amazonMaxRequestAttempts = 3,
  amazonRetryBaseMs = 1000,
  amazonForbiddenCooldownMs = 15 * 60 * 1000,
  clock = () => Date.now(),
  sleep = (milliseconds) => new Promise(resolveSleep => setTimeout(resolveSleep, milliseconds)),
  setIntervalImpl = setInterval,
  setTimeoutImpl = setTimeout,
  logger = console,
} = options;
const url = options.url ?? AMAZON_GRAPHQL_URL;
const TG_TOKEN = telegramToken;
const TG_CHAT_ID = telegramChatId;
const db = suppliedDatabase ?? new Database(databasePath);
const wait = (milliseconds) => sleep(milliseconds);
const nowIso = () => new Date(clock()).toISOString();
let lastAmazonRequestAt = null;
let amazonCooldownUntil = 0;
let amazonRequestQueue = Promise.resolve();

class AmazonCooldownError extends Error {
  constructor(until) {
    super(`Amazon requests are paused until ${new Date(until).toISOString()}`);
    this.name = 'AmazonCooldownError';
    this.code = 'AMAZON_COOLDOWN';
    this.retryAt = until;
  }
}

function getRetryAfterMs(response) {
  const value = response.headers?.get?.('retry-after')
    ?? response.headers?.['retry-after'];
  if (value === undefined || value === null || value === '') return 60 * 1000;

  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);

  const retryAt = Date.parse(value);
  return Number.isFinite(retryAt)
    ? Math.max(0, retryAt - clock())
    : 60 * 1000;
}

function applyAmazonCooldown(response) {
  const delay = response.status === 403
    ? amazonForbiddenCooldownMs
    : getRetryAfterMs(response);
  amazonCooldownUntil = Math.max(amazonCooldownUntil, clock() + delay);
  persistAmazonCooldownStmt.run(
    'cooldown_until',
    new Date(amazonCooldownUntil).toISOString(),
  );
}

function fetchAmazonOnce(requestUrl, requestOptions) {
  const request = amazonRequestQueue.then(async () => {
    if (clock() < amazonCooldownUntil) {
      throw new AmazonCooldownError(amazonCooldownUntil);
    }

    if (lastAmazonRequestAt !== null) {
      const pacingDelay = Math.max(
        0,
        lastAmazonRequestAt + amazonRequestIntervalMs - clock(),
      );
      if (pacingDelay > 0) await wait(pacingDelay);
    }

    if (clock() < amazonCooldownUntil) {
      throw new AmazonCooldownError(amazonCooldownUntil);
    }

    lastAmazonRequestAt = clock();
    const signal = requestOptions.signal ?? AbortSignal.timeout(amazonRequestTimeoutMs);
    return fetchImpl(requestUrl, {
      ...requestOptions,
      ...(dispatcher ? { dispatcher } : {}),
      signal,
    });
  });
  amazonRequestQueue = request.then(() => undefined, () => undefined);
  return request;
}

async function fetchAmazon(requestUrl, requestOptions) {
  for (let attempt = 1; attempt <= amazonMaxRequestAttempts; attempt += 1) {
    let response;
    try {
      response = await fetchAmazonOnce(requestUrl, requestOptions);
    } catch (error) {
      if (error?.code === 'AMAZON_COOLDOWN' || attempt === amazonMaxRequestAttempts) {
        throw error;
      }
      await wait(amazonRetryBaseMs * (2 ** (attempt - 1)));
      continue;
    }

    if (response.status === 403 || response.status === 429) {
      applyAmazonCooldown(response);
      return response;
    }
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
    jobs_found INTEGER NOT NULL DEFAULT 0,
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

const getPendingJobsStmt = db.prepare("SELECT job_id, region FROM job_queue WHERE status = 'PENDING' LIMIT 10");
const markJobProcessedStmt = db.prepare("UPDATE job_queue SET status = 'PROCESSED' WHERE job_id = ?");
const markJobFailedStmt = db.prepare("UPDATE job_queue SET status = 'FAILED' WHERE job_id = ?");
const markJobPendingStmt = db.prepare("UPDATE job_queue SET status = 'PENDING' WHERE job_id = ?");
let loop2Running = false;
let loop1Running = false;
let loop3Running = false;

const insertFailedStmt = db.prepare('INSERT INTO failed_requests (task_type, payload, error, retry_count, last_tried) VALUES (?, ?, ?, ?, ?)');
const findFailedScheduleStmt = db.prepare(
  "SELECT * FROM failed_requests WHERE task_type = 'LOOP2_SCHEDULE' AND payload = ? ORDER BY id DESC LIMIT 1",
);
const getFailedStmt = db.prepare(
  "SELECT * FROM failed_requests WHERE task_type = 'LOOP2_SCHEDULE' ORDER BY id",
);
const updateFailedStmt = db.prepare(
  'UPDATE failed_requests SET retry_count = retry_count + 1, last_tried = ? WHERE id = ?',
);
const updateFailedErrorStmt = db.prepare(
  'UPDATE failed_requests SET retry_count = retry_count + 1, last_tried = ?, error = ? WHERE id = ?',
);
const deleteFailedStmt = db.prepare('DELETE FROM failed_requests WHERE id = ?');
const deleteFailedScheduleStmt = db.prepare(
  "DELETE FROM failed_requests WHERE task_type = 'LOOP2_SCHEDULE' AND payload = ?",
);
const getJobQueueStatusStmt = db.prepare('SELECT status FROM job_queue WHERE job_id = ?');
const requeueFailedJobStmt = db.prepare(`
  INSERT INTO job_queue (job_id, region, status, discovered_at)
  VALUES (?, 'GB', 'PENDING', ?)
  ON CONFLICT(job_id) DO UPDATE SET
    status = CASE WHEN job_queue.status = 'PROCESSED' THEN 'PROCESSED' ELSE 'PENDING' END
`);
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
  SET finished_at = ?, status = ?, pages_fetched = ?, jobs_found = ?,
      error = ?, retry_at = ?
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

// Common headers for Amazon GraphQL
const amazonHeaders = {
  'Content-Type': 'application/json',
  'Authorization': auth,
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Origin': 'https://www.jobsatamazon.co.uk',
  'Referer': 'https://www.jobsatamazon.co.uk/',
};

const JOB_SEARCH_QUERY = `
  query searchJobCardsByLocation($searchJobRequest: SearchJobRequest!) {
    searchJobCardsByLocation(searchJobRequest: $searchJobRequest) {
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
    .map((error) => error?.message)
    .filter(Boolean)
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

function finishScanRun(runId, loopName, { pagesFetched, jobsFound, error }) {
  const finishedAt = nowIso();
  const errorMessage = error
    ? String(error?.message ?? error).slice(0, 2000)
    : null;
  const blocked = error?.status === 403
    || error?.status === 429
    || error?.code === 'AMAZON_COOLDOWN';
  const status = !error
    ? 'COMPLETE'
    : blocked && pagesFetched === 0
      ? 'BLOCKED'
      : pagesFetched > 0
        ? 'PARTIAL'
        : 'FAILED';
  const retryAt = amazonCooldownUntil > clock()
    ? new Date(amazonCooldownUntil).toISOString()
    : null;

  const finish = db.transaction(() => {
    finishScanRunStmt.run(
      finishedAt,
      status,
      pagesFetched,
      jobsFound,
      errorMessage,
      retryAt,
      runId,
    );
    updateLoopHealthStmt.run(
      finishedAt,
      status,
      status,
      errorMessage,
      pagesFetched,
      jobsFound,
      loopName,
    );
  });
  finish.immediate();
  return { status, pagesFetched, jobsFound, error: errorMessage, retryAt };
}

async function runNationwideScan(loopName) {
  const runId = startScanRun(loopName);
  let pagesFetched = 0;
  const foundJobIds = new Set();
  let error = null;
  const seenTokens = new Set();
  let nextToken = null;

  try {
    while (true) {
      const searchJobRequest = {
        country: 'GB',
        locale: 'en_GB',
        keyWords: '',
        pageSize: amazonPageSize,
        ...(nextToken ? { nextToken } : {}),
      };
      const response = await fetchAmazon(url, {
        method: 'POST',
        headers: amazonHeaders,
        body: JSON.stringify({
          operationName: 'searchJobCardsByLocation',
          query: JOB_SEARCH_QUERY,
          variables: { searchJobRequest },
        }),
      });

      if (response.status !== 200) {
        throw createAmazonHttpError(response, 'nationwide job search');
      }
      const payload = await response.json();
      const graphQLError = getGraphQLErrorMessage(payload);
      if (graphQLError) {
        throw new Error(`Amazon job search GraphQL error: ${graphQLError}`);
      }

      const result = payload?.data?.searchJobCardsByLocation;
      const jobCards = Array.isArray(result)
        ? result
        : result?.jobCards;
      if (!Array.isArray(jobCards)) {
        throw new Error('Amazon job search response did not contain a jobCards list');
      }

      pagesFetched += 1;
      for (const job of jobCards) {
        if (!job?.jobId) continue;
        saveDiscoveredJob.immediate(job, nowIso());
        foundJobIds.add(job.jobId);
      }

      const pageToken = Array.isArray(result) ? null : result?.nextToken;
      if (!pageToken) break;
      if (seenTokens.has(pageToken)) {
        throw new Error('Amazon job search returned a repeated pagination token');
      }
      seenTokens.add(pageToken);
      nextToken = pageToken;
    }
  } catch (scanError) {
    error = scanError;
    logger.error(`❌ [${loopName}] Nationwide scan did not finish:`, scanError.message);
  }

  return finishScanRun(runId, loopName, {
    pagesFetched,
    jobsFound: foundJobIds.size,
    error,
  });
}

// ----------------------------------------------------
// LOOP 1: Nationwide Job Discovery & Pagination
// ----------------------------------------------------
async function runLoop1Discovery() {
  if (loop1Running) {
    logger.log('🌍 [Loop 1] Skipping this pass because the previous pass is still running.');
    return { status: 'SKIPPED', pagesFetched: 0, jobsFound: 0 };
  }

  loop1Running = true;
  try {
    logger.log('🌍 [Loop 1] Starting an unfiltered nationwide UK job scan...');
    return await runNationwideScan('loop1');
  } catch (error) {
    logger.error('❌ [Loop 1 fatal error]:', error.message);
    return { status: 'FAILED', pagesFetched: 0, jobsFound: 0, error: error.message };
  } finally {
    loop1Running = false;
  }
}

// ----------------------------------------------------
// LOOP 2: Schedule Processing & Detailed Retrieval
// ----------------------------------------------------
function recordScheduleFailure(jobId, failure) {
  const payload = JSON.stringify({ jobId });
  const errorMessage = String(failure?.message ?? failure).slice(0, 2000);
  const triedAt = nowIso();
  const existing = findFailedScheduleStmt.get(payload);
  if (existing) {
    updateFailedErrorStmt.run(triedAt, errorMessage, existing.id);
  } else {
    insertFailedStmt.run('LOOP2_SCHEDULE', payload, errorMessage, 0, triedAt);
  }
}

async function retrieveSchedulesForJob(job) {
  let nextToken = null;
  const seenTokens = new Set();

  while (true) {
    const searchScheduleRequest = {
      country: 'GB',
      locale: 'en_GB',
      jobId: job.job_id,
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
    });

    if (response.status !== 200) {
      throw createAmazonHttpError(response, `schedule search for job ${job.job_id}`);
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
      throw new Error(`Amazon schedule response for job ${job.job_id} did not contain a scheduleCards list`);
    }

    for (const schedule of scheduleCards) {
      if (!schedule?.scheduleId) {
        throw new Error(`Amazon returned a schedule without scheduleId for job ${job.job_id}`);
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
        job.job_id,
        formatShiftAlert(shiftInfo),
        nowIso(),
      );
      if (enqueued) {
        logger.log(`🚨 [Loop 2] New schedule discovered & queued for alert! ID: ${schedule.scheduleId}`);
      }
    }

    const pageToken = Array.isArray(result) ? null : result?.nextToken;
    if (!pageToken) return;
    if (seenTokens.has(pageToken)) {
      throw new Error(`Amazon returned a repeated schedule pagination token for job ${job.job_id}`);
    }
    seenTokens.add(pageToken);
    nextToken = pageToken;
  }
}

async function processPendingScheduleJobs() {
  if (clock() < amazonCooldownUntil) {
    logger.warn('⚠️ [Loop 2] Schedule processing is paused by Amazon cooldown.');
    return;
  }

  const pendingJobs = getPendingJobsStmt.all();
  if (pendingJobs.length === 0) {
    logger.log('⚡ [Loop 2] No pending jobs in queue.');
  }

  for (const job of pendingJobs) {
    try {
      await retrieveSchedulesForJob(job);
      markJobProcessedStmt.run(job.job_id);
      deleteFailedScheduleStmt.run(JSON.stringify({ jobId: job.job_id }));
    } catch (error) {
      if (
        error?.code === 'AMAZON_COOLDOWN'
        || error?.status === 403
        || error?.status === 429
      ) {
        markJobPendingStmt.run(job.job_id);
        logger.warn(`⚠️ [Loop 2] Pausing job ${job.job_id} until Amazon allows requests again.`);
        break;
      }

      logger.error(`❌ [Loop 2 error for job ${job.job_id}]:`, error.message);
      markJobFailedStmt.run(job.job_id);
      recordScheduleFailure(job.job_id, error);
    }
  }
}

async function runLoop2Schedules() {
  if (loop2Running) {
    logger.log('⚡ [Loop 2] Skipping this pass because the previous pass is still running.');
    return { status: 'SKIPPED' };
  }

  loop2Running = true;
  try {
    logger.log('⚡ [Loop 2] Starting its own nationwide UK job scan...');
    const scanResult = await runNationwideScan('loop2');
    await processPendingScheduleJobs();
    await deliverPendingNotifications();
    return scanResult;
  } catch (error) {
    logger.error('❌ [Loop 2 fatal error]:', error.message);
    return { status: 'FAILED', error: error.message };
  } finally {
    loop2Running = false;
  }
}

// ----------------------------------------------------
// LOOP 3: Error Recovery, Failed Requests & Safe Retries
// ----------------------------------------------------
async function retryFailedScheduleJobs() {
  const failedTasks = getFailedStmt.all();
  const visitedJobs = new Set();

  for (const task of failedTasks) {
    let payload;
    try {
      payload = JSON.parse(task.payload || '{}');
    } catch (error) {
      logger.error(`❌ [Loop 3] Invalid retry payload for task ${task.id}:`, error.message);
      continue;
    }

    const jobId = payload.jobId;
    if (!jobId || visitedJobs.has(jobId)) continue;
    visitedJobs.add(jobId);

    const queueRow = getJobQueueStatusStmt.get(jobId);
    if (queueRow?.status === 'PROCESSED') {
      deleteFailedStmt.run(task.id);
      continue;
    }
    if (queueRow?.status === 'PENDING') continue;

    const lastTriedAt = Date.parse(task.last_tried || '');
    const retryDelay = Math.min(
      5000 * (2 ** Math.min(task.retry_count, 10)),
      60 * 60 * 1000,
    );
    if (Number.isFinite(lastTriedAt) && clock() - lastTriedAt < retryDelay) continue;

    const retriedAt = nowIso();
    requeueFailedJobStmt.run(jobId, retriedAt);
    updateFailedStmt.run(retriedAt, task.id);
    logger.log(`🛡️ [Loop 3] Re-queued failed schedule work for job ${jobId}.`);
  }
}

async function runLoop3Recovery() {
  if (loop3Running) {
    logger.log('🛡️ [Loop 3] Skipping this pass because the previous pass is still running.');
    return { status: 'SKIPPED' };
  }

  loop3Running = true;
  try {
    logger.log('🛡️ [Loop 3] Starting its own nationwide UK job scan and recovery pass...');
    const scanResult = await runNationwideScan('loop3');
    try {
      await retryFailedScheduleJobs();
    } catch (error) {
      logger.error('❌ [Loop 3 recovery error]:', error.message);
    }
    try {
      await deliverPendingNotifications();
    } catch (error) {
      logger.error('❌ [Loop 3 notification recovery error]:', error.message);
    }
    return scanResult;
  } catch (error) {
    logger.error('❌ [Loop 3 fatal error]:', error.message);
    return { status: 'FAILED', error: error.message };
  } finally {
    loop3Running = false;
  }
}

async function start() {
  logger.log('🚀 Starting Nationwide UK Amazon Shift Tracker (Three-Loop Architecture)...');
  const initialLoop1Run = runLoop1Discovery();

  // Start Loop 1 continuous cycle (every 60 seconds)
  setIntervalImpl(runLoop1Discovery, 60 * 1000);

  // Stagger Loop 2 start by 5 seconds
  setTimeoutImpl(() => {
    logger.log('⚡ Initializing Loop 2 (Schedule Processing)...');
    const initialLoop2Run = runLoop2Schedules();
    setIntervalImpl(runLoop2Schedules, 30 * 1000);
    return initialLoop2Run;
  }, 5000);

  // Stagger Loop 3 start by 10 seconds
  setTimeoutImpl(() => {
    logger.log('🛡️ Initializing Loop 3 (Error Recovery & Retries)...');
    const initialLoop3Run = runLoop3Recovery();
    setIntervalImpl(runLoop3Recovery, 90 * 1000);
    return initialLoop3Run;
  }, 10000);

  await sendTelegramAlert("🚀 *Nationwide UK Amazon Shift Tracker Online!*\nThree-Loop Architecture (Discovery + Schedule Processing + Error Recovery) initiated.");
  return initialLoop1Run;
}

return {
  db,
  runLoop1Discovery,
  runLoop2Schedules,
  runLoop3Recovery,
  runNotificationDelivery: deliverPendingNotifications,
  getLoopHealth: () => getLoopHealthStmt.all(),
  getScanRuns: (limit = 50) => getScanRunsStmt.all(limit),
  sendTelegramAlert,
  start,
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
