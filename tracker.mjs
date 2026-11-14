import Database from 'better-sqlite3';
import { fetch, ProxyAgent } from 'undici';

// Initialize SQLite database (persisted on Railway volume or local file)
const db = new Database('shift_tracker.db');

// Existing schedule records are preserved; outbox columns are additive migrations.
db.exec(`
  CREATE TABLE IF NOT EXISTS discovered_schedules (
    schedule_id TEXT PRIMARY KEY,
    discovered_at TEXT,
    amazon_detected_at TEXT,
    notification_queued_at TEXT,
    telegram_sent_at TEXT
  );

  CREATE TABLE IF NOT EXISTS telegram_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id TEXT,
    message TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TEXT
  );
`);

// Normalize legacy SQLite UTC timestamps to ISO 8601 without changing records.
db.exec(`
  UPDATE discovered_schedules
  SET discovered_at = CASE
        WHEN discovered_at GLOB '????-??-?? ??:??:??'
          THEN substr(discovered_at, 1, 10) || 'T' || substr(discovered_at, 12, 8) || '.000Z'
        ELSE discovered_at
      END,
      amazon_detected_at = CASE
        WHEN amazon_detected_at GLOB '????-??-?? ??:??:??'
          THEN substr(amazon_detected_at, 1, 10) || 'T' || substr(amazon_detected_at, 12, 8) || '.000Z'
        ELSE amazon_detected_at
      END,
      notification_queued_at = CASE
        WHEN notification_queued_at GLOB '????-??-?? ??:??:??'
          THEN substr(notification_queued_at, 1, 10) || 'T' || substr(notification_queued_at, 12, 8) || '.000Z'
        ELSE notification_queued_at
      END,
      telegram_sent_at = CASE
        WHEN telegram_sent_at GLOB '????-??-?? ??:??:??'
          THEN substr(telegram_sent_at, 1, 10) || 'T' || substr(telegram_sent_at, 12, 8) || '.000Z'
        ELSE telegram_sent_at
      END
  WHERE discovered_at GLOB '????-??-?? ??:??:??'
     OR amazon_detected_at GLOB '????-??-?? ??:??:??'
     OR notification_queued_at GLOB '????-??-?? ??:??:??'
     OR telegram_sent_at GLOB '????-??-?? ??:??:??';
`);

const outboxColumns = new Set(
  db.prepare('PRAGMA table_info(telegram_outbox)').all().map(({ name }) => name),
);
for (const [name, definition] of [
  ['notification_key', 'TEXT'],
  ['schedule_ids', 'TEXT'],
  ['attempt_count', 'INTEGER NOT NULL DEFAULT 0'],
  ['next_attempt_at', 'TEXT'],
  ['last_error', 'TEXT'],
  ['claim_until', 'TEXT'],
]) {
  if (!outboxColumns.has(name)) {
    db.exec(`ALTER TABLE telegram_outbox ADD COLUMN ${name} ${definition}`);
  }
}
db.prepare(`
  UPDATE telegram_outbox
  SET notification_key = 'legacy-row:' || id
  WHERE notification_key IS NULL OR notification_key = ''
`).run();
for (const row of db.prepare(
  'SELECT id, schedule_id FROM telegram_outbox WHERE schedule_ids IS NULL',
).all()) {
  db.prepare('UPDATE telegram_outbox SET schedule_ids = ? WHERE id = ?').run(
    JSON.stringify(row.schedule_id ? [row.schedule_id] : []),
    row.id,
  );
}
db.exec(`
  CREATE UNIQUE INDEX IF NOT EXISTS telegram_outbox_notification_key_unique
  ON telegram_outbox(notification_key);
`);

// Prepared statements for high performance & atomic safety
const stmtClaimSchedule = db.prepare(`
  INSERT OR IGNORE INTO discovered_schedules (schedule_id, discovered_at, amazon_detected_at)
  VALUES (?, ?, ?)
`);

const stmtUpdateQueueTime = db.prepare(`
  UPDATE discovered_schedules SET notification_queued_at = ? WHERE schedule_id = ?
`);

const stmtEnqueueTelegram = db.prepare(`
  INSERT OR IGNORE INTO telegram_outbox
    (schedule_id, notification_key, schedule_ids, message, status, created_at)
  VALUES (?, ?, ?, ?, 'pending', datetime('now'))
`);

const stmtGetPendingAlerts = db.prepare(`
  SELECT id, schedule_id, schedule_ids, message, attempt_count
  FROM telegram_outbox
  WHERE status = 'pending'
    AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
  ORDER BY id ASC
  LIMIT 5
`);

const stmtClaimAlert = db.prepare(`
  UPDATE telegram_outbox
  SET status = 'processing', attempt_count = attempt_count + 1, claim_until = ?
  WHERE id = ? AND status = 'pending'
    AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
`);

const stmtRecoverExpiredAlerts = db.prepare(`
  UPDATE telegram_outbox
  SET status = CASE WHEN attempt_count >= ? THEN 'failed' ELSE 'pending' END,
      last_error = CASE
        WHEN attempt_count >= ? THEN 'Claim expired after the final delivery attempt'
        ELSE last_error
      END,
      claim_until = NULL
  WHERE status = 'processing' AND claim_until <= ?
`);

const stmtMarkAlertSent = db.prepare(`
  UPDATE telegram_outbox
  SET status = 'sent', claim_until = NULL, next_attempt_at = NULL, last_error = NULL
  WHERE id = ?
`);

const stmtMarkAlertFailed = db.prepare(`
  UPDATE telegram_outbox
  SET status = ?, next_attempt_at = ?, last_error = ?, claim_until = NULL
  WHERE id = ?
`);

const stmtUpdateSentTime = db.prepare(`
  UPDATE discovered_schedules SET telegram_sent_at = ? WHERE schedule_id = ?
`);

const stmtGetTelemetryTimestamps = db.prepare(`
  SELECT amazon_detected_at, notification_queued_at, telegram_sent_at
  FROM discovered_schedules WHERE schedule_id = ?
`);

// Configuration from environment variables
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const AMAZON_AUTH_TOKEN = process.env.AMAZON_AUTH_TOKEN || process.env.AUTHORIZATION;
const PROXY_URL = process.env.SHIFT_PROXY_URL || process.env.PROXY_URL;

// Configure Undici proxy agent if proxy is provided
let dispatcher = undefined;
if (PROXY_URL) {
  dispatcher = new ProxyAgent(PROXY_URL);
  console.log('[Bot] Configured proxy agent for secure requests.');
}

let telegramWorkerRunning = false;
const TELEGRAM_MAX_ATTEMPTS = 3;
const TELEGRAM_RETRY_BASE_MS = 5000;
const TELEGRAM_RETRY_MAX_MS = 60000;

function recordTelegramFailure(alert, error) {
  const attemptCount = alert.attempt_count;
  const terminal = attemptCount >= TELEGRAM_MAX_ATTEMPTS;
  const retryDelay = Math.min(
    TELEGRAM_RETRY_BASE_MS * (2 ** Math.max(0, attemptCount - 1)),
    TELEGRAM_RETRY_MAX_MS,
  );
  const retryAt = terminal ? null : new Date(Date.now() + retryDelay).toISOString();
  const message = String(error?.message ?? error).slice(0, 500);
  stmtMarkAlertFailed.run(terminal ? 'failed' : 'pending', retryAt, message, alert.id);
  console.error(
    `[Telegram] ${terminal ? '❌ Permanently failed' : '⚠️ Retry scheduled'} `
    + `for outbox record ${alert.id} (attempt ${attemptCount}/${TELEGRAM_MAX_ATTEMPTS}): ${message}`,
  );
}

async function runTelegramWorkerCycle() {
  if (telegramWorkerRunning) return;
  telegramWorkerRunning = true;
  try {
    const now = new Date().toISOString();
    stmtRecoverExpiredAlerts.run(TELEGRAM_MAX_ATTEMPTS, TELEGRAM_MAX_ATTEMPTS, now);
    const pendingAlerts = stmtGetPendingAlerts.all(now);
    for (const alert of pendingAlerts) {
      const claimUntil = new Date(Date.now() + 60000).toISOString();
      const claimed = stmtClaimAlert.run(claimUntil, alert.id, now);
      if (claimed.changes !== 1) continue;

      const attempt = { ...alert, attempt_count: alert.attempt_count + 1 };
      try {
        if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
          throw new Error('Telegram credentials are not configured');
        }

        const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: TELEGRAM_CHAT_ID,
            text: alert.message
          })
        });
        let telegramPayload;
        try {
          telegramPayload = await res.json();
        } catch {
          throw new Error(`Telegram returned invalid JSON (HTTP ${res.status})`);
        }

        if (!res.ok || telegramPayload?.ok !== true) {
          const description = typeof telegramPayload?.description === 'string'
            ? `: ${telegramPayload.description.slice(0, 300)}`
            : '';
          throw new Error(`Telegram rejected the message (HTTP ${res.status})${description}`);
        }

        stmtMarkAlertSent.run(alert.id);
        const sentAt = new Date().toISOString();
        let scheduleIds = [];
        try {
          scheduleIds = JSON.parse(alert.schedule_ids || '[]');
        } catch {
          scheduleIds = alert.schedule_id ? [alert.schedule_id] : [];
        }
        for (const scheduleId of scheduleIds) {
          stmtUpdateSentTime.run(sentAt, scheduleId);
          const row = stmtGetTelemetryTimestamps.get(scheduleId);
          if (row?.amazon_detected_at && row?.notification_queued_at && row?.telegram_sent_at) {
            const tAmazon = new Date(row.amazon_detected_at).getTime();
            const tQueued = new Date(row.notification_queued_at).getTime();
            const tSent = new Date(row.telegram_sent_at).getTime();
            console.log(
              `[TELEMETRY LATENCY] Schedule ${scheduleId} | `
              + `Discovery->Queue: ${tQueued - tAmazon}ms | `
              + `Queue->Telegram: ${tSent - tQueued}ms | Total Pipeline: ${tSent - tAmazon}ms`,
            );
          }
        }
        console.log('[Telegram] Sent OK');
      } catch (error) {
        recordTelegramFailure(attempt, error);
        if (error?.message?.includes('HTTP 429')) break;
      }

      await new Promise(resolve => setTimeout(resolve, 800));
    }
  } catch (err) {
    console.error('[Telegram] ❌ Worker exception:', err.message);
  } finally {
    telegramWorkerRunning = false;
  }
}

// --- ASYNCHRONOUS TELEGRAM WORKER WITH LATENCY TELEMETRY ---
function startTelegramWorker() {
  return setInterval(runTelegramWorkerCycle, 1000);
}

function safeAmazonDiagnosticText(value) {
  return String(value ?? 'Unknown Amazon error')
    .replace(/\bBearer\s+[^\s,;]+/gi, 'Bearer [REDACTED]')
    .replace(/\b(authorization|token|password|cookie|secret|api[_-]?key)\s*[:=]\s*[^\s,;]+/gi, '$1=[REDACTED]')
    .replace(/https?:\/\/[^\s/@:]+:[^\s/@]+@/gi, '[REDACTED_URL]')
    .replace(/[\r\n]+/g, ' ')
    .slice(0, 240);
}

function summarizeGraphQLErrors(errors) {
  const messages = errors.slice(0, 3).map((error) => (
    safeAmazonDiagnosticText(error?.message ?? 'Unspecified GraphQL error')
  ));
  const omittedCount = errors.length - messages.length;
  const omitted = omittedCount > 0 ? `; ${omittedCount} more` : '';
  return `${errors.length} error(s): ${messages.join('; ')}${omitted}`;
}

function reportAmazonGraphQLErrors(operation, errors, pageCount, jobId) {
  if (!Array.isArray(errors) || errors.length === 0) return;
  const jobLabel = jobId ? ` for job ${safeAmazonDiagnosticText(jobId)}` : '';
  console.warn(
    `[Scan Health] PARTIAL: Amazon GraphQL ${operation} errors on page ${pageCount}`
    + `${jobLabel}: ${summarizeGraphQLErrors(errors)}`,
  );
}

function retryAfterLogSuffix(response) {
  const value = response?.headers?.get?.('retry-after');
  if (typeof value !== 'string' || value.trim() === '') return '';
  return ` Retry-After=${safeAmazonDiagnosticText(value.trim()).slice(0, 80)}`;
}

function reportAmazonHttpFailure(operation, response, pageCount, jobId) {
  const jobLabel = jobId ? ` for job ${safeAmazonDiagnosticText(jobId)}` : '';
  const context = `${operation} page ${pageCount}${jobLabel}`;
  const retryAfter = retryAfterLogSuffix(response);

  if (response.status === 403) {
    console.warn(`[Scan Health] BLOCKED: Amazon HTTP 403 on ${context}.`);
  } else if (response.status === 429) {
    console.warn(`[Scan Health] THROTTLED: Amazon HTTP 429 on ${context}.${retryAfter}`);
  } else {
    console.warn(
      `[Scan Health] PARTIAL: Amazon HTTP ${response.status} on ${context}.${retryAfter}`,
    );
  }
}

// --- FETCH SCHEDULE CARDS WITH FULL PAGINATION (`nextToken` Crawler) ---
async function fetchSchedulesForJob(jobId) {
  let allSchedules = [];
  let nextToken = null;
  let pageCount = 0;
  let paginationComplete = true;
  const maxPages = 5;

  do {
    pageCount++;
    const url = 'https://www.jobsatamazon.co.uk/graphql';
    const query = `
      query searchScheduleCards($searchScheduleRequest: SearchScheduleRequest!) {
        searchScheduleCards(searchScheduleRequest: $searchScheduleRequest) {
          nextToken
          scheduleCards {
            scheduleId
            jobId
            firstDayOnSite
            firstDayOnSiteL10N
            scheduleText
            scheduleType
            hoursPerWeek
          }
        }
      }
    `;

    const variables = {
      searchScheduleRequest: {
        country: 'United Kingdom',
        jobId: jobId,
        locale: 'en-GB',
        pageSize: 100,
        dateFilters: [],
        rangeFilters: [],
        equalFilters: [],
        excludeFilters: [],
        orFilters: [],
        ...(nextToken ? { nextToken } : {})
      }
    };

    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Origin': 'https://www.jobsatamazon.co.uk',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          ...(AMAZON_AUTH_TOKEN ? { 'Authorization': AMAZON_AUTH_TOKEN } : {})
        },
        body: JSON.stringify({ query, variables }),
        ...(dispatcher ? { dispatcher } : {})
      });

      if (!res.ok) {
        reportAmazonHttpFailure('schedule feed', res, pageCount, jobId);
        paginationComplete = false;
        break;
      }
      const data = await res.json();
      const resultData = data?.data?.searchScheduleCards;
      if (resultData && resultData.scheduleCards) {
        allSchedules.push(...resultData.scheduleCards);
      }
      nextToken = resultData?.nextToken || null;
      if (Array.isArray(data?.errors) && data.errors.length > 0) {
        reportAmazonGraphQLErrors('schedule feed', data.errors, pageCount, jobId);
        paginationComplete = false;
        break;
      }
    } catch (err) {
      console.error(
        `[Scan Health] PARTIAL/FAILED: Amazon schedule feed exception on page ${pageCount}`
        + ` for job ${safeAmazonDiagnosticText(jobId)}: ${safeAmazonDiagnosticText(err?.message)}`,
      );
      paginationComplete = false;
      break;
    }

    if (pageCount >= maxPages && nextToken) {
      paginationComplete = false;
      break;
    }
  } while (nextToken);

  return { schedules: allSchedules, paginationComplete };
}

// --- CORE JOB & SCHEDULE PROCESSOR ---
async function processDiscoveredJob(job, amazonDetectedAt) {
  const { schedules: scheduleCards, paginationComplete } = await fetchSchedulesForJob(job.jobId);
  const queued = db.transaction(() => {
    const newlyDiscovered = [];
    for (const card of scheduleCards) {
      if (!card?.scheduleId) continue;
      const result = stmtClaimSchedule.run(card.scheduleId, amazonDetectedAt, amazonDetectedAt);
      if (result.changes === 1) newlyDiscovered.push(card);
    }

    if (newlyDiscovered.length === 0) return { count: 0, scheduleIds: [] };

    const queuedAt = new Date().toISOString();
    const locationName = job.locationName || job.city || 'UK Delivery Station';
    const postalCode = job.postalCode || '';
    const title = job.jobTitle || 'Delivery Station Warehouse Associate';
    const payRate = job.totalPayRateMax ? job.totalPayRateMax.toString() : '15.10';
    const shiftCount = newlyDiscovered.length;

    let message = `📍 ${locationName} ${postalCode}\n` +
                  `🏷️ ${title} | ${shiftCount}\n` +
                  `💼 Seasonal | Full-time;Part-time\n` +
                  `💰 £${payRate} /hr\n`;

    newlyDiscovered.forEach((shift, index) => {
      message += `\n📋 Shift ${index + 1}:\n` +
                 `📅 First Day: ${shift.firstDayOnSiteL10N || shift.firstDayOnSite || 'Upcoming'}\n` +
                 `⏰ Schedule: ${shift.scheduleText || 'Working Days'}\n` +
                 `🕐 Hours/Week: ${shift.hoursPerWeek || '20'}\n`;
    });

    const scheduleIds = newlyDiscovered.map(({ scheduleId }) => scheduleId).sort();
    const notificationKey = JSON.stringify([job.jobId, scheduleIds]);
    for (const scheduleId of scheduleIds) {
      stmtUpdateQueueTime.run(queuedAt, scheduleId);
    }
    stmtEnqueueTelegram.run(
      scheduleIds[0],
      notificationKey,
      JSON.stringify(scheduleIds),
      message,
    );

    console.log(
      `[DEDUP] Queued one grouped alert for job ${job.jobId} `
      + `with ${shiftCount} newly discovered shift(s) at ${queuedAt}`,
    );
    return { count: shiftCount, scheduleIds };
  }).immediate();

  return { ...queued, paginationComplete };
}

// --- FULL NATIONWIDE FEED FETCHER WITH PAGINATION & SCAN HEALTH TRACKING ---
async function fetchAmazonJobFeed() {
  let allJobCards = [];
  let nextToken = null;
  let pageCount = 0;
  let scanStatus = 'COMPLETE';
  const maxPages = 10;

  do {
    pageCount++;
    const url = 'https://www.jobsatamazon.co.uk/graphql';
    const query = `
      query searchJobCardsByLocation($searchJobRequest: SearchJobRequest!) {
        searchJobCardsByLocation(searchJobRequest: $searchJobRequest) {
          nextToken
          jobCards {
            jobId
            jobTitle
            city
            postalCode
            locationName
            scheduleCount
            totalPayRateMax
          }
        }
      }
    `;

    const variables = {
      searchJobRequest: {
        locale: 'en-GB',
        country: 'United Kingdom',
        keyWords: '',
        equalFilters: [],
        containFilters: [{ key: 'isPrivateSchedule', val: ['true', 'false'] }],
        rangeFilters: [],
        orFilters: [],
        dateFilters: [],
        sorters: [{ fieldName: 'totalPayRateMax', ascending: 'false' }],
        pageSize: 100,
        consolidateSchedule: true,
        ...(nextToken ? { nextToken } : {})
      }
    };

    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Origin': 'https://www.jobsatamazon.co.uk',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          ...(AMAZON_AUTH_TOKEN ? { 'Authorization': AMAZON_AUTH_TOKEN } : {})
        },
        body: JSON.stringify({ query, variables }),
        ...(dispatcher ? { dispatcher } : {})
      });

      if (res.status === 403) {
        reportAmazonHttpFailure('job feed', res, pageCount);
        scanStatus = 'BLOCKED';
        break;
      }

      if (!res.ok) {
        reportAmazonHttpFailure('job feed', res, pageCount);
        scanStatus = 'PARTIAL';
        break;
      }

      const data = await res.json();
      const resultData = data?.data?.searchJobCardsByLocation;
      if (resultData && resultData.jobCards) {
        for (const job of resultData.jobCards) {
          allJobCards.push({
            ...job,
            _scannerDetectedAt: new Date().toISOString(),
          });
        }
      }

      nextToken = resultData?.nextToken || null;
      if (Array.isArray(data?.errors) && data.errors.length > 0) {
        reportAmazonGraphQLErrors('job feed', data.errors, pageCount);
        scanStatus = 'PARTIAL';
      }
    } catch (err) {
      console.error(
        `[Scan Health] PARTIAL/FAILED: Amazon job feed exception on page ${pageCount}:`
        + ` ${safeAmazonDiagnosticText(err?.message)}`,
      );
      scanStatus = 'PARTIAL';
      break;
    }

    if (pageCount >= maxPages && nextToken) {
      scanStatus = 'PARTIAL';
      break;
    }
  } while (nextToken);

  return { jobCards: allJobCards, scanStatus };
}

// --- ORGANIC REQUEST JITTER ---
async function randomizedDelay() {
  const jitter = Math.floor(Math.random() * (5000 - 3000 + 1)) + 3000;
  await new Promise(resolve => setTimeout(resolve, jitter));
}

// --- MAIN OVERLAPPING SCANNER LOOP ---
async function runScannerLoop(loopName) {
  console.log(`[${loopName}] Started nationwide feed scanning loop.`);

  while (true) {
    try {
      const { jobCards, scanStatus } = await fetchAmazonJobFeed();
      let completedScanStatus = scanStatus;

      if (scanStatus !== 'BLOCKED' && jobCards && jobCards.length > 0) {
        for (const job of jobCards) {
          const result = await processDiscoveredJob(job, job._scannerDetectedAt);
          completedScanStatus = includeSchedulePaginationStatus(
            completedScanStatus,
            result.paginationComplete,
          );
        }
      }

      console.log(
        `[Scan Health] [${loopName}] Scan completed with status: `
        + `[${completedScanStatus}] | Retrieved ${jobCards.length} job card(s).`,
      );
      await randomizedDelay();
    } catch (err) {
      console.error(`[${loopName}] ⚠️ Loop error:`, err.message);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

function includeSchedulePaginationStatus(scanStatus, paginationComplete) {
  if (!paginationComplete && scanStatus === 'COMPLETE') return 'PARTIAL';
  return scanStatus;
}

// --- APP INITIALIZATION ---
console.log('[Bot] Starting Amazon Shift Tracker (Production Telemetry & Pagination Edition)...');
console.log('[Activation] Runtime: Node.js ES Modules | Database: better-sqlite3 | Pagination: Active nextToken Crawler');
console.log('[Bot] Targets: Nationwide UK Feed Scans with Scan Health Tracking & Latency Telemetry');
console.log('[Watchdog] Started - monitoring 3 global overlapping loops with staggered delays.');

startTelegramWorker();

// 3 Overlapping Loops checking the full nationwide feed concurrently
console.log('[Loop-1] Started.');
runScannerLoop('Loop-1');

console.log('[Loop-2] Starting in 5s...');
setTimeout(() => {
  console.log('[Loop-2] Started.');
  runScannerLoop('Loop-2');
}, 5000);

console.log('[Loop-3] Starting in 10s...');
setTimeout(() => {
  console.log('[Loop-3] Started.');
  runScannerLoop('Loop-3');
}, 10000);
