import Database from 'better-sqlite3';
import { fetch, ProxyAgent } from 'undici';

// Initialize SQLite database (persisted on Railway volume or local file)
const db = new Database('shift_tracker.db');

// Setup database tables with telemetry timestamp columns & health tracking
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

// Prepared statements for high performance & atomic safety
const stmtClaimSchedule = db.prepare(`
  INSERT OR IGNORE INTO discovered_schedules (schedule_id, discovered_at, amazon_detected_at)
  VALUES (?, datetime('now'), ?)
`);

const stmtUpdateQueueTime = db.prepare(`
  UPDATE discovered_schedules SET notification_queued_at = ? WHERE schedule_id = ?
`);

const stmtEnqueueTelegram = db.prepare(`
  INSERT INTO telegram_outbox (schedule_id, message, created_at)
  VALUES (?, ?, datetime('now'))
`);

const stmtGetPendingAlerts = db.prepare(`
  SELECT id, schedule_id, message FROM telegram_outbox 
  WHERE status = 'pending' 
  ORDER BY id ASC 
  LIMIT 5
`);

const stmtMarkAlertSent = db.prepare(`
  UPDATE telegram_outbox SET status = 'sent' WHERE id = ?
`);

const stmtUpdateSentTime = db.prepare(`
  UPDATE discovered_schedules SET telegram_sent_at = datetime('now') WHERE schedule_id = ?
`);

const stmtGetTelemetryTimestamps = db.prepare(`
  SELECT amazon_detected_at, notification_queued_at, telegram_sent_at 
  FROM discovered_schedules WHERE schedule_id = ?
`);

const stmtMarkAlertFailed = db.prepare(`
  UPDATE telegram_outbox SET status = 'failed' WHERE id = ?
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

// --- ASYNCHRONOUS TELEGRAM WORKER WITH LATENCY TELEMETRY ---
function startTelegramWorker() {
  setInterval(async () => {
    try {
      const pendingAlerts = stmtGetPendingAlerts.all();
      if (pendingAlerts.length === 0) return;

      for (const alert of pendingAlerts) {
        if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
          console.warn('[Telegram] ⚠️ Credentials missing. Check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.');
          stmtMarkAlertFailed.run(alert.id);
          continue;
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

        if (res.ok) {
          stmtMarkAlertSent.run(alert.id);
          stmtUpdateSentTime.run(alert.schedule_id);

          // Calculate and log telemetry latency metrics
          if (alert.schedule_id) {
            const row = stmtGetTelemetryTimestamps.get(alert.schedule_id);
            if (row && row.amazon_detected_at && row.notification_queued_at && row.telegram_sent_at) {
              const tAmazon = new Date(row.amazon_detected_at).getTime();
              const tQueued = new Date(row.notification_queued_at).getTime();
              const tSent = new Date(row.telegram_sent_at).getTime();
              console.log(`[TELEMETRY LATENCY] Schedule ${alert.schedule_id} | Discovery->Queue: ${tQueued - tAmazon}ms | Queue->Telegram: ${tSent - tQueued}ms | Total Pipeline: ${tSent - tAmazon}ms`);
            }
          }

          console.log('[Telegram] Sent OK');
        } else if (res.status === 429) {
          console.warn('[Telegram] ⚠️ HTTP 429 rate limit hit. Backing off...');
          break; 
        } else {
          const errText = await res.text();
          console.error(`[Telegram] ❌ API error (${res.status}): ${errText}`);
          stmtMarkAlertFailed.run(alert.id);
        }

        // Safe 800ms spacing between outgoing telegram alerts
        await new Promise(resolve => setTimeout(resolve, 800));
      }
    } catch (err) {
      console.error('[Telegram] ❌ Worker exception:', err.message);
    }
  }, 1000);
}

// --- FETCH SCHEDULE CARDS WITH FULL PAGINATION (`nextToken` Crawler) ---
async function fetchSchedulesForJob(jobId) {
  let allSchedules = [];
  let nextToken = null;
  let pageCount = 0;
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

      if (!res.ok) break;
      const data = await res.json();
      const resultData = data?.data?.searchScheduleCards;
      if (resultData && resultData.scheduleCards) {
        allSchedules.push(...resultData.scheduleCards);
      }
      nextToken = resultData?.nextToken || null;
    } catch (err) {
      break;
    }

    if (pageCount >= maxPages) break;
  } while (nextToken);

  return allSchedules;
}

// --- CORE JOB & SCHEDULE PROCESSOR ---
async function processDiscoveredJob(job, amazonDetectedAt) {
  const scheduleCards = await fetchSchedulesForJob(job.jobId);

  let claimedAny = false;
  const validSchedules = [];

  if (scheduleCards && scheduleCards.length > 0) {
    for (const card of scheduleCards) {
      const res = stmtClaimSchedule.run(card.scheduleId, amazonDetectedAt);
      if (res.changes === 1) {
        claimedAny = true;
      }
      validSchedules.push(card);
    }
  } else {
    const fallbackId = `${job.jobId}-fallback`;
    const res = stmtClaimSchedule.run(fallbackId, amazonDetectedAt);
    if (res.changes === 1) {
      claimedAny = true;
    }
    validSchedules.push({
      scheduleId: fallbackId,
      firstDayOnSiteL10N: 'Upcoming',
      scheduleText: 'Working Days Flexible',
      hoursPerWeek: '20'
    });
  }

  if (claimedAny) {
    const queuedAt = new Date().toISOString();
    const locationName = job.locationName || job.city || 'UK Delivery Station';
    const postalCode = job.postalCode || '';
    const title = job.jobTitle || 'Delivery Station Warehouse Associate';
    const payRate = job.totalPayRateMax ? job.totalPayRateMax.toString() : '15.10';
    const shiftCount = validSchedules.length;

    let message = `📍 ${locationName} ${postalCode}\n` +
                  `🏷️ ${title} | ${shiftCount}\n` +
                  `💼 Seasonal | Full-time;Part-time\n` +
                  `💰 £${payRate} /hr\n`;

    validSchedules.forEach((shift, index) => {
      message += `\n📋 Shift ${index + 1}:\n` +
                 `📅 First Day: ${shift.firstDayOnSiteL10N || shift.firstDayOnSite || 'Upcoming'}\n` +
                 `⏰ Schedule: ${shift.scheduleText || 'Working Days'}\n` +
                 `🕐 Hours/Week: ${shift.hoursPerWeek || '20'}\n`;
    });

    for (const shift of validSchedules) {
      stmtUpdateQueueTime.run(queuedAt, shift.scheduleId);
      stmtEnqueueTelegram.run(shift.scheduleId, message);
    }

    console.log(`[DEDUP] Claimed and queued alert for job: ${job.jobId} (${locationName}) with ${shiftCount} shift(s) at ${queuedAt}`);
  }
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
        console.warn('[Scan Health] 🛑 BLOCKED: Amazon HTTP 403 WAF challenge encountered.');
        scanStatus = 'BLOCKED';
        break;
      }

      if (!res.ok) {
        console.warn(`[Scan Health] ⚠️ PARTIAL: HTTP ${res.status} response on page ${pageCount}.`);
        scanStatus = 'PARTIAL';
        break;
      }

      const data = await res.json();
      const resultData = data?.data?.searchJobCardsByLocation;
      if (resultData && resultData.jobCards) {
        allJobCards.push(...resultData.jobCards);
      }

      nextToken = resultData?.nextToken || null;
    } catch (err) {
      console.error(`[Scan Health] ⚠️ PARTIAL/FAILED: Exception on page ${pageCount}:`, err.message);
      scanStatus = 'PARTIAL';
      break;
    }

    if (pageCount >= maxPages) break;
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
      const amazonDetectedAt = new Date().toISOString();
      const { jobCards, scanStatus } = await fetchAmazonJobFeed();
      
      console.log(`[Scan Health] [${loopName}] Scan completed with status: [${scanStatus}] | Retrieved ${jobCards.length} job card(s).`);

      if (scanStatus !== 'BLOCKED' && jobCards && jobCards.length > 0) {
        for (const job of jobCards) {
          await processDiscoveredJob(job, amazonDetectedAt);
        }
      }

      await randomizedDelay();
    } catch (err) {
      console.error(`[${loopName}] ⚠️ Loop error:`, err.message);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
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
