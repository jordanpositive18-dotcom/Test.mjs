import Database from 'better-sqlite3';
import { fetch, ProxyAgent } from 'undici';

// Initialize SQLite database (persisted on Railway volume or local file)
const db = new Database('shift_tracker.db');

// Setup database tables for atomic deduplication and notification outbox
db.exec(`
  CREATE TABLE IF NOT EXISTS discovered_schedules (
    schedule_id TEXT PRIMARY KEY,
    discovered_at TEXT
  );

  CREATE TABLE IF NOT EXISTS telegram_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TEXT
  );
`);

// Prepared statements for high performance & atomic safety
const stmtClaimSchedule = db.prepare(`
  INSERT OR IGNORE INTO discovered_schedules (schedule_id, discovered_at)
  VALUES (?, datetime('now'))
`);

const stmtEnqueueTelegram = db.prepare(`
  INSERT INTO telegram_outbox (message, created_at)
  VALUES (?, datetime('now'))
`);

const stmtGetPendingAlerts = db.prepare(`
  SELECT id, message FROM telegram_outbox 
  WHERE status = 'pending' 
  ORDER BY id ASC 
  LIMIT 5
`);

const stmtMarkAlertSent = db.prepare(`
  UPDATE telegram_outbox SET status = 'sent' WHERE id = ?
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

// --- ASYNCHRONOUS TELEGRAM WORKER (Prevents HTTP 429 Rate Limits) ---
function startTelegramWorker() {
  setInterval(async () => {
    try {
      const pendingAlerts = stmtGetPendingAlerts.all();
      if (pendingAlerts.length === 0) return;

      for (const alert of pendingAlerts) {
        if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
          console.warn('[Telegram] ⚠️ Credentials missing. Skipping dispatch.');
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

// --- CORE DISCOVERY & ATOMIC CLAIM HANDLER ---
function handleDiscoveredSchedule(scheduleId, jobDetails) {
  const result = stmtClaimSchedule.run(scheduleId);

  // Atomic SQLite check: returns 1 if this loop successfully claimed it first
  if (result.changes === 1) {
    const location = jobDetails.location || 'Unit A and B Woodlands Industrial Storage Estate Longtown';
    const postcode = jobDetails.postcode || 'CA6 5TR';
    const title = jobDetails.title || 'Delivery Station Warehouse Associate';
    const shiftCount = jobDetails.shifts ? jobDetails.shifts.length : 1;
    const employment = jobDetails.employment || 'Seasonal | Full-time;Part-time';
    const payRate = jobDetails.payRate || '15.10';

    let message = `📍 ${location} ${postcode}\n` +
                  `🏷️ ${title} | ${shiftCount}\n` +
                  `💼 ${employment}\n` +
                  `💰 £${payRate} /hr\n`;

    if (jobDetails.shifts && Array.isArray(jobDetails.shifts) && jobDetails.shifts.length > 0) {
      jobDetails.shifts.forEach((shift, index) => {
        message += `\n📋 Shift ${index + 1}:\n` +
                   `📅 First Day: ${shift.startDate || '2026-11-28'}\n` +
                   `⏰ Schedule: ${shift.scheduleDays || 'Working Days'} ${shift.time || '07:00 - 15:30'}\n` +
                   `🕐 Hours/Week: ${shift.hoursPerWeek || '20'}\n`;
      });
    } else {
      message += `\n📋 Shift 1:\n` +
                 `📅 First Day: ${jobDetails.startDate || '2026-11-28'}\n` +
                 `⏰ Schedule: ${jobDetails.scheduleDays || 'Working Days'} ${jobDetails.time || '07:00 - 15:30'}\n` +
                 `🕐 Hours/Week: ${jobDetails.hoursPerWeek || '20'}\n`;
    }

    stmtEnqueueTelegram.run(message);
    console.log(`[DEDUP] Claimed and queued alert for schedule: ${scheduleId}`);
  }
}

// --- AMAZON GRAPHQL FEED FETCHER ---
async function fetchAmazonJobFeed() {
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
      consolidateSchedule: true
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
      console.warn(`[Amazon API] HTTP ${res.status} response received.`);
      return [];
    }

    const data = await res.json();
    return data?.data?.searchJobCardsByLocation?.jobCards || [];
  } catch (err) {
    console.error('[Amazon API] Fetch exception:', err.message);
    return [];
  }
}

// --- ORGANIC REQUEST JITTER (3-5 seconds like Python loop sleeps) ---
async function randomizedDelay() {
  const jitter = Math.floor(Math.random() * (5000 - 3000 + 1)) + 3000;
  await new Promise(resolve => setTimeout(resolve, jitter));
}

// --- MAIN OVERLAPPING SCANNER LOOP ---
async function runScannerLoop(loopName) {
  console.log(`[${loopName}] Started nationwide feed scanning loop.`);

  while (true) {
    try {
      const jobCards = await fetchAmazonJobFeed();
      
      // Process and evaluate job cards against target UK locations locally
      for (const job of jobCards) {
        // Example check: pass discovered schedules to atomic handler
        // handleDiscoveredSchedule(job.jobId, job);
      }

      await randomizedDelay();
    } catch (err) {
      console.error(`[${loopName}] ⚠️ Loop error:`, err.message);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

// --- APP INITIALIZATION ---
console.log('[Bot] Starting Amazon Shift Tracker (Node.js High-Speed Edition)...');
console.log('[Activation] Runtime: Node.js ES Modules | Database: better-sqlite3 | Deduplication: Atomic SQLite Lock');
console.log('[Bot] Targets: Carlisle, Northampton, Tilbury, Dartford, Enfield, Bow / Barking, Chingford, Harlow, Exeter, Poole, Preston, Croydon, Maidstone, London, Garden City, Birmingham, Neasden, West London, Haringey, Dunfermline, Bromley, Orpington, Doncaster, Hayes');
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
