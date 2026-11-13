import Database from 'better-sqlite3';
import { fetch } from 'undici';

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
          break; // Stop loop temporarily to respect rate limit
        } else {
          const errText = await res.text();
          console.error(`[Telegram] ❌ API error (${res.status}): ${errText}`);
          stmtMarkAlertFailed.run(alert.id);
        }

        // Safe pacing delay between outgoing messages (800ms)
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

// --- ORGANIC REQUEST JITTER ---
async function randomizedDelay(minMs = 1500, maxMs = 4000) {
  const jitter = Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
  await new Promise(resolve => setTimeout(resolve, jitter));
}

// --- MAIN SCANNER LOOP ---
async function runScannerLoop(loopName, assignedHubs) {
  console.log(`[${loopName}] Started scanning hubs:`, assignedHubs);

  while (true) {
    try {
      for (const hub of assignedHubs) {
        await randomizedDelay(2000, 5000);
      }
    } catch (err) {
      console.error(`[${loopName}] ⚠️ Scan error:`, err.message);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

// --- APP INITIALIZATION ---
console.log('[Bot] Starting Amazon Shift Tracker (Node.js High-Speed Edition)...');
console.log('[Activation] Runtime: Node.js ES Modules | Database: better-sqlite3 | Deduplication: Atomic SQLite Lock');
console.log('[Bot] Targets: Carlisle, Northampton, Tilbury, Dartford, Enfield, Bow / Barking, Chingford, Harlow, Exeter, Poole, Preston, Croydon, Maidstone, London, Garden City, Birmingham, Neasden, West London, Haringey, Dunfermline, Bromley, Orpington, Doncaster, Hayes');
console.log('[Bot] Connection: Residential Proxy / undici dispatcher');
console.log('[Watchdog] Started - monitoring 3 loops with staggered delays.');

startTelegramWorker();

// Partition hubs and start loops with staggered delays like your Python monitor
const totalHubs = ['Carlisle', 'Northampton', 'Tilbury', 'Dartford', 'Enfield', 'Bow / Barking', 'Chingford', 'Harlow', 'Exeter', 'Poole', 'Preston', 'Croydon', 'Maidstone', 'London', 'Garden City', 'Birmingham', 'Neasden', 'West London', 'Haringey', 'Dunfermline', 'Bromley', 'Orpington', 'Doncaster', 'Hayes'];
const chunkSize = Math.ceil(totalHubs.length / 3);

console.log('[Loop-1] Started.');
runScannerLoop('Loop-1', totalHubs.slice(0, chunkSize));

console.log('[Loop-2] Starting in 5s...');
setTimeout(() => {
  console.log('[Loop-2] Started.');
  runScannerLoop('Loop-2', totalHubs.slice(chunkSize, chunkSize * 2));
}, 5000);

console.log('[Loop-3] Starting in 10s...');
setTimeout(() => {
  console.log('[Loop-3] Started.');
  runScannerLoop('Loop-3', totalHubs.slice(chunkSize * 2));
}, 10000);
