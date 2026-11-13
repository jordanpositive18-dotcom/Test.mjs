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
          console.warn('⚠️ Telegram credentials missing. Skipping dispatch.');
          stmtMarkAlertFailed.run(alert.id);
          continue;
        }

        const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: TELEGRAM_CHAT_ID,
            text: alert.message,
            parse_mode: 'HTML'
          })
        });

        if (res.ok) {
          stmtMarkAlertSent.run(alert.id);
        } else if (res.status === 429) {
          console.warn('⚠️ Telegram HTTP 429 rate limit hit. Backing off...');
          break; // Stop loop temporarily to respect rate limit
        } else {
          const errText = await res.text();
          console.error(`❌ Telegram API error (${res.status}): ${errText}`);
          stmtMarkAlertFailed.run(alert.id);
        }

        // Safe pacing delay between outgoing messages
        await new Promise(resolve => setTimeout(resolve, 800));
      }
    } catch (err) {
      console.error('❌ Telegram worker exception:', err.message);
    }
  }, 1000);
}

// --- CORE DISCOVERY & ATOMIC CLAIM HANDLER ---
function handleDiscoveredSchedule(scheduleId, jobDetails) {
  // ATOMIC LOCK: Tries to insert into SQLite. 
  // result.changes === 1 -> YOU claimed it first.
  // result.changes === 0 -> Another loop already found it. Ignore!
  const result = stmtClaimSchedule.run(scheduleId);

  if (result.changes === 1) {
    const message = `🚨 <b>New Shift Discovered!</b>\n` +
                    `• Job ID: <code>${jobDetails.jobId || 'N/A'}</code>\n` +
                    `• Schedule ID: <code>${scheduleId}</code>\n` +
                    `• Location: ${jobDetails.location || 'UK Hub'}\n` +
                    `• Date/Time: ${jobDetails.startTime || 'Flash Shift'}`;

    stmtEnqueueTelegram.run(message);
    console.log(`[DEDUP] Claimed and queued alert for schedule: ${scheduleId}`);
  }
}

// --- ORGANIC REQUEST JITTER (Prevents Amazon HTTP 403 Blocks) ---
async function randomizedDelay(minMs = 1500, maxMs = 4000) {
  const jitter = Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
  await new Promise(resolve => setTimeout(resolve, jitter));
}

// --- MAIN SCANNER LOOP SIMULATION ---
async function runScannerLoop(loopName, assignedHubs) {
  console.log(`🚀 [${loopName}] Started scanning hubs:`, assignedHubs);

  while (true) {
    try {
      // Simulate scanning hubs with organic request jitter
      for (const hub of assignedHubs) {
        await randomizedDelay(2000, 5000);
      }
    } catch (err) {
      console.error(`⚠️ [${loopName}] Scan error:`, err.message);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

// --- APP INITIALIZATION ---
console.log('🌟 Initializing High-Performance UK Shift Tracker...');
startTelegramWorker();

// Partition 24 UK hub terms evenly across 3 concurrent loops to prevent overlap & 403 bursts
const totalHubs = Array.from({ length: 24 }, (_, i) => `HUB-UK-${i + 1}`);
const chunkSize = Math.ceil(totalHubs.length / 3);

runScannerLoop('loop1', totalHubs.slice(0, chunkSize));
runScannerLoop('loop2', totalHubs.slice(chunkSize, chunkSize * 2));
runScannerLoop('loop3', totalHubs.slice(chunkSize * 2));
