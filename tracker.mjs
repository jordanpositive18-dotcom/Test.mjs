import { ProxyAgent, fetch } from 'undici';
import Database from 'better-sqlite3';

// 1. Environment & Config
const url = 'https://www.jobsatamazon.co.uk/graphql';
let auth = process.env.AMAZON_AUTHORIZATION;
let proxyUrl = process.env.AMAZON_PROXY_URL;
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TG_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

if (!auth || !proxyUrl) {
  console.error('❌ Missing required environment variables (AMAZON_AUTHORIZATION or AMAZON_PROXY_URL)');
  process.exit(1);
}

// Clean proxy URL
proxyUrl = proxyUrl.replace(/["']/g, '').trim();
const spaceIdx = proxyUrl.indexOf(' ');
if (spaceIdx !== -1) {
    proxyUrl = proxyUrl.substring(0, spaceIdx);
}

if (!auth.startsWith('Bearer ') && !auth.startsWith('Basic ')) {
    auth = `Bearer ${auth}`;
}

const dispatcher = new ProxyAgent(proxyUrl);
const CHECK_INTERVAL_MS = 60 * 1000; // Check every 60 seconds

// 2. Initialize SQLite Database for Persistent Deduplication
const db = new Database('database.sqlite');
db.exec(`
  CREATE TABLE IF NOT EXISTS seen_schedules (
    id TEXT PRIMARY KEY,
    discovered_at TEXT
  )
`);

const checkStmt = db.prepare('SELECT id FROM seen_schedules WHERE id = ?');
const insertStmt = db.prepare('INSERT INTO seen_schedules (id, discovered_at) VALUES (?, ?)');

// 3. Telegram Message Formatter matching your exact layout
function formatShiftAlert(shift) {
  return `1️⃣ 📍 ${shift.location}
📮 Postcode: ${shift.postcode}
💰 Pay: £${shift.pay} /hr
📅 Start Date: ${shift.startDate}
📆 Working Days: ${shift.workingDays}
⏰ Shift Time: ${shift.shiftTime}
🕐 Hours/Week: ${shift.hoursPerWeek}`;
}

// 4. Telegram Notification Sender
async function sendTelegramAlert(message) {
  if (!TG_TOKEN || !TG_CHAT_ID) {
    console.log('📱 [Telegram Alert (Mock - Credentials missing in Railway Variables)]:\n', message);
    return;
  }
  try {
    const tgUrl = `https://api.telegram.org/bot${TG_TOKEN}/sendMessage`;
    const res = await fetch(tgUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TG_CHAT_ID, text: message })
    });
    if (res.ok) {
      console.log('✅ Telegram alert sent successfully!');
    } else {
      const errText = await res.text();
      console.error(`⚠️ Telegram API returned status ${res.status}:`, errText);
    }
  } catch (err) {
    console.error('⚠️ Failed to send Telegram notification:', err.message);
  }
}

// 5. Core Scanning Function
async function scanShifts() {
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Origin': 'https://www.jobsatamazon.co.uk',
        'Referer': 'https://www.jobsatamazon.co.uk/',
      },
      body: JSON.stringify({ query: '{ __typename }' }),
      dispatcher,
    });

    console.log(`[${new Date().toISOString()}] Scan status: ${res.status}`);

    // Even if status fluctuates, you can add your schedule parser here:
    // const discoveredSchedules = parseSchedulesFromResponse(data);
    // for (const shift of discoveredSchedules) {
    //   const exists = checkStmt.get(shift.id);
    //   if (!exists) {
    //     insertStmt.run(shift.id, new Date().toISOString());
    //     const message = formatShiftAlert(shift);
    //     await sendTelegramAlert(message);
    //   }
    // }

  } catch (error) {
    console.error('❌ Error during scan cycle:', error.message);
  }
}

// 6. Continuous Loop Execution
console.log('🚀 Starting 24/7 Amazon Shift Tracker on Railway...');

// Send an immediate test message on startup to verify Telegram is working!
await sendTelegramAlert("🚀 *Amazon Tracker Deployed Successfully!*\nTesting your custom alert format:\n\n" + formatShiftAlert({
  location: "London, England (Dartford area) DA17 6AS",
  postcode: "DA17 6AS",
  pay: "16.10",
  startDate: "21 Nov 2026",
  workingDays: "Sat, Sun, Mon, Tue, Wed",
  shiftTime: "7:00 - 13:30",
  hoursPerWeek: "30"
}));

await scanShifts();
setInterval(scanShifts, CHECK_INTERVAL_MS);
