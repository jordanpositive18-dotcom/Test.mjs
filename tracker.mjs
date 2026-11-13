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

// Clean proxy URL (handles accidental quotes or extra text)
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

// 2. Initialize SQLite Database for Deduplication
const db = new Database('database.sqlite');
db.exec(`
  CREATE TABLE IF NOT EXISTS seen_schedules (
    id TEXT PRIMARY KEY,
    discovered_at TEXT
  )
`);

const checkStmt = db.prepare('SELECT id FROM seen_schedules WHERE id = ?');
const insertStmt = db.prepare('INSERT INTO seen_schedules (id, discovered_at) VALUES (?, ?)');

// 3. Telegram Notification Helper
async function sendTelegramAlert(message) {
  if (!TG_TOKEN || !TG_CHAT_ID) {
    console.log('📱 [Telegram Alert - Mock]:', message);
    return;
  }
  try {
    const tgUrl = `https://api.telegram.org/bot${TG_TOKEN}/sendMessage`;
    await fetch(tgUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TG_CHAT_ID, text: message, parse_mode: 'Markdown' })
    });
  } catch (err) {
    console.error('⚠️ Failed to send Telegram notification:', err.message);
  }
}

// 4. Core Scanning Function
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
      // GraphQL payload querying jobs/schedules
      body: JSON.stringify({ 
        query: `query GetJobs { __typename }` 
      }),
      dispatcher,
    });

    console.log(`[${new Date().toISOString()}] Scan status: ${res.status}`);

    if (res.status === 200) {
      const data = await res.json();
      
      // TODO: Parse jobs and schedules from `data` response structure.
      // Example loop simulation for discovered schedules:
      // const discoveredSchedules = parseSchedulesFromResponse(data);
      // 
      // for (const schedule of discoveredSchedules) {
      //   const exists = checkStmt.get(schedule.id);
      //   if (!exists) {
      //     insertStmt.run(schedule.id, new Date().toISOString());
      //     await sendTelegramAlert(`🚨 **New Amazon Shift Found!**\nRole: ${schedule.title}\nDetails: ${schedule.details}`);
      //   }
      // }
    } else {
      console.warn(`⚠️ Received non-200 status code during scan: ${res.status}`);
    }
  } catch (error) {
    console.error('❌ Error during scan cycle:', error.message);
  }
}

// 5. Continuous Loop Execution
console.log('🚀 Starting 24/7 Amazon Shift Tracker on Railway...');
await scanShifts();
setInterval(scanShifts, CHECK_INTERVAL_MS);
