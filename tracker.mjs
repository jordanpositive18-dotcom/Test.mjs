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

// 5. Core Scanning & Parsing Logic
async function scanShifts() {
  try {
    // Step A: Search for job cards by location
    const jobSearchQuery = `
      query SearchJobCardsByLocation($request: SearchJobRequest!) {
        searchJobCardsByLocation(request: $request) {
          jobId
          __typename
        }
      }
    `;

    const jobRes = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Origin': 'https://www.jobsatamazon.co.uk',
        'Referer': 'https://www.jobsatamazon.co.uk/',
      },
      body: JSON.stringify({
        query: jobSearchQuery,
        variables: {
          request: {
            country: "GB",
            locale: "en_GB",
            query: "Dartford" // Target location / query filter
          }
        }
      }),
      dispatcher,
    });

    console.log(`[${new Date().toISOString()}] Job Search Scan status: ${jobRes.status}`);

    if (jobRes.status === 200) {
      const jobData = await jobRes.json();
      const jobCards = jobData?.data?.searchJobCardsByLocation || [];
      console.log(`🔍 Found ${jobCards.length} job card(s). Fetching schedules...`);

      // Step B: For each job ID, fetch schedule cards
      for (const job of jobCards) {
        const jobId = job.jobId;
        if (!jobId) continue;

        const scheduleQuery = `
          query SearchScheduleCards($request: SearchScheduleRequest!) {
            searchScheduleCards(request: $request) {
              scheduleId
              hireStartDate
              basePay
              hoursPerWeek
              workingDays
              startTime
              finishTime
              location
              postcode
            }
          }
        `;

        const schedRes = await fetch(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': auth,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Origin': 'https://www.jobsatamazon.co.uk',
            'Referer': 'https://www.jobsatamazon.co.uk/',
          },
          body: JSON.stringify({
            query: scheduleQuery,
            variables: {
              request: {
                jobId: jobId,
                country: "GB",
                locale: "en_GB"
              }
            }
          }),
          dispatcher,
        });

        if (schedRes.status === 200) {
          const schedData = await schedRes.json();
          const schedules = schedData?.data?.searchScheduleCards || [];

          for (const sch of schedules) {
            const shiftId = sch.scheduleId || `${jobId}-${sch.startTime}`;
            
            // Check SQLite deduplication
            const exists = checkStmt.get(shiftId);
            if (!exists) {
              // Save to SQLite so we never alert twice for the same shift
              insertStmt.run(shiftId, new Date().toISOString());

              // Map properties to your exact template format
              const shiftInfo = {
                location: sch.location || "London, England (Dartford area) DA17 6AS",
                postcode: sch.postcode || "DA17 6AS",
                pay: sch.basePay || "16.10",
                startDate: sch.hireStartDate || "21 Nov 2026",
                workingDays: sch.workingDays || "Sat, Sun, Mon, Tue, Wed",
                shiftTime: `${sch.startTime || "7:00"} - ${sch.finishTime || "13:30"}`,
                hoursPerWeek: sch.hoursPerWeek || "30"
              };

              const message = formatShiftAlert(shiftInfo);
              await sendTelegramAlert(message);
              console.log(`🚨 New shift detected and alert sent for ID: ${shiftId}`);
            }
          }
        }
      }
    } else {
      const errText = await jobRes.text();
      console.warn(`⚠️ Non-200 status code during job scan: ${jobRes.status}`, errText);
    }
  } catch (error) {
    console.error('❌ Error during scan cycle:', error.message);
  }
}

// 6. Continuous Loop Execution
console.log('🚀 Starting 24/7 Live Amazon Shift Tracker on Railway...');

// Optional startup test message to verify Telegram connectivity
await sendTelegramAlert("🚀 *Amazon Live Shift Tracker Online!*\nScanning every 60 seconds...");

await scanShifts();
setInterval(scanShifts, CHECK_INTERVAL_MS);
