import { ProxyAgent, fetch } from 'undici';
import Database from 'better-sqlite3';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const AMAZON_GRAPHQL_URL = 'https://www.jobsatamazon.co.uk/graphql';
const UK_REGIONS = [
  "England", "Scotland", "Wales", "Northern Ireland",
  "London", "Manchester", "Birmingham", "Glasgow", "Edinburgh",
  "Cardiff", "Belfast", "Bristol", "Leeds", "Newcastle",
  "Sheffield", "Southampton", "Liverpool", "Nottingham", "Leicester"
];

export function createScanner(options = {}) {
const {
  fetchImpl = fetch,
  databasePath = 'database.sqlite',
  database: suppliedDatabase,
  dispatcher,
  auth = '',
  telegramToken,
  telegramChatId,
  regions = UK_REGIONS,
  sendTelegram: telegramOverride,
  sleep = (milliseconds) => new Promise(resolveSleep => setTimeout(resolveSleep, milliseconds)),
  logger = console,
} = options;
const url = options.url ?? AMAZON_GRAPHQL_URL;
const TG_TOKEN = telegramToken;
const TG_CHAT_ID = telegramChatId;
const db = suppliedDatabase ?? new Database(databasePath);
const wait = (milliseconds) => sleep(milliseconds);
const fetchAmazon = (requestUrl, requestOptions) => fetchImpl(
  requestUrl,
  dispatcher ? { ...requestOptions, dispatcher } : requestOptions
);

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
`);

const checkSeenStmt = db.prepare('SELECT id FROM seen_schedules WHERE id = ?');
const insertSeenStmt = db.prepare('INSERT INTO seen_schedules (id, discovered_at) VALUES (?, ?)');

const upsertJobStmt = db.prepare(`
  INSERT INTO job_queue (job_id, region, status, discovered_at) 
  VALUES (?, ?, 'PENDING', ?) 
  ON CONFLICT(job_id) DO UPDATE SET status = CASE WHEN job_queue.status = 'PROCESSED' THEN 'PROCESSED' ELSE 'PENDING' END
`);

const getPendingJobsStmt = db.prepare("SELECT job_id, region FROM job_queue WHERE status = 'PENDING' LIMIT 10");
const markJobProcessedStmt = db.prepare("UPDATE job_queue SET status = 'PROCESSED' WHERE job_id = ?");
const markJobFailedStmt = db.prepare("UPDATE job_queue SET status = 'FAILED' WHERE job_id = ?");

const insertFailedStmt = db.prepare('INSERT INTO failed_requests (task_type, payload, error, retry_count, last_tried) VALUES (?, ?, ?, ?, ?)');
const getFailedStmt = db.prepare('SELECT * FROM failed_requests WHERE retry_count < 3 LIMIT 5');
const updateFailedStmt = db.prepare('UPDATE failed_requests SET retry_count = retry_count + 1, last_tried = ? WHERE id = ?');
const deleteFailedStmt = db.prepare('DELETE FROM failed_requests WHERE id = ?');

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

async function sendTelegramAlert(message) {
  if (telegramOverride) {
    await telegramOverride(message);
    return;
  }
  if (!TG_TOKEN || !TG_CHAT_ID) {
    logger.log('📱 [Telegram Mock Alert]:\n', message);
    return;
  }
  try {
    const tgUrl = `https://api.telegram.org/bot${TG_TOKEN}/sendMessage`;
    const res = await fetchImpl(tgUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TG_CHAT_ID, text: message })
    });
    if (!res.ok) {
      logger.error(`⚠️ Telegram API returned status ${res.status}`);
    }
  } catch (err) {
    logger.error('⚠️ Failed to send Telegram notification:', err.message);
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

// ----------------------------------------------------
// LOOP 1: Nationwide Job Discovery & Pagination
// ----------------------------------------------------
async function runLoop1Discovery() {
  logger.log('🌍 [Loop 1] Starting nationwide UK job discovery scan across regions...');
  for (const region of regions) {
    try {
      const jobSearchQuery = `
        query SearchJobCardsByLocation($request: SearchJobRequest!) {
          searchJobCardsByLocation(request: $request) {
            jobId
            __typename
          }
        }
      `;

      const res = await fetchAmazon(url, {
        method: 'POST',
        headers: amazonHeaders,
        body: JSON.stringify({
          query: jobSearchQuery,
          variables: {
            request: {
              country: "GB",
              locale: "en_GB",
              query: region
            }
          }
        }),
        dispatcher,
      });

      if (res.status === 200) {
        const data = await res.json();
        const jobCards = data?.data?.searchJobCardsByLocation || [];
        const now = new Date().toISOString();

        let addedCount = 0;
        for (const job of jobCards) {
          if (job && job.jobId) {
            upsertJobStmt.run(job.jobId, region, now);
            addedCount++;
          }
        }
        logger.log(`🌍 [Loop 1] Region [${region}]: Found ${jobCards.length} jobs (${addedCount} queued/updated).`);
      } else {
        const errText = await res.text();
        logger.warn(`⚠️ [Loop 1] Region [${region}] returned status ${res.status}`);
        insertFailedStmt.run('LOOP1_DISCOVERY', JSON.stringify({ region }), `HTTP ${res.status}: ${errText}`, 0, new Date().toISOString());
      }
    } catch (error) {
      logger.error(`❌ [Loop 1 error in region ${region}]:`, error.message);
      insertFailedStmt.run('LOOP1_DISCOVERY', JSON.stringify({ region }), error.message, 0, new Date().toISOString());
    }

    // Controlled pacing between region requests
    await wait(3000);
  }
}

// ----------------------------------------------------
// LOOP 2: Schedule Processing & Detailed Retrieval
// ----------------------------------------------------
async function runLoop2Schedules() {
  logger.log('⚡ [Loop 2] Processing pending job queue for schedule details...');
  try {
    const pendingJobs = getPendingJobsStmt.all();
    if (pendingJobs.length === 0) {
      logger.log('⚡ [Loop 2] No pending jobs in queue.');
      return;
    }

    for (const job of pendingJobs) {
      try {
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

        const res = await fetchAmazon(url, {
          method: 'POST',
          headers: amazonHeaders,
          body: JSON.stringify({
            query: scheduleQuery,
            variables: {
              request: {
                jobId: job.job_id,
                country: "GB",
                locale: "en_GB"
              }
            }
          }),
          dispatcher,
        });

        if (res.status === 200) {
          const data = await res.json();
          const schedules = data?.data?.searchScheduleCards || [];

          for (const sch of schedules) {
            const shiftId = sch.scheduleId || `${job.job_id}-${sch.startTime || 'unknown'}`;
            const exists = checkSeenStmt.get(shiftId);

            if (!exists) {
              insertSeenStmt.run(shiftId, new Date().toISOString());

              const shiftInfo = {
                location: sch.location || `UK Location (${job.region})`,
                postcode: sch.postcode || "N/A",
                pay: sch.basePay || "16.10",
                startDate: sch.hireStartDate || "Upcoming",
                workingDays: sch.workingDays || "Flexible",
                shiftTime: `${sch.startTime || "07:00"} - ${sch.finishTime || "15:30"}`,
                hoursPerWeek: sch.hoursPerWeek || "30"
              };

              const alertMsg = formatShiftAlert(shiftInfo);
              await sendTelegramAlert(alertMsg);
              logger.log(`🚨 [Loop 2] New shift discovered & alerted! ID: ${shiftId}`);
            }
          }

          markJobProcessedStmt.run(job.job_id);
        } else {
          const errText = await res.text();
          logger.warn(`⚠️ [Loop 2] Job ${job.job_id} schedule fetch returned status ${res.status}`);
          markJobFailedStmt.run(job.job_id);
          insertFailedStmt.run('LOOP2_SCHEDULE', JSON.stringify({ jobId: job.job_id }), `HTTP ${res.status}: ${errText}`, 0, new Date().toISOString());
        }
      } catch (err) {
        logger.error(`❌ [Loop 2 error for job ${job.job_id}]:`, err.message);
        markJobFailedStmt.run(job.job_id);
        insertFailedStmt.run('LOOP2_SCHEDULE', JSON.stringify({ jobId: job.job_id }), err.message, 0, new Date().toISOString());
      }

      // Pacing between schedule requests
      await wait(2000);
    }
  } catch (error) {
    logger.error('❌ [Loop 2 fatal error]:', error.message);
  }
}

// ----------------------------------------------------
// LOOP 3: Error Recovery, Failed Requests & Safe Retries
// ----------------------------------------------------
async function runLoop3Recovery() {
  logger.log('🛡️ [Loop 3] Checking error logs and retrying failed tasks safely...');
  try {
    const failedTasks = getFailedStmt.all();
    if (failedTasks.length === 0) {
      logger.log('🛡️ [Loop 3] No failed tasks requiring retry.');
      return;
    }

    for (const task of failedTasks) {
      logger.log(`🛡️ [Loop 3] Retrying failed task #${task.id} (${task.task_type})...`);
      const payloadData = JSON.parse(task.payload || '{}');
      const now = new Date().toISOString();

      try {
        if (task.task_type === 'LOOP1_DISCOVERY') {
          const region = payloadData.region;
          const jobSearchQuery = `
            query SearchJobCardsByLocation($request: SearchJobRequest!) {
              searchJobCardsByLocation(request: $request) {
                jobId
                __typename
              }
            }
          `;
          const res = await fetchAmazon(url, {
            method: 'POST',
            headers: amazonHeaders,
            body: JSON.stringify({
              query: jobSearchQuery,
              variables: { request: { country: "GB", locale: "en_GB", query: region } }
            }),
            dispatcher
          });
          if (res.status === 200) {
            const data = await res.json();
            const jobCards = data?.data?.searchJobCardsByLocation || [];
            for (const job of jobCards) {
              if (job && job.jobId) upsertJobStmt.run(job.jobId, region, now);
            }
            deleteFailedStmt.run(task.id);
            logger.log(`✅ [Loop 3] Retry successful for region ${region}`);
          } else {
            updateFailedStmt.run(now, task.id);
          }
        } else if (task.task_type === 'LOOP2_SCHEDULE') {
          const jobId = payloadData.jobId;
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
          const res = await fetchAmazon(url, {
            method: 'POST',
            headers: amazonHeaders,
            body: JSON.stringify({
              query: scheduleQuery,
              variables: { request: { jobId, country: "GB", locale: "en_GB" } }
            }),
            dispatcher
          });
          if (res.status === 200) {
            deleteFailedStmt.run(task.id);
            logger.log(`✅ [Loop 3] Retry successful for job ${jobId}`);
          } else {
            updateFailedStmt.run(now, task.id);
          }
        }
      } catch (retryErr) {
        logger.error(`❌ [Loop 3 retry error for task ${task.id}]:`, retryErr.message);
        updateFailedStmt.run(now, task.id);
      }

      await wait(3000);
    }
  } catch (error) {
    logger.error('❌ [Loop 3 fatal error]:', error.message);
  }
}

async function start() {
  logger.log('🚀 Starting Nationwide UK Amazon Shift Tracker (Three-Loop Architecture)...');
  await sendTelegramAlert("🚀 *Nationwide UK Amazon Shift Tracker Online!*\nThree-Loop Architecture (Discovery + Schedule Processing + Error Recovery) initiated.");

  // Initial immediate run of Loop 1
  await runLoop1Discovery();

  // Start Loop 1 continuous cycle (every 60 seconds)
  setInterval(runLoop1Discovery, 60 * 1000);

  // Stagger Loop 2 start by 5 seconds
  setTimeout(() => {
    logger.log('⚡ Initializing Loop 2 (Schedule Processing)...');
    runLoop2Schedules();
    setInterval(runLoop2Schedules, 30 * 1000);
  }, 5000);

  // Stagger Loop 3 start by 10 seconds
  setTimeout(() => {
    logger.log('🛡️ Initializing Loop 3 (Error Recovery & Retries)...');
    runLoop3Recovery();
    setInterval(runLoop3Recovery, 90 * 1000);
  }, 10000);
}

return {
  db,
  runLoop1Discovery,
  runLoop2Schedules,
  runLoop3Recovery,
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
