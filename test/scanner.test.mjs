import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import Database from 'better-sqlite3';
import test from 'node:test';
import { createScanner } from '../tracker.mjs';

const BASE_TIME = Date.parse('2026-11-13T12:00:00.000Z');
const DEFAULT_SCHEDULE = {
  scheduleId: 'SHIFT-1',
  hireStartDate: '2026-12-01',
  basePay: '17.25',
  hoursPerWeek: '30',
  workingDays: 'Monday-Friday',
  startTime: '07:00',
  finishTime: '15:30',
  location: 'London Warehouse',
  postcode: 'E1 6AN',
};
const QUIET_LOGGER = {
  log() {},
  warn() {},
  error() {},
};

function response(body, status = 200, headers = {}) {
  return {
    status,
    ok: status >= 200 && status < 300,
    headers: {
      get(name) {
        return headers[name.toLowerCase()] ?? null;
      },
    },
    async json() {
      return body;
    },
    async text() {
      return JSON.stringify(body);
    },
  };
}

function createContext(t, { initialTime = BASE_TIME } = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'amazon-shift-finder-test-'));
  const databasePath = join(directory, 'test.sqlite');
  const closeCallbacks = [];
  let currentTime = initialTime;

  t.after(() => {
    for (const close of closeCallbacks) close();
    rmSync(directory, { recursive: true, force: true });
  });

  return {
    databasePath,
    getTime: () => currentTime,
    setTime: (nextTime) => {
      currentTime = nextTime;
    },
    createFixture({
      amazonResponder = async () => response({
        data: { searchScheduleCards: [DEFAULT_SCHEDULE] },
      }),
      jobSearchResponder = async (_url, _options, scanner) => response({
        data: {
          searchJobCardsByLocation: {
            jobCards: scanner.db.prepare('SELECT job_id FROM job_queue').all()
              .map(({ job_id }) => ({ jobId: job_id })),
            nextToken: null,
          },
        },
      }),
      telegramResponder = async () => response({ ok: true, result: { message_id: 1 } }),
      telegramTimeoutMs = 15000,
      notificationBaseBackoffMs = 5000,
      notificationMaxBackoffMs = 60 * 60 * 1000,
      amazonPageSize = 20,
      amazonMaxConcurrentRequests = 3,
      amazonMaxRequestAttempts = 3,
      amazonForbiddenCooldownMs = 15 * 60 * 1000,
    } = {}) {
      const amazonCalls = [];
      const telegramCalls = [];
      const scanner = createScanner({
        databasePath,
        clock: () => currentTime,
        sleep: async () => {},
        logger: QUIET_LOGGER,
        amazonPageSize,
        amazonMaxConcurrentRequests,
        amazonMinimumRequestIntervalMs: 0,
        jobSearchTerms: ['London'],
        amazonMaxRequestAttempts,
        amazonForbiddenCooldownMs,
        telegramTimeoutMs,
        notificationBaseBackoffMs,
        notificationMaxBackoffMs,
        fetchImpl: async (url, options) => {
          const request = JSON.parse(options.body);
          amazonCalls.push({ url, options, operationName: request.operationName });
          return request.operationName === 'searchJobCardsByLocation'
            ? jobSearchResponder(url, options, scanner)
            : amazonResponder(url, options);
        },
        telegramTransport: async (message, options) => {
          telegramCalls.push({ message, options });
          return telegramResponder(message, options);
        },
      });
      let closed = false;
      const close = () => {
        if (!closed) {
          scanner.close();
          closed = true;
        }
      };
      closeCallbacks.push(close);
      return { scanner, amazonCalls, telegramCalls, close };
    },
  };
}

async function processJob(fixture, jobId, discoveredAt = 'test') {
  fixture.scanner.db.prepare(
    "INSERT INTO job_queue (job_id, region, status, discovered_at) VALUES (?, 'London', 'PENDING', ?)",
  ).run(jobId, discoveredAt);
  await fixture.scanner.runLoop2Schedules();
  await fixture.scanner.waitForNotificationDelivery();
}

function getNotification(scanner, scheduleId = DEFAULT_SCHEDULE.scheduleId) {
  return scanner.db.prepare(
    'SELECT * FROM telegram_notifications WHERE schedule_id = ?',
  ).get(scheduleId);
}

test('schedule and notification are saved atomically and preserve existing database records', async (t) => {
  const context = createContext(t);
  const legacyDb = new Database(context.databasePath);
  legacyDb.exec(`
    CREATE TABLE seen_schedules (id TEXT PRIMARY KEY, discovered_at TEXT);
    CREATE TABLE job_queue (
      job_id TEXT PRIMARY KEY,
      region TEXT,
      status TEXT DEFAULT 'PENDING',
      discovered_at TEXT
    );
    CREATE TABLE failed_requests (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_type TEXT,
      payload TEXT,
      error TEXT,
      retry_count INTEGER DEFAULT 0,
      last_tried TEXT
    );
  `);
  legacyDb.prepare(
    'INSERT INTO seen_schedules (id, discovered_at) VALUES (?, ?)',
  ).run('OLD-SHIFT', 'old-record');
  legacyDb.close();

  const fixture = context.createFixture({
    telegramResponder: async () => response({ ok: false, description: 'retry later' }),
  });
  assert.deepEqual(
    fixture.scanner.db.prepare('SELECT id, discovered_at FROM seen_schedules').all(),
    [{ id: 'OLD-SHIFT', discovered_at: 'old-record' }],
  );
  assert.equal(
    fixture.scanner.db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'telegram_notifications'").get().name,
    'telegram_notifications',
  );

  fixture.scanner.db.exec(`
    CREATE TRIGGER reject_notification_insert
    BEFORE INSERT ON telegram_notifications
    BEGIN
      SELECT RAISE(ABORT, 'test transaction rollback');
    END;
  `);
  await processJob(fixture, 'JOB-ROLLBACK');
  fixture.scanner.db.exec('DROP TRIGGER reject_notification_insert');

  assert.equal(
    fixture.scanner.db.prepare("SELECT COUNT(*) AS count FROM seen_schedules WHERE id = 'SHIFT-1'").get().count,
    0,
  );
  assert.equal(
    fixture.scanner.db.prepare("SELECT COUNT(*) AS count FROM telegram_notifications WHERE schedule_id = 'SHIFT-1'").get().count,
    0,
  );
  assert.deepEqual(
    fixture.scanner.db.prepare('SELECT id, discovered_at FROM seen_schedules').all(),
    [{ id: 'OLD-SHIFT', discovered_at: 'old-record' }],
  );
});

test('successful delivery preserves the exact alert format and records sent time and attempts', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture();

  await processJob(fixture, 'JOB-1');

  const notification = getNotification(fixture.scanner);
  assert.equal(notification.status, 'SENT');
  assert.equal(notification.attempt_count, 1);
  assert.equal(notification.sent_at, new Date(context.getTime()).toISOString());
  assert.equal(notification.last_error, null);
  assert.equal(fixture.telegramCalls.length, 1);
  assert.equal(
    fixture.telegramCalls[0].message,
    '1️⃣ 📍 London Warehouse\n' +
      '📮 Postcode: E1 6AN\n' +
      '💰 Pay: £17.25 /hr\n' +
      '📅 Start Date: 2026-12-01\n' +
      '📆 Working Days: Monday-Friday\n' +
      '⏰ Shift Time: 07:00 - 15:30\n' +
      '🕐 Hours/Week: 30',
  );
});

test('Loop 2 skips overlapping passes and accepts the next pass afterward', async (t) => {
  const context = createContext(t);
  let releaseFirstRequest;
  let signalFirstRequestStarted;
  const firstRequestGate = new Promise((resolveGate) => {
    releaseFirstRequest = resolveGate;
  });
  const firstRequestStarted = new Promise((resolveStarted) => {
    signalFirstRequestStarted = resolveStarted;
  });
  let amazonCallCount = 0;
  const fixture = context.createFixture({
    jobSearchResponder: async () => {
      amazonCallCount += 1;
      signalFirstRequestStarted();
      if (amazonCallCount === 1) await firstRequestGate;
      return response({
        data: {
          searchJobCardsByLocation: {
            jobCards: [],
            nextToken: null,
          },
        },
      });
    },
  });

  const firstPass = fixture.scanner.runLoop2Schedules();
  await firstRequestStarted;
  assert.equal(amazonCallCount, 1);

  await fixture.scanner.runLoop2Schedules();
  assert.equal(amazonCallCount, 1);

  releaseFirstRequest();
  await firstPass;
  await fixture.scanner.runLoop2Schedules();
  assert.equal(amazonCallCount, 2);
});

test('Loop 2 releases its guard after a processing error', async (t) => {
  const context = createContext(t);
  let scheduleCallCount = 0;
  const fixture = context.createFixture({
    amazonResponder: async () => {
      scheduleCallCount += 1;
      if (scheduleCallCount <= 3) throw new Error('mock schedule request failure');
      return response({ data: { searchScheduleCards: [] } });
    },
  });

  await processJob(fixture, 'JOB-ERROR');
  assert.equal(
    fixture.scanner.db.prepare(
      "SELECT status FROM job_queue WHERE job_id = 'JOB-ERROR'",
    ).get().status,
    'FAILED',
  );

  await processJob(fixture, 'JOB-RECOVERS');
  assert.equal(scheduleCallCount, 4);
  assert.equal(
    fixture.scanner.db.prepare(
      "SELECT status FROM job_queue WHERE job_id = 'JOB-RECOVERS'",
    ).get().status,
    'PROCESSED',
  );
});

test('HTTP errors and Telegram ok:false remain pending and are not marked sent', async (t) => {
  const cases = [
    {
      name: 'HTTP error even when the JSON says ok',
      telegramResponder: async () => response({ ok: true }, 503),
      expectedError: /HTTP 503/,
    },
    {
      name: 'HTTP success with Telegram ok:false',
      telegramResponder: async () => response({ ok: false, description: 'chat unavailable' }),
      expectedError: /chat unavailable/,
    },
  ];

  for (const scenario of cases) {
    await t.test(scenario.name, async (subtest) => {
      const context = createContext(subtest);
      const fixture = context.createFixture({
        telegramResponder: scenario.telegramResponder,
      });
      await processJob(fixture, `JOB-${scenario.name}`);

      const notification = getNotification(fixture.scanner);
      assert.equal(notification.status, 'PENDING');
      assert.equal(notification.attempt_count, 1);
      assert.equal(notification.sent_at, null);
      assert.match(notification.last_error, scenario.expectedError);
    });
  }
});

test('network errors and request timeouts remain queued for retry', async (t) => {
  await t.test('network rejection', async (subtest) => {
    const context = createContext(subtest);
    const fixture = context.createFixture({
      telegramResponder: async () => {
        throw new Error('mock network reset');
      },
    });
    await processJob(fixture, 'JOB-NETWORK');

    const notification = getNotification(fixture.scanner);
    assert.equal(notification.status, 'PENDING');
    assert.equal(notification.sent_at, null);
    assert.match(notification.last_error, /mock network reset/);
  });

  await t.test('timeout signal', async (subtest) => {
    const context = createContext(subtest);
    const fixture = context.createFixture({
      telegramTimeoutMs: 5,
      telegramResponder: async (_message, { signal }) => new Promise((_resolve, reject) => {
        if (signal.aborted) {
          reject(signal.reason);
          return;
        }
        signal.addEventListener('abort', () => reject(signal.reason), { once: true });
      }),
    });
    await processJob(fixture, 'JOB-TIMEOUT');

    const notification = getNotification(fixture.scanner);
    assert.equal(notification.status, 'PENDING');
    assert.equal(notification.sent_at, null);
    assert.ok(notification.last_error);
  });
});

test('retry delay doubles to its cap and notifications are never discarded', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({
    notificationMaxBackoffMs: 20000,
    telegramResponder: async () => response({ ok: false, description: 'offline' }),
  });
  await processJob(fixture, 'JOB-BACKOFF');

  let notification = getNotification(fixture.scanner);
  assert.equal(notification.attempt_count, 1);
  assert.equal(Date.parse(notification.next_attempt_at) - context.getTime(), 5000);

  for (const expectedDelay of [10000, 20000, 20000]) {
    context.setTime(Date.parse(notification.next_attempt_at));
    await fixture.scanner.runNotificationDelivery();
    notification = getNotification(fixture.scanner);
    assert.equal(notification.status, 'PENDING');
    assert.equal(notification.sent_at, null);
    assert.equal(
      Date.parse(notification.next_attempt_at) - context.getTime(),
      expectedDelay,
    );
  }

  assert.equal(notification.attempt_count, 4);
  assert.equal(
    fixture.scanner.db.prepare('SELECT COUNT(*) AS count FROM telegram_notifications').get().count,
    1,
  );
});

test('pending notifications survive a restart and are delivered by the next process', async (t) => {
  const context = createContext(t);
  const firstProcess = context.createFixture({
    telegramResponder: async () => response({ ok: false, description: 'temporary failure' }),
  });
  await processJob(firstProcess, 'JOB-RESTART');
  const pending = getNotification(firstProcess.scanner);
  assert.equal(pending.status, 'PENDING');
  firstProcess.close();

  context.setTime(Date.parse(pending.next_attempt_at));
  const restartedProcess = context.createFixture();
  const result = await restartedProcess.scanner.runNotificationDelivery();

  assert.deepEqual(result, { sentCount: 1, failedCount: 0 });
  const delivered = getNotification(restartedProcess.scanner);
  assert.equal(delivered.status, 'SENT');
  assert.equal(delivered.attempt_count, 2);
  assert.ok(delivered.sent_at);
});

test('an expired claim left by a stopped process is recovered after restart', async (t) => {
  const context = createContext(t);
  const firstProcess = context.createFixture({
    telegramResponder: async () => response({ ok: false, description: 'temporary failure' }),
  });
  await processJob(firstProcess, 'JOB-EXPIRED-CLAIM');
  firstProcess.scanner.db.prepare(`
    UPDATE telegram_notifications
    SET status = 'CLAIMED',
        attempt_count = 2,
        claim_token = 'abandoned-worker',
        claim_expires_at = ?
    WHERE schedule_id = 'SHIFT-1'
  `).run(new Date(context.getTime() + 60000).toISOString());
  firstProcess.close();

  context.setTime(context.getTime() + 60000);
  const restartedProcess = context.createFixture();
  await restartedProcess.scanner.runNotificationDelivery();

  const recovered = getNotification(restartedProcess.scanner);
  assert.equal(recovered.status, 'SENT');
  assert.equal(recovered.attempt_count, 3);
  assert.ok(recovered.sent_at);
});

test('two workers cannot claim and send the same notification simultaneously', async (t) => {
  const context = createContext(t);
  const seeder = context.createFixture({
    telegramResponder: async () => response({ ok: false, description: 'retry' }),
  });
  await processJob(seeder, 'JOB-WORKERS');
  const pending = getNotification(seeder.scanner);
  seeder.close();
  context.setTime(Date.parse(pending.next_attempt_at));

  let releaseFirstWorker;
  const firstWorkerGate = new Promise((resolveGate) => {
    releaseFirstWorker = resolveGate;
  });
  let firstWorkerSends = 0;
  let secondWorkerSends = 0;
  const firstWorker = context.createFixture({
    telegramResponder: async () => {
      firstWorkerSends += 1;
      await firstWorkerGate;
      return response({ ok: true });
    },
  });
  const secondWorker = context.createFixture({
    telegramResponder: async () => {
      secondWorkerSends += 1;
      return response({ ok: true });
    },
  });

  const firstDelivery = firstWorker.scanner.runNotificationDelivery();
  assert.equal(firstWorkerSends, 1);
  const secondResult = await secondWorker.scanner.runNotificationDelivery();

  assert.deepEqual(secondResult, { sentCount: 0, failedCount: 0 });
  assert.equal(secondWorkerSends, 0);
  assert.equal(getNotification(secondWorker.scanner).status, 'CLAIMED');

  releaseFirstWorker();
  await firstDelivery;
  assert.equal(getNotification(firstWorker.scanner).status, 'SENT');
  assert.equal(getNotification(firstWorker.scanner).attempt_count, 2);
});

test('a repeated schedule creates only one notification record', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({
    telegramResponder: async () => response({ ok: false, description: 'retry' }),
  });

  await processJob(fixture, 'JOB-DUPLICATE-1');
  await processJob(fixture, 'JOB-DUPLICATE-2');

  assert.equal(
    fixture.scanner.db.prepare(
      "SELECT COUNT(*) AS count FROM telegram_notifications WHERE schedule_id = 'SHIFT-1'",
    ).get().count,
    1,
  );
  assert.equal(
    fixture.scanner.db.prepare(
      "SELECT COUNT(*) AS count FROM seen_schedules WHERE id = 'SHIFT-1'",
    ).get().count,
    1,
  );
});
