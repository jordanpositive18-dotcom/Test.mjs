import assert from 'node:assert/strict';
import test from 'node:test';
import { countRows, createScannerHarness, mockResponse } from './scanner-harness.mjs';

const job = {
  jobId: 'JOB-1',
  jobTitle: 'Warehouse Associate',
  locationName: 'London Delivery Station',
  postalCode: 'E1 6AN',
  totalPayRateMax: 17.25,
};

const schedule = (scheduleId, scheduleText) => ({
  scheduleId,
  jobId: job.jobId,
  firstDayOnSiteL10N: '1 December 2026',
  scheduleText,
  hoursPerWeek: '30',
});

test('only newly inserted schedule IDs enter one grouped Telegram alert', async (t) => {
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: {
        searchScheduleCards: {
          scheduleCards: [schedule('S-OLD', 'Existing shift'), schedule('S-NEW', 'New shift')],
          nextToken: null,
        },
      },
    }),
  });
  const { db } = harness.api;
  db.prepare(`
    INSERT INTO discovered_schedules (schedule_id, discovered_at, amazon_detected_at)
    VALUES (?, datetime('now'), ?)
  `).run('S-OLD', new Date().toISOString());

  const first = await harness.api.processDiscoveredJob(job, new Date().toISOString());
  const second = await harness.api.processDiscoveredJob(job, new Date().toISOString());

  assert.equal(first.count, 1);
  assert.deepEqual(JSON.parse(JSON.stringify(first.scheduleIds)), ['S-NEW']);
  assert.equal(second.count, 0);
  assert.equal(countRows(db, 'discovered_schedules'), 2);
  assert.equal(countRows(db, 'telegram_outbox'), 1);
  const legacyKey = db.prepare('SELECT notification_key FROM telegram_outbox').get().notification_key;
  const duplicate = db.prepare(`
    INSERT OR IGNORE INTO telegram_outbox
      (schedule_id, notification_key, schedule_ids, message, status, created_at)
    VALUES ('S-NEW', ?, '["S-NEW"]', 'duplicate alert', 'pending', datetime('now'))
  `).run(legacyKey);
  assert.equal(duplicate.changes, 0);

  const alert = db.prepare('SELECT * FROM telegram_outbox').get();
  assert.deepEqual(JSON.parse(alert.schedule_ids), ['S-NEW']);
  assert.match(alert.message, /New shift/);
  assert.doesNotMatch(alert.message, /Existing shift/);
  assert.equal((alert.message.match(/📋 Shift 1:/g) ?? []).length, 1);
  assert.equal((alert.message.match(/📋 Shift 2:/g) ?? []).length, 0);

  const uniqueIndex = db.prepare(`
    PRAGMA index_list('telegram_outbox')
  `).all().find((index) => index.name === 'telegram_outbox_notification_key_unique');
  assert.equal(uniqueIndex?.unique, 1);
});

test('multiple new schedules produce one grouped message, not one copy per schedule', async (t) => {
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: {
        searchScheduleCards: {
          scheduleCards: [schedule('S-1', 'Early shift'), schedule('S-2', 'Late shift')],
          nextToken: null,
        },
      },
    }),
  });

  const result = await harness.api.processDiscoveredJob(job, new Date().toISOString());
  const alerts = harness.api.db.prepare('SELECT * FROM telegram_outbox').all();

  assert.equal(result.count, 2);
  assert.equal(alerts.length, 1);
  assert.deepEqual(JSON.parse(alerts[0].schedule_ids), ['S-1', 'S-2']);
  assert.equal((alerts[0].message.match(/📋 Shift 1:/g) ?? []).length, 1);
  assert.equal((alerts[0].message.match(/📋 Shift 2:/g) ?? []).length, 1);
});

test('Telegram worker cycles do not overlap while a request is in flight', async (t) => {
  let resolveTelegram;
  let signalStarted;
  const started = new Promise((resolve) => { signalStarted = resolve; });
  const responsePending = new Promise((resolve) => { resolveTelegram = resolve; });
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: { searchScheduleCards: { scheduleCards: [schedule('S-WORKER', 'Night shift')] } },
    }),
    telegramResponder: async () => {
      signalStarted();
      return responsePending;
    },
  });
  await harness.api.processDiscoveredJob(job, new Date().toISOString());
  harness.api.startTelegramWorker();
  assert.equal(harness.intervals.length, 1);
  assert.equal(harness.intervals[0].milliseconds, 1000);

  const firstCycle = harness.intervals[0].callback();
  await started;
  await harness.intervals[0].callback();
  assert.equal(harness.telegramCalls.length, 1);
  resolveTelegram(mockResponse({ ok: true, result: { message_id: 1 } }));
  await firstCycle;

  assert.equal(harness.telegramCalls.length, 1);
  assert.equal(
    harness.api.db.prepare('SELECT status FROM telegram_outbox').get().status,
    'sent',
  );
});

test('HTTP success with Telegram ok:false is retried, never marked sent, then fails safely', async (t) => {
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: { searchScheduleCards: { scheduleCards: [schedule('S-FAIL', 'Weekend shift')] } },
    }),
    telegramResponder: async () => mockResponse({ ok: false, description: 'rejected' }),
  });
  await harness.api.processDiscoveredJob(job, new Date().toISOString());
  harness.api.startTelegramWorker();
  const runCycle = harness.intervals[0].callback;

  await runCycle();
  let notification = harness.api.db.prepare('SELECT * FROM telegram_outbox').get();
  assert.equal(notification.status, 'pending');
  assert.equal(notification.attempt_count, 1);
  assert.equal(notification.last_error.includes('rejected'), true);
  assert.equal(
    harness.api.db.prepare('SELECT telegram_sent_at FROM discovered_schedules').get().telegram_sent_at,
    null,
  );

  for (let attempt = 0; attempt < 2; attempt += 1) {
    harness.api.db.prepare(
      "UPDATE telegram_outbox SET next_attempt_at = '2000-01-01T00:00:00.000Z' WHERE id = 1",
    ).run();
    await runCycle();
  }
  notification = harness.api.db.prepare('SELECT * FROM telegram_outbox').get();
  assert.equal(notification.status, 'failed');
  assert.equal(notification.attempt_count, 3);
  const attempts = harness.telegramCalls.length;
  await runCycle();
  assert.equal(harness.telegramCalls.length, attempts);
});

test('failed network delivery is recorded and retried only after backoff', async (t) => {
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: { searchScheduleCards: { scheduleCards: [schedule('S-NET', 'Day shift')] } },
    }),
    telegramResponder: async () => {
      throw new Error('mock network failure');
    },
  });
  await harness.api.processDiscoveredJob(job, new Date().toISOString());
  harness.api.startTelegramWorker();
  const runCycle = harness.intervals[0].callback;

  await runCycle();
  const notification = harness.api.db.prepare('SELECT * FROM telegram_outbox').get();
  assert.equal(notification.status, 'pending');
  assert.equal(notification.attempt_count, 1);
  assert.match(notification.last_error, /mock network failure/);
  assert.ok(Date.parse(notification.next_attempt_at) > Date.now());

  await runCycle();
  assert.equal(harness.telegramCalls.length, 1);
});

test('job discovery, queue, and Telegram timestamps are ISO 8601 and latencies parse correctly', async (t) => {
  const logs = [];
  const harness = createScannerHarness(t, {
    prepareDatabase(db) {
      db.exec(`
        CREATE TABLE discovered_schedules (
          schedule_id TEXT PRIMARY KEY,
          discovered_at TEXT,
          amazon_detected_at TEXT,
          notification_queued_at TEXT,
          telegram_sent_at TEXT
        );
        INSERT INTO discovered_schedules (
          schedule_id, discovered_at, amazon_detected_at,
          notification_queued_at, telegram_sent_at
        ) VALUES (
          'S-OLD-TIMES', '2026-11-14 12:00:00', '2026-11-14 12:00:01',
          '2026-11-14 12:00:02', '2026-11-14 12:00:03'
        );
      `);
    },
    jobResponder: async () => mockResponse({
      data: {
        searchJobCardsByLocation: {
          jobCards: [{ ...job, jobId: 'JOB-TIMESTAMP' }],
          nextToken: null,
        },
      },
    }),
    scheduleResponder: async () => mockResponse({
      data: {
        searchScheduleCards: {
          scheduleCards: [schedule('S-TIMESTAMP', 'Timestamped shift')],
          nextToken: null,
        },
      },
    }),
    logger: {
      log: (...values) => logs.push(values.join(' ')),
      warn() {},
      error() {},
    },
  });

  const feed = await harness.api.fetchAmazonJobFeed();
  const discoveredJob = feed.jobCards[0];
  const detectedAt = discoveredJob._scannerDetectedAt;
  assert.match(detectedAt, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
  await harness.api.processDiscoveredJob(discoveredJob, detectedAt);

  const detectedRecord = harness.api.db.prepare(
    "SELECT * FROM discovered_schedules WHERE schedule_id = 'S-TIMESTAMP'",
  ).get();
  assert.equal(detectedRecord.discovered_at, detectedAt);
  assert.equal(detectedRecord.amazon_detected_at, detectedAt);
  assert.match(detectedRecord.notification_queued_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);

  harness.api.startTelegramWorker();
  await harness.intervals[0].callback();
  const sentRecord = harness.api.db.prepare(
    "SELECT * FROM discovered_schedules WHERE schedule_id = 'S-TIMESTAMP'",
  ).get();
  assert.match(sentRecord.telegram_sent_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);

  const detectedMs = Date.parse(sentRecord.amazon_detected_at);
  const queuedMs = Date.parse(sentRecord.notification_queued_at);
  const sentMs = Date.parse(sentRecord.telegram_sent_at);
  assert.ok(queuedMs >= detectedMs);
  assert.ok(sentMs >= queuedMs);
  assert.ok(logs.some((line) => line.includes(
    `Discovery->Queue: ${queuedMs - detectedMs}ms | Queue->Telegram: ${sentMs - queuedMs}ms | Total Pipeline: ${sentMs - detectedMs}ms`,
  )));

  const migrated = harness.api.db.prepare(
    "SELECT * FROM discovered_schedules WHERE schedule_id = 'S-OLD-TIMES'",
  ).get();
  assert.equal(migrated.discovered_at, '2026-11-14T12:00:00.000Z');
  assert.equal(migrated.amazon_detected_at, '2026-11-14T12:00:01.000Z');
  assert.equal(migrated.notification_queued_at, '2026-11-14T12:00:02.000Z');
  assert.equal(migrated.telegram_sent_at, '2026-11-14T12:00:03.000Z');
});
