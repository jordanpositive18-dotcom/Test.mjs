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

test('only newly inserted schedule IDs enter the individual Telegram outbox', async (t) => {
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
  assert.match(alert.message, /Amazon Shift Alert/);
  assert.match(alert.message, /https:\/\/www\.jobsatamazon\.co\.uk\/app#\/jobDetail\?jobId=JOB-1/);
  assert.equal(alert.notification_key, 'S-NEW');

  const uniqueIndex = db.prepare(`
    PRAGMA index_list('telegram_outbox')
  `).all().find((index) => index.name === 'telegram_outbox_notification_key_unique');
  assert.equal(uniqueIndex?.unique, 1);
});

test('multiple new schedules create one detailed message per schedule', async (t) => {
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
  const alerts = harness.api.db.prepare('SELECT * FROM telegram_outbox ORDER BY id').all();

  assert.equal(result.count, 2);
  assert.equal(alerts.length, 2);
  assert.deepEqual(alerts.map((alert) => alert.schedule_id), ['S-1', 'S-2']);
  assert.deepEqual(alerts.map((alert) => JSON.parse(alert.schedule_ids)), [['S-1'], ['S-2']]);
  assert.ok(alerts.every((alert) => alert.message.includes('Amazon Shift Alert')));
  assert.ok(alerts.every((alert) => alert.message.includes('https://www.jobsatamazon.co.uk/app#/jobDetail?jobId=JOB-1')));
});

test('schedule ownership is validated and missing Amazon fields use honest fallbacks', async (t) => {
  const sparseJob = { jobId: 'JOB-SPARSE' };
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: {
        searchScheduleCards: {
          scheduleCards: [
            { scheduleId: 'S-VALID', jobId: 'JOB-SPARSE' },
            { scheduleId: 'S-WRONG-PARENT', jobId: 'JOB-OTHER', scheduleText: 'Wrong job' },
            { jobId: 'JOB-SPARSE', scheduleText: 'No schedule ID' },
          ],
          nextToken: null,
        },
      },
    }),
  });

  const result = await harness.api.processDiscoveredJob(sparseJob, new Date().toISOString());
  const alerts = harness.api.db.prepare('SELECT * FROM telegram_outbox').all();
  assert.equal(result.count, 1);
  assert.equal(alerts.length, 1);
  assert.deepEqual(JSON.parse(alerts[0].schedule_ids), ['S-VALID']);
  for (const fallback of [
    'Location: Not supplied',
    'Postcode: Not supplied',
    'Job title: Not supplied',
    'Contract type: Not supplied',
    'Pay rate/range: Not supplied',
    'First day: Not supplied',
    'Schedule days and times: Not supplied',
    'Hours per week: Not supplied',
  ]) {
    assert.ok(alerts[0].message.includes(fallback), `missing fallback ${fallback}`);
  }
  assert.doesNotMatch(alerts[0].message, /15\.10|20 hours|Upcoming|Working Days/);
  assert.match(alerts[0].message, /jobDetail\?jobId=JOB-SPARSE/);
});

test('Telegram worker cycles do not overlap while a request is in flight', async (t) => {
  let resolveTelegram;
  let signalStarted;
  const started = new Promise((resolve) => { signalStarted = resolve; });
  const responsePending = new Promise((resolve) => { resolveTelegram = resolve; });
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: { searchScheduleCards: { scheduleCards: [schedule('S-WORKER', 'Night shift')], nextToken: null } },
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
      data: { searchScheduleCards: { scheduleCards: [schedule('S-FAIL', 'Weekend shift')], nextToken: null } },
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

test('Telegram HTTP 429 honors Retry-After and keeps the alert pending', async (t) => {
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: { searchScheduleCards: { scheduleCards: [schedule('S-429', 'Day shift')], nextToken: null } },
    }),
    telegramResponder: async () => mockResponse(
      { ok: false, description: 'rate limited' },
      429,
      { 'Retry-After': '75' },
    ),
  });
  await harness.api.processDiscoveredJob(job, new Date().toISOString());
  harness.api.startTelegramWorker();
  await harness.intervals[0].callback();

  const outbox = harness.api.db.prepare('SELECT * FROM telegram_outbox').get();
  assert.equal(outbox.status, 'pending');
  assert.equal(outbox.attempt_count, 1);
  assert.ok(Date.parse(outbox.next_attempt_at) >= Date.now() + 74000);
  assert.ok(Date.parse(outbox.next_attempt_at) <= Date.now() + 75000);
  assert.equal(harness.telegramCalls.length, 1);
});

test('Telegram server errors remain pending with capped exponential retry', async (t) => {
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: { searchScheduleCards: { scheduleCards: [schedule('S-500', 'Evening shift')], nextToken: null },
      },
    }),
    telegramResponder: async () => mockResponse({ ok: false }, 500),
  });
  await harness.api.processDiscoveredJob(job, new Date().toISOString());
  harness.api.startTelegramWorker();
  const runCycle = harness.intervals[0].callback;

  for (let attempt = 0; attempt < 4; attempt += 1) {
    harness.api.db.prepare(
      "UPDATE telegram_outbox SET next_attempt_at = '2000-01-01T00:00:00.000Z' WHERE id = 1",
    ).run();
    await runCycle();
  }
  const outbox = harness.api.db.prepare('SELECT * FROM telegram_outbox').get();
  assert.equal(outbox.status, 'pending');
  assert.equal(outbox.attempt_count, 4);
  assert.ok(outbox.next_attempt_at);
});

test('Telegram delivery continues independently while Amazon is in cooldown', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 403),
    scheduleResponder: async () => mockResponse({
      data: { searchScheduleCards: { scheduleCards: [schedule('S-DURING-COOLDOWN', 'Day shift')], nextToken: null },
      },
    }),
  });
  await harness.api.processDiscoveredJob(job, new Date().toISOString());
  await harness.api.fetchAmazonJobFeed('Loop-1');
  assert.equal(harness.api.getAmazonProtectionState().blocked, true);

  harness.api.startTelegramWorker();
  await harness.intervals[0].callback();

  assert.equal(harness.telegramCalls.length, 1);
  assert.equal(harness.api.db.prepare('SELECT status FROM telegram_outbox').get().status, 'sent');
});

test('failed network delivery is recorded and retried only after backoff', async (t) => {
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      data: { searchScheduleCards: { scheduleCards: [schedule('S-NET', 'Day shift')], nextToken: null } },
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
          scheduleCards: [{ ...schedule('S-TIMESTAMP', 'Timestamped shift'), jobId: 'JOB-TIMESTAMP' }],
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
