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
