import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createScannerHarness, mockResponse } from './scanner-harness.mjs';

function successfulJobResponse() {
  return mockResponse({
    data: { searchJobCardsByLocation: { jobCards: [], nextToken: null } },
  });
}

function protectionRow(db) {
  return db.prepare('SELECT reason, expires_at FROM amazon_request_protection WHERE id = 1').get();
}

test('successful Amazon responses complete normally and clear the timeout resource', async (t) => {
  const harness = createScannerHarness(t);

  const result = await harness.api.fetchAmazonJobFeed('Loop-1');

  assert.equal(result.scanStatus, 'COMPLETE');
  assert.equal(harness.amazonCalls.length, 1);
  assert.equal(harness.amazonCalls[0].options.signal.aborted, false);
  assert.equal(harness.requestTimeouts.length, 1);
  assert.equal(harness.requestTimeouts[0].cleared, true);
  assert.equal(protectionRow(harness.api.db), undefined);
});

test('30-second request timeout aborts cleanly and applies the shared transient cooldown', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async () => new Promise(() => {}),
  });

  const request = harness.api.fetchAmazonJobFeed('Loop-1');
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(harness.requestTimeouts[0]?.milliseconds, 30000);
  harness.requestTimeouts[0].callback();
  const result = await request;

  assert.equal(result.scanStatus, 'PARTIAL');
  assert.equal(harness.amazonCalls[0].options.signal.aborted, true);
  assert.equal(harness.requestTimeouts[0].cleared, true);
  assert.equal(protectionRow(harness.api.db).reason, 'request_timeout');
  assert.equal(harness.amazonCalls.length, 1);
  assert.equal((await harness.api.fetchAmazonJobFeed('Loop-2')).scanStatus, 'BLOCKED');
  assert.equal(harness.amazonCalls.length, 1);
});

test('HTTP 403 persists a shared 15-minute cooldown across all three loops and feeds', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 403),
  });
  const before = Date.now();

  const result = await harness.api.fetchAmazonJobFeed('Loop-1');
  const cooldown = protectionRow(harness.api.db);

  assert.equal(result.scanStatus, 'BLOCKED');
  assert.equal(cooldown.reason, 'http_403');
  assert.ok(Date.parse(cooldown.expires_at) - before >= 15 * 60 * 1000 - 100);
  assert.equal((await harness.api.fetchAmazonJobFeed('Loop-2')).scanStatus, 'BLOCKED');
  assert.equal((await harness.api.fetchAmazonJobFeed('Loop-3')).scanStatus, 'BLOCKED');
  assert.equal((await harness.api.fetchSchedulesForJob('JOB-1', 'Loop-2')).paginationComplete, false);
  assert.equal(harness.amazonCalls.length, 1);
});

test('HTTP 429 Retry-After seconds set the same shared cooldown', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 429, { 'Retry-After': '17' }),
  });
  const before = Date.now();

  await harness.api.fetchAmazonJobFeed('Loop-1');

  const expiry = Date.parse(protectionRow(harness.api.db).expires_at);
  assert.equal(protectionRow(harness.api.db).reason, 'http_429');
  assert.ok(expiry - before >= 16900 && expiry - before <= 17100);
  await harness.api.fetchSchedulesForJob('JOB-1', 'Loop-3');
  assert.equal(harness.amazonCalls.length, 1);
});

test('HTTP 429 Retry-After HTTP-date is respected', async (t) => {
  const retryAt = new Date(Date.now() + 120000);
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 429, { 'Retry-After': retryAt.toUTCString() }),
  });

  await harness.api.fetchAmazonJobFeed('Loop-1');

  const expiry = Date.parse(protectionRow(harness.api.db).expires_at);
  assert.ok(Math.abs(expiry - retryAt.getTime()) < 1100);
  assert.equal(protectionRow(harness.api.db).reason, 'http_429');
});

test('HTTP 429 without Retry-After uses the 60-second fallback', async (t) => {
  const before = Date.now();
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 429),
  });

  await harness.api.fetchAmazonJobFeed('Loop-1');

  const expiry = Date.parse(protectionRow(harness.api.db).expires_at);
  assert.equal(protectionRow(harness.api.db).reason, 'http_429');
  assert.ok(expiry - before >= 59900 && expiry - before <= 60100);
});

test('HTTP 5xx applies the shared 30-second cooldown', async (t) => {
  const before = Date.now();
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 503),
  });

  const result = await harness.api.fetchAmazonJobFeed('Loop-1');

  assert.equal(result.scanStatus, 'PARTIAL');
  assert.equal(protectionRow(harness.api.db).reason, 'http_503');
  assert.ok(Date.parse(protectionRow(harness.api.db).expires_at) - before >= 29900);
  await harness.api.fetchSchedulesForJob('JOB-1', 'Loop-3');
  assert.equal(harness.amazonCalls.length, 1);
});

test('network failure applies a shared cooldown and does not immediately retry', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async () => { throw new TypeError('mock network failure'); },
  });

  const result = await harness.api.fetchAmazonJobFeed('Loop-1');

  assert.equal(result.scanStatus, 'PARTIAL');
  assert.equal(protectionRow(harness.api.db).reason, 'network_error');
  assert.equal(harness.amazonCalls.length, 1);
  await harness.api.fetchAmazonJobFeed('Loop-2');
  assert.equal(harness.amazonCalls.length, 1);
});

test('HTTP 401 applies a protective pause and prevents repeat authentication attempts', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 401),
  });

  const first = await harness.api.fetchAmazonJobFeed('Loop-1');
  const second = await harness.api.fetchAmazonJobFeed('Loop-2');

  assert.equal(first.scanStatus, 'BLOCKED');
  assert.equal(second.scanStatus, 'BLOCKED');
  assert.equal(protectionRow(harness.api.db).reason, 'http_401_authentication');
  assert.equal(harness.amazonCalls.length, 1);
});

test('unexpired cooldown survives scanner process restart', async (t) => {
  const directory = mkdtempSync(join(tmpdir(), 'amazon-protection-restart-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const databasePath = join(directory, 'restart.sqlite');
  const firstProcess = createScannerHarness(t, {
    databasePath,
    jobResponder: async () => mockResponse({}, 403),
  });

  await firstProcess.api.fetchAmazonJobFeed('Loop-1');
  firstProcess.api.db.close();
  const restartedProcess = createScannerHarness(t, { databasePath });

  const result = await restartedProcess.api.fetchAmazonJobFeed('Loop-2');

  assert.equal(result.scanStatus, 'BLOCKED');
  assert.equal(restartedProcess.amazonCalls.length, 0);
  assert.equal(protectionRow(restartedProcess.api.db).reason, 'http_403');
});

test('cooldown-state database errors fail closed and resume only after state is recovered', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 403),
  });
  harness.api.db.pragma('query_only = ON');

  const initial = await harness.api.fetchAmazonJobFeed('Loop-1');
  const blockedWhileUnavailable = await harness.api.fetchAmazonJobFeed('Loop-2');

  assert.equal(initial.scanStatus, 'BLOCKED');
  assert.equal(blockedWhileUnavailable.scanStatus, 'BLOCKED');
  assert.equal(harness.amazonCalls.length, 1);

  harness.api.db.pragma('query_only = OFF');
  const recovered = await harness.api.fetchAmazonJobFeed('Loop-3');
  assert.equal(recovered.scanStatus, 'BLOCKED');
  assert.equal(protectionRow(harness.api.db).reason, 'http_403');
  assert.equal(harness.amazonCalls.length, 1);
});

test('requests attempted during cooldown are dropped, not queued for release at expiry', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async (_call, page) => (
      page === 1 ? mockResponse({}, 429, { 'Retry-After': '60' }) : successfulJobResponse()
    ),
  });

  await harness.api.fetchAmazonJobFeed('Loop-1');
  await harness.api.fetchAmazonJobFeed('Loop-1');
  await harness.api.fetchSchedulesForJob('JOB-1', 'Loop-2');
  await harness.api.fetchAmazonJobFeed('Loop-3');

  assert.equal(harness.amazonCalls.length, 1);
  assert.equal(harness.requestTimeouts.every((timeout) => timeout.cleared), true);
  assert.equal(harness.delays.length, 0);

  harness.api.db.prepare(
    "UPDATE amazon_request_protection SET expires_at = '2000-01-01T00:00:00.000Z' WHERE id = 1",
  ).run();
  await harness.api.fetchAmazonJobFeed('Loop-1');

  assert.equal(harness.amazonCalls.length, 2);
});

test('successful nationwide scans continue to use existing cadence and pagination logic', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async (_call, page) => mockResponse({
      data: {
        searchJobCardsByLocation: {
          jobCards: [],
          nextToken: page === 1 ? 'page-2' : null,
        },
      },
    }),
  });

  const result = await harness.api.fetchAmazonJobFeed('Loop-3');

  assert.equal(result.scanStatus, 'COMPLETE');
  assert.equal(harness.amazonCalls.length, 2);
  assert.equal(harness.amazonCalls[1].body.variables.searchJobRequest.nextToken, 'page-2');
  assert.equal(harness.delays.length, 0);
});

test('existing schedule and outbox records remain intact when a cooldown is recorded', async (t) => {
  const harness = createScannerHarness(t, {
    prepareDatabase(db) {
      db.exec(`
        CREATE TABLE discovered_schedules (
          schedule_id TEXT PRIMARY KEY, discovered_at TEXT, amazon_detected_at TEXT,
          notification_queued_at TEXT, telegram_sent_at TEXT
        );
        CREATE TABLE telegram_outbox (
          id INTEGER PRIMARY KEY AUTOINCREMENT, schedule_id TEXT, message TEXT NOT NULL,
          status TEXT DEFAULT 'pending', created_at TEXT
        );
      `);
      db.prepare(`
        INSERT INTO discovered_schedules (schedule_id, discovered_at)
        VALUES ('EXISTING-SCHEDULE', '2026-11-14T10:00:00.000Z')
      `).run();
      db.prepare(`
        INSERT INTO telegram_outbox (schedule_id, message, status, created_at)
        VALUES ('EXISTING-SCHEDULE', 'existing message', 'pending', '2026-11-14T10:00:00.000Z')
      `).run();
    },
    jobResponder: async () => mockResponse({}, 403),
  });

  await harness.api.fetchAmazonJobFeed('Loop-1');

  assert.equal(harness.api.db.prepare(
    "SELECT discovered_at FROM discovered_schedules WHERE schedule_id = 'EXISTING-SCHEDULE'",
  ).get().discovered_at, '2026-11-14T10:00:00.000Z');
  assert.equal(harness.api.db.prepare(
    "SELECT message FROM telegram_outbox WHERE schedule_id = 'EXISTING-SCHEDULE'",
  ).get().message, 'existing message');
});

test('Amazon request URL, GraphQL payloads, authorization and proxy routing remain unchanged', async (t) => {
  const harness = createScannerHarness(t, {
    env: {
      AMAZON_PROXY_URL: 'http://proxy.example.test:8080',
      AMAZON_AUTHORIZATION: 'offline-auth-placeholder',
    },
  });

  await harness.api.fetchAmazonJobFeed('Loop-1');
  await harness.api.fetchSchedulesForJob('JOB-KEEP', 'Loop-1');

  assert.equal(harness.amazonCalls.length, 2);
  for (const call of harness.amazonCalls) {
    assert.equal(call.url, 'https://www.jobsatamazon.co.uk/graphql');
    assert.equal(call.options.method, 'POST');
    assert.equal(call.options.dispatcher.url, 'http://proxy.example.test:8080');
    assert.equal(call.options.headers.Authorization, 'offline-auth-placeholder');
    assert.equal(call.options.headers['Content-Type'], 'application/json');
    assert.equal(call.options.headers.Accept, 'application/json');
    assert.equal(call.options.signal instanceof AbortSignal, true);
  }
  assert.match(harness.amazonCalls[0].body.query, /searchJobCardsByLocation/);
  assert.deepEqual(
    JSON.parse(JSON.stringify(harness.amazonCalls[0].body.variables.searchJobRequest)),
    {
      locale: 'en-GB',
      country: 'United Kingdom',
      keyWords: '',
      equalFilters: [],
      containFilters: [{ key: 'isPrivateSchedule', val: ['true', 'false'] }],
      rangeFilters: [],
      orFilters: [],
      dateFilters: [],
      sorters: [{ fieldName: 'totalPayRateMax', ascending: 'false' }],
      pageSize: 100,
      consolidateSchedule: true,
    },
  );
  assert.match(harness.amazonCalls[1].body.query, /searchScheduleCards/);
  assert.deepEqual(
    JSON.parse(JSON.stringify(harness.amazonCalls[1].body.variables.searchScheduleRequest)),
    {
      country: 'United Kingdom',
      jobId: 'JOB-KEEP',
      locale: 'en-GB',
      pageSize: 100,
      dateFilters: [],
      rangeFilters: [],
      equalFilters: [],
      excludeFilters: [],
      orFilters: [],
    },
  );
});

test('other HTTP errors are classified as partial without claiming scan success', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 400),
  });

  const result = await harness.api.fetchAmazonJobFeed('Loop-1');

  assert.equal(result.scanStatus, 'PARTIAL');
  assert.equal(protectionRow(harness.api.db), undefined);
  assert.equal(harness.amazonCalls.length, 1);
});

test('a cooldown aborts other in-flight Amazon requests instead of allowing them to continue', async (t) => {
  let resolvePending;
  const pending = new Promise((resolve) => { resolvePending = resolve; });
  const harness = createScannerHarness(t, {
    jobResponder: async (_call, page) => (page === 1 ? pending : mockResponse({}, 403)),
  });

  const firstRequest = harness.api.fetchAmazonJobFeed('Loop-1');
  await new Promise((resolve) => setImmediate(resolve));
  const secondRequest = harness.api.fetchAmazonJobFeed('Loop-2');
  const secondResult = await secondRequest;
  const firstResult = await firstRequest;

  assert.equal(secondResult.scanStatus, 'BLOCKED');
  assert.equal(firstResult.scanStatus, 'BLOCKED');
  assert.equal(harness.amazonCalls[0].options.signal.aborted, true);
  assert.equal(harness.amazonCalls.length, 2);
  resolvePending(successfulJobResponse());
});
