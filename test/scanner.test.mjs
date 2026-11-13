import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { createScanner } from '../tracker.mjs';

const QUIET_LOGGER = {
  log() {},
  warn() {},
  error() {},
};

function response(body, status = 200) {
  return {
    status,
    async json() {
      return body;
    },
    async text() {
      return JSON.stringify(body);
    },
  };
}

function createFixture(t, { fetchImpl, regions = ['London'] }) {
  const directory = mkdtempSync(join(tmpdir(), 'amazon-shift-finder-test-'));
  const databasePath = join(directory, 'test.sqlite');
  const calls = [];
  const notifications = [];
  const scanner = createScanner({
    databasePath,
    regions,
    fetchImpl: async (url, options) => {
      calls.push({ url, options });
      return fetchImpl(url, options);
    },
    sendTelegram: async (message) => notifications.push(message),
    sleep: async () => {},
    logger: QUIET_LOGGER,
  });

  t.after(() => {
    scanner.close();
    rmSync(directory, { recursive: true, force: true });
  });

  return { scanner, calls, notifications, databasePath };
}

test('discovery stores mocked job results in the SQLite queue', async (t) => {
  const fixture = createFixture(t, {
    fetchImpl: async () => response({
      data: {
        searchJobCardsByLocation: [{ jobId: 'JOB-1' }, { jobId: 'JOB-2' }],
      },
    }),
  });

  await fixture.scanner.runLoop1Discovery();

  assert.equal(fixture.calls.length, 1);
  assert.match(fixture.calls[0].url, /jobsatamazon\.co\.uk\/graphql$/);
  assert.equal(fixture.calls[0].options.method, 'POST');
  const payload = JSON.parse(fixture.calls[0].options.body);
  assert.match(payload.query, /searchJobCardsByLocation/);
  assert.equal(payload.variables.request.query, 'London');
  assert.deepEqual(
    fixture.scanner.db.prepare('SELECT job_id, region, status FROM job_queue ORDER BY job_id').all(),
    [
      { job_id: 'JOB-1', region: 'London', status: 'PENDING' },
      { job_id: 'JOB-2', region: 'London', status: 'PENDING' },
    ],
  );
  assert.equal(fixture.notifications.length, 0);
});

test('schedule processing deduplicates shifts and sends one mocked alert', async (t) => {
  const fixture = createFixture(t, {
    fetchImpl: async () => response({
      data: {
        searchScheduleCards: [{
          scheduleId: 'SHIFT-1',
          hireStartDate: '2026-12-01',
          basePay: '17.25',
          hoursPerWeek: '30',
          workingDays: 'Monday-Friday',
          startTime: '07:00',
          finishTime: '15:30',
          location: 'London Warehouse',
          postcode: 'E1 6AN',
        }],
      },
    }),
  });
  const insertJob = fixture.scanner.db.prepare(
    "INSERT INTO job_queue (job_id, region, status, discovered_at) VALUES (?, 'London', 'PENDING', 'test')",
  );
  insertJob.run('JOB-1');
  insertJob.run('JOB-2');

  await fixture.scanner.runLoop2Schedules();

  assert.equal(fixture.calls.length, 2);
  assert.ok(fixture.calls.every(({ url }) => url.endsWith('/graphql')));
  assert.equal(
    fixture.scanner.db.prepare('SELECT COUNT(*) AS count FROM seen_schedules').get().count,
    1,
  );
  assert.equal(
    fixture.scanner.db.prepare('SELECT id FROM seen_schedules').get().id,
    'SHIFT-1',
  );
  assert.deepEqual(
    fixture.scanner.db.prepare('SELECT status FROM job_queue ORDER BY job_id').all(),
    [{ status: 'PROCESSED' }, { status: 'PROCESSED' }],
  );
  assert.equal(fixture.notifications.length, 1);
  assert.match(fixture.notifications[0], /London Warehouse/);
  assert.match(fixture.notifications[0], /£17\.25 \/hr/);
});

test('recovery retries a mocked discovery request and clears its failure record', async (t) => {
  const fixture = createFixture(t, {
    fetchImpl: async () => response({
      data: {
        searchJobCardsByLocation: [{ jobId: 'JOB-RECOVERED' }],
      },
    }),
  });
  fixture.scanner.db.prepare(
    'INSERT INTO failed_requests (task_type, payload, error, retry_count, last_tried) VALUES (?, ?, ?, ?, ?)',
  ).run(
    'LOOP1_DISCOVERY',
    JSON.stringify({ region: 'London' }),
    'temporary offline-test failure',
    0,
    'test',
  );

  await fixture.scanner.runLoop3Recovery();

  assert.equal(fixture.calls.length, 1);
  assert.equal(
    fixture.scanner.db.prepare('SELECT job_id FROM job_queue').get().job_id,
    'JOB-RECOVERED',
  );
  assert.equal(
    fixture.scanner.db.prepare('SELECT COUNT(*) AS count FROM failed_requests').get().count,
    0,
  );
  assert.equal(fixture.notifications.length, 0);
});
