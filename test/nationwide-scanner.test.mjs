import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import Database from 'better-sqlite3';
import test from 'node:test';
import { createScanner } from '../tracker.mjs';

const BASE_TIME = Date.parse('2026-11-13T12:00:00.000Z');
const DEFAULT_JOB = {
  jobId: 'JOB-UK-1',
  jobTitle: 'Warehouse Operative',
  city: 'London',
  postalCode: 'E1 6AN',
};
const DEFAULT_SCHEDULE = {
  scheduleId: 'SCHEDULE-UK-1',
  hireStartDate: '2026-12-01',
  basePay: '17.25',
  hoursPerWeek: '30',
  scheduleType: 'Full-time',
  city: 'London',
  postalCode: 'E1 6AN',
};
const QUIET_LOGGER = { log() {}, warn() {}, error() {} };

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

function jobPage(jobCards = [], nextToken = null) {
  return response({
    data: { searchJobCardsByLocation: { jobCards, nextToken } },
  });
}

function schedulePage(scheduleCards = [], nextToken = null) {
  return response({
    data: { searchScheduleCards: { scheduleCards, nextToken } },
  });
}

function createContext(t, { initialTime = BASE_TIME } = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'amazon-nationwide-test-'));
  const databasePath = join(directory, 'test.sqlite');
  const fixtures = [];
  let currentTime = initialTime;

  t.after(() => {
    for (const fixture of fixtures) fixture.close();
    rmSync(directory, { recursive: true, force: true });
  });

  return {
    databasePath,
    getTime: () => currentTime,
    setTime: (value) => {
      currentTime = value;
    },
    createFixture({
      jobSearchResponder = async () => jobPage(),
      scheduleResponder = async () => schedulePage([DEFAULT_SCHEDULE]),
      telegramResponder = async () => response({ ok: true, result: { message_id: 1 } }),
      amazonPageSize = 20,
      amazonRequestIntervalMs = 0,
      amazonMaxRequestAttempts = 3,
      amazonForbiddenCooldownMs = 15 * 60 * 1000,
      setIntervalImpl = setInterval,
      setTimeoutImpl = setTimeout,
    } = {}) {
      const amazonCalls = [];
      const telegramCalls = [];
      const scanner = createScanner({
        databasePath,
        clock: () => currentTime,
        sleep: async () => {},
        logger: QUIET_LOGGER,
        amazonPageSize,
        amazonRequestIntervalMs,
        amazonMaxRequestAttempts,
        amazonForbiddenCooldownMs,
        setIntervalImpl,
        setTimeoutImpl,
        fetchImpl: async (url, options) => {
          const request = JSON.parse(options.body);
          const call = { url, request, options };
          amazonCalls.push(call);
          if (request.operationName === 'searchJobCardsByLocation') {
            return jobSearchResponder(request, call);
          }
          if (request.operationName === 'searchScheduleCards') {
            return scheduleResponder(request, call);
          }
          throw new Error(`Unexpected mocked Amazon operation: ${request.operationName}`);
        },
        telegramTransport: async (message, options) => {
          telegramCalls.push({ message, options });
          return telegramResponder(message, options);
        },
      });
      let closed = false;
      const fixture = {
        scanner,
        amazonCalls,
        telegramCalls,
        close() {
          if (!closed) {
            scanner.close();
            closed = true;
          }
        },
      };
      fixtures.push(fixture);
      return fixture;
    },
  };
}

function getCount(scanner, table) {
  return scanner.db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count;
}

test('all three loops use the same unfiltered UK search request', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({ amazonPageSize: 17 });

  await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.runLoop2Schedules();
  await fixture.scanner.runLoop3Recovery();

  const searches = fixture.amazonCalls.filter(
    ({ request }) => request.operationName === 'searchJobCardsByLocation',
  );
  assert.equal(searches.length, 3);
  for (const { request } of searches) {
    assert.deepEqual(request.variables.searchJobRequest, {
      country: 'GB',
      locale: 'en_GB',
      keyWords: '',
      pageSize: 17,
    });
    assert.match(request.query, /nextToken/);
    assert.match(request.query, /jobCards/);
    assert.equal('city' in request.variables.searchJobRequest, false);
    assert.equal('postcode' in request.variables.searchJobRequest, false);
    assert.equal('postalCode' in request.variables.searchJobRequest, false);
  }
  assert.deepEqual(
    fixture.scanner.getScanRuns().map((run) => [run.loop_name, run.status]),
    [
      ['loop3', 'COMPLETE'],
      ['loop2', 'COMPLETE'],
      ['loop1', 'COMPLETE'],
    ],
  );
});

test('all three loops can discover the same job without duplicate jobs or alerts', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({
    jobSearchResponder: async () => jobPage([DEFAULT_JOB]),
  });

  await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.runLoop2Schedules();
  await fixture.scanner.runLoop3Recovery();

  assert.equal(getCount(fixture.scanner, 'amazon_jobs'), 1);
  assert.equal(getCount(fixture.scanner, 'job_queue'), 1);
  assert.equal(
    fixture.scanner.db.prepare("SELECT status FROM job_queue WHERE job_id = 'JOB-UK-1'").get().status,
    'PROCESSED',
  );
  assert.equal(getCount(fixture.scanner, 'amazon_schedules'), 1);
  assert.equal(getCount(fixture.scanner, 'seen_schedules'), 1);
  assert.equal(getCount(fixture.scanner, 'telegram_notifications'), 1);
  assert.equal(fixture.telegramCalls.length, 1);
});

test('a later loop can discover a job that earlier loops did not return', async (t) => {
  const context = createContext(t);
  let searchCount = 0;
  const fixture = context.createFixture({
    jobSearchResponder: async () => {
      searchCount += 1;
      return searchCount === 3 ? jobPage([DEFAULT_JOB]) : jobPage();
    },
  });

  await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.runLoop2Schedules();
  await fixture.scanner.runLoop3Recovery();

  assert.equal(searchCount, 3);
  assert.equal(getCount(fixture.scanner, 'amazon_jobs'), 1);
  assert.equal(
    fixture.scanner.db.prepare("SELECT status FROM job_queue WHERE job_id = 'JOB-UK-1'").get().status,
    'PENDING',
  );
});

test('nationwide job search follows nextToken pages and records each page', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({
    amazonPageSize: 2,
    jobSearchResponder: async (request) => {
      if (!request.variables.searchJobRequest.nextToken) {
        return jobPage([{ ...DEFAULT_JOB, jobId: 'JOB-PAGE-1' }], 'cursor-2');
      }
      assert.equal(request.variables.searchJobRequest.nextToken, 'cursor-2');
      return jobPage([{ ...DEFAULT_JOB, jobId: 'JOB-PAGE-2' }]);
    },
  });

  const result = await fixture.scanner.runLoop1Discovery();

  assert.deepEqual(
    fixture.amazonCalls.map(({ request }) => request.variables.searchJobRequest.nextToken ?? null),
    [null, 'cursor-2'],
  );
  assert.equal(result.status, 'COMPLETE');
  assert.equal(result.pagesFetched, 2);
  assert.equal(result.jobsFound, 2);
  assert.equal(getCount(fixture.scanner, 'amazon_jobs'), 2);
  assert.equal(getCount(fixture.scanner, 'job_queue'), 2);
});

test('schedule pagination stores schedules and queues only one alert per scheduleId', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({
    jobSearchResponder: async () => jobPage([DEFAULT_JOB]),
    scheduleResponder: async (request) => {
      if (!request.variables.searchScheduleRequest.nextToken) {
        return schedulePage([DEFAULT_SCHEDULE], 'schedule-cursor-2');
      }
      assert.equal(
        request.variables.searchScheduleRequest.nextToken,
        'schedule-cursor-2',
      );
      return schedulePage([
        DEFAULT_SCHEDULE,
        { ...DEFAULT_SCHEDULE, scheduleId: 'SCHEDULE-UK-2', city: 'Leeds' },
      ]);
    },
  });

  await fixture.scanner.runLoop2Schedules();

  assert.equal(getCount(fixture.scanner, 'amazon_schedules'), 2);
  assert.equal(getCount(fixture.scanner, 'seen_schedules'), 2);
  assert.equal(getCount(fixture.scanner, 'telegram_notifications'), 2);
  assert.equal(fixture.telegramCalls.length, 2);
  assert.equal(
    fixture.scanner.db.prepare(
      "SELECT status FROM job_queue WHERE job_id = 'JOB-UK-1'",
    ).get().status,
    'PROCESSED',
  );
});

test('a failed discovery loop does not prevent the other loops from scanning', async (t) => {
  const context = createContext(t);
  let searchCount = 0;
  const fixture = context.createFixture({
    jobSearchResponder: async () => {
      searchCount += 1;
      if (searchCount <= 3) return response({}, 500);
      return jobPage([DEFAULT_JOB]);
    },
  });

  const loop1 = await fixture.scanner.runLoop1Discovery();
  const loop2 = await fixture.scanner.runLoop2Schedules();
  const loop3 = await fixture.scanner.runLoop3Recovery();

  assert.equal(loop1.status, 'FAILED');
  assert.equal(loop2.status, 'COMPLETE');
  assert.equal(loop3.status, 'COMPLETE');
  assert.equal(searchCount, 5);
  assert.equal(getCount(fixture.scanner, 'amazon_jobs'), 1);
  assert.deepEqual(
    fixture.scanner.getLoopHealth().map((health) => [health.loop_name, health.last_status]),
    [
      ['loop1', 'FAILED'],
      ['loop2', 'COMPLETE'],
      ['loop3', 'COMPLETE'],
    ],
  );
});

test('403 and 429 responses pause Amazon requests without rapid retries', async (t) => {
  await t.test('403 uses a persisted cooldown', async (subtest) => {
    const context = createContext(subtest);
    let searchCount = 0;
    const fixture = context.createFixture({
      amazonForbiddenCooldownMs: 10000,
      jobSearchResponder: async () => {
        searchCount += 1;
        return searchCount === 1 ? response({}, 403) : jobPage();
      },
    });

    assert.equal((await fixture.scanner.runLoop1Discovery()).status, 'BLOCKED');
    assert.equal((await fixture.scanner.runLoop2Schedules()).status, 'BLOCKED');
    assert.equal(searchCount, 1);
    assert.equal(getCount(fixture.scanner, 'amazon_control'), 1);

    context.setTime(context.getTime() + 10000);
    assert.equal((await fixture.scanner.runLoop3Recovery()).status, 'COMPLETE');
    assert.equal(searchCount, 2);
  });

  await t.test('429 honors Retry-After', async (subtest) => {
    const context = createContext(subtest);
    let searchCount = 0;
    const fixture = context.createFixture({
      jobSearchResponder: async () => {
        searchCount += 1;
        return searchCount === 1
          ? response({}, 429, { 'retry-after': '120' })
          : jobPage();
      },
    });

    assert.equal((await fixture.scanner.runLoop1Discovery()).status, 'BLOCKED');
    assert.equal((await fixture.scanner.runLoop2Schedules()).status, 'BLOCKED');
    assert.equal(searchCount, 1);

    const cooldownUntil = Date.parse(
      fixture.scanner.db.prepare(
        "SELECT value FROM amazon_control WHERE key = 'cooldown_until'",
      ).get().value,
    );
    assert.equal(cooldownUntil - context.getTime(), 120000);

    context.setTime(cooldownUntil);
    assert.equal((await fixture.scanner.runLoop1Discovery()).status, 'COMPLETE');
    assert.equal(searchCount, 2);
  });
});

test('a partial paginated scan is recorded and completed on a later run', async (t) => {
  const context = createContext(t);
  let pageTwoAttempt = 0;
  const firstProcess = context.createFixture({
    jobSearchResponder: async (request) => {
      if (!request.variables.searchJobRequest.nextToken) {
        return jobPage([{ ...DEFAULT_JOB, jobId: 'JOB-PARTIAL-1' }], 'cursor-2');
      }
      pageTwoAttempt += 1;
      throw new Error('mock page-two connection failure');
    },
  });

  const partial = await firstProcess.scanner.runLoop1Discovery();
  assert.equal(partial.status, 'PARTIAL');
  assert.equal(partial.pagesFetched, 1);
  assert.equal(getCount(firstProcess.scanner, 'amazon_jobs'), 1);
  firstProcess.close();

  const restarted = context.createFixture({
    jobSearchResponder: async (request) => {
      if (!request.variables.searchJobRequest.nextToken) {
        return jobPage([{ ...DEFAULT_JOB, jobId: 'JOB-PARTIAL-1' }], 'cursor-2');
      }
      return jobPage([{ ...DEFAULT_JOB, jobId: 'JOB-PARTIAL-2' }]);
    },
  });
  context.setTime(context.getTime() + 1000);
  const complete = await restarted.scanner.runLoop1Discovery();

  assert.equal(pageTwoAttempt, 3);
  assert.equal(complete.status, 'COMPLETE');
  assert.equal(complete.pagesFetched, 2);
  assert.equal(getCount(restarted.scanner, 'amazon_jobs'), 2);
  assert.deepEqual(
    restarted.scanner.getScanRuns().map((run) => run.status),
    ['COMPLETE', 'PARTIAL'],
  );
});

test('failed schedule work is re-queued and completed after a process restart', async (t) => {
  const context = createContext(t);
  const firstProcess = context.createFixture({
    scheduleResponder: async () => {
      throw new Error('mock schedule service failure');
    },
  });
  firstProcess.scanner.db.prepare(`
    INSERT INTO job_queue (job_id, region, status, discovered_at)
    VALUES ('JOB-RECOVER', 'GB', 'PENDING', ?)
  `).run(new Date(context.getTime()).toISOString());

  await firstProcess.scanner.runLoop2Schedules();
  assert.equal(
    firstProcess.scanner.db.prepare(
      "SELECT status FROM job_queue WHERE job_id = 'JOB-RECOVER'",
    ).get().status,
    'FAILED',
  );
  assert.equal(getCount(firstProcess.scanner, 'failed_requests'), 1);
  firstProcess.close();

  context.setTime(context.getTime() + 90000);
  const restarted = context.createFixture();
  await restarted.scanner.runLoop3Recovery();
  assert.equal(
    restarted.scanner.db.prepare(
      "SELECT status FROM job_queue WHERE job_id = 'JOB-RECOVER'",
    ).get().status,
    'PENDING',
  );

  await restarted.scanner.runLoop2Schedules();
  assert.equal(
    restarted.scanner.db.prepare(
      "SELECT status FROM job_queue WHERE job_id = 'JOB-RECOVER'",
    ).get().status,
    'PROCESSED',
  );
  assert.equal(getCount(restarted.scanner, 'amazon_schedules'), 1);
  assert.equal(getCount(restarted.scanner, 'telegram_notifications'), 1);
  assert.equal(getCount(restarted.scanner, 'failed_requests'), 0);
});

test('each loop skips overlaps but runs again after its active pass finishes', async (t) => {
  const loopCases = [
    ['Loop 1', 'runLoop1Discovery'],
    ['Loop 2', 'runLoop2Schedules'],
    ['Loop 3', 'runLoop3Recovery'],
  ];

  for (const [label, method] of loopCases) {
    await t.test(label, async (subtest) => {
      const context = createContext(subtest);
      let releaseRequest;
      let signalRequestStarted;
      const requestGate = new Promise((resolveGate) => {
        releaseRequest = resolveGate;
      });
      const requestStarted = new Promise((resolveStarted) => {
        signalRequestStarted = resolveStarted;
      });
      let searchCount = 0;
      const fixture = context.createFixture({
        jobSearchResponder: async () => {
          searchCount += 1;
          signalRequestStarted();
          if (searchCount === 1) await requestGate;
          return jobPage();
        },
      });

      const firstPass = fixture.scanner[method]();
      await requestStarted;
      const overlappingPass = await fixture.scanner[method]();
      assert.equal(overlappingPass.status, 'SKIPPED');
      assert.equal(searchCount, 1);

      releaseRequest();
      await firstPass;
      await fixture.scanner[method]();
      assert.equal(searchCount, 2);
    });
  }
});

test('start launches Loop 1 immediately and staggers Loop 2 and Loop 3', async (t) => {
  const context = createContext(t);
  const timeouts = [];
  const intervals = [];
  const fixture = context.createFixture({
    setTimeoutImpl: (callback, delay) => {
      timeouts.push({ callback, delay });
      return timeouts.length;
    },
    setIntervalImpl: (callback, delay) => {
      intervals.push({ callback, delay });
      return intervals.length;
    },
  });

  await fixture.scanner.start();
  assert.deepEqual(intervals.map(({ delay }) => delay), [60000]);
  assert.deepEqual(timeouts.map(({ delay }) => delay), [5000, 10000]);
  assert.equal(
    fixture.scanner.db.prepare(
      "SELECT last_status FROM loop_health WHERE loop_name = 'loop1'",
    ).get().last_status,
    'COMPLETE',
  );

  await timeouts[0].callback();
  await timeouts[1].callback();
  assert.deepEqual(intervals.map(({ delay }) => delay), [60000, 30000, 90000]);
});
