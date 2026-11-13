import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import Database from 'better-sqlite3';
import test from 'node:test';
import { createScanner, UK_HUB_SEARCH_TERMS } from '../tracker.mjs';

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
      amazonMaxConcurrentRequests = 3,
      amazonMinimumRequestIntervalMs = 0,
      jobSearchTerms = ['London'],
      amazonMaxRequestAttempts = 3,
      amazonForbiddenCooldownMs = 15 * 60 * 1000,
      maxCyclesPerLoop = Infinity,
      logger = QUIET_LOGGER,
      sleep = async () => {},
    } = {}) {
      const amazonCalls = [];
      const telegramCalls = [];
      const scanner = createScanner({
        databasePath,
        clock: () => currentTime,
        sleep,
        logger,
        amazonPageSize,
        amazonMaxConcurrentRequests,
        amazonMinimumRequestIntervalMs,
        jobSearchTerms,
        amazonMaxRequestAttempts,
        amazonForbiddenCooldownMs,
        maxCyclesPerLoop,
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

test('all three loops independently search the same complete hub list without blank queries', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({ jobSearchTerms: UK_HUB_SEARCH_TERMS });

  await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.runLoop2Schedules();
  await fixture.scanner.runLoop3Recovery();

  const searches = fixture.amazonCalls.filter(
    ({ request }) => request.operationName === 'searchJobCardsByLocation',
  );
  assert.equal(searches.length, UK_HUB_SEARCH_TERMS.length * 3);
  assert.equal(new Set(UK_HUB_SEARCH_TERMS).size, UK_HUB_SEARCH_TERMS.length);
  for (let index = 0; index < searches.length; index += 1) {
    const { request } = searches[index];
    assert.deepEqual(request.variables.searchJobRequest, {
      country: 'United Kingdom',
      locale: 'en-GB',
      query: UK_HUB_SEARCH_TERMS[index % UK_HUB_SEARCH_TERMS.length],
    });
    assert.match(request.query, /nextToken/);
    assert.match(request.query, /jobCards/);
    assert.match(request.query, /\$searchJobRequest: SearchJobRequest!/);
    assert.match(request.query, /searchJobCardsByLocation\(searchJobRequest: \$searchJobRequest\)/);
    assert.equal('request' in request.variables, false);
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
  await fixture.scanner.waitForNotificationDelivery();

  assert.equal(getCount(fixture.scanner, 'amazon_jobs'), 1);
  assert.equal(getCount(fixture.scanner, 'job_queue'), 1);
  assert.equal(
    fixture.scanner.db.prepare("SELECT status FROM job_queue WHERE job_id = 'JOB-UK-1'").get().status,
    'PROCESSED',
  );
  assert.equal(getCount(fixture.scanner, 'amazon_schedules'), 1);
  assert.equal(getCount(fixture.scanner, 'seen_schedules'), 1);
  assert.equal(getCount(fixture.scanner, 'telegram_notifications'), 1);
  assert.equal(
    fixture.amazonCalls.filter(
      ({ request }) => request.operationName === 'searchScheduleCards',
    ).length,
    3,
  );
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
  await fixture.scanner.waitForNotificationDelivery();

  assert.equal(searchCount, 3);
  assert.equal(getCount(fixture.scanner, 'amazon_jobs'), 1);
  assert.equal(
    fixture.scanner.db.prepare("SELECT status FROM job_queue WHERE job_id = 'JOB-UK-1'").get().status,
    'PROCESSED',
  );
  assert.equal(getCount(fixture.scanner, 'amazon_schedules'), 1);
  assert.equal(getCount(fixture.scanner, 'telegram_notifications'), 1);
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
    fixture.amazonCalls
      .filter(({ request }) => request.operationName === 'searchJobCardsByLocation')
      .map(({ request }) => request.variables.searchJobRequest.nextToken ?? null),
    [null, 'cursor-2'],
  );
  assert.equal(result.status, 'COMPLETE');
  assert.equal(result.pagesFetched, 2);
  assert.equal(result.jobsFound, 2);
  assert.equal(getCount(fixture.scanner, 'amazon_jobs'), 2);
  assert.equal(getCount(fixture.scanner, 'job_queue'), 2);
});

test('scan metrics record requests, pages, duration, and completed-scan spacing', async (t) => {
  const context = createContext(t);
  const logs = [];
  const fixture = context.createFixture({
    logger: {
      log: (...args) => logs.push(args.join(' ')),
      warn() {},
      error() {},
    },
    jobSearchResponder: async (request) => (
      request.variables.searchJobRequest.nextToken
        ? jobPage([{ ...DEFAULT_JOB, jobId: 'JOB-METRICS-2' }])
        : jobPage([{ ...DEFAULT_JOB, jobId: 'JOB-METRICS-1' }], 'metrics-page-2')
    ),
  });

  await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.waitForNotificationDelivery();
  context.setTime(context.getTime() + 20);
  await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.waitForNotificationDelivery();

  const [latest, previous] = fixture.scanner.getScanRuns(2);
  assert.equal(latest.request_count, 4);
  assert.equal(latest.pages_fetched, 2);
  assert.equal(latest.schedule_pages_fetched, 2);
  assert.equal(latest.pagination_complete, 1);
  assert.ok(latest.duration_ms >= 0);
  assert.ok(latest.time_since_previous_ms >= 0);
  assert.equal(previous.time_since_previous_ms, null);
  const metricsLog = logs.find((line) => line.includes('[loop1] scan=COMPLETE'));
  assert.match(metricsLog, /requests=4/);
  assert.match(metricsLog, /pagination=complete/);
});

test('scan metric migration preserves pre-existing scan history', async (t) => {
  const context = createContext(t);
  const legacyDatabase = new Database(context.databasePath);
  legacyDatabase.exec(`
    CREATE TABLE scan_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      loop_name TEXT NOT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT,
      status TEXT NOT NULL,
      pages_fetched INTEGER NOT NULL DEFAULT 0,
      jobs_found INTEGER NOT NULL DEFAULT 0,
      error TEXT,
      retry_at TEXT
    );
  `);
  legacyDatabase.prepare(`
    INSERT INTO scan_runs
      (loop_name, started_at, finished_at, status, pages_fetched, jobs_found)
    VALUES ('loop1', 'before-migration', 'before-migration', 'COMPLETE', 4, 12)
  `).run();
  legacyDatabase.close();

  const fixture = context.createFixture();
  const existing = fixture.scanner.getScanRuns(1)[0];
  assert.equal(existing.started_at, 'before-migration');
  assert.equal(existing.pages_fetched, 4);
  assert.equal(existing.jobs_found, 12);
  assert.equal(existing.request_count, 0);
  assert.equal(existing.pagination_complete, 0);
  assert.equal(existing.duration_ms, 0);
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

  await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.runLoop2Schedules();
  await fixture.scanner.runLoop3Recovery();
  await fixture.scanner.waitForNotificationDelivery();

  const scheduleRequests = fixture.amazonCalls.filter(
    ({ request }) => request.operationName === 'searchScheduleCards',
  );
  assert.equal(scheduleRequests.length, 6);
  for (const { request } of scheduleRequests) {
    assert.equal(request.variables.searchScheduleRequest.country, 'United Kingdom');
    assert.equal(request.variables.searchScheduleRequest.locale, 'en-GB');
  }
  assert.deepEqual(
    scheduleRequests.map(({ request }) => request.variables.searchScheduleRequest.nextToken ?? null),
    [null, 'schedule-cursor-2', null, 'schedule-cursor-2', null, 'schedule-cursor-2'],
  );
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

test('notification batches continue draining a backlog beyond ten alerts', async (t) => {
  const context = createContext(t);
  const schedules = Array.from({ length: 12 }, (_, index) => ({
    ...DEFAULT_SCHEDULE,
    scheduleId: `SCHEDULE-BATCH-${index + 1}`,
  }));
  const fixture = context.createFixture({
    jobSearchResponder: async () => jobPage([DEFAULT_JOB]),
    scheduleResponder: async () => schedulePage(schedules),
  });

  await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.waitForNotificationDelivery();

  assert.equal(fixture.telegramCalls.length, 12);
  assert.equal(
    fixture.scanner.db.prepare(
      "SELECT COUNT(*) AS count FROM telegram_notifications WHERE status = 'SENT'",
    ).get().count,
    12,
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
  await firstProcess.scanner.waitForNotificationDelivery();
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
    jobSearchResponder: async () => jobPage([
      { ...DEFAULT_JOB, jobId: 'JOB-RECOVER' },
    ]),
    scheduleResponder: async () => {
      throw new Error('mock schedule service failure');
    },
  });

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
  const restarted = context.createFixture({
    jobSearchResponder: async () => jobPage([
      { ...DEFAULT_JOB, jobId: 'JOB-RECOVER' },
    ]),
  });
  await restarted.scanner.runLoop3Recovery();
  assert.equal(
    restarted.scanner.db.prepare(
      "SELECT status FROM job_queue WHERE job_id = 'JOB-RECOVER'",
    ).get().status,
    'PROCESSED',
  );

  await restarted.scanner.waitForNotificationDelivery();
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

test('start launches three simultaneous loops and runs only the requested cycles', async (t) => {
  const context = createContext(t);
  let activeRequests = 0;
  let maximumActiveRequests = 0;
  let startedRequests = 0;
  let releaseRequests;
  let notifyAllStarted;
  const requestGate = new Promise((resolve) => {
    releaseRequests = resolve;
  });
  const allStarted = new Promise((resolve) => {
    notifyAllStarted = resolve;
  });
  const fixture = context.createFixture({
    maxCyclesPerLoop: 1,
    jobSearchResponder: async () => {
      activeRequests += 1;
      maximumActiveRequests = Math.max(maximumActiveRequests, activeRequests);
      startedRequests += 1;
      if (startedRequests === 3) notifyAllStarted();
      await requestGate;
      activeRequests -= 1;
      return jobPage();
    },
  });

  assert.deepEqual(fixture.scanner.start(), { status: 'STARTED', loops: 3 });
  assert.equal(fixture.scanner.getRequestConcurrency(), 3);
  await allStarted;
  assert.equal(maximumActiveRequests, 3);
  releaseRequests();
  await fixture.scanner.waitForLoops();

  assert.deepEqual(
    new Set(fixture.scanner.getScanRuns().map((run) => run.loop_name)),
    new Set(['loop1', 'loop2', 'loop3']),
  );
  assert.equal(
    fixture.scanner.getScanRuns().every((run) => run.status === 'COMPLETE'),
    true,
  );
  await fixture.scanner.waitForNotificationDelivery();
});

test('successful scans restart immediately without fixed-cycle sleeps', async (t) => {
  const context = createContext(t);
  const sleeps = [];
  const fixture = context.createFixture({
    maxCyclesPerLoop: 2,
    sleep: async (delay) => sleeps.push(delay),
  });

  fixture.scanner.start();
  await fixture.scanner.waitForLoops();

  assert.equal(fixture.amazonCalls.filter(
    ({ request }) => request.operationName === 'searchJobCardsByLocation',
  ).length, 6);
  assert.deepEqual(sleeps, []);
  assert.equal(
    fixture.scanner.getScanRuns().every((run) => run.status === 'COMPLETE'),
    true,
  );
});

test('hub searches reset pagination per term and deduplicate overlapping job results', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({
    jobSearchTerms: ['Dartford', 'Tilbury', ' Dartford '],
    jobSearchResponder: async ({ variables: { searchJobRequest: request } }) => (
      request.nextToken
        ? jobPage([{ ...DEFAULT_JOB, jobId: `JOB-${request.query}` }])
        : jobPage([DEFAULT_JOB], 'shared-cursor')
    ),
  });

  const result = await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.waitForNotificationDelivery();

  const requests = fixture.amazonCalls
    .filter(({ request }) => request.operationName === 'searchJobCardsByLocation')
    .map(({ request }) => [
      request.variables.searchJobRequest.query,
      request.variables.searchJobRequest.nextToken ?? null,
    ]);
  assert.deepEqual(requests, [
    ['Dartford', null],
    ['Dartford', 'shared-cursor'],
    ['Tilbury', null],
    ['Tilbury', 'shared-cursor'],
  ]);
  assert.equal(result.status, 'COMPLETE');
  assert.equal(result.jobPagesFetched, 4);
  assert.equal(result.jobsFound, 3);
  assert.equal(getCount(fixture.scanner, 'amazon_jobs'), 3);
  assert.equal(getCount(fixture.scanner, 'telegram_notifications'), 1);
  assert.equal(fixture.telegramCalls.length, 1);
});

test('request spacing is shared by all loops and schedule requests', async (t) => {
  const context = createContext(t);
  const startedAt = [];
  const delays = [];
  const fixture = context.createFixture({
    amazonMinimumRequestIntervalMs: 4000,
    sleep: async (delay) => {
      delays.push(delay);
      context.setTime(context.getTime() + delay);
    },
    jobSearchResponder: async () => {
      startedAt.push(context.getTime());
      return jobPage([DEFAULT_JOB]);
    },
    scheduleResponder: async () => {
      startedAt.push(context.getTime());
      return schedulePage();
    },
  });

  await fixture.scanner.runLoop1Discovery();
  await fixture.scanner.runLoop2Schedules();
  await fixture.scanner.runLoop3Recovery();

  assert.equal(startedAt.length, 6);
  assert.deepEqual(
    startedAt.map((time) => time - BASE_TIME),
    [0, 4000, 8000, 12000, 16000, 20000],
  );
  assert.deepEqual(delays, [4000, 4000, 4000, 4000, 4000]);
});

test('403 stops the remaining hub searches and honors a longer Retry-After', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({
    jobSearchTerms: ['Dartford', 'Tilbury'],
    jobSearchResponder: async () => response({}, 403, { 'retry-after': '1800' }),
  });

  assert.equal((await fixture.scanner.runLoop1Discovery()).status, 'BLOCKED');
  assert.equal((await fixture.scanner.runLoop2Schedules()).status, 'BLOCKED');
  assert.equal((await fixture.scanner.runLoop3Recovery()).status, 'BLOCKED');
  assert.equal(fixture.amazonCalls.length, 1);
  const row = fixture.scanner.db.prepare(
    "SELECT value FROM amazon_control WHERE key = 'cooldown_until'",
  ).get();
  assert.equal(Date.parse(row.value) - BASE_TIME, 1800000);
});

test('GraphQL input failures identify the term and structured fields without logging arbitrary extensions', async (t) => {
  const context = createContext(t);
  const fixture = context.createFixture({
    jobSearchTerms: ['Dartford', 'Tilbury'],
    jobSearchResponder: async () => response({
      errors: [{
        message: 'Invalid input',
        path: ['searchJobCardsByLocation'],
        locations: [{ line: 2, column: 3 }],
        extensions: { code: 'INVALID_INPUT', debug: 'must-not-be-logged' },
      }],
    }),
  });

  const result = await fixture.scanner.runLoop1Discovery();
  assert.equal(result.status, 'FAILED');
  assert.equal(result.paginationComplete, false);
  assert.equal(result.jobPagesFetched, 0);
  assert.equal(fixture.amazonCalls.length, 1);
  assert.match(result.error, /Dartford/);
  assert.match(result.error, /code=INVALID_INPUT/);
  assert.match(result.error, /path=searchJobCardsByLocation/);
  assert.match(result.error, /locations=2:3/);
  assert.doesNotMatch(result.error, /must-not-be-logged/);
  assert.equal(getCount(fixture.scanner, 'telegram_notifications'), 0);
});

test('simultaneous loops wait their turn at the shared request pacer', async (t) => {
  const context = createContext(t);
  const waiting = [];
  const starts = [];
  const fixture = context.createFixture({
    maxCyclesPerLoop: 1,
    amazonMinimumRequestIntervalMs: 4000,
    sleep: (delay) => new Promise((resolveWait) => waiting.push({ delay, resolveWait })),
    jobSearchResponder: async () => {
      starts.push(context.getTime() - BASE_TIME);
      return jobPage();
    },
  });
  const flush = () => new Promise((resolveFlush) => setImmediate(resolveFlush));

  fixture.scanner.start();
  await flush();
  assert.deepEqual(starts, [0]);
  assert.equal(waiting.length, 1);

  for (const expectedStart of [4000, 8000]) {
    const next = waiting.shift();
    assert.equal(next.delay, 4000);
    context.setTime(BASE_TIME + expectedStart);
    next.resolveWait();
    await flush();
  }
  await fixture.scanner.waitForLoops();
  assert.deepEqual(starts, [0, 4000, 8000]);
  assert.equal(waiting.length, 0);
});

test('a restarted process retains a forbidden-response cooldown', async (t) => {
  const context = createContext(t);
  const first = context.createFixture({
    jobSearchResponder: async () => response({}, 403),
  });
  assert.equal((await first.scanner.runLoop1Discovery()).status, 'BLOCKED');
  first.close();

  const restarted = context.createFixture();
  assert.equal((await restarted.scanner.runLoop2Schedules()).status, 'BLOCKED');
  assert.equal(restarted.amazonCalls.length, 0);
  context.setTime(BASE_TIME + 15 * 60 * 1000);
  assert.equal((await restarted.scanner.runLoop3Recovery()).status, 'COMPLETE');
  assert.equal(restarted.amazonCalls.length, 1);
});
