import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { createScannerHarness, mockResponse } from './scanner-harness.mjs';

const jobSource = {
  jobId: 'JOB-PAGES',
  jobTitle: 'Fulfillment Associate',
  locationName: 'UK Fulfillment Centre',
};

test('exactly three independent nationwide loops retain their 5s and 10s startup timing', () => {
  const trackerPath = fileURLToPath(new URL('../tracker.mjs', import.meta.url));
  const source = readFileSync(trackerPath, 'utf8');
  const configuredLoops = source.match(/runScannerLoop\('Loop-[123]'\)/g) ?? [];

  assert.equal(configuredLoops.length, 3);
  assert.equal(configuredLoops.filter((loop) => loop.includes('Loop-1')).length, 1);
  assert.equal(configuredLoops.filter((loop) => loop.includes('Loop-2')).length, 1);
  assert.equal(configuredLoops.filter((loop) => loop.includes('Loop-3')).length, 1);
  assert.match(source, /runScannerLoop\('Loop-1'\);/);
  assert.match(source, /runScannerLoop\('Loop-2'\);\s*\},\s*5000\)/);
  assert.match(source, /runScannerLoop\('Loop-3'\);\s*\},\s*10000\)/);
});

test('three loops share each nationwide feed and process distinct schedule work', async (t) => {
  const jobs = Array.from({ length: 6 }, (_, index) => ({
    ...jobSource,
    jobId: `JOB-SHARED-${index + 1}`,
  }));
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({
      data: { searchJobCardsByLocation: { jobCards: jobs, nextToken: null } },
    }),
    scheduleResponder: async (call) => mockResponse({
      data: {
        searchScheduleCards: {
          scheduleCards: [{
            scheduleId: `S-${call.body.variables.searchScheduleRequest.jobId}`,
            jobId: call.body.variables.searchScheduleRequest.jobId,
          }],
          nextToken: null,
        },
      },
    }),
  });

  const results = await Promise.all([
    harness.api.runCoordinatedScannerPass('Loop-1'),
    harness.api.runCoordinatedScannerPass('Loop-2'),
    harness.api.runCoordinatedScannerPass('Loop-3'),
  ]);

  assert.equal(harness.amazonCalls.filter(({ body }) => body.query.includes('searchJobCardsByLocation')).length, 1);
  assert.equal(harness.amazonCalls.filter(({ body }) => body.query.includes('searchScheduleCards')).length, 6);
  assert.equal(new Set(results.map((result) => result.generationId)).size, 1);
  assert.ok(results.every((result) => result.retrievedJobCount === 6));
  assert.deepEqual(results.map((result) => result.assignedJobCount), [2, 2, 2]);
  const processedJobIds = results.flatMap((result) => result.discoveredJobIds);
  assert.equal(new Set(processedJobIds).size, 6);
  assert.deepEqual(new Set(processedJobIds), new Set(jobs.map(({ jobId }) => jobId)));
});

test('simultaneous schedule requests for one job share a single Amazon request', async (t) => {
  let releaseResponse;
  let requestStarted;
  const started = new Promise((resolve) => { requestStarted = resolve; });
  const response = new Promise((resolve) => { releaseResponse = resolve; });
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => {
      requestStarted();
      return response;
    },
  });
  const first = harness.api.fetchSchedulesForJob('JOB-ONE-FLIGHT', 'Loop-1');
  await started;
  const second = harness.api.fetchSchedulesForJob('JOB-ONE-FLIGHT', 'Loop-2');
  releaseResponse(mockResponse({
    data: {
      searchScheduleCards: {
        scheduleCards: [{ scheduleId: 'S-ONE-FLIGHT', jobId: 'JOB-ONE-FLIGHT' }],
        nextToken: null,
      },
    },
  }));

  const [firstResult, secondResult] = await Promise.all([first, second]);
  assert.equal(harness.amazonCalls.length, 1);
  assert.deepEqual(
    JSON.parse(JSON.stringify(firstResult.schedules)),
    JSON.parse(JSON.stringify(secondResult.schedules)),
  );
});

test('an incomplete schedule response on one loop does not stop other loop work', async (t) => {
  const jobs = Array.from({ length: 3 }, (_, index) => ({
    ...jobSource,
    jobId: `JOB-ISOLATED-${index + 1}`,
  }));
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({
      data: { searchJobCardsByLocation: { jobCards: jobs, nextToken: null } },
    }),
    scheduleResponder: async (call) => {
      const jobId = call.body.variables.searchScheduleRequest.jobId;
      if (jobId === 'JOB-ISOLATED-1') return mockResponse({ data: {} });
      return mockResponse({
        data: {
          searchScheduleCards: {
            scheduleCards: [{ scheduleId: `S-${jobId}`, jobId }],
            nextToken: null,
          },
        },
      });
    },
  });

  const results = await Promise.all([
    harness.api.runCoordinatedScannerPass('Loop-1'),
    harness.api.runCoordinatedScannerPass('Loop-2'),
    harness.api.runCoordinatedScannerPass('Loop-3'),
  ]);
  assert.deepEqual(results.map(({ scanStatus }) => scanStatus), ['PARTIAL', 'COMPLETE', 'COMPLETE']);
  assert.equal(harness.api.db.prepare('SELECT COUNT(*) AS count FROM discovered_schedules').get().count, 2);
  assert.equal(harness.amazonCalls.filter(({ body }) => body.query.includes('searchScheduleCards')).length, 3);
});

test('malformed successful GraphQL responses are marked partial, not empty-successful', async (t) => {
  const jobHarness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({ data: { unrelated: {} } }),
  });
  const jobResult = await jobHarness.api.fetchAmazonJobFeed();
  assert.equal(jobResult.scanStatus, 'PARTIAL');
  assert.deepEqual(JSON.parse(JSON.stringify(jobResult.jobCards)), []);

  const scheduleHarness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({ data: { searchScheduleCards: { scheduleCards: [] } } }),
  });
  const scheduleResult = await scheduleHarness.api.fetchSchedulesForJob(jobSource.jobId);
  assert.equal(scheduleResult.paginationComplete, false);
  assert.deepEqual(JSON.parse(JSON.stringify(scheduleResult.schedules)), []);
});

test('duplicate job cards across changing pagination pages are processed once per feed', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async (_call, page) => mockResponse({
      data: {
        searchJobCardsByLocation: {
          jobCards: page === 1
            ? [jobSource]
            : [{ ...jobSource, jobTitle: 'Updated title' }],
          nextToken: page === 1 ? 'page-2' : null,
        },
      },
    }),
  });
  const result = await harness.api.fetchAmazonJobFeed();
  assert.equal(result.scanStatus, 'COMPLETE');
  assert.equal(result.jobCards.length, 1);
  assert.equal(result.jobCards[0].jobTitle, 'Fulfillment Associate');
  assert.equal(harness.amazonCalls.length, 2);
});

test('duplicate schedule IDs across paginated results create one schedule record and alert', async (t) => {
  const harness = createScannerHarness(t, {
    scheduleResponder: async (_call, page) => mockResponse({
      data: {
        searchScheduleCards: {
          scheduleCards: page === 1
            ? [
              { scheduleId: 'S-PAGE-DUP', jobId: jobSource.jobId },
              { scheduleId: 'S-PAGE-1', jobId: jobSource.jobId },
            ]
            : [
              { scheduleId: 'S-PAGE-DUP', jobId: jobSource.jobId },
              { scheduleId: 'S-PAGE-2', jobId: jobSource.jobId },
            ],
          nextToken: page === 1 ? 'schedule-page-2' : null,
        },
      },
    }),
  });

  const result = await harness.api.processDiscoveredJob(jobSource, new Date().toISOString());
  assert.equal(result.count, 3);
  assert.equal(harness.api.db.prepare('SELECT COUNT(*) AS count FROM discovered_schedules').get().count, 3);
  assert.equal(harness.api.db.prepare('SELECT COUNT(*) AS count FROM telegram_outbox').get().count, 3);
  assert.equal(harness.amazonCalls.length, 2);
});

test('post-cooldown recovery starts one loop first, then resumes all loops next generation', async (t) => {
  const jobs = Array.from({ length: 3 }, (_, index) => ({
    ...jobSource,
    jobId: `JOB-RECOVERY-${index + 1}`,
  }));
  const harness = createScannerHarness(t, {
    prepareDatabase(db) {
      db.exec(`
        CREATE TABLE amazon_request_protection (
          id INTEGER PRIMARY KEY, reason TEXT NOT NULL, expires_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
      `);
      db.prepare(`
        INSERT INTO amazon_request_protection (id, reason, expires_at, updated_at)
        VALUES (1, 'http_403', ?, ?)
      `).run(
        new Date(Date.now() - 10000).toISOString(),
        new Date(Date.now() - 90000).toISOString(),
      );
    },
    jobResponder: async () => mockResponse({
      data: { searchJobCardsByLocation: { jobCards: jobs, nextToken: null } },
    }),
    scheduleResponder: async (call) => mockResponse({
      data: {
        searchScheduleCards: {
          scheduleCards: [{
            scheduleId: `S-${call.body.variables.searchScheduleRequest.jobId}`,
            jobId: call.body.variables.searchScheduleRequest.jobId,
          }],
          nextToken: null,
        },
      },
    }),
  });

  await Promise.all([
    harness.api.runCoordinatedScannerPass('Loop-1'),
    harness.api.runCoordinatedScannerPass('Loop-2'),
    harness.api.runCoordinatedScannerPass('Loop-3'),
  ]);
  assert.equal(harness.amazonCalls.filter(({ body }) => body.query.includes('searchJobCardsByLocation')).length, 1);
  assert.equal(harness.amazonCalls.filter(({ body }) => body.query.includes('searchScheduleCards')).length, 1);

  await Promise.all([
    harness.api.runCoordinatedScannerPass('Loop-1'),
    harness.api.runCoordinatedScannerPass('Loop-2'),
    harness.api.runCoordinatedScannerPass('Loop-3'),
  ]);
  assert.equal(harness.amazonCalls.filter(({ body }) => body.query.includes('searchJobCardsByLocation')).length, 2);
  assert.equal(harness.amazonCalls.filter(({ body }) => body.query.includes('searchScheduleCards')).length, 4);
  assert.equal(harness.api.db.prepare('SELECT COUNT(*) AS count FROM discovered_schedules').get().count, 3);
});

test('job feed preserves Amazon request URL, query, variables and headers', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({
      data: { searchJobCardsByLocation: { jobCards: [], nextToken: null } },
    }),
  });
  const result = await harness.api.fetchAmazonJobFeed();

  assert.equal(result.scanStatus, 'COMPLETE');
  assert.equal(harness.amazonCalls.length, 1);
  const { url, options, body } = harness.amazonCalls[0];
  assert.equal(url, 'https://www.jobsatamazon.co.uk/graphql');
  assert.match(body.query, /query searchJobCardsByLocation\(\$searchJobRequest: SearchJobRequest!\)/);
  assert.match(body.query, /searchJobCardsByLocation\(searchJobRequest: \$searchJobRequest\)/);
  assert.deepEqual(JSON.parse(JSON.stringify(body.variables.searchJobRequest)), {
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
  });
  assert.deepEqual(JSON.parse(JSON.stringify(options.headers)), {
    'Content-Type': 'application/json',
    Accept: 'application/json',
    'Accept-Language': 'en-GB,en;q=0.9',
    Origin: 'https://www.jobsatamazon.co.uk',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  });
});

test('Railway Amazon variables are applied to both job and schedule requests', async (t) => {
  const harness = createScannerHarness(t, {
    env: {
      AMAZON_PROXY_URL: 'http://amazon-proxy.test:8080',
      SHIFT_PROXY_URL: 'http://fallback-proxy.test:8080',
      PROXY_URL: 'http://legacy-proxy.test:8080',
      AMAZON_AUTHORIZATION: 'mock-amazon-authorization',
      AMAZON_AUTH_TOKEN: 'mock-fallback-authorization',
      AUTHORIZATION: 'mock-legacy-authorization',
    },
  });

  await harness.api.fetchAmazonJobFeed();
  await harness.api.fetchSchedulesForJob(jobSource.jobId);

  assert.equal(harness.amazonCalls.length, 2);
  for (const { options } of harness.amazonCalls) {
    assert.equal(options.dispatcher.url, 'http://amazon-proxy.test:8080');
    assert.equal(options.headers.Authorization, 'mock-amazon-authorization');
  }
});

test('legacy proxy and authorization environment-variable fallbacks remain supported', async (t) => {
  const firstFallbackHarness = createScannerHarness(t, {
    env: {
      SHIFT_PROXY_URL: 'http://shift-proxy.test:8080',
      PROXY_URL: 'http://legacy-proxy.test:8080',
      AMAZON_AUTH_TOKEN: 'mock-amazon-token',
      AUTHORIZATION: 'mock-legacy-authorization',
    },
  });
  await firstFallbackHarness.api.fetchAmazonJobFeed();
  await firstFallbackHarness.api.fetchSchedulesForJob(jobSource.jobId);

  for (const { options } of firstFallbackHarness.amazonCalls) {
    assert.equal(options.dispatcher.url, 'http://shift-proxy.test:8080');
    assert.equal(options.headers.Authorization, 'mock-amazon-token');
  }

  const finalFallbackHarness = createScannerHarness(t, {
    env: {
      PROXY_URL: 'http://legacy-proxy.test:8080',
      AUTHORIZATION: 'mock-legacy-authorization',
    },
  });
  await finalFallbackHarness.api.fetchAmazonJobFeed();
  await finalFallbackHarness.api.fetchSchedulesForJob(jobSource.jobId);

  for (const { options } of finalFallbackHarness.amazonCalls) {
    assert.equal(options.dispatcher.url, 'http://legacy-proxy.test:8080');
    assert.equal(options.headers.Authorization, 'mock-legacy-authorization');
  }
});

test('HTTP-success job responses with GraphQL errors are logged and marked PARTIAL', async (t) => {
  const logs = [];
  const harness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({
      errors: [{ message: 'Invalid SearchJobRequest; authorization=mock-secret' }],
      data: {
        searchJobCardsByLocation: {
          jobCards: [jobSource],
          nextToken: null,
        },
      },
    }),
    logger: {
      log() {},
      warn: (...values) => logs.push(values.join(' ')),
      error: (...values) => logs.push(values.join(' ')),
    },
  });

  const result = await harness.api.fetchAmazonJobFeed();
  assert.equal(result.scanStatus, 'PARTIAL');
  assert.equal(result.jobCards.length, 1);
  assert.ok(logs.some((line) => line.includes('Amazon GraphQL job feed errors')));
  assert.ok(logs.some((line) => line.includes('Invalid SearchJobRequest')));
  assert.doesNotMatch(logs.join('\n'), /mock-secret/);
  assert.deepEqual(harness.delays, []);
});

test('job-feed HTTP 403 and 429 responses are logged with status details', async (t) => {
  const logs = [];
  const logger = {
    log() {},
    warn: (...values) => logs.push(values.join(' ')),
    error: (...values) => logs.push(values.join(' ')),
  };
  const blockedHarness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 403),
    logger,
  });
  const blocked = await blockedHarness.api.fetchAmazonJobFeed();
  assert.equal(blocked.scanStatus, 'BLOCKED');
  assert.ok(logs.some((line) => line.includes('BLOCKED: Amazon HTTP 403 on job feed page 1')));
  assert.deepEqual(blockedHarness.delays, []);

  logs.length = 0;
  const throttledHarness = createScannerHarness(t, {
    jobResponder: async () => mockResponse({}, 429, { 'Retry-After': '45' }),
    logger,
  });
  const throttled = await throttledHarness.api.fetchAmazonJobFeed();
  assert.equal(throttled.scanStatus, 'PARTIAL');
  assert.ok(logs.some((line) => (
    line.includes('THROTTLED: Amazon HTTP 429 on job feed page 1')
    && line.includes('Retry-After=45')
  )));
  assert.deepEqual(throttledHarness.delays, []);
});

test('job pagination with an unconsumed nextToken is reported PARTIAL at the page limit', async (t) => {
  const harness = createScannerHarness(t, {
    jobResponder: async (_call, page) => mockResponse({
      data: {
        searchJobCardsByLocation: {
          jobCards: [],
          nextToken: `job-page-${page + 1}`,
        },
      },
    }),
  });

  const result = await harness.api.fetchAmazonJobFeed();
  assert.equal(harness.amazonCalls.length, 10);
  assert.equal(result.scanStatus, 'PARTIAL');
  assert.deepEqual(
    harness.amazonCalls.map(({ body }) => body.variables.searchJobRequest.nextToken ?? null),
    [null, ...Array.from({ length: 9 }, (_, index) => `job-page-${index + 2}`)],
  );
});

test('schedule pagination with an unconsumed nextToken is reported incomplete', async (t) => {
  const harness = createScannerHarness(t, {
    scheduleResponder: async (_call, page) => mockResponse({
      data: {
        searchScheduleCards: {
          scheduleCards: [],
          nextToken: `schedule-page-${page + 1}`,
        },
      },
    }),
  });

  const result = await harness.api.fetchSchedulesForJob(jobSource.jobId);
  assert.equal(harness.amazonCalls.length, 5);
  assert.equal(result.paginationComplete, false);
  assert.equal(result.schedules.length, 0);
  assert.deepEqual(
    harness.amazonCalls.map(({ body }) => body.variables.searchScheduleRequest.nextToken ?? null),
    [null, 'schedule-page-2', 'schedule-page-3', 'schedule-page-4', 'schedule-page-5'],
  );
});

test('HTTP-success schedule responses with GraphQL errors are logged and incomplete', async (t) => {
  const logs = [];
  const harness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({
      errors: [{ message: 'Schedule selection failed; token=mock-secret' }],
      data: {
        searchScheduleCards: {
          scheduleCards: [{ scheduleId: 'S-PARTIAL', jobId: jobSource.jobId }],
          nextToken: null,
        },
      },
    }),
    logger: {
      log() {},
      warn: (...values) => logs.push(values.join(' ')),
      error: (...values) => logs.push(values.join(' ')),
    },
  });

  const result = await harness.api.fetchSchedulesForJob(jobSource.jobId);
  assert.equal(result.paginationComplete, false);
  assert.equal(result.schedules.length, 1);
  assert.ok(logs.some((line) => line.includes('Amazon GraphQL schedule feed errors')));
  assert.ok(logs.some((line) => line.includes('Schedule selection failed')));
  assert.doesNotMatch(logs.join('\n'), /mock-secret/);
});

test('schedule HTTP 403 and 429 responses are logged with status details', async (t) => {
  const logs = [];
  const logger = {
    log() {},
    warn: (...values) => logs.push(values.join(' ')),
    error: (...values) => logs.push(values.join(' ')),
  };
  const blockedHarness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({}, 403),
    logger,
  });
  const blocked = await blockedHarness.api.fetchSchedulesForJob(jobSource.jobId);
  assert.equal(blocked.paginationComplete, false);
  assert.ok(logs.some((line) => (
    line.includes(`BLOCKED: Amazon HTTP 403 on schedule feed page 1 for job ${jobSource.jobId}`)
  )));

  logs.length = 0;
  const throttledHarness = createScannerHarness(t, {
    scheduleResponder: async () => mockResponse({}, 429, { 'retry-after': '30' }),
    logger,
  });
  const throttled = await throttledHarness.api.fetchSchedulesForJob(jobSource.jobId);
  assert.equal(throttled.paginationComplete, false);
  assert.ok(logs.some((line) => (
    line.includes(`THROTTLED: Amazon HTTP 429 on schedule feed page 1 for job ${jobSource.jobId}`)
    && line.includes('Retry-After=30')
  )));
});

test('an incomplete schedule scan cannot be reported as COMPLETE', async (t) => {
  const logs = [];
  const harness = createScannerHarness(t, {
    jobResponder: async (_call, page) => {
      if (page > 1) throw new Error('stop after one offline scan');
      return mockResponse({
        data: {
          searchJobCardsByLocation: {
            jobCards: [jobSource],
            nextToken: null,
          },
        },
      });
    },
    scheduleResponder: async () => mockResponse({
      data: {
        searchScheduleCards: {
          scheduleCards: [],
          nextToken: 'still-more-schedules',
        },
      },
    }),
    timeoutHandler(milliseconds) {
      if (milliseconds < 10000) throw new Error('stop after scan status');
      if (milliseconds === 10000) throw new Error('stop loop after status log');
    },
    logger: {
      log: (...values) => logs.push(values.join(' ')),
      warn: (...values) => logs.push(values.join(' ')),
      error: (...values) => logs.push(values.join(' ')),
    },
  });
  await assert.rejects(harness.api.runScannerLoop('Loop-test'), /stop loop after status log/);
  assert.ok(logs.some((line) => line.includes('status: [PARTIAL]')));
  assert.equal(logs.some((line) => line.includes('status: [COMPLETE]')), false);
});

test('schedule pagination status helper keeps blocked and partial states', async (t) => {
  const harness = createScannerHarness(t);
  assert.equal(harness.api.includeSchedulePaginationStatus('COMPLETE', false), 'PARTIAL');
  assert.equal(harness.api.includeSchedulePaginationStatus('PARTIAL', false), 'PARTIAL');
  assert.equal(harness.api.includeSchedulePaginationStatus('BLOCKED', false), 'BLOCKED');
});
