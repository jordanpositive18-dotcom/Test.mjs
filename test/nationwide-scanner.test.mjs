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
