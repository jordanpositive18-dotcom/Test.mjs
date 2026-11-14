import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import vm from 'node:vm';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'node:url';

export function mockResponse(payload, status = 200, responseHeaders = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: {
      get(name) {
        const key = Object.keys(responseHeaders)
          .find((headerName) => headerName.toLowerCase() === name.toLowerCase());
        return key ? String(responseHeaders[key]) : null;
      },
    },
    async json() {
      return payload;
    },
    async text() {
      return JSON.stringify(payload);
    },
  };
}

export function createScannerHarness(t, {
  jobResponder = async () => mockResponse({
    data: { searchJobCardsByLocation: { jobCards: [], nextToken: null } },
  }),
  scheduleResponder = async () => mockResponse({
    data: { searchScheduleCards: { scheduleCards: [], nextToken: null } },
  }),
  telegramResponder = async () => mockResponse({ ok: true }),
  prepareDatabase = () => {},
  timeoutHandler = () => {},
  logger = { log() {}, warn() {}, error() {} },
} = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'amazon-restored-scanner-'));
  const databasePath = join(directory, 'test.sqlite');
  if (prepareDatabase) {
    const existingDatabase = new Database(databasePath);
    prepareDatabase(existingDatabase);
    existingDatabase.close();
  }
  const amazonCalls = [];
  const telegramCalls = [];
  const intervals = [];
  const delays = [];
  let scheduleCallCount = 0;
  let jobCallCount = 0;

  const sourcePath = fileURLToPath(new URL('../tracker.mjs', import.meta.url));
  const source = readFileSync(sourcePath, 'utf8')
    .replace(/^import .*;\r?\n/gm, '')
    .split('// --- APP INITIALIZATION ---')[0]
    + `
globalThis.__testApi = {
  db,
  fetchAmazonJobFeed,
  fetchSchedulesForJob,
  processDiscoveredJob,
  runScannerLoop,
  runTelegramWorkerCycle,
  startTelegramWorker,
  includeSchedulePaginationStatus,
};
`;

  const fetchMock = async (url, options = {}) => {
    if (url.includes('api.telegram.org')) {
      const call = { url, options, body: JSON.parse(options.body) };
      telegramCalls.push(call);
      return telegramResponder(call);
    }

    const body = JSON.parse(options.body);
    const call = { url, options, body };
    amazonCalls.push(call);
    if (body.query.includes('query searchScheduleCards(')) {
      scheduleCallCount += 1;
      return scheduleResponder(call, scheduleCallCount);
    }
    if (body.query.includes('query searchJobCardsByLocation(')) {
      jobCallCount += 1;
      return jobResponder(call, jobCallCount);
    }
    throw new Error('Unexpected mocked Amazon query');
  };

  class TestDatabase extends Database {
    constructor() {
      super(databasePath);
    }
  }

  const context = vm.createContext({
    Database: TestDatabase,
    fetch: fetchMock,
    ProxyAgent: class ProxyAgent {
      constructor(url) {
        this.url = url;
      }
    },
    process: {
      env: {
        TELEGRAM_BOT_TOKEN: 'offline-test-token',
        TELEGRAM_CHAT_ID: 'offline-test-chat',
      },
    },
    console: logger,
    setInterval(callback, milliseconds) {
      intervals.push({ callback, milliseconds });
      return intervals.length;
    },
    setTimeout(callback, milliseconds) {
      delays.push(milliseconds);
      timeoutHandler(milliseconds);
      queueMicrotask(callback);
      return delays.length;
    },
    clearInterval() {},
    Promise,
    Date,
    Math,
    JSON,
  });

  try {
    vm.runInContext(source, context, { filename: 'tracker.mjs' });
  } catch (error) {
    rmSync(directory, { recursive: true, force: true });
    throw error;
  }
  const api = context.__testApi;
  t.after(() => {
    api.db.close();
    rmSync(directory, { recursive: true, force: true });
  });

  return {
    api,
    amazonCalls,
    telegramCalls,
    intervals,
    delays,
    databasePath,
  };
}

export function countRows(db, table) {
  assert.match(table, /^[a-z_]+$/);
  return db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count;
}
