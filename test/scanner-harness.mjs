import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import * as fs from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
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
  databasePath: sharedDatabasePath,
  fileSystem = {},
  logger = { log() {}, warn() {}, error() {} },
  env = {},
} = {}) {
  const ownedDirectory = sharedDatabasePath ? null : mkdtempSync(join(tmpdir(), 'amazon-restored-scanner-'));
  const databasePath = sharedDatabasePath ?? join(ownedDirectory, 'test.sqlite');
  if (prepareDatabase) {
    const existingDatabase = new Database(databasePath);
    prepareDatabase(existingDatabase);
    existingDatabase.close();
  }
  const amazonCalls = [];
  const telegramCalls = [];
  const intervals = [];
  const delays = [];
  const requestTimeouts = [];
  let scheduleCallCount = 0;
  let jobCallCount = 0;

  const sourcePath = fileURLToPath(new URL('../tracker.mjs', import.meta.url));
  const source = readFileSync(sourcePath, 'utf8')
    .replace(/^import[\s\S]*?;\r?\n/gm, '')
    .split('// --- APP INITIALIZATION ---')[0]
    + `
globalThis.__testApi = {
  db,
  fetchAmazonJobFeed,
  fetchSchedulesForJob,
  processDiscoveredJob,
  runCoordinatedScannerPass,
  runScannerLoop,
  runTelegramWorkerCycle,
  startTelegramWorker,
  includeSchedulePaginationStatus,
  protectedAmazonFetch,
  getAmazonProtectionState,
  activateAmazonCooldown,
  persistAmazonProtectionAtomically,
  recoveryMarkerPath: AMAZON_PROTECTION_RECOVERY_MARKER,
};
`;

  const signalError = (signal) => {
    const error = signal.reason instanceof Error ? signal.reason : new Error('Mock request aborted');
    error.name = error.name === 'Error' ? 'AbortError' : error.name;
    return error;
  };

  const fetchMock = async (url, options = {}) => {
    if (url.includes('api.telegram.org')) {
      const call = { url, options, body: JSON.parse(options.body) };
      telegramCalls.push(call);
      return withAbortSignal(telegramResponder(call), options.signal);
    }

    const body = JSON.parse(options.body);
    const call = { url, options, body };
    amazonCalls.push(call);
    if (body.query.includes('query searchScheduleCards(')) {
      scheduleCallCount += 1;
      return withAbortSignal(scheduleResponder(call, scheduleCallCount), options.signal);
    }
    if (body.query.includes('query searchJobCardsByLocation(')) {
      jobCallCount += 1;
      return withAbortSignal(jobResponder(call, jobCallCount), options.signal);
    }
    throw new Error('Unexpected mocked Amazon query');
  };

  function withAbortSignal(responsePromise, signal) {
    if (!signal) return responsePromise;
    return new Promise((resolve, reject) => {
      const onAbort = () => reject(signalError(signal));
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener('abort', onAbort, { once: true });
      Promise.resolve(responsePromise).then(
        (response) => {
          signal.removeEventListener('abort', onAbort);
          resolve(response);
        },
        (error) => {
          signal.removeEventListener('abort', onAbort);
          reject(error);
        },
      );
    });
  }

  class TestDatabase extends Database {
    constructor() {
      super(databasePath);
    }
  }

  const context = vm.createContext({
    closeSync: fs.closeSync,
    existsSync: fs.existsSync,
    fsyncSync: fs.fsyncSync,
    openSync: fs.openSync,
    readFileSync: fs.readFileSync,
    renameSync: fs.renameSync,
    unlinkSync: fs.unlinkSync,
    writeFileSync: fs.writeFileSync,
    dirname,
    ...fileSystem,
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
        ...env,
      },
    },
    console: logger,
    setInterval(callback, milliseconds) {
      intervals.push({ callback, milliseconds });
      return intervals.length;
    },
    clearInterval() {},
    AbortController,
    Promise,
    Date,
    Math,
    JSON,
    clearTimeout(handle) {
      const timeout = requestTimeouts[handle - 1];
      if (timeout) timeout.cleared = true;
    },
    setTimeout(callback, milliseconds) {
      if (milliseconds === 30000) {
        requestTimeouts.push({ callback, milliseconds, cleared: false });
        return requestTimeouts.length;
      }
      delays.push(milliseconds);
      timeoutHandler(milliseconds);
      queueMicrotask(callback);
      return delays.length;
    },
  });

  try {
    vm.runInContext(source, context, { filename: 'tracker.mjs' });
  } catch (error) {
    if (ownedDirectory) rmSync(ownedDirectory, { recursive: true, force: true });
    throw error;
  }
  const api = context.__testApi;
  t.after(() => {
    if (api.db.open) api.db.close();
    if (ownedDirectory) rmSync(ownedDirectory, { recursive: true, force: true });
  });

  return {
    api,
    amazonCalls,
    telegramCalls,
    intervals,
    delays,
    requestTimeouts,
    databasePath,
  };
}

export function countRows(db, table) {
  assert.match(table, /^[a-z_]+$/);
  return db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count;
}
