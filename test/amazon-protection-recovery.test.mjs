import assert from 'node:assert/strict';
import test from 'node:test';
import {
  existsSync, mkdirSync, mkdtempSync, openSync, readFileSync, rmSync, writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { createScannerHarness, mockResponse, countRows } from './scanner-harness.mjs';

const row = (h) => h.api.db.prepare(
  'SELECT reason, expires_at FROM amazon_request_protection WHERE id = 1',
).get();
const activate = (h, reason, duration) => h.api.activateAmazonCooldown(
  reason, duration, 'Loop-test', 'searchJobCardsByLocation', undefined,
);

function databaseFixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'amazon-recovery-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return join(directory, 'test.sqlite');
}

function childRun(databasePath, setup, body) {
  const script = `
    import { createScannerHarness, mockResponse } from './test/scanner-harness.mjs';
    import { existsSync, readFileSync, unlinkSync } from 'node:fs';
    ${setup}
    const result = await (async () => { ${body} })();
    console.log(JSON.stringify(result));
    process.exit(0);
  `;
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', script], {
    cwd: process.cwd(),
    timeout: 15000,
    encoding: 'utf8',
  });
  assert.equal(child.error, undefined);
  assert.equal(child.status, 0, child.stderr);
  return JSON.parse(child.stdout.trim());
}

const setupHarness = (databasePath, extra = '') => `
  const h = createScannerHarness({ after() {} }, {
    databasePath: ${JSON.stringify(databasePath)},
    ${extra}
  });
`;

test('SQLite write failure survives a real child-process exit and restart', (t) => {
  const databasePath = databaseFixture(t);
  const failed = childRun(databasePath, setupHarness(databasePath, `
    jobResponder: async () => mockResponse({}, 403),
  `), `
    h.api.db.pragma('query_only = ON');
    await h.api.fetchAmazonJobFeed('Loop-1');
    return {
      marker: JSON.parse(readFileSync(h.api.recoveryMarkerPath, 'utf8')),
      saved: h.api.db.prepare('SELECT * FROM amazon_request_protection').get(),
    };
  `);
  assert.equal(failed.marker.reason, 'http_403');
  assert.equal(failed.saved.reason, 'none');

  const restarted = childRun(databasePath, setupHarness(databasePath), `
    const result = await h.api.fetchAmazonJobFeed('Loop-2');
    return {
      status: result.scanStatus,
      calls: h.amazonCalls.length,
      saved: h.api.db.prepare('SELECT * FROM amazon_request_protection').get(),
      markerExists: existsSync(h.api.recoveryMarkerPath),
    };
  `);
  assert.equal(restarted.status, 'BLOCKED');
  assert.equal(restarted.calls, 0);
  assert.equal(restarted.saved.expires_at, failed.marker.expires_at);
  assert.equal(restarted.saved.reason, 'http_403');
  assert.equal(restarted.markerExists, false);
});

test('crash after SQLite commit but before marker removal reconciles idempotently', (t) => {
  const databasePath = databaseFixture(t);
  const failed = childRun(databasePath, `
    let preventRemoval = false;
    ${setupHarness(databasePath, `
      jobResponder: async () => mockResponse({}, 403),
      fileSystem: {
        unlinkSync(path) {
          if (preventRemoval) throw new Error('simulated crash boundary');
          return unlinkSync(path);
        },
      },
    `)}
    preventRemoval = true;
  `, `
    await h.api.fetchAmazonJobFeed('Loop-1');
    return {
      saved: h.api.db.prepare('SELECT * FROM amazon_request_protection').get(),
      markerExists: existsSync(h.api.recoveryMarkerPath),
    };
  `);
  assert.equal(failed.markerExists, true);
  assert.equal(failed.saved.reason, 'http_403');
  const restarted = childRun(databasePath, setupHarness(databasePath), `
    return {
      state: h.api.getAmazonProtectionState(),
      markerExists: existsSync(h.api.recoveryMarkerPath),
    };
  `);
  assert.equal(restarted.state.blocked, true);
  assert.equal(restarted.state.expiresAt, failed.saved.expires_at);
  assert.equal(restarted.markerExists, false);
});

test('reconciliation preserves the longer database expiry and stronger journal reason', (t) => {
  const databasePath = databaseFixture(t);
  const first = createScannerHarness(t, { databasePath });
  activate(first, 'http_429', 1800000);
  const originalExpiry = row(first).expires_at;
  writeFileSync(first.api.recoveryMarkerPath, JSON.stringify({
    version: 1, reason: 'http_403', expires_at: new Date(Date.now() + 900000).toISOString(),
  }));
  first.api.db.close();
  const restarted = createScannerHarness(t, { databasePath });
  assert.equal(row(restarted).expires_at, originalExpiry);
  assert.equal(row(restarted).reason, 'http_403');
  assert.equal(existsSync(restarted.api.recoveryMarkerPath), false);
});

for (const kind of ['corrupt', 'unreadable', 'incomplete-temp']) {
  test(`${kind} recovery marker blocks both feeds after restart`, async (t) => {
    const databasePath = databaseFixture(t);
    const first = createScannerHarness(t, { databasePath });
    const marker = first.api.recoveryMarkerPath;
    first.api.db.close();
    if (kind === 'unreadable') mkdirSync(marker);
    else writeFileSync(kind === 'incomplete-temp' ? `${marker}.tmp` : marker, '{invalid-json');
    const restarted = createScannerHarness(t, { databasePath });
    assert.equal((await restarted.api.fetchAmazonJobFeed('Loop-1')).scanStatus, 'BLOCKED');
    assert.equal((await restarted.api.fetchSchedulesForJob('JOB', 'Loop-2')).paginationComplete, false);
    assert.equal(restarted.amazonCalls.length, 0);
  });
}

test('15-minute protection cannot be shortened by later 30-second failures, including pending writes', (t) => {
  const h = createScannerHarness(t);
  h.api.db.pragma('query_only = ON');
  activate(h, 'http_403', 900000);
  const first = JSON.parse(readFileSync(h.api.recoveryMarkerPath, 'utf8'));
  activate(h, 'network_error', 30000);
  activate(h, 'http_503', 30000);
  const pending = JSON.parse(readFileSync(h.api.recoveryMarkerPath, 'utf8'));
  assert.equal(pending.expires_at, first.expires_at);
  assert.equal(pending.reason, 'http_403');
  h.api.db.pragma('query_only = OFF');
  assert.equal(h.api.getAmazonProtectionState().blocked, true);
  assert.equal(row(h).expires_at, first.expires_at);
  assert.equal(row(h).reason, 'http_403');
  activate(h, 'network_error', 30000);
  assert.equal(row(h).expires_at, first.expires_at);
});

test('multiple loop activations preserve maximum expiry and strongest overlapping reason', async (t) => {
  const h = createScannerHarness(t);
  await Promise.all([
    Promise.resolve().then(() => activate(h, 'http_429', 1800000)),
    Promise.resolve().then(() => activate(h, 'http_403', 900000)),
    Promise.resolve().then(() => activate(h, 'network_error', 30000)),
  ]);
  assert.equal(row(h).reason, 'http_403');
  assert.ok(Date.parse(row(h).expires_at) > Date.now() + 1700000);
});

test('atomic SQLite update rolls back completely if an update is rejected', (t) => {
  const h = createScannerHarness(t);
  activate(h, 'http_403', 900000);
  const before = row(h);
  h.api.db.exec(`
    CREATE TRIGGER reject_protection_update BEFORE UPDATE ON amazon_request_protection
    BEGIN SELECT RAISE(ABORT, 'mock SQLite fault'); END;
  `);
  assert.throws(() => h.api.persistAmazonProtectionAtomically({
    reason: 'http_429', expires_at: new Date(Date.now() + 1800000).toISOString(),
  }));
  assert.deepEqual(row(h), before);
  assert.equal(h.api.db.inTransaction, false);
});

test('two real processes cannot shorten SQLite expiry through competing transactions', async (t) => {
  const databasePath = databaseFixture(t);
  const h = createScannerHarness(t, { databasePath });
  const children = [];
  t.after(() => children.forEach((child) => { if (child.exitCode === null) child.kill(); }));
  async function startWriter() {
    const child = spawn(process.execPath, ['--input-type=module', '-e', `
      import { createScannerHarness } from './test/scanner-harness.mjs';
      ${setupHarness(databasePath)}
      process.on('message', (record) => {
        try {
          for (let i = 0; i < 15; i++) h.api.persistAmazonProtectionAtomically(record);
          h.api.db.close();
          process.send({ done: true }, () => process.exit(0));
        } catch (error) {
          process.send({ error: error.message }, () => process.exit(1));
        }
      });
      process.send({ ready: true });
    `], { cwd: process.cwd(), stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
    children.push(child);
    const [message] = await once(child, 'message');
    assert.equal(message.ready, true);
    return child;
  }
  // Only SQLite writers run concurrently; sidecar ownership remains single-process.
  const longWriter = await startWriter();
  const shortWriter = await startWriter();
  const maximum = new Date(Date.now() + 900000).toISOString();
  const longDone = once(longWriter, 'message');
  const shortDone = once(shortWriter, 'message');
  const longExit = once(longWriter, 'exit');
  const shortExit = once(shortWriter, 'exit');
  longWriter.send({ reason: 'http_403', expires_at: maximum });
  shortWriter.send({ reason: 'network_error', expires_at: new Date(Date.now() + 30000).toISOString() });
  const [[longResult], [shortResult]] = await Promise.all([longDone, shortDone]);
  assert.equal(longResult.done, true, JSON.stringify(longResult));
  assert.equal(shortResult.done, true, JSON.stringify(shortResult));
  await Promise.all([longExit, shortExit]);
  assert.equal(row(h).expires_at, maximum);
  assert.equal(row(h).reason, 'http_403');
});

for (const feed of ['job', 'schedule']) {
  test(`${feed} response resolved immediately before shared abort cannot be processed`, async (t) => {
    let h;
    let jsonReads = 0;
    const payload = feed === 'job'
      ? { data: { searchJobCardsByLocation: { jobCards: [{ jobId: 'REJECTED' }] } } }
      : { data: { searchScheduleCards: { scheduleCards: [{ scheduleId: 'REJECTED' }] } } };
    const response = {
      ok: true,
      get status() {
        activate(h, 'http_403', 900000);
        return 200;
      },
      async json() { jsonReads += 1; return payload; },
    };
    h = createScannerHarness(t, {
      [feed === 'job' ? 'jobResponder' : 'scheduleResponder']: async () => response,
    });
    if (feed === 'job') {
      const result = await h.api.fetchAmazonJobFeed('Loop-1');
      assert.equal(result.scanStatus, 'BLOCKED');
      assert.equal(result.jobCards.length, 0);
    } else {
      const result = await h.api.processDiscoveredJob({ jobId: 'JOB' }, new Date().toISOString(), 'Loop-2');
      assert.equal(result.paginationComplete, false);
      assert.equal(result.count, 0);
    }
    assert.equal(jsonReads, 0);
    assert.equal(h.amazonCalls[0].options.signal.aborted, true);
    assert.equal(countRows(h.api.db, 'discovered_schedules'), 0);
    assert.equal(countRows(h.api.db, 'telegram_outbox'), 0);
  });
}

test('shared state is rechecked after body parsing even without a local abort signal', async (t) => {
  let h;
  h = createScannerHarness(t, {
    jobResponder: async () => ({
      ok: true, status: 200,
      async json() {
        h.api.persistAmazonProtectionAtomically({
          reason: 'http_403', expires_at: new Date(Date.now() + 900000).toISOString(),
        });
        return { data: { searchJobCardsByLocation: { jobCards: [{ jobId: 'REJECTED' }] } } };
      },
    }),
  });
  const result = await h.api.fetchAmazonJobFeed('Loop-1');
  assert.equal(result.scanStatus, 'BLOCKED');
  assert.equal(result.jobCards.length, 0);
});

test('cooldown during schedule body parsing discards schedules and creates no alerts', async (t) => {
  let h;
  h = createScannerHarness(t, {
    scheduleResponder: async () => ({
      ok: true, status: 200,
      async json() {
        activate(h, 'http_403', 900000);
        return { data: { searchScheduleCards: { scheduleCards: [{ scheduleId: 'REJECTED' }] } } };
      },
    }),
  });
  const result = await h.api.processDiscoveredJob({ jobId: 'JOB' }, new Date().toISOString(), 'Loop-3');
  assert.equal(result.paginationComplete, false);
  assert.equal(countRows(h.api.db, 'discovered_schedules'), 0);
  assert.equal(countRows(h.api.db, 'telegram_outbox'), 0);
});

test('timeout also aborts a stalled response body and releases the request', async (t) => {
  const h = createScannerHarness(t, {
    jobResponder: async () => ({ ok: true, status: 200, json: async () => new Promise(() => {}) }),
  });
  const pending = h.api.fetchAmazonJobFeed('Loop-1');
  await new Promise((resolve) => setImmediate(resolve));
  h.requestTimeouts[0].callback();
  const result = await pending;
  assert.equal(result.scanStatus, 'PARTIAL');
  assert.equal(row(h).reason, 'request_timeout');
  assert.equal(h.amazonCalls[0].options.signal.aborted, true);
  assert.equal(h.requestTimeouts[0].cleared, true);
});

test('a missing protection row in an initialized database never grants permission to send', async (t) => {
  const h = createScannerHarness(t);
  h.api.db.exec('DELETE FROM amazon_request_protection');
  assert.equal((await h.api.fetchAmazonJobFeed('Loop-1')).scanStatus, 'BLOCKED');
  assert.equal((await h.api.fetchAmazonJobFeed('Loop-2')).scanStatus, 'BLOCKED');
  assert.equal(h.amazonCalls.length, 0);
  assert.equal(row(h).reason, 'protection_state_unknown');
});

test('marker write failure still saves SQLite protection and remains blocked until recovery', async (t) => {
  const databasePath = databaseFixture(t);
  let denyMarkerWrite = false;
  const h = createScannerHarness(t, {
    databasePath,
    fileSystem: {
      openSync(path, ...args) {
        if (denyMarkerWrite && String(path).endsWith('.json.tmp')) throw new Error('mock EACCES');
        return openSync(path, ...args);
      },
    },
    jobResponder: async () => mockResponse({}, 403),
  });
  denyMarkerWrite = true;
  await h.api.fetchAmazonJobFeed('Loop-1');
  assert.equal(row(h).reason, 'http_403');
  const expiry = row(h).expires_at;
  assert.equal((await h.api.fetchAmazonJobFeed('Loop-2')).scanStatus, 'BLOCKED');
  assert.equal(h.amazonCalls.length, 1);
  h.api.db.close();
  const restarted = createScannerHarness(t, { databasePath });
  assert.equal(restarted.api.getAmazonProtectionState().blocked, true);
  assert.equal(row(restarted).expires_at, expiry);
});

test('valid temporary marker from a crash before rename is recovered on restart', (t) => {
  const databasePath = databaseFixture(t);
  const first = createScannerHarness(t, { databasePath });
  const expiry = new Date(Date.now() + 900000).toISOString();
  writeFileSync(`${first.api.recoveryMarkerPath}.tmp`, JSON.stringify({
    version: 1, reason: 'http_403', expires_at: expiry,
  }));
  first.api.db.close();
  const restarted = createScannerHarness(t, { databasePath });
  assert.equal(row(restarted).expires_at, expiry);
  assert.equal(restarted.api.getAmazonProtectionState().blocked, true);
  assert.equal(existsSync(`${restarted.api.recoveryMarkerPath}.tmp`), false);
});

test('missing SQLite protection state on restart is not treated as a fresh installation', async (t) => {
  const databasePath = databaseFixture(t);
  const first = createScannerHarness(t, { databasePath });
  first.api.db.exec('DELETE FROM amazon_request_protection');
  first.api.db.close();
  const restarted = createScannerHarness(t, { databasePath });
  assert.equal((await restarted.api.fetchAmazonJobFeed('Loop-3')).scanStatus, 'BLOCKED');
  assert.equal(restarted.amazonCalls.length, 0);
  assert.equal(row(restarted).reason, 'protection_state_unknown');
});
