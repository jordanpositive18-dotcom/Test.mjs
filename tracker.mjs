import Database from 'better-sqlite3';
import { fetch, ProxyAgent } from 'undici';
import {
  closeSync,
  existsSync,
  fsyncSync,
  openSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs';
import { dirname } from 'node:path';

// Initialize SQLite database (persisted on Railway volume or local file)
const db = new Database('shift_tracker.db');
const protectionTableExistedBeforeMigration = Boolean(db.prepare(`
  SELECT 1 AS present FROM sqlite_master
  WHERE type = 'table' AND name = 'amazon_request_protection'
`).get());

// Existing schedule records are preserved; outbox columns are additive migrations.
db.exec(`
  CREATE TABLE IF NOT EXISTS discovered_schedules (
    schedule_id TEXT PRIMARY KEY,
    discovered_at TEXT,
    amazon_detected_at TEXT,
    notification_queued_at TEXT,
    telegram_sent_at TEXT
  );

  CREATE TABLE IF NOT EXISTS telegram_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id TEXT,
    message TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TEXT
  );

  CREATE TABLE IF NOT EXISTS amazon_request_protection (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    reason TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
`);

// Normalize legacy SQLite UTC timestamps to ISO 8601 without changing records.
db.exec(`
  UPDATE discovered_schedules
  SET discovered_at = CASE
        WHEN discovered_at GLOB '????-??-?? ??:??:??'
          THEN substr(discovered_at, 1, 10) || 'T' || substr(discovered_at, 12, 8) || '.000Z'
        ELSE discovered_at
      END,
      amazon_detected_at = CASE
        WHEN amazon_detected_at GLOB '????-??-?? ??:??:??'
          THEN substr(amazon_detected_at, 1, 10) || 'T' || substr(amazon_detected_at, 12, 8) || '.000Z'
        ELSE amazon_detected_at
      END,
      notification_queued_at = CASE
        WHEN notification_queued_at GLOB '????-??-?? ??:??:??'
          THEN substr(notification_queued_at, 1, 10) || 'T' || substr(notification_queued_at, 12, 8) || '.000Z'
        ELSE notification_queued_at
      END,
      telegram_sent_at = CASE
        WHEN telegram_sent_at GLOB '????-??-?? ??:??:??'
          THEN substr(telegram_sent_at, 1, 10) || 'T' || substr(telegram_sent_at, 12, 8) || '.000Z'
        ELSE telegram_sent_at
      END
  WHERE discovered_at GLOB '????-??-?? ??:??:??'
     OR amazon_detected_at GLOB '????-??-?? ??:??:??'
     OR notification_queued_at GLOB '????-??-?? ??:??:??'
     OR telegram_sent_at GLOB '????-??-?? ??:??:??';
`);

const outboxColumns = new Set(
  db.prepare('PRAGMA table_info(telegram_outbox)').all().map(({ name }) => name),
);
for (const [name, definition] of [
  ['notification_key', 'TEXT'],
  ['schedule_ids', 'TEXT'],
  ['attempt_count', 'INTEGER NOT NULL DEFAULT 0'],
  ['next_attempt_at', 'TEXT'],
  ['last_error', 'TEXT'],
  ['claim_until', 'TEXT'],
]) {
  if (!outboxColumns.has(name)) {
    db.exec(`ALTER TABLE telegram_outbox ADD COLUMN ${name} ${definition}`);
  }
}
db.prepare(`
  UPDATE telegram_outbox
  SET notification_key = 'legacy-row:' || id
  WHERE notification_key IS NULL OR notification_key = ''
`).run();
for (const row of db.prepare(
  'SELECT id, schedule_id FROM telegram_outbox WHERE schedule_ids IS NULL',
).all()) {
  db.prepare('UPDATE telegram_outbox SET schedule_ids = ? WHERE id = ?').run(
    JSON.stringify(row.schedule_id ? [row.schedule_id] : []),
    row.id,
  );
}
db.exec(`
  CREATE UNIQUE INDEX IF NOT EXISTS telegram_outbox_notification_key_unique
  ON telegram_outbox(notification_key);
`);

// Prepared statements for high performance & atomic safety
const stmtClaimSchedule = db.prepare(`
  INSERT OR IGNORE INTO discovered_schedules (schedule_id, discovered_at, amazon_detected_at)
  VALUES (?, ?, ?)
`);

const stmtUpdateQueueTime = db.prepare(`
  UPDATE discovered_schedules SET notification_queued_at = ? WHERE schedule_id = ?
`);

const stmtEnqueueTelegram = db.prepare(`
  INSERT OR IGNORE INTO telegram_outbox
    (schedule_id, notification_key, schedule_ids, message, status, created_at)
  VALUES (?, ?, ?, ?, 'pending', datetime('now'))
`);

const stmtGetPendingAlerts = db.prepare(`
  SELECT id, schedule_id, schedule_ids, message, attempt_count
  FROM telegram_outbox
  WHERE status = 'pending'
    AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
  ORDER BY id ASC
  LIMIT 5
`);

const stmtClaimAlert = db.prepare(`
  UPDATE telegram_outbox
  SET status = 'processing', attempt_count = attempt_count + 1, claim_until = ?
  WHERE id = ? AND status = 'pending'
    AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
`);

const stmtRecoverExpiredAlerts = db.prepare(`
  UPDATE telegram_outbox
  SET status = CASE WHEN attempt_count >= ? THEN 'failed' ELSE 'pending' END,
      last_error = CASE
        WHEN attempt_count >= ? THEN 'Claim expired after the final delivery attempt'
        ELSE last_error
      END,
      claim_until = NULL
  WHERE status = 'processing' AND claim_until <= ?
`);

const stmtMarkAlertSent = db.prepare(`
  UPDATE telegram_outbox
  SET status = 'sent', claim_until = NULL, next_attempt_at = NULL, last_error = NULL
  WHERE id = ?
`);

const stmtMarkAlertFailed = db.prepare(`
  UPDATE telegram_outbox
  SET status = ?, next_attempt_at = ?, last_error = ?, claim_until = NULL
  WHERE id = ?
`);

const stmtUpdateSentTime = db.prepare(`
  UPDATE discovered_schedules SET telegram_sent_at = ? WHERE schedule_id = ?
`);

const stmtGetTelemetryTimestamps = db.prepare(`
  SELECT amazon_detected_at, notification_queued_at, telegram_sent_at
  FROM discovered_schedules WHERE schedule_id = ?
`);

const stmtGetAmazonProtection = db.prepare(`
  SELECT reason, expires_at
  FROM amazon_request_protection
  WHERE id = 1
`);

const stmtSetAmazonProtection = db.prepare(`
  INSERT INTO amazon_request_protection (id, reason, expires_at, updated_at)
  VALUES (1, ?, ?, ?)
  ON CONFLICT(id) DO UPDATE SET
    reason = excluded.reason,
    expires_at = excluded.expires_at,
    updated_at = excluded.updated_at
`);

// Configuration from environment variables
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const AMAZON_AUTH_TOKEN = process.env.AMAZON_AUTHORIZATION
  || process.env.AMAZON_AUTH_TOKEN
  || process.env.AUTHORIZATION;
const PROXY_URL = process.env.AMAZON_PROXY_URL
  || process.env.SHIFT_PROXY_URL
  || process.env.PROXY_URL;

// Configure Undici proxy agent if proxy is provided
let dispatcher = undefined;
if (PROXY_URL) {
  dispatcher = new ProxyAgent(PROXY_URL);
  console.log('[Bot] Configured proxy agent for secure requests.');
}

let telegramWorkerRunning = false;
const TELEGRAM_MAX_ATTEMPTS = 3;
const TELEGRAM_RETRY_BASE_MS = 5000;
const TELEGRAM_RETRY_MAX_MS = 60000;
const AMAZON_REQUEST_TIMEOUT_MS = 30000;
const AMAZON_403_COOLDOWN_MS = 15 * 60 * 1000;
const AMAZON_429_FALLBACK_COOLDOWN_MS = 60 * 1000;
const AMAZON_TRANSIENT_COOLDOWN_MS = 30 * 1000;
const AMAZON_AUTH_COOLDOWN_MS = 15 * 60 * 1000;
const AMAZON_PROTECTION_RECOVERY_MARKER = `${db.name}.amazon-protection-recovery.json`;
const AMAZON_PROTECTION_RECOVERY_TEMP = `${AMAZON_PROTECTION_RECOVERY_MARKER}.tmp`;
const AMAZON_PROTECTION_UNKNOWN_EXPIRY = '9999-12-31T23:59:59.999Z';
const AMAZON_PROTECTION_CLEAR_EXPIRY = '1970-01-01T00:00:00.000Z';
const activeAmazonControllers = new Set();
let pendingProtectionWrite = null;
let protectionStateUnavailable = false;
let protectionStateInitialized = false;
let allowProtectionBootstrap = !protectionTableExistedBeforeMigration;

function protectionReasonPriority(reason) {
  if (reason === 'protection_state_unknown') return 100;
  if (reason === 'http_403') return 90;
  if (reason === 'http_401_authentication') return 80;
  if (reason === 'http_429') return 70;
  if (reason.startsWith('http_5')) return 60;
  if (reason === 'request_timeout' || reason === 'network_error') return 50;
  return reason === 'none' ? 0 : 40;
}

function normalizeProtectionRecord(record) {
  if (!record || typeof record.reason !== 'string' || typeof record.expires_at !== 'string') {
    return null;
  }
  const expiryMs = Date.parse(record.expires_at);
  if (!Number.isFinite(expiryMs)) return null;
  return {
    reason: record.reason,
    expires_at: new Date(expiryMs).toISOString(),
  };
}

function mergeProtectionRecords(records, now = Date.now()) {
  const valid = records.filter(Boolean).map((record) => {
    const normalized = normalizeProtectionRecord(record);
    if (!normalized) throw new Error('Invalid Amazon protection state');
    return normalized;
  });
  if (valid.length === 0) {
    return { reason: 'none', expires_at: AMAZON_PROTECTION_CLEAR_EXPIRY };
  }
  const maxExpiry = Math.max(...valid.map((record) => Date.parse(record.expires_at)));
  const active = valid.filter((record) => Date.parse(record.expires_at) > now);
  const candidates = active.length ? active : valid.filter(
    (record) => Date.parse(record.expires_at) === maxExpiry,
  );
  const strongest = candidates.sort((left, right) => (
    protectionReasonPriority(right.reason) - protectionReasonPriority(left.reason)
    || Date.parse(right.expires_at) - Date.parse(left.expires_at)
  ))[0];
  return {
    reason: strongest.reason,
    expires_at: new Date(maxExpiry).toISOString(),
  };
}

function readAmazonProtectionMarkers() {
  const records = [];
  for (const path of [AMAZON_PROTECTION_RECOVERY_MARKER, AMAZON_PROTECTION_RECOVERY_TEMP]) {
    if (!existsSync(path)) continue;
    let parsed;
    try {
      parsed = JSON.parse(readFileSync(path, 'utf8'));
    } catch {
      throw new Error('Amazon protection recovery marker is unreadable');
    }
    const record = normalizeProtectionRecord(parsed);
    if (parsed?.version !== 1 || !record) {
      throw new Error('Amazon protection recovery marker is invalid');
    }
    records.push(record);
  }
  return records;
}

function syncProtectionMarkerDirectory() {
  const directoryHandle = openSync(dirname(AMAZON_PROTECTION_RECOVERY_MARKER), 'r');
  try {
    fsyncSync(directoryHandle);
  } finally {
    closeSync(directoryHandle);
  }
}

function writeAmazonProtectionMarker(record) {
  const fileHandle = openSync(AMAZON_PROTECTION_RECOVERY_TEMP, 'w', 0o600);
  try {
    writeFileSync(fileHandle, JSON.stringify({
      version: 1,
      reason: record.reason,
      expires_at: record.expires_at,
      updated_at: new Date().toISOString(),
    }));
    fsyncSync(fileHandle);
  } finally {
    closeSync(fileHandle);
  }
  syncProtectionMarkerDirectory();
  renameSync(AMAZON_PROTECTION_RECOVERY_TEMP, AMAZON_PROTECTION_RECOVERY_MARKER);
  syncProtectionMarkerDirectory();
}

function clearAmazonProtectionMarkers() {
  let changed = false;
  for (const path of [AMAZON_PROTECTION_RECOVERY_MARKER, AMAZON_PROTECTION_RECOVERY_TEMP]) {
    if (existsSync(path)) {
      unlinkSync(path);
      changed = true;
    }
  }
  if (changed) syncProtectionMarkerDirectory();
}

function persistAmazonProtectionAtomically(candidate) {
  // The scanner uses one Node process. BEGIN IMMEDIATE also serializes state updates
  // if another process accidentally shares this SQLite database.
  return db.transaction(() => {
    const existing = stmtGetAmazonProtection.get();
    const merged = mergeProtectionRecords([existing, candidate]);
    stmtSetAmazonProtection.run(merged.reason, merged.expires_at, new Date().toISOString());
    const saved = stmtGetAmazonProtection.get();
    const normalizedSaved = normalizeProtectionRecord(saved);
    if (!normalizedSaved
      || Date.parse(normalizedSaved.expires_at) < Date.parse(merged.expires_at)
      || protectionReasonPriority(normalizedSaved.reason) < protectionReasonPriority(merged.reason)) {
      throw new Error('Amazon cooldown state did not persist safely');
    }
    return normalizedSaved;
  }).immediate();
}

function reconcileAmazonProtectionState() {
  try {
    const markers = readAmazonProtectionMarkers();
    const databaseState = stmtGetAmazonProtection.get();
    let records = [...markers, databaseState, pendingProtectionWrite];

    if (markers.length === 0 && !databaseState && !pendingProtectionWrite) {
      if (!allowProtectionBootstrap) {
        records.push({
          reason: 'protection_state_unknown',
          expires_at: AMAZON_PROTECTION_UNKNOWN_EXPIRY,
        });
      } else {
        records.push({ reason: 'none', expires_at: AMAZON_PROTECTION_CLEAR_EXPIRY });
      }
    }

    const merged = mergeProtectionRecords(records);
    if (markers.length === 0 || pendingProtectionWrite) {
      writeAmazonProtectionMarker(merged);
    }
    const persisted = persistAmazonProtectionAtomically(merged);
    if (Date.parse(persisted.expires_at) < Date.parse(merged.expires_at)
      || protectionReasonPriority(persisted.reason) < protectionReasonPriority(merged.reason)) {
      throw new Error('Amazon cooldown reconciliation was incomplete');
    }
    clearAmazonProtectionMarkers();
    pendingProtectionWrite = null;
    protectionStateUnavailable = false;
    protectionStateInitialized = true;
    allowProtectionBootstrap = false;
    return true;
  } catch {
    protectionStateUnavailable = true;
    protectionStateInitialized = false;
    return false;
  }
}

function getAmazonProtectionState() {
  try {
    if (readAmazonProtectionMarkers().length) protectionStateUnavailable = true;
  } catch {
    return { blocked: true, reason: 'protection_marker_unavailable', expiresAt: null };
  }
  if (!protectionStateInitialized || protectionStateUnavailable || pendingProtectionWrite) {
    if (!reconcileAmazonProtectionState()) {
      return {
        blocked: true,
        reason: 'protection_state_unavailable',
        expiresAt: null,
      };
    }
  }

  try {
    const state = stmtGetAmazonProtection.get();
    const normalized = normalizeProtectionRecord(state);
    if (!normalized) {
      protectionStateUnavailable = true;
      return {
        blocked: true,
        reason: 'protection_state_missing',
        expiresAt: null,
      };
    }
    const expiresAtMs = Date.parse(normalized.expires_at);
    return {
      blocked: normalized.reason !== 'none' && expiresAtMs > Date.now(),
      reason: normalized.reason === 'none' ? null : normalized.reason,
      expiresAt: normalized.expires_at,
    };
  } catch {
    protectionStateUnavailable = true;
    return {
      blocked: true,
      reason: 'protection_state_unavailable',
      expiresAt: null,
    };
  }
}

function abortOtherAmazonRequests(exceptController, reason) {
  for (const controller of activeAmazonControllers) {
    if (controller !== exceptController) {
      controller.abort({ type: 'amazon-shared-cooldown', reason });
    }
  }
}

function activateAmazonCooldown(reason, durationMs, loopName, operation, controller, status = 'none') {
  const now = Date.now();
  const candidate = {
    reason,
    expires_at: new Date(now + durationMs).toISOString(),
  };
  let merged = mergeProtectionRecords([pendingProtectionWrite, candidate]);
  let stateWasPersisted = false;

  try {
    const markerRecords = readAmazonProtectionMarkers();
    // Journal the failure using every durable/in-memory value known without
    // consulting SQLite. A failed SQLite read must not erase this response.
    merged = mergeProtectionRecords([
      ...markerRecords,
      pendingProtectionWrite,
      candidate,
    ]);
    let markerWritten = false;
    try {
      writeAmazonProtectionMarker(merged);
      markerWritten = true;
    } catch {
      // Continue to SQLite; either durable store may still preserve the cooldown.
    }
    pendingProtectionWrite = merged;

    let databaseState;
    try {
      databaseState = stmtGetAmazonProtection.get();
    } catch {
      // The marker already contains the received failure and remains on disk.
    }

    if (databaseState) {
      merged = mergeProtectionRecords([
        ...markerRecords,
        databaseState,
        pendingProtectionWrite,
        candidate,
      ]);
      try {
        writeAmazonProtectionMarker(merged);
        markerWritten = true;
      } catch {
        // Keep the previously written marker and fail closed after the DB attempt.
        markerWritten = false;
      }
      pendingProtectionWrite = merged;
    }

    const persisted = persistAmazonProtectionAtomically(merged);
    merged = mergeProtectionRecords([merged, persisted]);
    stateWasPersisted = true;
    if (!markerWritten) throw new Error('Amazon protection marker could not be written');
    clearAmazonProtectionMarkers();
    pendingProtectionWrite = null;
    protectionStateUnavailable = false;
    protectionStateInitialized = true;
  } catch {
    pendingProtectionWrite = mergeProtectionRecords([pendingProtectionWrite, merged, candidate]);
    protectionStateUnavailable = true;
    protectionStateInitialized = false;
  }

  abortOtherAmazonRequests(controller, reason);
  const expiry = merged.expires_at;
  console.warn(
    `[AmazonRequest] loop=${loopName} operation=${operation} status=${status} `
    + `cooldownReason=${merged.reason} cooldownExpiresAt=${expiry} `
    + `persisted=${stateWasPersisted}`,
  );
  return expiry;
}

function parseRetryAfterMs(response, now = Date.now()) {
  const value = response?.headers?.get?.('retry-after');
  if (typeof value !== 'string' || value.trim() === '') return null;
  const trimmed = value.trim();
  if (/^\d+(?:\.\d+)?$/.test(trimmed)) {
    return Math.max(0, Math.ceil(Number(trimmed) * 1000));
  }
  const dateMs = Date.parse(trimmed);
  if (!Number.isFinite(dateMs)) return null;
  return Math.max(0, dateMs - now);
}

function amazonFailureCooldown(response) {
  if (response.status === 403) {
    return { reason: 'http_403', durationMs: AMAZON_403_COOLDOWN_MS };
  }
  if (response.status === 429) {
    return {
      reason: 'http_429',
      durationMs: parseRetryAfterMs(response) ?? AMAZON_429_FALLBACK_COOLDOWN_MS,
    };
  }
  if (response.status === 401) {
    return { reason: 'http_401_authentication', durationMs: AMAZON_AUTH_COOLDOWN_MS };
  }
  if (response.status >= 500) {
    return { reason: `http_${response.status}`, durationMs: AMAZON_TRANSIENT_COOLDOWN_MS };
  }
  return null;
}

async function protectedAmazonFetch(operation, loopName, url, options) {
  const before = getAmazonProtectionState();
  if (before.blocked) {
    console.warn(
      `[AmazonRequest] loop=${loopName} operation=${operation} status=none durationMs=0 `
      + `cooldownReason=${before.reason} cooldownExpiresAt=${before.expiresAt ?? 'unknown'} `
      + `scan=BLOCKED`,
    );
    return null;
  }

  const controller = new AbortController();
  activeAmazonControllers.add(controller);
  const startedAt = Date.now();
  let timeoutHandle;
  let status = 'none';
  const onAbort = () => rejectAborted(new Error('Amazon request aborted'));
  let rejectAborted;
  const aborted = new Promise((_, reject) => { rejectAborted = reject; });
  controller.signal.addEventListener('abort', onAbort, { once: true });
  try {
    timeoutHandle = setTimeout(() => {
      controller.abort({ type: 'amazon-timeout' });
    }, AMAZON_REQUEST_TIMEOUT_MS);
    const response = await Promise.race([
      fetch(url, { ...options, signal: controller.signal }),
      aborted,
    ]);
    status = response.status;
    const cooldown = !response.ok ? amazonFailureCooldown(response) : null;
    if (cooldown) {
      const expiry = activateAmazonCooldown(
        cooldown.reason,
        cooldown.durationMs,
        loopName,
        operation,
        controller,
        response.status,
      );
      try {
        await Promise.race([response.body?.cancel?.(), aborted]);
      } catch {
        // Closing an already-aborted or empty error body must not disable protection.
      }
      console.warn(
        `[AmazonRequest] loop=${loopName} operation=${operation} status=${response.status} `
        + `durationMs=${Date.now() - startedAt} cooldownExpiresAt=${expiry} scan=BLOCKED`,
      );
      return { response, data: null };
    }

    if (controller.signal.aborted
      && controller.signal.reason?.type !== 'amazon-shared-cooldown') {
      throw new Error('Amazon request aborted before response acceptance');
    }
    if (controller.signal.reason?.type === 'amazon-shared-cooldown') {
      try {
        await Promise.race([response.body?.cancel?.(), aborted]);
      } catch {
        // Discard an already-aborted response.
      }
      const state = getAmazonProtectionState();
      console.warn(
        `[AmazonRequest] loop=${loopName} operation=${operation} status=aborted `
        + `durationMs=${Date.now() - startedAt} cooldownReason=${state.reason ?? 'shared_cooldown'} `
        + `cooldownExpiresAt=${state.expiresAt ?? 'unknown'} scan=BLOCKED`,
      );
      return null;
    }

    const responseProtection = getAmazonProtectionState();
    if (responseProtection.blocked) {
      controller.abort({
        type: 'amazon-shared-cooldown',
        reason: responseProtection.reason,
      });
      try {
        await Promise.race([response.body?.cancel?.(), aborted]);
      } catch {
        // The response is discarded while the shared cooldown remains active.
      }
      return null;
    }
    {
      const data = response.ok ? await Promise.race([response.json(), aborted]) : null;
      if (controller.signal.aborted
        && controller.signal.reason?.type !== 'amazon-shared-cooldown') {
        throw new Error('Amazon request aborted during response processing');
      }
      if (controller.signal.reason?.type === 'amazon-shared-cooldown') {
        const state = getAmazonProtectionState();
        console.warn(
          `[AmazonRequest] loop=${loopName} operation=${operation} status=aborted `
          + `durationMs=${Date.now() - startedAt} cooldownReason=${state.reason ?? 'shared_cooldown'} `
          + `cooldownExpiresAt=${state.expiresAt ?? 'unknown'} scan=BLOCKED`,
        );
        return null;
      }
      const afterBodyProtection = getAmazonProtectionState();
      if (afterBodyProtection.blocked) {
        controller.abort({
          type: 'amazon-shared-cooldown',
          reason: afterBodyProtection.reason,
        });
        return null;
      }
      console.log(
        `[AmazonRequest] loop=${loopName} operation=${operation} status=${response.status} `
        + `durationMs=${Date.now() - startedAt} scan=${response.ok ? 'REQUEST_OK' : 'PARTIAL'}`,
      );
      return { response, data };
    }
  } catch (error) {
    if (controller.signal.reason?.type === 'amazon-shared-cooldown') {
      const state = getAmazonProtectionState();
      console.warn(
        `[AmazonRequest] loop=${loopName} operation=${operation} status=aborted `
        + `durationMs=${Date.now() - startedAt} cooldownReason=${state.reason ?? 'shared_cooldown'} `
        + `cooldownExpiresAt=${state.expiresAt ?? 'unknown'} scan=BLOCKED`,
      );
      return null;
    }

    const timedOut = controller.signal.reason?.type === 'amazon-timeout';
    const reason = timedOut ? 'request_timeout' : 'network_error';
    const expiry = activateAmazonCooldown(
      reason,
      AMAZON_TRANSIENT_COOLDOWN_MS,
      loopName,
      operation,
      controller,
      status,
    );
    console.error(
      `[AmazonRequest] loop=${loopName} operation=${operation} status=${status} `
      + `errorCategory=${reason} durationMs=${Date.now() - startedAt} `
      + `cooldownExpiresAt=${expiry} scan=PARTIAL`,
    );
    throw error;
  } finally {
    if (timeoutHandle !== undefined) clearTimeout(timeoutHandle);
    controller.signal.removeEventListener('abort', onAbort);
    activeAmazonControllers.delete(controller);
  }
}

// Reconcile the durable journal before any loop or Telegram worker is started.
reconcileAmazonProtectionState();

function recordTelegramFailure(alert, error) {
  const attemptCount = alert.attempt_count;
  const terminal = attemptCount >= TELEGRAM_MAX_ATTEMPTS;
  const retryDelay = Math.min(
    TELEGRAM_RETRY_BASE_MS * (2 ** Math.max(0, attemptCount - 1)),
    TELEGRAM_RETRY_MAX_MS,
  );
  const retryAt = terminal ? null : new Date(Date.now() + retryDelay).toISOString();
  const message = String(error?.message ?? error).slice(0, 500);
  stmtMarkAlertFailed.run(terminal ? 'failed' : 'pending', retryAt, message, alert.id);
  console.error(
    `[Telegram] ${terminal ? '❌ Permanently failed' : '⚠️ Retry scheduled'} `
    + `for outbox record ${alert.id} (attempt ${attemptCount}/${TELEGRAM_MAX_ATTEMPTS}): ${message}`,
  );
}

async function runTelegramWorkerCycle() {
  if (telegramWorkerRunning) return;
  telegramWorkerRunning = true;
  try {
    const now = new Date().toISOString();
    stmtRecoverExpiredAlerts.run(TELEGRAM_MAX_ATTEMPTS, TELEGRAM_MAX_ATTEMPTS, now);
    const pendingAlerts = stmtGetPendingAlerts.all(now);
    for (const alert of pendingAlerts) {
      const claimUntil = new Date(Date.now() + 60000).toISOString();
      const claimed = stmtClaimAlert.run(claimUntil, alert.id, now);
      if (claimed.changes !== 1) continue;

      const attempt = { ...alert, attempt_count: alert.attempt_count + 1 };
      try {
        if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
          throw new Error('Telegram credentials are not configured');
        }

        const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: TELEGRAM_CHAT_ID,
            text: alert.message
          })
        });
        let telegramPayload;
        try {
          telegramPayload = await res.json();
        } catch {
          throw new Error(`Telegram returned invalid JSON (HTTP ${res.status})`);
        }

        if (!res.ok || telegramPayload?.ok !== true) {
          const description = typeof telegramPayload?.description === 'string'
            ? `: ${telegramPayload.description.slice(0, 300)}`
            : '';
          throw new Error(`Telegram rejected the message (HTTP ${res.status})${description}`);
        }

        stmtMarkAlertSent.run(alert.id);
        const sentAt = new Date().toISOString();
        let scheduleIds = [];
        try {
          scheduleIds = JSON.parse(alert.schedule_ids || '[]');
        } catch {
          scheduleIds = alert.schedule_id ? [alert.schedule_id] : [];
        }
        for (const scheduleId of scheduleIds) {
          stmtUpdateSentTime.run(sentAt, scheduleId);
          const row = stmtGetTelemetryTimestamps.get(scheduleId);
          if (row?.amazon_detected_at && row?.notification_queued_at && row?.telegram_sent_at) {
            const tAmazon = new Date(row.amazon_detected_at).getTime();
            const tQueued = new Date(row.notification_queued_at).getTime();
            const tSent = new Date(row.telegram_sent_at).getTime();
            console.log(
              `[TELEMETRY LATENCY] Schedule ${scheduleId} | `
              + `Discovery->Queue: ${tQueued - tAmazon}ms | `
              + `Queue->Telegram: ${tSent - tQueued}ms | Total Pipeline: ${tSent - tAmazon}ms`,
            );
          }
        }
        console.log('[Telegram] Sent OK');
      } catch (error) {
        recordTelegramFailure(attempt, error);
        if (error?.message?.includes('HTTP 429')) break;
      }

      await new Promise(resolve => setTimeout(resolve, 800));
    }
  } catch (err) {
    console.error('[Telegram] ❌ Worker exception:', err.message);
  } finally {
    telegramWorkerRunning = false;
  }
}

// --- ASYNCHRONOUS TELEGRAM WORKER WITH LATENCY TELEMETRY ---
function startTelegramWorker() {
  return setInterval(runTelegramWorkerCycle, 1000);
}

function safeAmazonDiagnosticText(value) {
  return String(value ?? 'Unknown Amazon error')
    .replace(/\bBearer\s+[^\s,;]+/gi, 'Bearer [REDACTED]')
    .replace(/\b(authorization|token|password|cookie|secret|api[_-]?key)\s*[:=]\s*[^\s,;]+/gi, '$1=[REDACTED]')
    .replace(/https?:\/\/[^\s/@:]+:[^\s/@]+@/gi, '[REDACTED_URL]')
    .replace(/[\r\n]+/g, ' ')
    .slice(0, 240);
}

function summarizeGraphQLErrors(errors) {
  const messages = errors.slice(0, 3).map((error) => (
    safeAmazonDiagnosticText(error?.message ?? 'Unspecified GraphQL error')
  ));
  const omittedCount = errors.length - messages.length;
  const omitted = omittedCount > 0 ? `; ${omittedCount} more` : '';
  return `${errors.length} error(s): ${messages.join('; ')}${omitted}`;
}

function reportAmazonGraphQLErrors(operation, errors, pageCount, jobId) {
  if (!Array.isArray(errors) || errors.length === 0) return;
  const jobLabel = jobId ? ` for job ${safeAmazonDiagnosticText(jobId)}` : '';
  console.warn(
    `[Scan Health] PARTIAL: Amazon GraphQL ${operation} errors on page ${pageCount}`
    + `${jobLabel}: ${summarizeGraphQLErrors(errors)}`,
  );
}

function retryAfterLogSuffix(response) {
  const value = response?.headers?.get?.('retry-after');
  if (typeof value !== 'string' || value.trim() === '') return '';
  return ` Retry-After=${safeAmazonDiagnosticText(value.trim()).slice(0, 80)}`;
}

function reportAmazonHttpFailure(operation, response, pageCount, jobId) {
  const jobLabel = jobId ? ` for job ${safeAmazonDiagnosticText(jobId)}` : '';
  const context = `${operation} page ${pageCount}${jobLabel}`;
  const retryAfter = retryAfterLogSuffix(response);

  if (response.status === 403) {
    console.warn(`[Scan Health] BLOCKED: Amazon HTTP 403 on ${context}.`);
  } else if (response.status === 401) {
    console.warn(`[Scan Health] BLOCKED: Amazon HTTP 401 authentication failure on ${context}.`);
  } else if (response.status === 429) {
    console.warn(`[Scan Health] THROTTLED: Amazon HTTP 429 on ${context}.${retryAfter}`);
  } else {
    console.warn(
      `[Scan Health] PARTIAL: Amazon HTTP ${response.status} on ${context}.${retryAfter}`,
    );
  }
}

// --- FETCH SCHEDULE CARDS WITH FULL PAGINATION (`nextToken` Crawler) ---
async function fetchSchedulesForJob(jobId, loopName = 'Loop-unknown') {
  let allSchedules = [];
  let nextToken = null;
  let pageCount = 0;
  let paginationComplete = true;
  const maxPages = 5;

  do {
    pageCount++;
    const url = 'https://www.jobsatamazon.co.uk/graphql';
    const query = `
      query searchScheduleCards($searchScheduleRequest: SearchScheduleRequest!) {
        searchScheduleCards(searchScheduleRequest: $searchScheduleRequest) {
          nextToken
          scheduleCards {
            scheduleId
            jobId
            firstDayOnSite
            firstDayOnSiteL10N
            scheduleText
            scheduleType
            hoursPerWeek
          }
        }
      }
    `;

    const variables = {
      searchScheduleRequest: {
        country: 'United Kingdom',
        jobId: jobId,
        locale: 'en-GB',
        pageSize: 100,
        dateFilters: [],
        rangeFilters: [],
        equalFilters: [],
        excludeFilters: [],
        orFilters: [],
        ...(nextToken ? { nextToken } : {})
      }
    };

    try {
      const protectedResult = await protectedAmazonFetch('searchScheduleCards', loopName, url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Origin': 'https://www.jobsatamazon.co.uk',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          ...(AMAZON_AUTH_TOKEN ? { 'Authorization': AMAZON_AUTH_TOKEN } : {})
        },
        body: JSON.stringify({ query, variables }),
        ...(dispatcher ? { dispatcher } : {})
      });

      if (!protectedResult
        || (protectedResult.response.ok && getAmazonProtectionState().blocked)) {
        paginationComplete = false;
        break;
      }
      const { response: res, data } = protectedResult;

      if (!res.ok) {
        reportAmazonHttpFailure('schedule feed', res, pageCount, jobId);
        paginationComplete = false;
        break;
      }
      const resultData = data?.data?.searchScheduleCards;
      if (resultData && resultData.scheduleCards) {
        allSchedules.push(...resultData.scheduleCards);
      }
      nextToken = resultData?.nextToken || null;
      if (Array.isArray(data?.errors) && data.errors.length > 0) {
        reportAmazonGraphQLErrors('schedule feed', data.errors, pageCount, jobId);
        paginationComplete = false;
        break;
      }
    } catch (err) {
      console.error(
        `[Scan Health] PARTIAL/FAILED: Amazon schedule feed exception on page ${pageCount}`
        + ` for job ${safeAmazonDiagnosticText(jobId)}: ${safeAmazonDiagnosticText(err?.message)}`,
      );
      paginationComplete = false;
      break;
    }

    if (pageCount >= maxPages && nextToken) {
      paginationComplete = false;
      break;
    }
  } while (nextToken);

  return { schedules: allSchedules, paginationComplete };
}

// --- CORE JOB & SCHEDULE PROCESSOR ---
async function processDiscoveredJob(job, amazonDetectedAt, loopName = 'Loop-unknown') {
  const { schedules: scheduleCards, paginationComplete } = await fetchSchedulesForJob(job.jobId, loopName);
  const queued = db.transaction(() => {
    const newlyDiscovered = [];
    for (const card of scheduleCards) {
      if (!card?.scheduleId) continue;
      const result = stmtClaimSchedule.run(card.scheduleId, amazonDetectedAt, amazonDetectedAt);
      if (result.changes === 1) newlyDiscovered.push(card);
    }

    if (newlyDiscovered.length === 0) return { count: 0, scheduleIds: [] };

    const queuedAt = new Date().toISOString();
    const locationName = job.locationName || job.city || 'UK Delivery Station';
    const postalCode = job.postalCode || '';
    const title = job.jobTitle || 'Delivery Station Warehouse Associate';
    const payRate = job.totalPayRateMax ? job.totalPayRateMax.toString() : '15.10';
    const shiftCount = newlyDiscovered.length;

    let message = `📍 ${locationName} ${postalCode}\n` +
                  `🏷️ ${title} | ${shiftCount}\n` +
                  `💼 Seasonal | Full-time;Part-time\n` +
                  `💰 £${payRate} /hr\n`;

    newlyDiscovered.forEach((shift, index) => {
      message += `\n📋 Shift ${index + 1}:\n` +
                 `📅 First Day: ${shift.firstDayOnSiteL10N || shift.firstDayOnSite || 'Upcoming'}\n` +
                 `⏰ Schedule: ${shift.scheduleText || 'Working Days'}\n` +
                 `🕐 Hours/Week: ${shift.hoursPerWeek || '20'}\n`;
    });

    const scheduleIds = newlyDiscovered.map(({ scheduleId }) => scheduleId).sort();
    const notificationKey = JSON.stringify([job.jobId, scheduleIds]);
    for (const scheduleId of scheduleIds) {
      stmtUpdateQueueTime.run(queuedAt, scheduleId);
    }
    stmtEnqueueTelegram.run(
      scheduleIds[0],
      notificationKey,
      JSON.stringify(scheduleIds),
      message,
    );

    console.log(
      `[DEDUP] Queued one grouped alert for job ${job.jobId} `
      + `with ${shiftCount} newly discovered shift(s) at ${queuedAt}`,
    );
    return { count: shiftCount, scheduleIds };
  }).immediate();

  return { ...queued, paginationComplete };
}

// --- FULL NATIONWIDE FEED FETCHER WITH PAGINATION & SCAN HEALTH TRACKING ---
async function fetchAmazonJobFeed(loopName = 'Loop-unknown') {
  let allJobCards = [];
  let nextToken = null;
  let pageCount = 0;
  let scanStatus = 'COMPLETE';
  const maxPages = 10;

  do {
    pageCount++;
    const url = 'https://www.jobsatamazon.co.uk/graphql';
    const query = `
      query searchJobCardsByLocation($searchJobRequest: SearchJobRequest!) {
        searchJobCardsByLocation(searchJobRequest: $searchJobRequest) {
          nextToken
          jobCards {
            jobId
            jobTitle
            city
            postalCode
            locationName
            scheduleCount
            totalPayRateMax
          }
        }
      }
    `;

    const variables = {
      searchJobRequest: {
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
        ...(nextToken ? { nextToken } : {})
      }
    };

    try {
      const protectedResult = await protectedAmazonFetch('searchJobCardsByLocation', loopName, url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'Accept-Language': 'en-GB,en;q=0.9',
          'Origin': 'https://www.jobsatamazon.co.uk',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          ...(AMAZON_AUTH_TOKEN ? { 'Authorization': AMAZON_AUTH_TOKEN } : {})
        },
        body: JSON.stringify({ query, variables }),
        ...(dispatcher ? { dispatcher } : {})
      });

      if (!protectedResult
        || (protectedResult.response.ok && getAmazonProtectionState().blocked)) {
        scanStatus = 'BLOCKED';
        break;
      }
      const { response: res, data } = protectedResult;

      if (res.status === 403 || res.status === 401) {
        reportAmazonHttpFailure('job feed', res, pageCount);
        scanStatus = 'BLOCKED';
        break;
      }

      if (!res.ok) {
        reportAmazonHttpFailure('job feed', res, pageCount);
        scanStatus = 'PARTIAL';
        break;
      }

      const resultData = data?.data?.searchJobCardsByLocation;
      if (resultData && resultData.jobCards) {
        for (const job of resultData.jobCards) {
          allJobCards.push({
            ...job,
            _scannerDetectedAt: new Date().toISOString(),
          });
        }
      }

      nextToken = resultData?.nextToken || null;
      if (Array.isArray(data?.errors) && data.errors.length > 0) {
        reportAmazonGraphQLErrors('job feed', data.errors, pageCount);
        scanStatus = 'PARTIAL';
      }
    } catch (err) {
      console.error(
        `[Scan Health] PARTIAL/FAILED: Amazon job feed exception on page ${pageCount}:`
        + ` ${safeAmazonDiagnosticText(err?.message)}`,
      );
      scanStatus = 'PARTIAL';
      break;
    }

    if (pageCount >= maxPages && nextToken) {
      scanStatus = 'PARTIAL';
      break;
    }
  } while (nextToken);

  return { jobCards: allJobCards, scanStatus };
}

// --- ORGANIC REQUEST JITTER ---
async function randomizedDelay() {
  const jitter = Math.floor(Math.random() * (5000 - 3000 + 1)) + 3000;
  await new Promise(resolve => setTimeout(resolve, jitter));
}

// --- MAIN OVERLAPPING SCANNER LOOP ---
async function runScannerLoop(loopName) {
  console.log(`[${loopName}] Started nationwide feed scanning loop.`);

  while (true) {
    try {
      const { jobCards, scanStatus } = await fetchAmazonJobFeed(loopName);
      let completedScanStatus = scanStatus;

      if (scanStatus !== 'BLOCKED' && jobCards && jobCards.length > 0) {
        for (const job of jobCards) {
          const result = await processDiscoveredJob(job, job._scannerDetectedAt, loopName);
          completedScanStatus = includeSchedulePaginationStatus(
            completedScanStatus,
            result.paginationComplete,
          );
        }
      }

      console.log(
        `[Scan Health] [${loopName}] Scan completed with status: `
        + `[${completedScanStatus}] | Retrieved ${jobCards.length} job card(s).`,
      );
      await randomizedDelay();
    } catch (err) {
      console.error(`[${loopName}] ⚠️ Loop error:`, err.message);
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

function includeSchedulePaginationStatus(scanStatus, paginationComplete) {
  if (!paginationComplete && scanStatus === 'COMPLETE') return 'PARTIAL';
  return scanStatus;
}

// --- APP INITIALIZATION ---
console.log('[Bot] Starting Amazon Shift Tracker (Production Telemetry & Pagination Edition)...');
console.log('[Activation] Runtime: Node.js ES Modules | Database: better-sqlite3 | Pagination: Active nextToken Crawler');
console.log('[Bot] Targets: Nationwide UK Feed Scans with Scan Health Tracking & Latency Telemetry');
console.log('[Watchdog] Started - monitoring 3 global overlapping loops with staggered delays.');

startTelegramWorker();

// 3 Overlapping Loops checking the full nationwide feed concurrently
console.log('[Loop-1] Started.');
runScannerLoop('Loop-1');

console.log('[Loop-2] Starting in 5s...');
setTimeout(() => {
  console.log('[Loop-2] Started.');
  runScannerLoop('Loop-2');
}, 5000);

console.log('[Loop-3] Starting in 10s...');
setTimeout(() => {
  console.log('[Loop-3] Started.');
  runScannerLoop('Loop-3');
}, 10000);
