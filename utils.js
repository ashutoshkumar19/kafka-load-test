'use strict';

// ─── Timestamp / duration ────────────────────────────────────────────────────

function formatTimestamp(date) {
  const p = (n, w = 2) => String(n).padStart(w, '0');
  return (
    `${date.getFullYear()}-${p(date.getMonth() + 1)}-${p(date.getDate())} ` +
    `${p(date.getHours())}:${p(date.getMinutes())}:${p(date.getSeconds())}.${p(date.getMilliseconds(), 3)}`
  );
}

function formatDuration(ms) {
  if (ms == null || ms < 0) return '—';
  const s   = ms / 1000;
  const h   = Math.floor(s / 3600);
  const m   = Math.floor((s % 3600) / 60);
  const sec = (s % 60).toFixed(3);
  const parts = [];
  if (h) parts.push(`${h}h`);
  if (m) parts.push(`${m}m`);
  parts.push(`${sec}s`);
  return parts.join(' ');
}

// ─── Number formatting ───────────────────────────────────────────────────────

function formatLag(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-US');
}

function formatRate(ratePerSec) {
  if (ratePerSec == null || isNaN(ratePerSec)) return '—';
  return `${formatLag(Math.round(ratePerSec))}/s`;
}

// ─── Percentile calculator ───────────────────────────────────────────────────
// Maintains a sorted array of samples; efficient enough for ~100k samples.

class PercentileTracker {
  constructor() {
    this._samples = [];
    this._dirty   = false;
  }

  record(value) {
    this._samples.push(value);
    this._dirty = true;
  }

  _sort() {
    if (this._dirty) {
      this._samples.sort((a, b) => a - b);
      this._dirty = false;
    }
  }

  percentile(p) {
    if (this._samples.length === 0) return null;
    this._sort();
    const idx = Math.ceil((p / 100) * this._samples.length) - 1;
    return this._samples[Math.max(0, idx)];
  }

  get count() { return this._samples.length; }
  get min()   { return this._samples.length ? (this._sort(), this._samples[0])                         : null; }
  get max()   { return this._samples.length ? (this._sort(), this._samples[this._samples.length - 1])  : null; }
}

// ─── Exponential moving average (for smooth rate estimates) ─────────────────

class EMA {
  constructor(alpha = 0.3) {
    this._alpha = alpha;
    this._value = null;
  }
  update(v) {
    this._value = this._value === null ? v : this._alpha * v + (1 - this._alpha) * this._value;
    return this._value;
  }
  get value() { return this._value; }
}

// ─── ASCII table renderer ─────────────────────────────────────────────────────

function table(rows, opts = {}) {
  if (!rows || rows.length === 0) return '(no data)';
  const keys   = Object.keys(rows[0]);
  const widths = {};
  for (const k of keys) widths[k] = (opts.headers?.[k] ?? k).length;
  for (const row of rows) {
    for (const k of keys) widths[k] = Math.max(widths[k], String(row[k] ?? '—').length);
  }
  const sep = '  ';
  const hr  = keys.map(k => '─'.repeat(widths[k])).join('─' + sep);
  const hdr = keys.map(k => (opts.headers?.[k] ?? k).padEnd(widths[k])).join(sep);
  const lines = ['', '  ' + hdr, '  ' + hr];
  for (const row of rows) {
    lines.push('  ' + keys.map(k => String(row[k] ?? '—').padEnd(widths[k])).join(sep));
  }
  lines.push('  ' + hr, '');
  return lines.join('\n');
}

// ─── ANSI helpers for live dashboard ─────────────────────────────────────────

const ANSI = {
  reset      : '\x1b[0m',
  bold       : '\x1b[1m',
  dim        : '\x1b[2m',
  green      : '\x1b[32m',
  yellow     : '\x1b[33m',
  red        : '\x1b[31m',
  cyan       : '\x1b[36m',
  clearLine  : '\x1b[2K',
  up         : (n) => `\x1b[${n}A`,
  col0       : '\x1b[0G',
};

// ─── Kafka offset helpers ────────────────────────────────────────────────────
// Kafka stores -1 (LATEST) / -2 (EARLIEST) as sentinel committed offsets.
// Treating -1 as 0 inflates lag to the full partition size when no offset
// is stored — use the watermark instead so lag reflects real consumer progress.

function resolveCommittedOffset(rawOffset, high, low) {
  if (rawOffset === undefined || rawOffset === null) return high;
  const n = Number(rawOffset);
  if (n === -1) return high; // LATEST / no committed offset
  if (n === -2) return low;  // EARLIEST marker
  if (n < 0) return high;
  return BigInt(rawOffset);
}

/** Derive load-test phase from lag and offset movement. */
function derivePhase(s, { lagNum, lagDelta, highDelta, committedDelta }) {
  if (!s.firstMessageAt) return 'IDLE';
  if (s.isCleared)       return 'CLEARED';
  if (s._settling)       return 'SETTLING';
  if (lagDelta > 0 || (highDelta > 0 && highDelta >= committedDelta)) return 'GROWING';
  if (lagDelta < 0 || committedDelta > 0 || lagNum > 0) return 'CLEARING';
  return 'SETTLING';
}

module.exports = {
  formatTimestamp,
  formatDuration,
  formatLag,
  formatRate,
  table,
  ANSI,
  PercentileTracker,
  EMA,
  resolveCommittedOffset,
  derivePhase,
};
