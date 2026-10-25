'use strict';

const { activeEnv } = require('./env');

const path = require('path');

/**
 * Build a timestamped CSV path under ./csv/ for each monitor run.
 * Override with CSV_PATH env var for a fixed location.
 */
function buildCsvPath() {
  if (process.env.CSV_PATH) return process.env.CSV_PATH;
  const now = new Date();
  const p = (n, w = 2) => String(n).padStart(w, '0');
  const stamp =
    `${now.getFullYear()}-${p(now.getMonth() + 1)}-${p(now.getDate())}-` +
    `${p(now.getHours())}:${p(now.getMinutes())}:${p(now.getSeconds())}`;
  return path.join('csv', `lag-report-${stamp}.csv`);
}

/**
 * Kafka Lag Monitor v3 — Configuration
 *
 * WATCH CONFIGURATION (group → topics)
 * ────────────────────────────────────
 *
 *  A) Explicit (recommended) — KAFKA_WATCHES
 *     KAFKA_WATCHES=group-a:topic-1,topic-2;group-b:topic-3
 *
 *  B) Paired by index — same count of groups and topics
 *     KAFKA_CONSUMER_GROUPS=group-a,group-b
 *     KAFKA_TOPICS=topic-1,topic-2
 *     → group-a/topic-1, group-b/topic-2
 *
 *  C) Single group — one group, many topics
 *     KAFKA_CONSUMER_GROUPS=group-a
 *     KAFKA_TOPICS=topic-1,topic-2
 *     → group-a watches topic-1 AND topic-2
 */

// ── Helper: parse a comma-separated env var into a trimmed array ─────────────
const csv = (envVar, fallback) =>
  (process.env[envVar] || fallback).split(',').map(s => s.trim()).filter(Boolean);

/** @returns {{ group: string, topics: string[] }[]} */
function buildWatches() {
  const raw = process.env.KAFKA_WATCHES;
  if (raw) {
    return raw.split(';').map(entry => {
      const colon = entry.indexOf(':');
      if (colon === -1) {
        throw new Error(`Invalid KAFKA_WATCHES entry "${entry}" — use group:topic-1,topic-2`);
      }
      const group  = entry.slice(0, colon).trim();
      const topics = entry.slice(colon + 1).split(',').map(t => t.trim()).filter(Boolean);
      if (!group || topics.length === 0) {
        throw new Error(`Invalid KAFKA_WATCHES entry "${entry}" — group and topics required`);
      }
      return { group, topics };
    });
  }

  const groups = csv('KAFKA_CONSUMER_GROUPS', '');
  const topics = csv('KAFKA_TOPICS', '');

  if (!groups.length && !topics.length) {
    return [{ group: 'my-consumer-group', topics: ['topic-a', 'topic-b'] }];
  }
  if (!groups.length || !topics.length) {
    throw new Error('Set both KAFKA_CONSUMER_GROUPS and KAFKA_TOPICS, or use KAFKA_WATCHES');
  }
  if (groups.length === 1) {
    return [{ group: groups[0], topics }];
  }
  if (groups.length === topics.length) {
    return groups.map((group, i) => ({ group, topics: [topics[i]] }));
  }

  throw new Error(
    `KAFKA_CONSUMER_GROUPS (${groups.length}) and KAFKA_TOPICS (${topics.length}) count mismatch. ` +
    'Use KAFKA_WATCHES=group-a:topic-1;group-b:topic-2 for explicit mappings.'
  );
}

const watches = buildWatches();

module.exports = {

  // ── Kafka connection ─────────────────────────────────────────────────────────
  kafka: {
    clientId : process.env.KAFKA_CLIENT_ID || 'kafka-lag-monitor',
    brokers  : csv('KAFKA_BROKERS', 'localhost:9092'),
    ssl      : process.env.KAFKA_SSL === 'true',
    sasl     : process.env.KAFKA_SASL_MECHANISM ? {
      mechanism : process.env.KAFKA_SASL_MECHANISM,
      username  : process.env.KAFKA_SASL_USERNAME || '',
      password  : process.env.KAFKA_SASL_PASSWORD || '',
    } : undefined,
  },

  // ── Watch matrix ─────────────────────────────────────────────────────────────
  // Array of { group: string, topics: string[] }
  watches,

  // ── Polling ──────────────────────────────────────────────────────────────────
  pollIntervalMs   : parseInt(process.env.POLL_INTERVAL_MS      || '2000',  10),
  logProgressEvery : parseInt(process.env.LOG_PROGRESS_EVERY_MS || '10000', 10),

  // ── Settling period ──────────────────────────────────────────────────────────
  // After lag hits 0, wait this long before declaring "cleared".
  // Guards against burst producers that briefly pause between batches.
  settlingPeriodMs : parseInt(process.env.SETTLING_PERIOD_MS || '10000', 10),

  // ── Auto-exit ────────────────────────────────────────────────────────────────
  exitWhenAllCleared : process.env.EXIT_WHEN_CLEARED !== 'false',

  // ── Percentile tracking ──────────────────────────────────────────────────────
  trackPercentiles : process.env.TRACK_PERCENTILES !== 'false',

  // ── CSV export ───────────────────────────────────────────────────────────────
  // One row per (group, topic) per poll. HTML report generated on exit.
  csvEnabled  : process.env.CSV_ENABLED !== 'false',
  csvPath     : buildCsvPath(),
  htmlEnabled : process.env.HTML_ENABLED !== 'false',

  // ── Prometheus ───────────────────────────────────────────────────────────────
  // All metrics include a `consumer_group` label.
  prometheusEnabled : process.env.PROMETHEUS_ENABLED === 'true',
  prometheusPort    : parseInt(process.env.PROMETHEUS_PORT || '9090', 10),

  // ── Live dashboard ───────────────────────────────────────────────────────────
  dashboardEnabled : process.env.DASHBOARD_ENABLED !== 'false',

  // ── Active environment (from env.js) ─────────────────────────────────────────
  env : activeEnv,
};