#!/usr/bin/env node
'use strict';

/**
 * Kafka load producer — push messages to a topic with configurable parallelism.
 *
 * Producer settings: producer-config.js
 * Kafka connection:   .env (ENV=local|dev|test|perf profile)
 */

const fs = require('fs');
const path = require('path');
const { Kafka, logLevel } = require('kafkajs');

require('./env');

const { activeEnv } = require('./env');
const producerConfig = require('./producer-config');

function loadPayload(cfg) {
  if (cfg.payloadFile) {
    const filePath = path.isAbsolute(cfg.payloadFile) ? cfg.payloadFile : path.resolve(__dirname, cfg.payloadFile);
    return fs.readFileSync(filePath, 'utf8');
  }
  return cfg.payload;
}

function loadHeaders(cfg) {
  if (cfg.headersFile) {
    const filePath = path.isAbsolute(cfg.headersFile) ? cfg.headersFile : path.resolve(__dirname, cfg.headersFile);
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  }
  return cfg.headers ?? {};
}

function normalizePayload(payload) {
  if (payload == null) return '';
  if (typeof payload === 'string') return payload;
  return JSON.stringify(payload);
}

function csv(value, fallback) {
  return (value || fallback)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}

function positiveInt(value, name) {
  const n = parseInt(value, 10);
  if (!Number.isFinite(n) || n < 1) {
    throw new Error(`${name} must be a positive integer (got "${value}")`);
  }
  return n;
}

function buildConfig() {
  const topic = producerConfig.topic?.trim();
  if (!topic) {
    throw new Error('producer-config.js: topic is required');
  }

  let payload = normalizePayload(loadPayload(producerConfig));
  const headers = loadHeaders(producerConfig);

  return {
    env: activeEnv,
    kafka: {
      clientId: producerConfig.clientId || 'kafka-load-producer',
      brokers: csv(process.env.KAFKA_BROKERS, 'localhost:9092'),
      ssl: process.env.KAFKA_SSL === 'true',
      sasl: process.env.KAFKA_SASL_MECHANISM
        ? {
            mechanism: process.env.KAFKA_SASL_MECHANISM,
            username: process.env.KAFKA_SASL_USERNAME || '',
            password: process.env.KAFKA_SASL_PASSWORD || '',
          }
        : undefined,
    },
    topic,
    key: producerConfig.key ?? null,
    fixedKey: Boolean(producerConfig.fixedKey),
    payload,
    headers,
    messageCount: positiveInt(producerConfig.messageCount ?? 100, 'messageCount'),
    parallelism: positiveInt(producerConfig.parallelism ?? 10, 'parallelism'),
    fireAndForget: Boolean(producerConfig.fireAndForget),
  };
}

function buildMessage(producerConfig, cfg, seq) {
  const key = cfg.fixedKey ? cfg.key : renderTemplate(cfg.key, seq);
  const value = renderTemplate(cfg.payload, seq);
  const message = { key, value };
  const headers = buildMessageHeaders(cfg.headers, seq);
  if (headers) message.headers = headers;
  return message;
}

async function sendOne(producer, cfg, seq) {
  return producer.send({
    topic: cfg.topic,
    messages: [buildMessage(producerConfig, cfg, seq)],
  });
}

/** Worker pool — always keeps `parallelism` sends in flight (no batch barrier). */
async function producePooled(producer, cfg, stats, started) {
  let seq = 0;

  async function worker() {
    while (true) {
      const n = ++seq;
      if (n > cfg.messageCount) return;
      try {
        await sendOne(producer, cfg, n);
        stats.succeeded++;
      } catch (err) {
        stats.failed++;
        console.error('\nSend failed:', err.message || err);
      }
      reportProgress(stats, cfg.messageCount, started);
    }
  }

  await Promise.all(Array.from({ length: cfg.parallelism }, () => worker()));
}

/** Fire-and-forget window — schedule without awaiting each ack, drain at end. */
async function produceFireAndForget(producer, cfg, stats, started) {
  const pending = new Set();

  function track(promise) {
    pending.add(promise);
    promise.finally(() => pending.delete(promise));
  }

  for (let seq = 1; seq <= cfg.messageCount; seq++) {
    while (pending.size >= cfg.parallelism) {
      await Promise.race(pending);
    }

    const n = seq;
    track(
      sendOne(producer, cfg, n)
        .then(() => {
          stats.succeeded++;
          reportProgress(stats, cfg.messageCount, started);
        })
        .catch((err) => {
          stats.failed++;
          console.error('\nSend failed:', err.message || err);
          reportProgress(stats, cfg.messageCount, started);
        }),
    );
  }

  await Promise.allSettled(pending);
}

function reportProgress(stats, total, started) {
  const done = stats.succeeded + stats.failed;
  const elapsed = Date.now() - started;
  process.stdout.write(
    `\r  Progress: ${done}/${total}  ` +
      `ok=${stats.succeeded}  fail=${stats.failed}  rate=${formatRate(stats.succeeded, elapsed)}`,
  );
}

function buildMessageHeaders(headerTemplate, seq) {
  if (!headerTemplate || typeof headerTemplate !== 'object' || Array.isArray(headerTemplate)) {
    return undefined;
  }
  const headers = {};
  for (const [name, value] of Object.entries(headerTemplate)) {
    if (value == null) continue;
    headers[name] = renderTemplate(String(value), seq);
  }
  return Object.keys(headers).length ? headers : undefined;
}

function renderTemplate(template, seq) {
  return template && typeof template === 'string'
    ? template.replace(/\{seq\}/g, String(seq)).replace(/\{ts\}/g, new Date().toISOString())
    : template;
}

function formatRate(count, ms) {
  if (!ms) return '—';
  return (count / (ms / 1000)).toFixed(1) + ' msg/s';
}

async function run() {
  const cfg = buildConfig();
  const W = 72;

  console.log('\n' + '═'.repeat(W));
  console.log('  KAFKA LOAD PRODUCER');
  console.log('  Environment : ' + cfg.env);
  console.log('  Brokers     : ' + cfg.kafka.brokers.join(', '));
  console.log('  Topic       : ' + cfg.topic);
  console.log('  Messages    : ' + cfg.messageCount);
  console.log('  Parallelism : ' + cfg.parallelism + (cfg.fireAndForget ? ' (fire-and-forget)' : ' (pipelined)'));
  console.log('  Key         : ' + (cfg.key ?? '(none)') + (cfg.fixedKey ? ' (fixed)' : cfg.key ? ' (+ {seq})' : ''));
  const headerNames = Object.keys(cfg.headers || {});
  console.log('  Headers     : ' + (headerNames.length ? headerNames.join(', ') : '(none)'));
  console.log('  Payload     : ' + (cfg.payload.length > 60 ? cfg.payload.slice(0, 57) + '…' : cfg.payload));
  console.log('═'.repeat(W) + '\n');

  const kafka = new Kafka({
    ...cfg.kafka,
    logLevel:
      process.env.KAFKAJS_LOG_LEVEL != null
        ? (logLevel[process.env.KAFKAJS_LOG_LEVEL] ?? logLevel.NOTHING)
        : logLevel.NOTHING,
    retry: { retries: 5, initialRetryTime: 300 },
  });

  const producer = kafka.producer();
  await producer.connect();
  console.log('Producer connected.\n');

  const stats = { succeeded: 0, failed: 0 };
  const started = Date.now();

  try {
    if (cfg.fireAndForget) {
      await produceFireAndForget(producer, cfg, stats, started);
    } else {
      await producePooled(producer, cfg, stats, started);
    }
  } finally {
    await producer.disconnect();
  }

  const { succeeded, failed } = stats;
  const elapsed = Date.now() - started;
  console.log('\n\n' + '─'.repeat(W));
  console.log(`  Done — ${succeeded} sent, ${failed} failed in ${elapsed}ms (${formatRate(succeeded, elapsed)})`);
  console.log('─'.repeat(W) + '\n');

  if (failed > 0) process.exit(1);
}

run().catch((err) => {
  console.error('Fatal:', err.message || err);
  process.exit(1);
});
