#!/usr/bin/env node
'use strict';

/**
 * Kafka Lag Monitor v3
 * ─────────────────────────────────────────────────────────────────────────────
 * Watches N consumer groups × M topics simultaneously.
 * State is keyed by "group\x00topic" so a topic consumed by two different
 * groups is tracked independently, with its own lag timeline, rates, and report.
 *
 * All metrics from v2 are preserved and now carry a `group` dimension:
 *   firstMessageAt, maxLag, maxLagAt, clearedAt, totalDuration,
 *   timeToPeak, lagGrowthRate, catchUpRate, avgThroughput,
 *   P50/P95/P99 lag, per-partition breakdown, pollCount
 *
 * Poll strategy
 *   • One fetchTopicOffsets call per unique topic (shared across groups)
 *   • One fetchOffsets call per consumer group (parallelised)
 *   • All calls are parallelised via Promise.allSettled
 */

const { Kafka, logLevel } = require('kafkajs');
const config           = require('./config');
const CsvWriter        = require('./csv-writer');
const { generateHtmlReport, buildHtmlPath } = require('./html-report');
const PrometheusServer = require('./prometheus');
const Dashboard        = require('./dashboard');
const {
  formatDuration, formatTimestamp, formatLag, formatRate,
  table, PercentileTracker, EMA, resolveCommittedOffset, derivePhase,
} = require('./utils');

// ─── State key ───────────────────────────────────────────────────────────────

const stateKey = (group, topic) => `${group}\x00${topic}`;

// ─── Per (group, topic) state factory ────────────────────────────────────────

function createState(group, topic) {
  return {
    group,
    topic,
    partitions      : {},         // partitionId → { highWater, committed, lag }

    // Core timings
    firstMessageAt  : null,
    maxLag          : 0,
    maxLagAt        : null,
    clearedAt       : null,
    totalDuration   : null,

    // Derived rates
    timeToPeak      : null,
    lagGrowthRate   : null,
    catchUpRate     : null,
    avgThroughput   : null,

    // EMA-smoothed live rates
    producerRateEMA : new EMA(0.3),
    consumerRateEMA : new EMA(0.3),

    // Statistics
    percentiles     : new PercentileTracker(),

    // Operational
    pollCount       : 0,
    isCleared       : false,

    // Offset tracking (detect activity even when lag stays at 0)
    totalProduced   : 0,
    totalConsumed   : 0,
    _baselineReady  : false,
    _prevHighTotal  : 0n,
    _prevCommittedTotal : 0n,
    _lastHighDelta  : 0,
    _lastCommittedDelta : 0,

    // Internal
    _settleStartAt  : null,
    _settling       : false,
    _prevLag        : 0,
    _lastLagDelta   : 0,
    _lastProgressLog: null,
  };
}

// ─── Logger ──────────────────────────────────────────────────────────────────

function log(level, group, topic, msg) {
  const ts     = formatTimestamp(new Date());
  const prefix = (group && topic) ? `[${group} / ${topic}]`
               : group            ? `[${group}]`
               :                    '[MONITOR]';
  process.stdout.write(`${ts}  ${level.padEnd(5)}  ${prefix.padEnd(48)}  ${msg}\n`);
}

// ─── Partition helpers ────────────────────────────────────────────────────────

function slowestPartition(partitions) {
  let maxPid = null, maxVal = -1;
  for (const [pid, p] of Object.entries(partitions)) {
    if ((p.lag || 0) > maxVal) { maxVal = p.lag; maxPid = pid; }
  }
  return maxPid != null ? { partition: maxPid, lag: maxVal } : null;
}

function lagBar(lag, maxLag, width = 30) {
  if (!maxLag) return '[' + '░'.repeat(width) + ']';
  const fill = Math.round((lag / maxLag) * width);
  return '[' + '█'.repeat(fill) + '░'.repeat(width - fill) + ']';
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function run() {
  const W = 96;

  // Expand watch matrix into a flat list of { group, topic } pairs
  const watchList = [];
  for (const { group, topics } of config.watches) {
    for (const topic of topics) {
      watchList.push({ group, topic });
    }
  }

  if (watchList.length === 0) {
    console.error('No watches configured. Set KAFKA_WATCHES or KAFKA_CONSUMER_GROUPS + KAFKA_TOPICS.');
    process.exit(1);
  }

  // Unique groups and topics (to deduplicate broker calls)
  const allGroups = [...new Set(config.watches.map(w => w.group))];
  const allTopics = [...new Set(watchList.map(w => w.topic))];

  console.log('\n' + '═'.repeat(W));
  console.log('  KAFKA LAG MONITOR  v3  (multi-group)');
  console.log('  Environment  : ' + config.env);
  console.log('  Started      : ' + formatTimestamp(new Date()));
  console.log('  Brokers      : ' + config.kafka.brokers.join(', '));
  console.log('  Poll interval: ' + config.pollIntervalMs + ' ms');
  console.log('  Settling     : ' + config.settlingPeriodMs + ' ms');
  console.log('  CSV export   : ' + (config.csvEnabled ? config.csvPath : 'disabled'));
  console.log('  HTML report  : ' + (config.htmlEnabled && config.csvEnabled
    ? buildHtmlPath(config.csvPath) : 'disabled'));
  console.log('  Prometheus   : ' + (config.prometheusEnabled
    ? `http://localhost:${config.prometheusPort}/metrics` : 'disabled'));
  console.log('');
  console.log('  Watch matrix:');
  for (const { group, topics } of config.watches) {
    console.log(`    ${group.padEnd(36)} → ${topics.join(', ')}`);
  }
  console.log('  Total watch cells: ' + watchList.length
    + `  (${allGroups.length} group${allGroups.length > 1 ? 's' : ''}`
    + ` × ${allTopics.length} unique topic${allTopics.length > 1 ? 's' : ''})`);
  console.log('═'.repeat(W) + '\n');

  // ── Build state map: stateKey(group, topic) → state ──────────────────────
  const states = {};
  for (const { group, topic } of watchList) {
    states[stateKey(group, topic)] = createState(group, topic);
  }

  // ── External services ─────────────────────────────────────────────────────
  const csv = new CsvWriter(config.csvPath, config.csvEnabled);

  const prom = new PrometheusServer({
    port      : config.prometheusPort,
    enabled   : config.prometheusEnabled,
    getStates : () => states,
  });
  prom.start();

  const dashboard = new Dashboard(config.dashboardEnabled);

  // ── Kafka admin ───────────────────────────────────────────────────────────
  const kafka = new Kafka({
    clientId : config.kafka.clientId,
    brokers  : config.kafka.brokers,
    ssl      : config.kafka.ssl || false,
    sasl     : config.kafka.sasl || undefined,
    logLevel : process.env.KAFKA_LOG_LEVEL ?? logLevel.NOTHING,
    retry    : { retries: 5, initialRetryTime: 300 },
  });

  const admin = kafka.admin();
  await admin.connect();
  log('INFO', null, null, 'Admin client connected');

  // Pre-flight: warn about missing topics
  const existingTopics = await admin.listTopics();
  for (const t of allTopics) {
    if (!existingTopics.includes(t)) {
      log('WARN', null, t, 'Topic not found on broker — will retry each poll');
    }
  }

  let globalPollCount = 0;

  // ── Core poll ─────────────────────────────────────────────────────────────
  async function poll() {
    const now = new Date();
    globalPollCount++;

    // 1. Fetch committed offsets for ALL groups in parallel
    //    Each group fetches only its own subscribed topics.
    const groupOffsetResults = await Promise.allSettled(
      config.watches.map(({ group, topics }) =>
        admin.fetchOffsets({ groupId: group, topics })
          .then(rows => ({ group, rows }))
      )
    );

    // Build: committedMap[group][topic][partition] = BigInt(offset)
    const committedMap = {};
    for (const result of groupOffsetResults) {
      if (result.status === 'rejected') {
        log('ERROR', null, null, `fetchOffsets failed: ${result.reason.message}`);
        continue;
      }
      const { group, rows } = result.value;
      committedMap[group] = committedMap[group] || {};
      for (const { topic, partitions } of rows) {
        committedMap[group][topic] = {};
        for (const { partition, offset } of partitions) {
          committedMap[group][topic][partition] = offset;
        }
      }
    }

    // 2. Fetch high-watermarks for all UNIQUE topics in parallel (once per topic)
    const watermarkResults = await Promise.allSettled(
      allTopics.map(t => admin.fetchTopicOffsets(t).then(wm => ({ topic: t, wm })))
    );

    // Build: watermarkMap[topic][partition] = { high, low }
    const watermarkMap = {};
    for (const result of watermarkResults) {
      if (result.status === 'rejected') {
        log('WARN', null, null, `fetchTopicOffsets failed: ${result.reason.message}`);
        continue;
      }
      const { topic, wm } = result.value;
      watermarkMap[topic] = {};
      for (const { partition, high, low } of wm) {
        watermarkMap[topic][partition] = {
          high : BigInt(high),
          low  : BigInt(low),
        };
      }
    }

    // 3. Process every (group, topic) cell
    for (const { group, topic } of watchList) {
      const s   = states[stateKey(group, topic)];
      if (s.isCleared) continue;

      const groupCommitted = committedMap[group]?.[topic];
      const topicWatermark = watermarkMap[topic];

      if (!topicWatermark) continue;  // watermark fetch failed for this topic

      s.pollCount++;

      let newTotalLag = 0n;
      let totalHigh = 0n;
      let totalCommitted = 0n;
      for (const [partition, marks] of Object.entries(topicWatermark)) {
        const { high: hw, low } = marks;
        const co  = resolveCommittedOffset(groupCommitted?.[partition], hw, low);
        const lag = hw > co ? hw - co : 0n;
        s.partitions[partition] = { highWater: hw, committed: co, lag: Number(lag) };
        newTotalLag += lag;
        totalHigh += hw;
        totalCommitted += co;
      }

      const lagNum   = Number(newTotalLag);
      const dtSec    = config.pollIntervalMs / 1000;

      // Establish baseline on first poll (avoid false start from absolute offsets)
      if (!s._baselineReady) {
        s._prevHighTotal      = totalHigh;
        s._prevCommittedTotal = totalCommitted;
        s._prevLag            = lagNum;
        s._baselineReady      = true;
      }

      const highDelta       = Number(totalHigh - s._prevHighTotal);
      const committedDelta  = Number(totalCommitted - s._prevCommittedTotal);
      const lagDelta        = lagNum - s._prevLag;
      const hasMovement     = highDelta > 0 || committedDelta > 0;

      // ── Event: activity started (lag or offset movement) ─────────────────────
      if (s.firstMessageAt === null && hasMovement) {
        s.firstMessageAt = now;
        log('START', group, topic,
          `⚡ Activity detected  lag=${formatLag(lagNum)}  ` +
          `produceΔ=${formatLag(highDelta)}  consumeΔ=${formatLag(committedDelta)}  ` +
          `partitions=${Object.keys(s.partitions).length}`);
      }

      // Percentile sampling
      if (config.trackPercentiles && s.firstMessageAt) {
        s.percentiles.record(lagNum);
      }

      // Accumulate throughput counters
      if (s.firstMessageAt) {
        if (highDelta > 0)      s.totalProduced += highDelta;
        if (committedDelta > 0) s.totalConsumed += committedDelta;
      }

      // Smoothed rate estimates (prefer offset deltas; fall back to lag delta)
      if (s.firstMessageAt) {
        if (highDelta > 0)           s.producerRateEMA.update(highDelta / dtSec);
        else if (lagDelta > 0)       s.producerRateEMA.update(lagDelta / dtSec);
        if (committedDelta > 0)      s.consumerRateEMA.update(committedDelta / dtSec);
        else if (lagDelta < 0)       s.consumerRateEMA.update(Math.abs(lagDelta) / dtSec);
      }

      const phase = derivePhase(s, { lagNum, lagDelta, highDelta, committedDelta });

      s._lastLagDelta         = lagDelta;
      s._lastHighDelta        = highDelta;
      s._lastCommittedDelta   = committedDelta;

      csv.write({
        timestamp     : formatTimestamp(now),
        consumerGroup : group,
        topic,
        pollCount     : s.pollCount,
        partitionCount: Object.keys(s.partitions).length,
        totalLag      : lagNum,
        lagDelta,
        highDelta,
        committedDelta,
        producerRate  : highDelta > 0 ? s.producerRateEMA.value : null,
        consumerRate  : committedDelta > 0 ? s.consumerRateEMA.value : null,
        phase,
      });

      // ── Event: new peak lag ──────────────────────────────────────────────────
      if (lagNum > s.maxLag) {
        const prev = s.maxLag;
        s.maxLag   = lagNum;
        s.maxLagAt = now;
        if (prev > 0) {
          log('PEAK ', group, topic,
            `📈 New peak  lag=${formatLag(lagNum)}  prev=${formatLag(prev)}  ` +
            `produce≈${formatRate(s.producerRateEMA.value)}`);
        }
      }

      // ── Settling guard (lag=0 AND no offset movement) ─────────────────────────
      const isQuiescent = lagNum === 0 && highDelta === 0 && committedDelta === 0;

      if (isQuiescent && s.firstMessageAt !== null && !s.isCleared) {
        if (s._settleStartAt === null) {
          s._settleStartAt = now;
          s._settling      = true;
          if (config.settlingPeriodMs > 0) {
            log('SETTL', group, topic,
              `⏳ Offsets idle — settling ${config.settlingPeriodMs}ms before declaring cleared`);
          }
        }

        if ((now - s._settleStartAt) >= config.settlingPeriodMs) {
          // ── Event: confirmed cleared ────────────────────────────────────────
          s.clearedAt     = now;
          s.isCleared     = true;
          s._settling     = false;
          s.totalDuration = s.clearedAt - s.firstMessageAt;

          if (s.maxLagAt) {
            s.timeToPeak    = s.maxLagAt - s.firstMessageAt;
            s.lagGrowthRate = s.timeToPeak > 0
              ? s.maxLag / (s.timeToPeak / 1000) : null;
            const drainMs   = s.clearedAt - s.maxLagAt;
            s.catchUpRate   = drainMs > 0
              ? s.maxLag / (drainMs / 1000) : null;
          }
          s.avgThroughput = s.totalDuration > 0
            ? (s.totalConsumed || s.maxLag) / (s.totalDuration / 1000) : null;

          printCellSummary(s);

          // Check if ALL cells are done
          const allDone = Object.values(states)
            .every(st => st.isCleared || st.firstMessageAt === null);
          if (allDone && Object.values(states).some(st => st.isCleared)) {
            dashboard.disable();
            printFinalReport(states, config);
            if (config.exitWhenAllCleared) {
              await cleanup(admin, csv, prom, config);
              process.exit(0);
            }
          }
        }
      } else if (!isQuiescent && s._settleStartAt !== null) {
        if (s._settling) {
          log('SETTL', group, topic,
            `⚠️  Activity resumed during settling (lag=${formatLag(lagNum)} ` +
            `produceΔ=${formatLag(highDelta)} consumeΔ=${formatLag(committedDelta)}) — resetting timer`);
        }
        s._settleStartAt = null;
        s._settling      = false;
      }

      // ── Progress log ──────────────────────────────────────────────────────────
      if (s.firstMessageAt && config.logProgressEvery > 0) {
        if (!s._lastProgressLog || (now - s._lastProgressLog) >= config.logProgressEvery) {
          s._lastProgressLog = now;
          const elapsed = now - s.firstMessageAt;
          const sp      = slowestPartition(s.partitions);
          log('PROG ', group, topic,
            `lag=${formatLag(lagNum)}  max=${formatLag(s.maxLag)}  ` +
            `elapsed=${formatDuration(elapsed)}  ` +
            `produce≈${formatRate(s.producerRateEMA.value)}  ` +
            `consume≈${formatRate(s.consumerRateEMA.value)}  ` +
            `produced=${formatLag(s.totalProduced)}  consumed=${formatLag(s.totalConsumed)}` +
            (sp ? `  slowest=p${sp.partition}(${formatLag(sp.lag)})` : '')
          );
        }
      }

      s._prevHighTotal      = totalHigh;
      s._prevCommittedTotal = totalCommitted;
      s._prevLag            = lagNum;
    }

    dashboard.render(states, globalPollCount);
  }

  // ── Poll loop ─────────────────────────────────────────────────────────────
  log('INFO', null, null,
    `Polling ${watchList.length} cells every ${config.pollIntervalMs}ms — waiting for activity…\n`);

  const interval = setInterval(async () => {
    try { await poll(); }
    catch (err) { log('ERROR', null, null, `Poll error: ${err.message}`); }
  }, config.pollIntervalMs);

  async function shutdown(sig) {
    process.stdout.write(`\n\nReceived ${sig} — shutting down…\n`);
    clearInterval(interval);
    dashboard.disable();
    printFinalReport(states, config);
    await cleanup(admin, csv, prom, config);
    process.exit(0);
  }

  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

// ─── Per-cell summary (printed when a cell clears) ──────────────────────────

function printCellSummary(s) {
  const tag = `${s.group} / ${s.topic}`;
  log('DONE ', s.group, s.topic, '━'.repeat(70));
  log('DONE ', s.group, s.topic, `✅ CLEARED  [${tag}]`);
  log('DONE ', s.group, s.topic, `   First lag at    : ${formatTimestamp(s.firstMessageAt)}`);
  log('DONE ', s.group, s.topic, `   Peak lag        : ${formatLag(s.maxLag)}  at ${formatTimestamp(s.maxLagAt)}`);
  log('DONE ', s.group, s.topic, `   Time to peak    : ${formatDuration(s.timeToPeak)}`);
  log('DONE ', s.group, s.topic, `   Lag cleared at  : ${formatTimestamp(s.clearedAt)}`);
  log('DONE ', s.group, s.topic, `   Total duration  : ${formatDuration(s.totalDuration)}`);
  log('DONE ', s.group, s.topic, `   Avg throughput  : ${formatRate(s.avgThroughput)}`);
  log('DONE ', s.group, s.topic, `   Growth rate     : ${formatRate(s.lagGrowthRate)}  (producer speed)`);
  log('DONE ', s.group, s.topic, `   Catch-up rate   : ${formatRate(s.catchUpRate)}  (consumer speed)`);
  log('DONE ', s.group, s.topic, `   Total produced  : ${formatLag(s.totalProduced)} msgs`);
  log('DONE ', s.group, s.topic, `   Total consumed  : ${formatLag(s.totalConsumed)} msgs`);
  log('DONE ', s.group, s.topic, `   Poll count      : ${s.pollCount}`);
  if (s.percentiles.count > 0) {
    log('DONE ', s.group, s.topic,
      `   Lag P50/P95/P99 : ${formatLag(s.percentiles.percentile(50))} / ` +
      `${formatLag(s.percentiles.percentile(95))} / ` +
      `${formatLag(s.percentiles.percentile(99))}`);
  }
  const sp = slowestPartition(s.partitions);
  log('DONE ', s.group, s.topic,
    `   Partitions      : ${Object.keys(s.partitions).length}` +
    (sp ? `  (slowest: p${sp.partition} @ ${formatLag(sp.lag)} lag at peak)` : ''));
  log('DONE ', s.group, s.topic, '━'.repeat(70));
}

// ─── Final report ─────────────────────────────────────────────────────────────

function printFinalReport(states, cfg) {
  const W = 96;
  console.log('\n' + '═'.repeat(W));
  console.log('  FINAL INGESTION REPORT  —  ' + formatTimestamp(new Date()));
  console.log('═'.repeat(W));

  // Group states by consumer group for the report
  const byGroup = new Map();
  for (const s of Object.values(states)) {
    if (!byGroup.has(s.group)) byGroup.set(s.group, []);
    byGroup.get(s.group).push(s);
  }

  for (const [group, groupStates] of byGroup) {
    console.log(`\n  Consumer Group: ${group}`);
    console.log('  ' + '─'.repeat(W - 2));

    const rows = groupStates.map(s => ({
      'Topic'         : s.topic,
      'First Lag At'  : s.firstMessageAt ? formatTimestamp(s.firstMessageAt) : '—',
      'Peak Lag'      : formatLag(s.maxLag) || '—',
      'Cleared At'    : s.clearedAt ? formatTimestamp(s.clearedAt) : s.firstMessageAt ? 'PENDING' : '—',
      'Duration'      : s.totalDuration != null ? formatDuration(s.totalDuration) : '—',
      'Avg msg/s'     : s.avgThroughput != null ? formatRate(s.avgThroughput)   : '—',
      'Growth msg/s'  : s.lagGrowthRate != null ? formatRate(s.lagGrowthRate)   : '—',
      'CatchUp msg/s' : s.catchUpRate   != null ? formatRate(s.catchUpRate)     : '—',
      'P50 Lag'       : cfg.trackPercentiles && s.percentiles.count > 0
                          ? formatLag(s.percentiles.percentile(50)) : '—',
      'P99 Lag'       : cfg.trackPercentiles && s.percentiles.count > 0
                          ? formatLag(s.percentiles.percentile(99)) : '—',
      'Polls'         : s.pollCount,
    }));
    console.log(table(rows));
  }

  // Cross-group comparison if more than one group
  if (byGroup.size > 1) {
    console.log('  CROSS-GROUP COMPARISON (topics seen in multiple groups)');
    console.log('  ' + '─'.repeat(W - 2));

    // Find topics that appear in more than one group
    const topicToGroups = new Map();
    for (const s of Object.values(states)) {
      if (!topicToGroups.has(s.topic)) topicToGroups.set(s.topic, []);
      topicToGroups.get(s.topic).push(s);
    }

    for (const [topic, cells] of topicToGroups) {
      if (cells.length < 2) continue;
      console.log(`\n  Topic: ${topic}`);
      const crossRows = cells.map(s => ({
        'Consumer Group': s.group,
        'Duration'      : s.totalDuration != null ? formatDuration(s.totalDuration) : '—',
        'Avg msg/s'     : s.avgThroughput != null ? formatRate(s.avgThroughput)   : '—',
        'Peak Lag'      : formatLag(s.maxLag) || '—',
        'CatchUp msg/s' : s.catchUpRate   != null ? formatRate(s.catchUpRate)     : '—',
        'P99 Lag'       : cfg.trackPercentiles && s.percentiles.count > 0
                            ? formatLag(s.percentiles.percentile(99)) : '—',
        'Status'        : s.isCleared ? 'CLEARED' : s.firstMessageAt ? 'PENDING' : 'NO LAG',
      }));
      console.log(table(crossRows));
    }
  }

  // Partition breakdown
  console.log('  PARTITION BREAKDOWN (lag at close of monitoring)');
  console.log('  ' + '─'.repeat(W - 2));
  for (const [group, groupStates] of byGroup) {
    for (const s of groupStates) {
      if (!s.firstMessageAt) continue;
      console.log(`  [${group}] ${s.topic}`);
      for (const [pid, p] of Object.entries(s.partitions).sort(([a], [b]) => Number(a) - Number(b))) {
        console.log(`    p${String(pid).padEnd(4)} ${formatLag(p.lag).padStart(12)}  ${lagBar(p.lag, s.maxLag)}`);
      }
    }
  }

  console.log('\n' + '═'.repeat(W) + '\n');
}

// ─── Cleanup ──────────────────────────────────────────────────────────────────

async function cleanup(admin, csv, prom, cfg) {
  try { csv.close(); }              catch (_) {}
  if (cfg?.htmlEnabled && cfg?.csvEnabled && csv.path) {
    try {
      const htmlPath = generateHtmlReport(csv.path);
      log('INFO', null, null, `HTML report written to ${htmlPath}`);
    } catch (err) {
      log('WARN', null, null, `HTML report failed: ${err.message}`);
    }
  }
  try { prom.stop(); }              catch (_) {}
  try { await admin.disconnect(); } catch (_) {}
}

function lagBar(lag, maxLag, width = 28) {
  if (!maxLag) return '[' + '░'.repeat(width) + ']';
  const fill = Math.round((lag / maxLag) * width);
  return '[' + '█'.repeat(fill) + '░'.repeat(width - fill) + ']';
}

run().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});