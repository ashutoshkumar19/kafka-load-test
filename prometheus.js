'use strict';

const http = require('http');

/**
 * Minimal Prometheus text-format metrics server.
 * No dependencies — uses Node's built-in http module.
 *
 * Exposed metrics (all gauges):
 *   kafka_lag_total{topic}            Current total lag
 *   kafka_lag_max{topic}              Peak lag observed
 *   kafka_lag_growth_rate{topic}      Estimated producer rate (msg/s)
 *   kafka_lag_consume_rate{topic}     Estimated consumer catch-up rate (msg/s)
 *   kafka_lag_partition{topic,partition}  Per-partition lag
 *   kafka_lag_poll_count{topic}       Number of polls performed
 *   kafka_lag_duration_ms{topic}      Total ingestion duration so far (ms)
 */
class PrometheusServer {
  /**
   * @param {Object}  opts
   * @param {number}  opts.port
   * @param {boolean} opts.enabled
   * @param {Function} opts.getStates  () => { [topic]: topicState }
   */
  constructor({ port, enabled, getStates }) {
    this._enabled   = enabled;
    this._port      = port;
    this._getStates = getStates;
    this._server    = null;
  }

  start() {
    if (!this._enabled) return;
    this._server = http.createServer((req, res) => {
      if (req.url !== '/metrics') {
        res.writeHead(404);
        res.end('Not found — try /metrics\n');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4' });
      res.end(this._render());
    });
    this._server.listen(this._port, () => {
      console.log(`  [PROM]  Prometheus metrics → http://localhost:${this._port}/metrics`);
    });
  }

  stop() {
    if (this._server) this._server.close();
  }

  _render() {
    const states = this._getStates();
    const lines  = [];

    const gauge = (name, help, type = 'gauge') => {
      lines.push(`# HELP ${name} ${help}`);
      lines.push(`# TYPE ${name} ${type}`);
    };

    // ── kafka_lag_total ──────────────────────────────────────────────────────
    gauge('kafka_lag_total', 'Current total consumer lag across all partitions');
    for (const s of Object.values(states)) {
      const lag = Object.values(s.partitions).reduce((a, p) => a + (p.lag || 0), 0);
      lines.push(`kafka_lag_total{topic="${s.topic}"} ${lag}`);
    }

    // ── kafka_lag_max ────────────────────────────────────────────────────────
    gauge('kafka_lag_max', 'Peak lag observed since monitoring started');
    for (const s of Object.values(states)) {
      lines.push(`kafka_lag_max{topic="${s.topic}"} ${s.maxLag}`);
    }

    // ── kafka_lag_growth_rate ────────────────────────────────────────────────
    gauge('kafka_lag_growth_rate', 'Estimated producer ingestion rate (messages/sec)');
    for (const s of Object.values(states)) {
      const v = s.producerRateEMA?.value;
      if (v != null) lines.push(`kafka_lag_growth_rate{topic="${s.topic}"} ${v.toFixed(2)}`);
    }

    // ── kafka_lag_consume_rate ───────────────────────────────────────────────
    gauge('kafka_lag_consume_rate', 'Estimated consumer catch-up rate (messages/sec)');
    for (const s of Object.values(states)) {
      const v = s.consumerRateEMA?.value;
      if (v != null) lines.push(`kafka_lag_consume_rate{topic="${s.topic}"} ${v.toFixed(2)}`);
    }

    // ── kafka_lag_partition ──────────────────────────────────────────────────
    gauge('kafka_lag_partition', 'Per-partition consumer lag');
    for (const s of Object.values(states)) {
      for (const [pid, p] of Object.entries(s.partitions)) {
        lines.push(`kafka_lag_partition{topic="${s.topic}",partition="${pid}"} ${p.lag ?? 0}`);
      }
    }

    // ── kafka_lag_poll_count ─────────────────────────────────────────────────
    gauge('kafka_lag_poll_count', 'Number of completed poll cycles', 'counter');
    for (const s of Object.values(states)) {
      lines.push(`kafka_lag_poll_count{topic="${s.topic}"} ${s.pollCount}`);
    }

    // ── kafka_lag_duration_ms ────────────────────────────────────────────────
    gauge('kafka_lag_duration_ms', 'Elapsed ms since first lag detected (0 if not yet started)');
    for (const s of Object.values(states)) {
      const dur = s.firstMessageAt
        ? (s.clearedAt ? s.totalDuration : Date.now() - s.firstMessageAt.getTime())
        : 0;
      lines.push(`kafka_lag_duration_ms{topic="${s.topic}"} ${dur}`);
    }

    lines.push('');
    return lines.join('\n');
  }
}

module.exports = PrometheusServer;
