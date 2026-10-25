'use strict';

const { formatLag, formatRate, formatDuration, formatTimestamp, ANSI, derivePhase } = require('./utils');

/**
 * Redraws a compact live table in-place using ANSI cursor control.
 * Call render(states) each poll; it moves the cursor up and overwrites previous output.
 */
class Dashboard {
  constructor(enabled) {
    this._enabled  = enabled && process.stdout.isTTY;
    this._linesSent = 0;
  }

  render(states, pollCount) {
    if (!this._enabled) return;

    const now   = new Date();
    const rows  = Object.values(states);
    const lines = [];

    // ── Header ───────────────────────────────────────────────────────────────
    lines.push(
      `${ANSI.bold}${ANSI.cyan}  ┌─ KAFKA LAG MONITOR  ` +
      `poll #${pollCount}  ${formatTimestamp(now)} ─┐${ANSI.reset}`
    );

    // ── Column header ────────────────────────────────────────────────────────
    lines.push(
      `${ANSI.dim}${'  Topic'.padEnd(32)}` +
      `${'Lag'.padStart(12)}` +
      `${'MaxLag'.padStart(12)}` +
      `${'Phase'.padStart(12)}` +
      `${'ProdRate'.padStart(12)}` +
      `${'ConsumeRate'.padStart(13)}` +
      `${'Elapsed'.padStart(11)}` +
      `${ANSI.reset}`
    );
    lines.push(`  ${'─'.repeat(104)}`);

    // ── One row per topic ────────────────────────────────────────────────────
    for (const s of rows) {
      const currentLag = Object.values(s.partitions).reduce((a, p) => a + (p.lag || 0), 0);
      const phase      = derivePhase(s, {
        lagNum          : currentLag,
        lagDelta        : s._lastLagDelta,
        highDelta       : s._lastHighDelta,
        committedDelta  : s._lastCommittedDelta,
      });
      const elapsed    = s.firstMessageAt
        ? (s.clearedAt ? s.totalDuration : now - s.firstMessageAt)
        : null;

      const lagColor   = currentLag === 0
        ? (s.firstMessageAt ? ANSI.green : ANSI.dim)
        : currentLag === s.maxLag ? ANSI.red : ANSI.yellow;

      const phaseColor = phase === 'CLEARED'   ? ANSI.green
                       : phase === 'CLEARING'  ? ANSI.yellow
                       : phase === 'GROWING'   ? ANSI.red
                       :                         ANSI.dim;

      lines.push(
        `  ${s.topic.slice(0, 30).padEnd(30)}` +
        `  ${lagColor}${formatLag(currentLag).padStart(12)}${ANSI.reset}` +
        `  ${formatLag(s.maxLag).padStart(12)}` +
        `  ${phaseColor}${phase.padStart(12)}${ANSI.reset}` +
        `  ${formatRate(s.producerRateEMA?.value).padStart(12)}` +
        `  ${formatRate(s.consumerRateEMA?.value).padStart(13)}` +
        `  ${(elapsed != null ? formatDuration(elapsed) : '—').padStart(11)}`
      );

      // Partition breakdown when active (lag or offset movement)
      const partIds = Object.keys(s.partitions);
      if (partIds.length > 1 && s.firstMessageAt && (currentLag > 0 || s._lastHighDelta > 0)) {
        const slowest = partIds.reduce((best, pid) =>
          (s.partitions[pid].lag || 0) > (s.partitions[best]?.lag || 0) ? pid : best, partIds[0]);
        lines.push(
          `${ANSI.dim}    └─ ${partIds.length} partitions  ` +
          `slowest: p${slowest} (${formatLag(s.partitions[slowest]?.lag)} lag)${ANSI.reset}`
        );
      }
    }

    lines.push(`${ANSI.dim}  ${'─'.repeat(104)}${ANSI.reset}`);

    // ── Erase previous render and write new one ──────────────────────────────
    if (this._linesSent > 0) {
      process.stdout.write(ANSI.up(this._linesSent));
    }
    const output = lines.map(l => ANSI.clearLine + ANSI.col0 + l).join('\n') + '\n';
    process.stdout.write(output);
    this._linesSent = lines.length;
  }

  /** Push the cursor below the dashboard before printing a structured log line */
  clearForLog() {
    if (!this._enabled) return;
    // Write a blank run below so the log appears after the dashboard
    if (this._linesSent > 0) {
      process.stdout.write('\n'.repeat(0)); // no-op; log lines naturally append below
    }
  }

  disable() {
    this._enabled = false;
  }
}

module.exports = Dashboard;
