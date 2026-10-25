'use strict';

const fs   = require('fs');
const path = require('path');

const PHASE_COLORS = {
  IDLE     : '#94a3b8',
  GROWING  : '#ef4444',
  CLEARING : '#22c55e',
  SETTLING : '#3b82f6',
};

/** Same basename as CSV, under ./html/ */
function buildHtmlPath(csvPath) {
  const basename = path.basename(csvPath, path.extname(csvPath));
  return path.join('html', `${basename}.html`);
}

function parseRate(value) {
  if (value === '' || value == null) return null;
  const n = parseFloat(value);
  return Number.isNaN(n) ? null : n;
}

function parseCsv(filePath) {
  const text = fs.readFileSync(filePath, 'utf8').trim();
  if (!text) return [];

  const lines = text.split('\n');
  const header = lines[0].split(',');
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    if (cols.length < header.length) continue;
    const row = {};
    header.forEach((key, idx) => { row[key] = cols[idx]; });
    rows.push({
      timestamp      : row.timestamp,
      consumerGroup  : row.consumer_group,
      topic          : row.topic,
      pollCount      : parseInt(row.poll_count, 10),
      partitionCount : parseInt(row.partition_count, 10),
      totalLag       : parseInt(row.total_lag, 10) || 0,
      lagDelta       : parseInt(row.lag_delta, 10) || 0,
      highDelta      : parseInt(row.high_delta, 10) || 0,
      committedDelta : parseInt(row.committed_delta, 10) || 0,
      producerRate   : parseRate(row.producer_rate_mps),
      consumerRate   : parseRate(row.consumer_rate_mps),
      phase          : row.phase,
    });
  }
  return rows;
}

function cellKey(row) {
  return `${row.consumerGroup}\x00${row.topic}`;
}

function groupRows(rows) {
  const map = new Map();
  for (const row of rows) {
    const key = cellKey(row);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(row);
  }
  return map;
}

function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function avgNonNull(values) {
  const nums = values.filter(v => v != null && !Number.isNaN(v));
  if (!nums.length) return null;
  return nums.reduce((sum, v) => sum + v, 0) / nums.length;
}

function maxNonNull(values) {
  const nums = values.filter(v => v != null && !Number.isNaN(v));
  return nums.length ? Math.max(...nums) : null;
}

function summarizeCell(rows) {
  const active = rows.filter(r => r.phase !== 'IDLE');
  const lagSamples = active.map(r => r.totalLag);
  const peakLag = lagSamples.length ? Math.max(0, ...lagSamples) : 0;
  const totalProduced = rows.reduce((sum, r) => sum + Math.max(0, r.highDelta), 0);
  const totalConsumed = rows.reduce((sum, r) => sum + Math.max(0, r.committedDelta), 0);
  const phaseCounts = {};
  for (const r of rows) {
    phaseCounts[r.phase] = (phaseCounts[r.phase] || 0) + 1;
  }

  const peakRow = rows.find(r => r.totalLag === peakLag && peakLag > 0);
  const peakIdx = peakRow ? rows.indexOf(peakRow) : -1;
  const firstActiveMs = active[0] ? new Date(active[0].timestamp).getTime() : null;
  const peakAtMs = peakRow ? new Date(peakRow.timestamp).getTime() : null;
  const lastActiveMs = active.length
    ? new Date(active[active.length - 1].timestamp).getTime()
    : null;

  const settlingRow = peakIdx >= 0
    ? rows.find((r, i) => i > peakIdx && r.phase === 'SETTLING' && r.totalLag === 0)
    : null;
  const clearedAtMs = settlingRow ? new Date(settlingRow.timestamp).getTime() : null;

  const timeToPeakMs = firstActiveMs != null && peakAtMs != null ? peakAtMs - firstActiveMs : null;
  const timeToClearMs = peakAtMs != null && clearedAtMs != null ? clearedAtMs - peakAtMs : null;
  const totalDurationMs = firstActiveMs != null
    ? (clearedAtMs != null ? clearedAtMs - firstActiveMs : lastActiveMs - firstActiveMs)
    : null;

  const lagGrowthRate = peakLag > 0 && timeToPeakMs > 0
    ? peakLag / (timeToPeakMs / 1000)
    : null;
  const catchUpRate = peakLag > 0 && timeToClearMs > 0
    ? peakLag / (timeToClearMs / 1000)
    : null;
  const avgThroughput = totalDurationMs > 0
    ? (totalConsumed || peakLag) / (totalDurationMs / 1000)
    : null;

  const rateRows = active.filter(r => r.producerRate != null || r.consumerRate != null);
  const cleared = settlingRow != null && active.length > 0;
  const topics = [...new Set(rows.map(r => r.topic))];
  const finalLag = rows[rows.length - 1]?.totalLag ?? 0;

  return {
    consumerGroup : rows[0].consumerGroup,
    topics,
    pollCount     : rows[rows.length - 1].pollCount,
    partitionCount: rows[0].partitionCount,
    peakLag,
    finalLag,
    totalProduced,
    totalConsumed,
    netBacklog    : totalProduced - totalConsumed,
    peakAt        : peakRow?.timestamp ?? '—',
    firstActiveAt : active[0]?.timestamp ?? '—',
    lastActiveAt  : active[active.length - 1]?.timestamp ?? '—',
    clearedAt     : settlingRow?.timestamp ?? '—',
    timeToPeakMs,
    timeToClearMs,
    durationMs    : totalDurationMs,
    lagGrowthRate,
    catchUpRate,
    avgThroughput,
    peakProducerRate : maxNonNull(active.map(r => r.producerRate)),
    peakConsumerRate : maxNonNull(active.map(r => r.consumerRate)),
    avgProducerRate  : avgNonNull(rateRows.map(r => r.producerRate)),
    avgConsumerRate  : avgNonNull(rateRows.map(r => r.consumerRate)),
    p50Lag        : lagSamples.length ? percentile(lagSamples, 50) : null,
    p95Lag        : lagSamples.length ? percentile(lagSamples, 95) : null,
    p99Lag        : lagSamples.length ? percentile(lagSamples, 99) : null,
    phaseCounts,
    cleared,
    hasActivity   : active.length > 0,
  };
}

function formatDurationMs(ms) {
  if (ms == null || ms < 0) return '—';
  const s = ms / 1000;
  const m = Math.floor(s / 60);
  const sec = (s % 60).toFixed(1);
  return m > 0 ? `${m}m ${sec}s` : `${sec}s`;
}

function formatNum(n) {
  if (n == null || isNaN(n)) return '—';
  return Number(n).toLocaleString('en-US');
}

function formatRateStat(n) {
  if (n == null || isNaN(n)) return '—';
  return Number(n).toFixed(1) + ' msg/s';
}

function statCard(label, value, small = false) {
  return `
        <div class="stat">
          <span class="label">${label}</span>
          <span class="value${small ? ' small' : ''}">${value}</span>
        </div>`;
}

function metricGroup(title, cards) {
  return `
      <div class="metric-group">
        <h3 class="metric-group-title">${title}</h3>
        <div class="stats-grid">${cards.join('')}</div>
      </div>`;
}

function chartBlock(id, title) {
  return `
    <div class="chart-card">
      <div class="chart-card-header">
        <h3>${title}</h3>
        <button type="button" class="chart-expand-btn" data-chart-id="${id}" aria-label="Expand ${title}" title="Expand chart">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <polyline points="15 3 21 3 21 9"></polyline>
            <polyline points="9 21 3 21 3 15"></polyline>
            <line x1="21" y1="3" x2="14" y2="10"></line>
            <line x1="3" y1="21" x2="10" y2="14"></line>
          </svg>
        </button>
      </div>
      <div class="chart-wrap"><canvas id="${id}"></canvas></div>
    </div>`;
}

function renderCellSection(rows, index, chartPayloads) {
  const summary = summarizeCell(rows);
  const slug = `cell-${index}`;

  const duration = formatDurationMs(summary.durationMs);
  const timeToPeak = formatDurationMs(summary.timeToPeakMs);
  const timeToClear = formatDurationMs(summary.timeToClearMs);
  const status = !summary.hasActivity
    ? '<span class="badge idle">No lag detected</span>'
    : summary.cleared
      ? '<span class="badge ok">Cleared</span>'
      : '<span class="badge pending">In progress / interrupted</span>';

  if (summary.hasActivity) {
    chartPayloads.push({
      slug,
      labels     : rows.map(r => r.pollCount),
      timestamps : rows.map(r => r.timestamp),
      totalLag   : rows.map(r => r.totalLag),
      lagDelta   : rows.map(r => r.lagDelta),
      highDelta  : rows.map(r => r.highDelta),
      committedDelta: rows.map(r => r.committedDelta),
      producerRate: rows.map(r => r.producerRate),
      consumerRate: rows.map(r => r.consumerRate),
      phaseCounts: summary.phaseCounts,
    });
  }

  const lagMetrics = metricGroup('Lag', [
    statCard('Peak lag', formatNum(summary.peakLag)),
    statCard('Final lag', formatNum(summary.finalLag)),
    statCard('P50 lag', formatNum(summary.p50Lag)),
    statCard('P95 lag', formatNum(summary.p95Lag)),
    statCard('P99 lag', formatNum(summary.p99Lag)),
    statCard('Peak at', summary.peakAt, true),
  ]);

  const throughputMetrics = metricGroup('Throughput', [
    statCard('Total produced', formatNum(summary.totalProduced)),
    statCard('Total consumed', formatNum(summary.totalConsumed)),
    statCard('Net backlog', formatNum(summary.netBacklog)),
    statCard('Peak producer rate', formatRateStat(summary.peakProducerRate)),
    statCard('Peak consumer rate', formatRateStat(summary.peakConsumerRate)),
    statCard('Avg producer rate', formatRateStat(summary.avgProducerRate)),
    statCard('Avg consumer rate', formatRateStat(summary.avgConsumerRate)),
    statCard('Avg throughput', formatRateStat(summary.avgThroughput)),
  ]);

  const timingMetrics = metricGroup('Timing & rates', [
    statCard('First activity', summary.firstActiveAt, true),
    statCard('Cleared at', summary.clearedAt, true),
    statCard('Time to peak', timeToPeak),
    statCard('Time to clear', timeToClear),
    statCard('Total duration', duration),
    statCard('Growth rate', formatRateStat(summary.lagGrowthRate)),
    statCard('Catch-up rate', formatRateStat(summary.catchUpRate)),
    statCard('Polls', formatNum(summary.pollCount)),
  ]);

  return `
    <section class="cell-section">
      <details class="cell-details" open>
        <summary class="cell-summary">
          <span class="cell-chevron" aria-hidden="true"></span>
          <div class="cell-summary-main">
            <h2 class="cell-title">
              <span class="cell-line"><span class="label">GroupId:</span> ${summary.consumerGroup}</span>
              <span class="cell-line"><span class="label">Topics:</span> ${summary.topics.join(', ')}</span>
            </h2>
            <p class="meta">${summary.partitionCount} partitions · ${summary.pollCount} polls · peak lag ${formatNum(summary.peakLag)}</p>
          </div>
          ${status}
        </summary>

        <div class="cell-body">
          ${summary.hasActivity ? `
          <div class="charts-grid">
            ${chartBlock(`${slug}-lag`, 'Total consumer lag')}
            ${chartBlock(`${slug}-delta`, 'Lag delta per poll')}
            ${chartBlock(`${slug}-volume`, 'Produced vs consumed per poll')}
            ${chartBlock(`${slug}-rates`, 'Producer vs consumer rate (msg/s)')}
            ${chartBlock(`${slug}-phases`, 'Phase distribution')}
          </div>` : '<p class="empty">No load-test activity recorded for this cell (no offset movement detected).</p>'}

          <div class="metrics-section">
            ${lagMetrics}
            ${throughputMetrics}
            ${timingMetrics}
          </div>
        </div>
      </details>
    </section>`;
}

const CHART_INIT_SCRIPT = `
const chartRegistry = new Map();
let modalChart = null;

function chartOpts(yLabel, timestamps) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: {
        title: { display: true, text: 'Poll #', color: '#94a3b8' },
        ticks: { maxTicksLimit: 10, color: '#94a3b8', font: { size: 10 } },
        grid: { color: 'rgba(51,65,85,0.5)' },
      },
      y: {
        title: { display: true, text: yLabel, color: '#94a3b8' },
        ticks: { color: '#94a3b8' },
        grid: { color: 'rgba(51,65,85,0.5)' },
      },
    },
    plugins: {
      legend: { labels: { color: '#cbd5e1' } },
      tooltip: {
        mode: 'index',
        intersect: false,
        callbacks: {
          title(items) {
            const i = items[0]?.dataIndex;
            return timestamps && i != null ? timestamps[i] : items[0]?.label;
          },
        },
      },
    },
  };
}

function registerChart(id, title, buildConfig) {
  chartRegistry.set(id, { title, buildConfig });
  return new Chart(document.getElementById(id), buildConfig());
}

function buildLagConfig(labels, timestamps, totalLag) {
  return {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Total lag',
        data: totalLag,
        borderColor: '#6366f1',
        backgroundColor: 'rgba(99,102,241,0.1)',
        fill: true,
        tension: 0.2,
        pointRadius: 0,
      }],
    },
    options: chartOpts('Messages', timestamps),
  };
}

function buildDeltaConfig(labels, timestamps, lagDelta) {
  return {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Lag delta',
        data: lagDelta,
        backgroundColor: lagDelta.map(v =>
          v > 0 ? 'rgba(239,68,68,0.7)' : v < 0 ? 'rgba(34,197,94,0.7)' : 'rgba(148,163,184,0.4)'),
      }],
    },
    options: chartOpts('Δ messages', timestamps),
  };
}

function buildVolumeConfig(labels, timestamps, highDelta, committedDelta) {
  return {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'Produced (high Δ)',
          data: highDelta,
          backgroundColor: 'rgba(239,68,68,0.65)',
        },
        {
          label: 'Consumed (commit Δ)',
          data: committedDelta,
          backgroundColor: 'rgba(34,197,94,0.65)',
        },
      ],
    },
    options: chartOpts('Messages', timestamps),
  };
}

function formatRate(val) {
  return val == null || Number.isNaN(val) ? '—' : Number(val).toFixed(2) + ' msg/s';
}

function resolvePollIndex(chart, tooltip) {
  if (tooltip.dataPoints?.length) {
    return tooltip.dataPoints[0].dataIndex;
  }
  const scale = chart.scales.x;
  if (!scale || tooltip.caretX == null) return null;
  const labels = chart.data.labels;
  if (typeof scale.getDataIndexForPixel === 'function') {
    const idx = scale.getDataIndexForPixel(tooltip.caretX);
    if (idx >= 0 && idx < labels.length) return idx;
  }
  const raw = scale.getValueForPixel(tooltip.caretX);
  if (raw == null) return null;
  const byLabel = labels.indexOf(raw);
  if (byLabel >= 0) return byLabel;
  const byLabelStr = labels.indexOf(String(raw));
  if (byLabelStr >= 0) return byLabelStr;
  const rounded = Math.round(Number(raw));
  if (rounded >= 0 && rounded < labels.length && labels[rounded] != null) return rounded;
  const byPoll = labels.indexOf(rounded);
  return byPoll >= 0 ? byPoll : null;
}

function ratesExternalTooltip(timestamps) {
  return function(context) {
    const { chart, tooltip } = context;
    const wrap = chart.canvas.parentNode;
    let el = wrap.querySelector('.rates-tooltip');
    if (!el) {
      el = document.createElement('div');
      el.className = 'rates-tooltip';
      wrap.appendChild(el);
    }

    if (!tooltip || tooltip.opacity === 0) {
      el.style.opacity = '0';
      return;
    }

    const idx = resolvePollIndex(chart, tooltip);
    if (idx == null) {
      el.style.opacity = '0';
      return;
    }

    const producer = chart.data.datasets[0].data[idx];
    const consumer = chart.data.datasets[1].data[idx];
    const title = timestamps?.[idx] ?? ('Poll #' + chart.data.labels[idx]);

    el.innerHTML =
      '<div class="rates-tooltip-title">' + title + '</div>' +
      '<div class="rates-tooltip-row"><span class="rates-dot producer"></span>Producer rate: ' + formatRate(producer) + '</div>' +
      '<div class="rates-tooltip-row"><span class="rates-dot consumer"></span>Consumer rate: ' + formatRate(consumer) + '</div>';

    const canvasRect = chart.canvas.getBoundingClientRect();
    const wrapRect = wrap.getBoundingClientRect();
    const left = canvasRect.left - wrapRect.left + tooltip.caretX;
    const top = canvasRect.top - wrapRect.top + tooltip.caretY;

    el.style.opacity = '1';
    el.style.left = left + 'px';
    el.style.top = top + 'px';
    el.style.transform = 'translate(-50%, calc(-100% - 10px))';
  };
}

function buildRatesConfig(labels, timestamps, producerRate, consumerRate) {
  const options = chartOpts('msg/s', timestamps);
  options.interaction = { mode: 'index', intersect: false, axis: 'x' };
  options.plugins.tooltip.enabled = false;
  options.plugins.tooltip.mode = 'index';
  options.plugins.tooltip.intersect = false;
  options.plugins.tooltip.axis = 'x';
  options.plugins.tooltip.external = ratesExternalTooltip(timestamps);

  return {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Producer rate',
          data: producerRate,
          borderColor: '#ef4444',
          spanGaps: true,
          tension: 0.2,
          pointRadius: 0,
          pointHitRadius: 12,
        },
        {
          label: 'Consumer rate',
          data: consumerRate,
          borderColor: '#22c55e',
          spanGaps: true,
          tension: 0.2,
          pointRadius: 0,
          pointHitRadius: 12,
        },
      ],
    },
    options,
  };
}

function buildPhasesConfig(phaseCounts, colors) {
  return {
    type: 'doughnut',
    data: {
      labels: Object.keys(phaseCounts),
      datasets: [{
        data: Object.values(phaseCounts),
        backgroundColor: Object.keys(phaseCounts).map(p => colors[p] || '#cbd5e1'),
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom', labels: { color: '#cbd5e1' } } },
    },
  };
}

function initCellCharts(cell, colors) {
  const {
    slug, labels, timestamps, totalLag, lagDelta,
    highDelta, committedDelta, producerRate, consumerRate, phaseCounts,
  } = cell;

  registerChart(slug + '-lag', 'Total consumer lag', () =>
    buildLagConfig(labels, timestamps, totalLag));

  registerChart(slug + '-delta', 'Lag delta per poll', () =>
    buildDeltaConfig(labels, timestamps, lagDelta));

  registerChart(slug + '-volume', 'Produced vs consumed per poll', () =>
    buildVolumeConfig(labels, timestamps, highDelta, committedDelta));

  registerChart(slug + '-rates', 'Producer vs consumer rate (msg/s)', () =>
    buildRatesConfig(labels, timestamps, producerRate, consumerRate));

  registerChart(slug + '-phases', 'Phase distribution', () =>
    buildPhasesConfig(phaseCounts, colors));
}

function openChartModal(chartId) {
  const entry = chartRegistry.get(chartId);
  const modal = document.getElementById('chart-modal');
  if (!entry || !modal) return;

  document.getElementById('chart-modal-title').textContent = entry.title;
  modal.classList.add('open');
  modal.setAttribute('aria-hidden', 'false');
  document.body.classList.add('modal-open');

  if (modalChart) {
    modalChart.destroy();
    modalChart = null;
  }

  modalChart = new Chart(document.getElementById('chart-modal-canvas'), entry.buildConfig());
}

function closeChartModal() {
  const modal = document.getElementById('chart-modal');
  if (!modal || !modal.classList.contains('open')) return;

  modal.classList.remove('open');
  modal.setAttribute('aria-hidden', 'true');
  document.body.classList.remove('modal-open');

  if (modalChart) {
    modalChart.destroy();
    modalChart = null;
  }
}

document.addEventListener('DOMContentLoaded', function() {
  const el = document.getElementById('chart-data');
  if (!el) return;
  const { cells, colors } = JSON.parse(el.textContent);
  for (const cell of cells) initCellCharts(cell, colors);

  document.addEventListener('click', function(e) {
    const expandBtn = e.target.closest('.chart-expand-btn');
    if (expandBtn) {
      openChartModal(expandBtn.dataset.chartId);
      return;
    }
    if (e.target.closest('.chart-modal-close') || e.target.classList.contains('chart-modal-backdrop')) {
      closeChartModal();
    }
  });

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') closeChartModal();
  });
});
`;

function generateHtmlReport(csvPath) {
  const rows = parseCsv(csvPath);
  const htmlPath = path.resolve(buildHtmlPath(csvPath));
  fs.mkdirSync(path.dirname(htmlPath), { recursive: true });

  const generatedAt = new Date().toISOString().replace('T', ' ').slice(0, 23);
  const reportName = path.basename(htmlPath, '.html');
  const cells = groupRows(rows);
  const chartPayloads = [];
  const cellSections = [...cells.values()]
    .map((cellRows, i) => renderCellSection(cellRows, i, chartPayloads))
    .join('\n');

  const chartDataJson = JSON.stringify({ cells: chartPayloads, colors: PHASE_COLORS });

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kafka Load Test — ${reportName}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"><\/script>
  <style>
    :root {
      --bg: #0f172a; --surface: #1e293b; --border: #334155;
      --text: #f1f5f9; --muted: #94a3b8; --accent: #6366f1;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: ui-sans-serif, system-ui, -apple-system, sans-serif;
      background: var(--bg); color: var(--text); line-height: 1.5;
      padding: 2rem 1.5rem 4rem;
    }
    .container { max-width: 1200px; margin: 0 auto; }
    header { margin-bottom: 2rem; padding-bottom: 1.5rem; border-bottom: 1px solid var(--border); }
    header h1 { font-size: 1.75rem; font-weight: 700; margin-bottom: 0.25rem; }
    header p { color: var(--muted); font-size: 0.9rem; }
    .source { color: var(--accent); word-break: break-all; }
    .cell-section {
      background: var(--surface); border: 1px solid var(--border);
      border-radius: 12px; margin-bottom: 1.5rem; overflow: hidden;
    }
    .cell-details > summary { list-style: none; }
    .cell-details > summary::-webkit-details-marker { display: none; }
    .cell-summary {
      display: flex; align-items: flex-start; gap: 0.75rem;
      padding: 1.25rem 1.5rem; cursor: pointer; user-select: none;
      border-bottom: 1px solid transparent;
    }
    .cell-details[open] > .cell-summary { border-bottom-color: var(--border); }
    .cell-chevron {
      flex-shrink: 0; width: 16px; height: 16px; margin-top: 0.2rem;
      position: relative; transition: transform 0.2s;
    }
    .cell-chevron::before {
      content: ''; position: absolute; top: 4px; left: 5px;
      border: solid var(--muted); border-width: 0 2px 2px 0;
      display: inline-block; padding: 3px; transform: rotate(-45deg);
    }
    .cell-details[open] > .cell-summary .cell-chevron { transform: rotate(90deg); }
    .cell-summary-main { flex: 1; min-width: 0; }
    .cell-summary:hover { background: rgba(99,102,241,0.04); }
    .cell-body { padding: 1.25rem 1.5rem 1.5rem; }
    .cell-header h2 { font-size: 1.05rem; font-weight: 600; }
    .cell-title { display: flex; flex-direction: column; gap: 0.2rem; }
    .cell-line .label { color: var(--muted); font-weight: 500; margin-right: 0.35rem; }
    .meta { color: var(--muted); font-size: 0.85rem; margin-top: 0.15rem; }
    .metrics-section { display: flex; flex-direction: column; gap: 1.25rem; margin-top: 1.5rem; }
    .charts-grid {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 1rem; margin-bottom: 0;
    }
    .metric-group-title {
      font-size: 0.72rem; font-weight: 600; text-transform: uppercase;
      letter-spacing: 0.06em; color: var(--muted); margin-bottom: 0.6rem;
    }
    .badge { font-size: 0.75rem; font-weight: 600; padding: 0.25rem 0.65rem; border-radius: 999px; white-space: nowrap; }
    .badge.ok { background: rgba(34,197,94,0.15); color: #4ade80; }
    .badge.pending { background: rgba(251,191,36,0.15); color: #fbbf24; }
    .badge.idle { background: rgba(148,163,184,0.15); color: #94a3b8; }
    .stats-grid {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 0.65rem;
    }
    .stat {
      background: var(--bg); border: 1px solid var(--border);
      border-radius: 8px; padding: 0.75rem 1rem;
    }
    .stat .label { display: block; font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }
    .stat .value { display: block; font-size: 1.25rem; font-weight: 700; margin-top: 0.15rem; }
    .stat .value.small { font-size: 0.8rem; font-weight: 500; }
    .chart-card {
      background: var(--bg); border: 1px solid var(--border);
      border-radius: 8px; padding: 1rem;
    }
    .chart-card-header {
      display: flex; align-items: flex-start; justify-content: space-between;
      gap: 0.5rem; margin-bottom: 0.75rem;
    }
    .chart-card-header h3 { font-size: 0.85rem; color: var(--muted); font-weight: 500; flex: 1; }
    .chart-expand-btn {
      flex-shrink: 0; display: inline-flex; align-items: center; justify-content: center;
      width: 28px; height: 28px; padding: 0; border: 1px solid var(--border);
      border-radius: 6px; background: var(--surface); color: var(--muted);
      cursor: pointer; transition: color 0.15s, border-color 0.15s, background 0.15s;
    }
    .chart-expand-btn:hover {
      color: var(--text); border-color: var(--accent); background: rgba(99,102,241,0.12);
    }
    .chart-wrap { position: relative; height: 220px; overflow: visible; }
    .rates-tooltip {
      position: absolute; pointer-events: none; z-index: 20;
      background: rgba(15, 23, 42, 0.96); border: 1px solid var(--border);
      border-radius: 8px; padding: 0.55rem 0.7rem; font-size: 0.78rem;
      color: var(--text); opacity: 0; box-shadow: 0 8px 24px rgba(0,0,0,0.35);
      min-width: 180px;
    }
    .rates-tooltip-title {
      font-weight: 600; margin-bottom: 0.35rem; color: #cbd5e1; font-size: 0.75rem;
    }
    .rates-tooltip-row { display: flex; align-items: center; gap: 0.4rem; line-height: 1.45; }
    .rates-dot {
      width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0;
    }
    .rates-dot.producer { background: #ef4444; }
    .rates-dot.consumer { background: #22c55e; }
    .chart-modal {
      display: none; position: fixed; inset: 0; z-index: 1000;
      align-items: center; justify-content: center; padding: 1.5rem;
    }
    .chart-modal.open { display: flex; }
    body.modal-open { overflow: hidden; }
    .chart-modal-backdrop {
      position: absolute; inset: 0; background: rgba(15,23,42,0.82);
    }
    .chart-modal-panel {
      position: relative; z-index: 1; width: min(1100px, 100%);
      max-height: calc(100vh - 3rem); background: var(--surface);
      border: 1px solid var(--border); border-radius: 12px;
      box-shadow: 0 24px 48px rgba(0,0,0,0.45);
      display: flex; flex-direction: column; overflow: hidden;
    }
    .chart-modal-header {
      display: flex; align-items: center; justify-content: space-between;
      gap: 1rem; padding: 1rem 1.25rem; border-bottom: 1px solid var(--border);
    }
    .chart-modal-header h3 { font-size: 1rem; font-weight: 600; color: var(--text); }
    .chart-modal-close {
      display: inline-flex; align-items: center; justify-content: center;
      width: 32px; height: 32px; padding: 0; border: 1px solid var(--border);
      border-radius: 6px; background: var(--bg); color: var(--muted);
      font-size: 1.25rem; line-height: 1; cursor: pointer;
    }
    .chart-modal-close:hover { color: var(--text); border-color: var(--accent); }
    .chart-modal-body { position: relative; height: min(70vh, 640px); padding: 1rem 1.25rem 1.25rem; overflow: visible; }
    .chart-modal-body canvas { width: 100% !important; height: 100% !important; }
    .empty { color: var(--muted); font-style: italic; padding: 1rem 0; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>Kafka Load Test Report</h1>
      <p>Generated ${generatedAt} · Source: <span class="source">${path.basename(csvPath)}</span></p>
    </header>
    ${cells.size === 0
      ? '<p class="empty">No CSV data found.</p>'
      : cellSections}
  </div>
  <div id="chart-modal" class="chart-modal" aria-hidden="true" role="dialog" aria-modal="true" aria-labelledby="chart-modal-title">
    <div class="chart-modal-backdrop"></div>
    <div class="chart-modal-panel">
      <div class="chart-modal-header">
        <h3 id="chart-modal-title"></h3>
        <button type="button" class="chart-modal-close" aria-label="Close expanded chart">&times;</button>
      </div>
      <div class="chart-modal-body">
        <canvas id="chart-modal-canvas"></canvas>
      </div>
    </div>
  </div>
  <script id="chart-data" type="application/json">${chartDataJson}</script>
  <script>${CHART_INIT_SCRIPT}</script>
</body>
</html>`;

  fs.writeFileSync(htmlPath, html, 'utf8');
  return htmlPath;
}

module.exports = { buildHtmlPath, generateHtmlReport };
