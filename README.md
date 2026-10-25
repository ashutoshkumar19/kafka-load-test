# Kafka Lag Monitor v2

A zero-dependency (one npm package) Node.js tool for **Kafka load-test observability**.  
Multi-group × multi-topic Kafka load-test observability.  
Every `(consumer group, topic)` pair is tracked independently with its own full metric timeline.

---

## Metrics collected

### Core timings
| Metric | Description |
|---|---|
| First lag detected | When lag first appeared (producer started) |
| Peak lag | Maximum backlog observed |
| Peak lag timestamp | When the backlog peaked |
| Lag cleared | When lag hit zero and remained zero through the settling period |
| Total duration | `clearedAt − firstMessageAt` — the primary load-test metric |

### Derived rates
| Metric | Formula | Meaning |
|---|---|---|
| Avg throughput | `maxLag / totalDuration` | Overall ingestion rate |
| Lag growth rate | `maxLag / timeToPeak` | Estimated **producer** speed during ramp-up |
| Catch-up rate | `maxLag / (clearedAt − maxLagAt)` | Estimated **consumer** speed during drain |
| Time to peak | `maxLagAt − firstMessageAt` | How fast producers outpaced consumers |

### Statistical
| Metric | Description |
|---|---|
| P50 / P95 / P99 lag | Percentile lag values over the entire run |

### Operational
| Metric | Description |
|---|---|
| Poll count | Number of completed broker queries per topic |
| Current lag | Live, per-poll reading (dashboard + CSV) |
| Per-partition lag | Lag per partition with slowest-partition detection |
| CSV export | One row per topic per poll for offline graphing |
| Prometheus `/metrics` | Scrape-ready endpoint for Grafana dashboards |

---

## Quick start

```bash
npm install

KAFKA_BROKERS=localhost:9092 \
KAFKA_CONSUMER_GROUP=my-consumer-group \
KAFKA_TOPICS=orders,payments,inventory \
node monitor.js
```

---

*****************
## Configuration modes

### A — Single group, many topics (env vars)
```bash
KAFKA_CONSUMER_GROUPS=ingestion-group \
KAFKA_TOPICS=orders,payments,inventory \
node monitor.js
```
Produces 3 watch cells: `ingestion-group/orders`, `ingestion-group/payments`, `ingestion-group/inventory`.

### B — Matrix (env vars)
```bash
KAFKA_CONSUMER_GROUPS=group-a,group-b \
KAFKA_TOPICS=topic-1,topic-2 \
node monitor.js
```
Produces 4 watch cells (every group × every topic):
```
group-a / topic-1
group-a / topic-2
group-b / topic-1
group-b / topic-2
```

### C — Explicit mapping (edit `config.js`)
Edit the `watches` array in `config.js` for non-uniform group→topic mappings:
```js
const watches = [
  { group: 'ingestion-group',  topics: ['raw-events', 'enriched-events'] },
  { group: 'analytics-group',  topics: ['enriched-events'] },            // only one topic
  { group: 'archiver-group',   topics: ['raw-events', 'dead-letter'] },
];
```

---

## Poll strategy

The monitor makes the minimum number of broker calls per cycle:

- **One** `fetchOffsets` per consumer group (parallelised across all groups)
- **One** `fetchTopicOffsets` per unique topic (shared across groups — a topic subscribed by 3 groups is fetched once)

All calls run via `Promise.allSettled`, so a failure in one group never blocks others.

---

## Configuration reference

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `KAFKA_CLIENT_ID` | `kafka-lag-monitor` | Kafka client identifier |
| `KAFKA_CONSUMER_GROUPS` | `my-consumer-group` | Comma-separated groups to watch |
| `KAFKA_TOPICS` | `topic-a,topic-b` | Comma-separated topics to watch |
| `KAFKA_SSL` | `false` | `true` to enable TLS |
| `KAFKA_SASL_MECHANISM` | _(none)_ | `plain` / `scram-sha-256` / `scram-sha-512` |
| `KAFKA_SASL_USERNAME` | _(none)_ | SASL username |
| `KAFKA_SASL_PASSWORD` | _(none)_ | SASL password |
| `POLL_INTERVAL_MS` | `2000` | How often to query the broker (ms) |
| `LOG_PROGRESS_EVERY_MS` | `10000` | Progress log interval per cell (0 = off) |
| `SETTLING_PERIOD_MS` | `10000` | Ms at zero lag before declaring cleared |
| `EXIT_WHEN_CLEARED` | `true` | Auto-exit when all cells settle |
| `TRACK_PERCENTILES` | `true` | Compute P50/P95/P99 lag |
| `CSV_ENABLED` | `true` | Write CSV export |
| `CSV_PATH` | `./lag-report.csv` | CSV output path |
| `PROMETHEUS_ENABLED` | `false` | Start `/metrics` endpoint |
| `PROMETHEUS_PORT` | `9090` | Prometheus server port |
| `DASHBOARD_ENABLED` | `true` | Live in-place dashboard (TTY only) |

---

## Settling period

A common failure mode in load tests: producers push in bursts. Without a settling period, the monitor may declare "cleared" between two producer batches, then see lag reappear.

`SETTLING_PERIOD_MS=10000` (default) means lag must stay at zero for **10 full seconds** before the topic is considered done. Set to `0` to declare cleared on the first zero reading.

---


## CSV format

One row per `(group, topic)` per poll:

```
timestamp,consumer_group,topic,poll_count,partition_count,total_lag,lag_delta,producer_rate_mps,consumer_rate_mps,phase
2024-01-15 09:00:04.002,group-a,orders,2,8,512000,512000,256000.0,,GROWING
2024-01-15 09:00:04.002,group-b,orders,2,8,310000,310000,155000.0,,GROWING
```

**Phases:**
- `IDLE` — no lag yet observed
- `GROWING` — lag is increasing (producers outpacing consumers)
- `CLEARING` — lag is decreasing (consumers draining the backlog)
- `SETTLING` — lag hit 0, waiting out the settling period

---

## Prometheus + Grafana

All metrics carry `consumer_group` and `topic` labels:

```
kafka_lag_total{consumer_group="group-a",topic="orders"}          1,800,000
kafka_lag_total{consumer_group="group-b",topic="orders"}            950,000
kafka_lag_partition{consumer_group="group-a",topic="orders",partition="3"}  225,000
kafka_lag_duration_ms{consumer_group="group-a",topic="orders"}    8004
```


```bash
PROMETHEUS_ENABLED=true node monitor.js
```

Metrics exposed at `http://localhost:9090/metrics`:

Add a Prometheus scrape job:
```yaml
scrape_configs:
  - job_name: kafka-lag-monitor
    scrape_interval: 5s
    static_configs:
      - targets: ['localhost:9090']
```


---

## Final report

When all cells have cleared, the report includes:

1. **Per-group summary table** — one table per consumer group, with all metrics per topic
2. **Cross-group comparison** — for topics watched by multiple groups, a side-by-side comparison of duration, throughput, and P99 lag (useful for comparing two consumer implementations on the same topic)
3. **Partition breakdown** — ASCII bar chart per `(group, topic)` pair

### Sample cross-group comparison

```
  Topic: orders

  Consumer Group    Duration      Avg msg/s   Peak Lag    CatchUp msg/s   P99 Lag     Status
  ─────────────── ─────────────── ──────────── ──────────── ─────────────── ──────────── ──────────
  group-a         4m 13.875s      8,066/s     2,048,000   8,384/s         2,038,400   CLEARED
  group-b         6m 02.100s      5,653/s     2,048,000   5,902/s         2,040,100   CLEARED
```
This immediately shows `group-b` is ~30% slower than `group-a` on the same topic.

---

## File structure

```
kafka-lag-monitor/
├── monitor.js      Main entry point — state machine, multi-group poll loop
├── config.js       Watch matrix + all env var defaults
├── utils.js        Formatting, PercentileTracker, EMA, ASCII table
├── csv-writer.js   Append-mode CSV (includes consumer_group column)
├── prometheus.js   /metrics endpoint (consumer_group label on every metric)
├── dashboard.js    ANSI in-place live dashboard (rows grouped by consumer group)
└── package.json
```
---

## Sample terminal output

```
════════════════════════════════════════════════════════════════════════════════════════════
  KAFKA LAG MONITOR  v2
  Started      : 2024-01-15 09:00:00.000
  Brokers      : localhost:9092
  Group        : my-consumer-group
  Topics       : orders, payments, inventory
  Poll interval: 2000 ms
  Settling     : 10000 ms
════════════════════════════════════════════════════════════════════════════════════════════

2024-01-15 09:00:02.001  INFO   [MONITOR]                               Polling every 2000ms — waiting for lag…

2024-01-15 09:00:08.134  START  [orders]                                ⚡ Lag appeared  total=1,024  partitions=8
2024-01-15 09:00:10.201  PEAK   [orders]                                📈 New peak  lag=2,048,000  prev=1,024  producerRate≈512,000/s
2024-01-15 09:00:18.134  PROG   [orders]                                lag=1,800,000  max=2,048,000  elapsed=10.000s  consume≈124,000/s  produce≈512,000/s  slowest=p3(250,000)

2024-01-15 09:04:12.003  SETTL  [orders]                                ⏳ Lag hit 0 — settling for 10000ms before declaring cleared
2024-01-15 09:04:22.009  DONE   [orders]                                ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
2024-01-15 09:04:22.009  DONE   [orders]                                ✅ CLEARED
2024-01-15 09:04:22.009  DONE   [orders]                                   First lag at    : 2024-01-15 09:00:08.134
2024-01-15 09:04:22.009  DONE   [orders]                                   Peak lag        : 2,048,000  at 2024-01-15 09:00:10.201
2024-01-15 09:04:22.009  DONE   [orders]                                   Time to peak    : 2.067s
2024-01-15 09:04:22.009  DONE   [orders]                                   Lag cleared at  : 2024-01-15 09:04:22.009
2024-01-15 09:04:22.009  DONE   [orders]                                   Total duration  : 4m 13.875s
2024-01-15 09:04:22.009  DONE   [orders]                                   Avg throughput  : 8,066/s
2024-01-15 09:04:22.009  DONE   [orders]                                   Growth rate     : 990,804/s  (producer speed)
2024-01-15 09:04:22.009  DONE   [orders]                                   Catch-up rate   : 8,384/s  (consumer speed)
2024-01-15 09:04:22.009  DONE   [orders]                                   Poll count      : 127
2024-01-15 09:04:22.009  DONE   [orders]                                   Lag P50/P95/P99 : 1,024,000 / 1,987,200 / 2,038,400
2024-01-15 09:04:22.009  DONE   [orders]                                   Partitions      : 8  (slowest: p3 at 256,000 lag at peak)

════════════════════════════════════════════════════════════════════════════════════════════
  FINAL INGESTION REPORT  —  2024-01-15 09:06:00.000

  Topic      First Lag At                  Peak Lag    Cleared At                    Total Duration  Avg msg/s  Growth msg/s  CatchUp msg/s  P50 Lag      P99 Lag      Polls
  ────────── ─────────────────────────── ─────────── ─────────────────────────── ─────────────── ────────── ───────────── ────────────── ──────────── ──────────── ──────
  orders     2024-01-15 09:00:08.134     2,048,000  2024-01-15 09:04:22.009     4m 13.875s       8,066/s   990,804/s    8,384/s        1,024,000    2,038,400    127
  payments   2024-01-15 09:00:12.445     1,200,000  2024-01-15 09:02:55.889     2m 43.444s      7,331/s    600,000/s    9,124/s          612,000    1,188,000     82
  inventory  —                            —          —                            —               —          —            —             —            —             12
```

---
