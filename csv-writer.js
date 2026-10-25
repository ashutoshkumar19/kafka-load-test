'use strict';

const fs   = require('fs');
const path = require('path');

const HEADER = [
  'timestamp',
  'consumer_group',
  'topic',
  'poll_count',
  'partition_count',
  'total_lag',
  'lag_delta',         // change vs previous poll (+ve = growing, -ve = shrinking)
  'high_delta',        // log-end offset movement since last poll (producer activity)
  'committed_delta',   // committed offset movement since last poll (consumer activity)
  'producer_rate_mps', // estimated msgs/s being produced
  'consumer_rate_mps', // estimated msgs/s being consumed
  'phase',             // IDLE | GROWING | CLEARING | SETTLING
].join(',');

class CsvWriter {
  /**
   * @param {string} filePath   Destination CSV file (created if absent)
   * @param {boolean} enabled   Pass false to create a no-op writer
   */
  constructor(filePath, enabled = true) {
    this._enabled = enabled;
    if (!enabled) return;

    this._path = path.resolve(filePath);
    fs.mkdirSync(path.dirname(this._path), { recursive: true });

    // Write header only if the file does not already exist
    const exists = fs.existsSync(this._path);
    this._fd = fs.openSync(this._path, 'a');
    if (!exists) {
      fs.writeSync(this._fd, HEADER + '\n');
    }
  }

  /**
   * Append one row per topic per poll.
   *
   * @param {Object} opts
   * @param {string}  opts.timestamp
   * @param {string}  opts.consumerGroup
   * @param {string}  opts.topic
   * @param {number}  opts.pollCount
   * @param {number}  opts.partitionCount
   * @param {number}  opts.totalLag
   * @param {number}  opts.lagDelta
   * @param {number}  opts.highDelta
   * @param {number}  opts.committedDelta
   * @param {number|null} opts.producerRate
   * @param {number|null} opts.consumerRate
   * @param {string}  opts.phase
   */
  write(opts) {
    if (!this._enabled) return;
    const row = [
      opts.timestamp,
      opts.consumerGroup,
      opts.topic,
      opts.pollCount,
      opts.partitionCount,
      opts.totalLag,
      opts.lagDelta,
      opts.highDelta,
      opts.committedDelta,
      opts.producerRate != null ? opts.producerRate.toFixed(1) : '',
      opts.consumerRate != null ? opts.consumerRate.toFixed(1) : '',
      opts.phase,
    ].join(',') + '\n';

    fs.writeSync(this._fd, row);
  }

  close() {
    if (!this._enabled || this._fd == null) return;
    try { fs.closeSync(this._fd); } catch (_) {}
  }

  get path() {
    return this._enabled ? this._path : null;
  }
}

module.exports = CsvWriter;