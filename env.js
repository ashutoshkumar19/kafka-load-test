'use strict';

require('dotenv').config();

/**
 * Multi-environment loader.
 *
 * Define per-environment Kafka settings once in .env using prefixes:
 *   LOCAL_KAFKA_BROKERS, DEV_KAFKA_WATCHES, PERF_KAFKA_SSL, …
 *
 * Set ENV=local|dev|test|perf to select the active profile.
 * Unprefixed KAFKA_* vars (if set) always win as manual overrides.
 */

const PROFILE_KEYS = [
  'KAFKA_BROKERS',
  'KAFKA_SSL',
  'KAFKA_WATCHES',
  'KAFKA_SASL_MECHANISM',
  'KAFKA_SASL_USERNAME',
  'KAFKA_SASL_PASSWORD',
];

const activeEnv = (process.env.ENV || 'local').trim().toLowerCase();
const prefix    = `${activeEnv.toUpperCase()}_`;

function applyProfile() {
  const applied = {};
  const missing = [];

  for (const key of PROFILE_KEYS) {
    if (process.env[key] !== undefined && process.env[key] !== '') continue;

    const profileVal = process.env[`${prefix}${key}`];
    if (profileVal !== undefined && profileVal !== '') {
      process.env[key] = profileVal;
      applied[key] = profileVal;
    } else if (key === 'KAFKA_BROKERS' || key === 'KAFKA_WATCHES') {
      missing.push(`${prefix}${key}`);
    }
  }

  if (missing.length > 0) {
    throw new Error(
      `ENV="${activeEnv}" is missing required profile vars: ${missing.join(', ')}. ` +
      `Define them in .env or set ${missing.map(k => k.replace(prefix, '')).join(', ')} directly.`
    );
  }

  return applied;
}

const applied = applyProfile();

module.exports = {
  activeEnv,
  prefix,
  applied,
};
