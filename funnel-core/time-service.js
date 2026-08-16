'use strict';

const { isDateOnly, cloneCanonicalRow } = require('./canonical-contract');

const TIME_ENGINE_VERSION = 'v1';

function validateTimeZone(timeZone) {
  if (typeof timeZone !== 'string' || !timeZone.trim()) {
    throw new Error('source_timezone is required; server UTC is not a silent fallback');
  }
  try {
    new Intl.DateTimeFormat('en-US', { timeZone }).format(new Date(0));
    return timeZone;
  } catch (error) {
    throw new Error(`Invalid IANA timezone: ${timeZone}`);
  }
}

function businessDateFromTimestamp(timestamp, sourceTimezone) {
  validateTimeZone(sourceTimezone);
  const date = timestamp instanceof Date ? timestamp : new Date(timestamp);
  if (Number.isNaN(date.getTime())) throw new Error('provider timestamp must be a valid timestamp');

  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: sourceTimezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(date);

  const values = Object.fromEntries(parts.filter((part) => part.type !== 'literal').map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function normalizeBusinessDate({ providerDate = null, providerTimestamp = null, sourceTimezone }) {
  validateTimeZone(sourceTimezone);

  if (providerDate !== null && providerDate !== undefined) {
    if (!isDateOnly(providerDate)) throw new Error('providerDate must be YYYY-MM-DD');
    return {
      source_timezone: sourceTimezone,
      business_date: providerDate,
      time_engine_version: TIME_ENGINE_VERSION
    };
  }

  if (providerTimestamp === null || providerTimestamp === undefined) {
    throw new Error('providerDate or providerTimestamp is required');
  }

  return {
    source_timezone: sourceTimezone,
    business_date: businessDateFromTimestamp(providerTimestamp, sourceTimezone),
    time_engine_version: TIME_ENGINE_VERSION
  };
}

function applyBusinessDate(row, context) {
  const normalized = normalizeBusinessDate(context);
  const next = cloneCanonicalRow(row);
  next.identity.date = normalized.business_date;
  next.time = normalized;
  return next;
}

module.exports = Object.freeze({
  TIME_ENGINE_VERSION,
  validateTimeZone,
  businessDateFromTimestamp,
  normalizeBusinessDate,
  applyBusinessDate
});
