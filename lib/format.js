'use strict';

/**
 * Compact number for status texts: 950 -> "950", 1234 -> "1.2k", 3400000 -> "3.4M".
 * @param {number} value
 * @returns {string}
 */
function compact(value) {
  const number = Number(value) || 0;
  const abs = Math.abs(number);
  if (abs < 1000) return String(Math.round(number * 10) / 10);
  const units = [['G', 1e9], ['M', 1e6], ['k', 1e3]];
  for (const [unit, size] of units) {
    if (abs >= size) {
      const scaled = number / size;
      const text = Math.abs(scaled) >= 100 ? String(Math.round(scaled)) : String(Math.round(scaled * 10) / 10);
      return `${text}${unit}`;
    }
  }
  return String(number);
}

/**
 * Converts a flat Redis reply ([k1, v1, k2, v2]) to an object.
 * @param {unknown[]} flat
 * @returns {Record<string, unknown>}
 */
function flatToObject(flat) {
  const result = {};
  if (!Array.isArray(flat)) return result;
  for (let index = 0; index + 1 < flat.length; index += 2) {
    result[String(flat[index])] = flat[index + 1];
  }
  return result;
}

/**
 * Decodes stream entry fields written by stream-out. Compound values (objects
 * and arrays) were serialised to JSON and are restored; scalar strings such as
 * "true", "42" or "null" stay strings to preserve Redis field semantics.
 * @param {unknown[]} flat
 * @returns {Record<string, unknown>}
 */
function decodeFields(flat) {
  const result = flatToObject(flat);
  for (const [field, value] of Object.entries(result)) {
    if (typeof value !== 'string' || !/^\s*[[{]/.test(value)) continue;
    try {
      result[field] = JSON.parse(value);
    } catch {
      // A value that only looks like JSON is still a valid Redis string.
    }
  }
  return result;
}

/**
 * Encodes a flat object to XADD field arguments. Format is unchanged from the
 * legacy palette: objects and arrays as JSON, everything else via String().
 * @param {unknown} payload
 * @returns {string[]}
 */
function encodeFields(payload) {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload) || Buffer.isBuffer(payload)) {
    throw codeError('PAYLOAD_INVALID', 'msg.payload must be a flat object (field -> value) for XADD');
  }
  const fields = [];
  for (const [field, value] of Object.entries(payload)) {
    if (value === undefined) continue;
    fields.push(field, value !== null && typeof value === 'object' && !Buffer.isBuffer(value) ? JSON.stringify(value) : String(value));
  }
  if (fields.length === 0) throw codeError('PAYLOAD_EMPTY', 'msg.payload must contain at least one field for XADD');
  return fields;
}

/**
 * Splits "a,b , c" into ["a","b","c"].
 * @param {unknown} value
 * @returns {string[]}
 */
function splitList(value) {
  if (Array.isArray(value)) return value.map((item) => String(item).trim()).filter(Boolean);
  if (typeof value !== 'string') return [];
  return value.split(',').map((item) => item.trim()).filter(Boolean);
}

/**
 * Error with a stable code. Messages never include values from msg.
 * @param {string} code
 * @param {string} message
 * @returns {Error & { code: string }}
 */
function codeError(code, message) {
  return Object.assign(new Error(message), { code });
}

/**
 * Merges metadata into msg.redkern.redis without touching other palettes' keys.
 * @param {Record<string, any>} msg
 * @param {Record<string, unknown>} values
 */
function setMeta(msg, values) {
  if (!msg.redkern || typeof msg.redkern !== 'object' || Array.isArray(msg.redkern)) msg.redkern = {};
  const current = msg.redkern.redis && typeof msg.redkern.redis === 'object' && !Array.isArray(msg.redkern.redis)
    ? msg.redkern.redis
    : {};
  msg.redkern.redis = { ...current, ...values };
  return msg.redkern.redis;
}

/**
 * Replaces msg.redkern.redis (used by source nodes that start a new message).
 * @param {Record<string, any>} msg
 * @param {Record<string, unknown>} values
 */
function replaceMeta(msg, values) {
  if (!msg.redkern || typeof msg.redkern !== 'object' || Array.isArray(msg.redkern)) msg.redkern = {};
  msg.redkern.redis = { ...values };
  return msg.redkern.redis;
}

module.exports = { compact, flatToObject, decodeFields, encodeFields, splitList, codeError, setMeta, replaceMeta };
