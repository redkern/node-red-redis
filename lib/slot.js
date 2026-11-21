'use strict';

const TABLE = new Uint16Array(256);
for (let index = 0; index < 256; index += 1) {
  let crc = index << 8;
  for (let bit = 0; bit < 8; bit += 1) {
    crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
  }
  TABLE[index] = crc;
}

function crc16(buffer) {
  let crc = 0;
  for (const byte of buffer) crc = ((crc << 8) & 0xffff) ^ TABLE[((crc >> 8) ^ byte) & 0xff];
  return crc;
}

/**
 * Redis Cluster hash slot of a key, honouring {hash tags}.
 * @param {string} key
 * @returns {number}
 */
function keySlot(key) {
  let value = String(key);
  const start = value.indexOf('{');
  if (start !== -1) {
    const end = value.indexOf('}', start + 1);
    if (end > start + 1) value = value.slice(start + 1, end);
  }
  return crc16(Buffer.from(value)) % 16384;
}

module.exports = { keySlot, crc16 };
