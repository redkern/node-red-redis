'use strict';

// Editor standard check (kit standard 8.1, RDS-UI-1). Fails on:
// wrong colour or category, missing or non-white 40x60 icon, missing or long
// paletteLabel, port label count != port count, missing help sections,
// unknown catalog keys, editor code outside an IIFE.

const fs = require('node:fs');
const path = require('node:path');
const { loadDefinitions, lookup, NODES_DIR } = require('./editor-defs.js');

const COLOR = '#F4B3A1';
const CATEGORY = 'redkern redis';
const problems = [];
const fail = (type, message) => problems.push(`${type}: ${message}`);

const definitions = loadDefinitions();
const EXPECTED = [
  'redkern-redis-config', 'redkern-redis-command', 'redkern-redis-multi', 'redkern-redis-scan', 'redkern-redis-subscribe',
  'redkern-redis-list-in', 'redkern-redis-stream-in', 'redkern-redis-stream-out', 'redkern-redis-stream-ack',
  'redkern-redis-stream-claim', 'redkern-redis-stream-gc', 'redkern-redis-stream-metrics', 'redkern-redis-stream-control'
];
for (const type of EXPECTED) {
  if (!definitions[type]) fail(type, 'no editor definition');
}
// Every type registered at runtime has an editor definition and vice versa.
const runtimeTypes = [];
require('../nodes/redkern-redis.js')({
  nodes: { createNode() {}, registerType: (type) => runtimeTypes.push(type), getNode() {} },
  settings: { adminAuth: {} },
  events: { on() {}, emit() {}, removeListener() {} },
  log: { warn() {}, info() {}, error() {}, debug() {} },
  httpAdmin: { get() {}, post() {} },
  auth: { needsPermission: () => () => {} },
  util: { generateId: () => 'x' },
  hooks: { add() {}, remove() {} }
});
for (const type of runtimeTypes) if (!definitions[type]) fail(type, 'registered at runtime but has no editor definition');
for (const type of Object.keys(definitions)) if (!runtimeTypes.includes(type)) fail(type, 'editor definition without runtime type');
const helpSource = fs.readFileSync(path.join(NODES_DIR, 'locales', 'en-US', 'redkern-redis.html'), 'utf8');

for (const [type, { source, script, definition, catalog }] of Object.entries(definitions)) {
  if (!/^\s*\(function \(\) \{[\s\S]*\}\)\(\);\s*$/.test(script)) fail(type, 'editor code must be wrapped in an IIFE');
  if (definition.color !== COLOR) fail(type, `colour ${definition.color} is not ${COLOR}`);
  const isConfig = definition.category === 'config';
  if (!isConfig && definition.category !== CATEGORY) fail(type, `category must be "${CATEGORY}"`);

  const paletteLabel = typeof definition.paletteLabel === 'function' ? definition.paletteLabel.call(definition) : definition.paletteLabel;
  if (!paletteLabel || paletteLabel.length > 14 || paletteLabel !== paletteLabel.toLowerCase()) fail(type, `paletteLabel "${paletteLabel}" must be lower case and at most 14 characters`);

  if (!isConfig) {
    const icon = path.join(NODES_DIR, 'icons', definition.icon || '');
    if (!definition.icon || !fs.existsSync(icon)) fail(type, `icon ${definition.icon} missing`);
    else {
      const svg = fs.readFileSync(icon, 'utf8');
      if (!/viewBox="0 0 40 60"/.test(svg)) fail(type, 'icon must have viewBox 0 0 40 60');
      const colours = [...svg.matchAll(/(?:fill|stroke)="([^"]+)"/g)].map((m) => m[1].toLowerCase());
      if (colours.some((colour) => !['none', '#fff', '#ffffff', 'white'].includes(colour))) fail(type, 'icon must use white only');
    }
    const node = { ...definition, ...Object.fromEntries(Object.entries(definition.defaults).map(([key, value]) => [key, value.value])) };
    const inputs = definition.inputs || 0;
    const outputCounts = definition.defaults.outputs ? [definition.outputs, 1, 2].filter((count, index, all) => all.indexOf(count) === index && (count <= 1 || definition.defaults.errorOutput)) : [definition.outputs || 0];
    if (inputs > 0) {
      const label = typeof definition.inputLabels === 'function' ? definition.inputLabels.call(node, 0) : definition.inputLabels;
      if (!label) fail(type, 'input label missing');
    }
    for (const outputs of outputCounts) {
      for (let index = 0; index < outputs; index += 1) {
        const label = typeof definition.outputLabels === 'function' ? definition.outputLabels.call(node, index) : definition.outputLabels?.[index];
        if (!label) fail(type, `output ${index} label missing`);
        if (definition.defaults.errorOutput && outputs === 2 && index === 1 && label !== 'error') fail(type, 'error output must be last and labelled error');
      }
    }
    if (typeof definition.labelStyle !== 'function') fail(type, 'default labels must be italic (labelStyle)');
  }

  for (const [, key] of source.matchAll(/data-i18n="(?:\[[a-z]+\])?([^"]+)"/g)) {
    if (typeof lookup(catalog, key) !== 'string') fail(type, `catalog key ${key} missing`);
  }
  for (const [, key] of script.matchAll(/_\("([^"]+)"\)/g)) {
    if (typeof lookup(catalog, key) !== 'string') fail(type, `catalog key ${key} missing`);
  }

  const block = helpSource.match(new RegExp(`<script type="text/html" data-help-name="${type}">([\\s\\S]*?)</script>`));
  if (!block) fail(type, 'help missing');
  else {
    const help = block[1];
    const required = ['Details', 'References'];
    if (!isConfig && definition.outputs) required.push('Outputs');
    for (const section of required) if (!help.includes(`<h3>${section}</h3>`)) fail(type, `help section ${section} missing`);
  }
}

if (problems.length) {
  console.error(problems.join('\n'));
  process.exit(1);
}
console.log(`check-editor: ${Object.keys(definitions).length} node types ok`);
