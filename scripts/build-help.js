'use strict';

// Writes nodes/locales/en-US/redkern-redis.html from scripts/help.en-US.js.
// Run: node scripts/build-help.js [--check]

const fs = require('node:fs');
const path = require('node:path');
const help = require('./help.en-US.js');

const README = 'https://github.com/redkern/node-red-redis#';

function props(list) {
  const rows = list.map(([name, type, text, optional]) => [
    `    <dt${optional ? ' class="optional"' : ''}>${name} <span class="property-type">${type}</span></dt>`,
    `    <dd>${text}</dd>`
  ].join('\n'));
  return ['<dl class="message-properties">', ...rows, '</dl>'].join('\n');
}

function render(type, entry) {
  const parts = [`<script type="text/html" data-help-name="${type}">`, `<p>${entry.summary}</p>`];
  if (entry.inputs) parts.push('<h3>Inputs</h3>', props(entry.inputs));
  if (entry.outputs) {
    parts.push('<h3>Outputs</h3>');
    if (entry.outputs.length === 1) parts.push(props(entry.outputs[0].props));
    else {
      parts.push('<ol class="node-ports">');
      for (const port of entry.outputs) parts.push(`<li>${port.label}`, props(port.props), '</li>');
      parts.push('</ol>');
    }
  }
  parts.push('<h3>Details</h3>', ...entry.details.map((text) => `<p>${text}</p>`));
  parts.push('<h3>References</h3>', `<ul><li><a href="${README}${entry.ref}" target="_blank" rel="noopener noreferrer">README: ${entry.ref}</a></li></ul>`);
  parts.push('</script>', '');
  return parts.join('\n');
}

// Node-RED reads help of a node set from locales/<lang>/<set>.html, so all
// help blocks go into one file, one <script data-help-name> per node type.
const file = path.join(__dirname, '..', 'nodes', 'locales', 'en-US', 'redkern-redis.html');
const content = Object.entries(help).map(([type, entry]) => render(type, entry)).join('\n');
if (process.argv.includes('--check')) {
  if (!fs.existsSync(file) || fs.readFileSync(file, 'utf8') !== content) {
    console.error('stale help: run node scripts/build-help.js');
    process.exit(1);
  }
} else {
  fs.writeFileSync(file, content);
}
