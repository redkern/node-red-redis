'use strict';

// Loads the editor definitions from editor/<type>.html (one node per file)
// in a sandbox with a stub RED, so CI checks can inspect them without a
// browser. The shipped nodes/redkern-redis.html is their concatenation.

const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const NODES_DIR = path.join(__dirname, '..', 'nodes');

const EDITOR_DIR = path.join(__dirname, '..', 'editor');

function catalogFor() {
  return JSON.parse(fs.readFileSync(path.join(NODES_DIR, 'locales', 'en-US', 'redkern-redis.json'), 'utf8'));
}

function lookup(catalog, key) {
  return key.split('.').reduce((value, part) => (value && typeof value === 'object' ? value[part] : undefined), catalog);
}

function loadDefinitions() {
  const definitions = {};
  for (const file of fs.readdirSync(EDITOR_DIR).filter((name) => name.endsWith('.html')).sort()) {
    const source = fs.readFileSync(path.join(EDITOR_DIR, file), 'utf8');
    const match = source.match(/<script type="text\/javascript">([\s\S]*?)<\/script>/);
    if (!match) throw new Error(`${file}: no editor script`);
    const type = file.replace(/\.html$/, '');
    const catalog = catalogFor();
    const translate = (key) => {
      const value = lookup(catalog, key);
      if (typeof value !== 'string') throw new Error(`${file}: missing catalog key ${key}`);
      return value;
    };
    const registered = [];
    const RED = {
      nodes: { registerType: (name, definition) => registered.push([name, definition]) },
      validators: { number: () => () => true, regex: () => () => true },
      _: translate
    };
    vm.runInNewContext(match[1], { RED, $: () => ({ prop: () => false }), console });
    for (const [name, definition] of registered) {
      definition._ = translate;
      definitions[name] = { file, type, source, script: match[1], definition, catalog, translate };
    }
  }
  return definitions;
}

module.exports = { loadDefinitions, lookup, NODES_DIR, EDITOR_DIR };
