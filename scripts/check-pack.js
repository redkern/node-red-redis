'use strict';

// Pack check (kit standard 8, rule 1): only whitelisted files, no tests,
// reports or env files, public scoped access, a repository URL that matches
// provenance, and node-red only as a devDependency.

const { execFileSync } = require('node:child_process');
const pkg = require('../package.json');

const problems = [];
const [result] = JSON.parse(execFileSync('npm', ['pack', '--dry-run', '--json'], { encoding: 'utf8' }));
const allowed = /^(lib|nodes|examples)\/|^(package\.json|README\.md|CHANGELOG\.md|SECURITY\.md|LICENSE)$/;
for (const { path } of result.files) {
  if (!allowed.test(path)) problems.push(`unexpected file in tarball: ${path}`);
  if (/ \d+\.\w+$/.test(path)) problems.push(`duplicate-looking file: ${path}`);
  if (/(^|\/)(test|tests|\.github|docs)\//.test(path) || /(^|\/)\.env/.test(path)) problems.push(`forbidden file: ${path}`);
}
if (pkg.publishConfig?.access !== 'public') problems.push('publishConfig.access must be public');
if (pkg.repository?.url !== 'git+https://github.com/redkern/node-red-redis.git') problems.push('repository.url must match the GitHub repo');
if (pkg.dependencies?.['node-red'] || pkg.peerDependencies?.['node-red']) problems.push('node-red must stay a devDependency');
if (!pkg.keywords.includes('node-red')) problems.push('keyword node-red is required');
if (pkg['node-red']?.version !== '>=4.1.0 <6.0.0') problems.push('node-red.version must be ">=4.1.0 <6.0.0"');
if (pkg.engines?.node !== '>=22') problems.push('engines.node must be ">=22"');
if (Object.values(pkg.dependencies).some((range) => /^(https?:|git|file:)/.test(range))) problems.push('URL dependencies are not allowed');
const changelog = require('node:fs').readFileSync(require('node:path').join(__dirname, '..', 'CHANGELOG.md'), 'utf8');
if (!changelog.includes(`## ${pkg.version} `)) problems.push(`CHANGELOG has no entry for ${pkg.version}`);

if (problems.length) {
  console.error(problems.join('\n'));
  process.exit(1);
}
console.log(`check-pack: ${result.files.length} files, ${result.size} bytes`);
