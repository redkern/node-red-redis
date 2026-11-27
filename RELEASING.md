# npm Release

This package publishes only from `.github/workflows/release.yml`; do not run `npm publish` from a workstation (`publishConfig.provenance` makes a local publish fail). Pre-release tags (`v1.0.0-rc.1`) produce a GitHub pre-release with the packed tarball and are never published to npm.

## One-time npmjs setup

The npm package and the GitHub repository must be public for npm provenance. In npmjs package settings, add a **GitHub Actions Trusted Publisher** with these exact values:

- GitHub owner: `redkern`
- Repository: `node-red-redis`
- Workflow filename: `release.yml`
- Environment: `npm`

A Trusted Publisher can only be added to a package that already exists. For the very first publish (1.0.0) only: create a granular npm token with publish rights on `@redkern` and a short expiry, store it as the `NPM_BOOTSTRAP_TOKEN` secret of the `npm` environment, push the tag, then add the Trusted Publisher above, delete the secret and revoke the token. Later releases use OIDC only; never store a long-lived `NPM_TOKEN`. In GitHub, create the `npm` environment with required reviewers and protect `v*` tags so only release maintainers can create them. Enable Private Vulnerability Reporting in the repository security settings (see SECURITY.md).

## Release order

1. `@redkern/node-red-kit@1.1.0` is already on npm; the lockfile resolves that registry artifact.
2. Confirm `package.json`, `package-lock.json` and `CHANGELOG.md` carry the same stable version and the changelog entry is dated (`## 1.0.0 - YYYY-MM-DD`).
3. Push the reviewed commit to `main` and wait for CI: Node.js 22/24 x Node-RED 4.1/5, verify (generated editor, catalogs, help and icons up to date, lint, editor, requirement IDs, unit tests), pack check, Redis integration and the Flow Library validation (`node-red-dev validate`, gated by `scripts/check-scorecard.js`).
4. Run the acceptance suites of spec section 9 against the release commit and keep their output with the release review: `npm run test:load`, `scripts/start-chaos-cluster.sh && npm run test:chaos`, `sudo -E npm run test:pods`, `npm run test:soak -- --minutes 15`.
5. Create and push the matching tag, for example `v1.0.0`. The release workflow repeats the gates, runs a dependency audit and a smoke install of the packed tarball, then publishes through OIDC with provenance and creates the GitHub release.
6. Verify: `npm view @redkern/node-red-redis@1.0.0 dist.attestations`, then `npm audit signatures` in a project that installs it, and install the package in a clean Node-RED user directory.

The workflow rejects tag mismatches, a lockfile version mismatch, URL dependencies, a missing or undated changelog entry for a stable version and a repository URL that does not match the GitHub repository.
