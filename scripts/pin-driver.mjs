#!/usr/bin/env node
// Re-pins driver-manifest.json to a gizmosql-adbc release: downloads the
// six platform tarballs for the given version, computes their SHA-256s,
// and rewrites the manifest.
//
//   node scripts/pin-driver.mjs 2.0.10
//
// Run this when bumping the native driver, then commit the manifest.

import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO = 'gizmodata/gizmosql-adbc';
const LIB_BASENAME = 'libadbc_driver_gizmosql';
const PLATFORMS = [
  'macos_arm64',
  'macos_amd64',
  'linux_amd64',
  'linux_arm64',
  'windows_amd64',
  'windows_arm64',
];

const version = process.argv[2]?.replace(/^v/, '');
if (!version) {
  console.error('usage: node scripts/pin-driver.mjs <version>   (e.g. 2.0.10)');
  process.exit(2);
}

const manifestPath = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'driver-manifest.json');
const sha256 = {};
for (const platform of PLATFORMS) {
  const asset = `${LIB_BASENAME}-v${version}-${platform}.tar.gz`;
  const url = `https://github.com/${REPO}/releases/download/v${version}/${asset}`;
  const response = await fetch(url);
  if (!response.ok) {
    console.error(`${url}: HTTP ${response.status}`);
    process.exit(1);
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  sha256[platform] = createHash('sha256').update(bytes).digest('hex');
  console.log(`${platform.padEnd(14)} ${sha256[platform]}  (${bytes.length} bytes)`);
}

const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
writeFileSync(manifestPath, JSON.stringify({ ...manifest, version, sha256 }, null, 2) + '\n');
console.log(`driver-manifest.json pinned to v${version}`);
