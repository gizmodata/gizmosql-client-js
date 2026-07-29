#!/usr/bin/env node
// Postinstall downloader for the native GizmoSQL ADBC driver library.
//
// Downloads libadbc_driver_gizmosql-v<VER>-<platform>.tar.gz from the
// gizmodata/gizmosql-adbc GitHub release pinned in driver-manifest.json,
// verifies its SHA-256 against the manifest, and extracts just the
// shared library into <pkg>/drivers/<VER>/ (atomically: temp + rename).
//
// Plain CommonJS, Node stdlib only — this runs at npm postinstall,
// before any build step and before dependencies are guaranteed.
//
// This script must NEVER fail `npm install`: on any error it prints a
// warning with remediation steps and exits 0. resolveDriverLib() throws
// a clear error at runtime if the library is actually missing.
//
// Set GIZMOSQL_DRIVER_SKIP_DOWNLOAD=1 to skip entirely (airgapped
// installs use the GIZMOSQL_DRIVER_LIB env var instead).

'use strict';

const crypto = require('node:crypto');
const fs = require('node:fs');
const https = require('node:https');
const path = require('node:path');
const zlib = require('node:zlib');

const REPO = 'gizmodata/gizmosql-adbc';
const LIB_BASENAME = 'libadbc_driver_gizmosql';
const MAX_REDIRECTS = 5;

const PLATFORM_MAP = {
  'darwin-arm64': { platform: 'macos_arm64', ext: '.dylib' },
  'darwin-x64': { platform: 'macos_amd64', ext: '.dylib' },
  'linux-x64': { platform: 'linux_amd64', ext: '.so' },
  'linux-arm64': { platform: 'linux_arm64', ext: '.so' },
  'win32-x64': { platform: 'windows_amd64', ext: '.dll' },
  'win32-arm64': { platform: 'windows_arm64', ext: '.dll' },
};

function warn(message) {
  console.warn(
    `[@gizmodata/gizmosql-client] WARNING: ${message}\n` +
      `[@gizmodata/gizmosql-client] The GizmoSQL ADBC driver library was NOT installed. ` +
      `The client will not work until you either:\n` +
      `  - re-run this script: node ${path.relative(process.cwd(), __filename)}\n` +
      `  - or set GIZMOSQL_DRIVER_LIB to a ${LIB_BASENAME} library you provide\n` +
      `  - or build from source: make -C gizmosql-adbc/go lib ` +
      `(https://github.com/${REPO}), then set GIZMOSQL_DRIVER_LIB.`
  );
}

// GET with redirect following (GitHub releases redirect to S3/Azure).
function fetchBuffer(url, redirectsLeft) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { 'user-agent': '@gizmodata/gizmosql-client postinstall' } }, (res) => {
        const status = res.statusCode || 0;
        if (status >= 300 && status < 400 && res.headers.location) {
          res.resume();
          if (redirectsLeft <= 0) {
            reject(new Error(`too many redirects fetching ${url}`));
            return;
          }
          resolve(fetchBuffer(new URL(res.headers.location, url).toString(), redirectsLeft - 1));
          return;
        }
        if (status !== 200) {
          res.resume();
          reject(new Error(`HTTP ${status} fetching ${url}`));
          return;
        }
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => resolve(Buffer.concat(chunks)));
        res.on('error', reject);
      })
      .on('error', reject);
  });
}

// Minimal tar reader: returns the contents of the first regular-file
// entry whose basename matches `wantedBasename`.
function extractFromTar(tarBuffer, wantedBasename) {
  let offset = 0;
  while (offset + 512 <= tarBuffer.length) {
    const header = tarBuffer.subarray(offset, offset + 512);
    // Two consecutive zero blocks mark end-of-archive; a zero name byte
    // in the first header position is close enough for our purposes.
    if (header[0] === 0) break;
    const name = header.subarray(0, 100).toString('utf8').replace(/\0.*$/, '');
    const prefix = header.subarray(345, 500).toString('utf8').replace(/\0.*$/, '');
    const fullName = prefix ? `${prefix}/${name}` : name;
    const size = parseInt(header.subarray(124, 136).toString('utf8').replace(/\0.*$/, '').trim(), 8) || 0;
    const typeflag = String.fromCharCode(header[156]);
    const dataStart = offset + 512;
    if ((typeflag === '0' || typeflag === '\0' || typeflag === '') &&
        path.posix.basename(fullName) === wantedBasename) {
      return tarBuffer.subarray(dataStart, dataStart + size);
    }
    offset = dataStart + Math.ceil(size / 512) * 512;
  }
  throw new Error(`${wantedBasename} not found in archive`);
}

async function main() {
  if (process.env.GIZMOSQL_DRIVER_SKIP_DOWNLOAD === '1') {
    console.log('[@gizmodata/gizmosql-client] GIZMOSQL_DRIVER_SKIP_DOWNLOAD=1 — skipping driver download.');
    return;
  }

  const root = path.resolve(__dirname, '..');
  const manifest = JSON.parse(fs.readFileSync(path.join(root, 'driver-manifest.json'), 'utf8'));
  const version = manifest.version;

  const key = `${process.platform}-${process.arch}`;
  const info = PLATFORM_MAP[key];
  if (!info) {
    warn(`unsupported platform ${key}; no prebuilt GizmoSQL ADBC driver is available.`);
    return;
  }

  const libFile = `${LIB_BASENAME}${info.ext}`;
  const destDir = path.join(root, 'drivers', version);
  const destPath = path.join(destDir, libFile);
  if (fs.existsSync(destPath)) {
    console.log(`[@gizmodata/gizmosql-client] GizmoSQL ADBC driver already present: ${destPath}`);
    return;
  }

  const expectedSha = (manifest.sha256 || {})[info.platform];
  if (!expectedSha) {
    warn(`no pinned SHA-256 for platform ${info.platform} in driver-manifest.json.`);
    return;
  }

  const assetName = `${LIB_BASENAME}-v${version}-${info.platform}.tar.gz`;
  const url = `https://github.com/${REPO}/releases/download/v${version}/${assetName}`;
  console.log(`[@gizmodata/gizmosql-client] Downloading GizmoSQL ADBC driver ${version} (${info.platform})...`);
  console.log(`[@gizmodata/gizmosql-client]   ${url}`);

  const tarGz = await fetchBuffer(url, MAX_REDIRECTS);

  const actualSha = crypto.createHash('sha256').update(tarGz).digest('hex');
  if (actualSha !== expectedSha.toLowerCase()) {
    throw new Error(
      `SHA-256 mismatch for ${assetName}: expected ${expectedSha}, got ${actualSha}`
    );
  }

  const lib = extractFromTar(zlib.gunzipSync(tarGz), libFile);

  fs.mkdirSync(destDir, { recursive: true });
  const tmpPath = path.join(
    destDir,
    `.${libFile}.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`
  );
  try {
    fs.writeFileSync(tmpPath, lib, { mode: 0o755 });
    fs.renameSync(tmpPath, destPath); // atomic within the same directory
  } catch (err) {
    try { fs.unlinkSync(tmpPath); } catch { /* best effort */ }
    throw err;
  }

  console.log(`[@gizmodata/gizmosql-client] Installed GizmoSQL ADBC driver: ${destPath}`);
}

main().catch((err) => {
  warn(`driver download failed: ${err && err.message ? err.message : err}`);
  process.exit(0); // never break npm install
});
