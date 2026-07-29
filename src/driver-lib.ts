// Resolution of the native GizmoSQL ADBC driver library
// (libadbc_driver_gizmosql) that the driver manager loads.
//
// Order:
//   1. GIZMOSQL_DRIVER_LIB env var (must point at an existing file)
//   2. the package-local cache populated by scripts/download-driver.cjs
//      at npm postinstall (<pkg>/drivers/<version>/libadbc_driver_gizmosql.*)
//   3. a clear error explaining how to fix it

import * as fs from 'node:fs';
import path from 'node:path';

export interface DriverPlatform {
  /** Release-asset platform key, e.g. "macos_arm64" */
  platform: string;
  /** Shared-library file extension including the dot, e.g. ".dylib" */
  ext: string;
}

const PLATFORM_MAP: Record<string, DriverPlatform> = {
  'darwin-arm64': { platform: 'macos_arm64', ext: '.dylib' },
  'darwin-x64': { platform: 'macos_amd64', ext: '.dylib' },
  'linux-x64': { platform: 'linux_amd64', ext: '.so' },
  'linux-arm64': { platform: 'linux_arm64', ext: '.so' },
  'win32-x64': { platform: 'windows_amd64', ext: '.dll' },
  'win32-arm64': { platform: 'windows_arm64', ext: '.dll' },
};

/**
 * Map a Node.js platform/arch pair to the gizmosql-adbc release-asset
 * platform key and shared-library extension. Throws on unsupported
 * platforms.
 */
export function driverPlatform(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch
): DriverPlatform {
  const key = `${platform}-${arch}`;
  const info = PLATFORM_MAP[key];
  if (!info) {
    throw new Error(
      `Unsupported platform for the GizmoSQL ADBC driver: ${key}. ` +
        `Supported platforms: ${Object.keys(PLATFORM_MAP).join(', ')}. ` +
        `You can build the driver from source (make -C gizmosql-adbc/go lib, ` +
        `from https://github.com/gizmodata/gizmosql-adbc) and point the ` +
        `GIZMOSQL_DRIVER_LIB environment variable at the resulting library.`
    );
  }
  return info;
}

/** Package root (the directory containing package.json / driver-manifest.json). */
function packageRoot(): string {
  // This file lives at <pkg>/src/driver-lib.ts (tests) or
  // <pkg>/dist/driver-lib.js (built), so the package root is one up.
  return path.resolve(__dirname, '..');
}

/** Pinned driver version from driver-manifest.json. */
export function driverVersion(root: string = packageRoot()): string {
  const manifest = JSON.parse(
    fs.readFileSync(path.join(root, 'driver-manifest.json'), 'utf8')
  ) as { version: string };
  return manifest.version;
}

/**
 * Path where the postinstall downloader places the driver library for
 * this platform (whether or not it exists yet).
 */
export function cachedDriverPath(
  root: string = packageRoot(),
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch
): string {
  const { ext } = driverPlatform(platform, arch);
  return path.join(
    root,
    'drivers',
    driverVersion(root),
    `libadbc_driver_gizmosql${ext}`
  );
}

/**
 * Resolve the absolute path of the native GizmoSQL ADBC driver library.
 *
 * Checks GIZMOSQL_DRIVER_LIB first, then the package-local download
 * cache; throws with remediation instructions when neither is present.
 * The optional parameters exist for testing.
 */
export function resolveDriverLib(
  root: string = packageRoot(),
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch
): string {
  const override = env.GIZMOSQL_DRIVER_LIB;
  if (override) {
    if (!fs.existsSync(override)) {
      throw new Error(
        `GIZMOSQL_DRIVER_LIB is set to "${override}" but no file exists there.`
      );
    }
    return override;
  }

  const cached = cachedDriverPath(root, platform, arch);
  if (fs.existsSync(cached)) {
    return cached;
  }

  throw new Error(
    `GizmoSQL ADBC driver library not found (expected at ${cached}).\n` +
      `The library is normally downloaded automatically at install time by ` +
      `this package's postinstall script (scripts/download-driver.cjs) from ` +
      `the gizmodata/gizmosql-adbc GitHub release; that download appears to ` +
      `have failed or been skipped. To fix:\n` +
      `  - re-run the download: node node_modules/@gizmodata/gizmosql-client/scripts/download-driver.cjs\n` +
      `  - or set GIZMOSQL_DRIVER_LIB to the path of a libadbc_driver_gizmosql ` +
      `library you provide (e.g. for airgapped installs)\n` +
      `  - or build it from source: make -C gizmosql-adbc/go lib ` +
      `(https://github.com/gizmodata/gizmosql-adbc), then set GIZMOSQL_DRIVER_LIB.`
  );
}
