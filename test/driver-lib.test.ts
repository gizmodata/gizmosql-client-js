import * as fs from 'node:fs';
import * as os from 'node:os';
import path from 'node:path';
import {
  cachedDriverPath,
  driverPlatform,
  driverVersion,
  resolveDriverLib,
} from '../src/driver-lib';

describe('driverPlatform', () => {
  it.each([
    ['darwin', 'arm64', 'macos_arm64', '.dylib'],
    ['darwin', 'x64', 'macos_amd64', '.dylib'],
    ['linux', 'x64', 'linux_amd64', '.so'],
    ['linux', 'arm64', 'linux_arm64', '.so'],
    ['win32', 'x64', 'windows_amd64', '.dll'],
    ['win32', 'arm64', 'windows_arm64', '.dll'],
  ])('maps %s-%s to %s (%s)', (platform, arch, expected, ext) => {
    const info = driverPlatform(platform as NodeJS.Platform, arch);
    expect(info.platform).toBe(expected);
    expect(info.ext).toBe(ext);
  });

  it('throws a clear error for unsupported platforms', () => {
    expect(() => driverPlatform('freebsd' as NodeJS.Platform, 'x64')).toThrow(
      /Unsupported platform.*freebsd-x64.*GIZMOSQL_DRIVER_LIB/s
    );
    expect(() => driverPlatform('linux' as NodeJS.Platform, 'ia32')).toThrow(
      /Unsupported platform.*linux-ia32/s
    );
  });

  it('mentions building from source in the unsupported-platform error', () => {
    expect(() => driverPlatform('aix' as NodeJS.Platform, 'ppc64')).toThrow(
      /make -C gizmosql-adbc\/go lib/
    );
  });
});

describe('resolver', () => {
  let tmpRoot: string;

  beforeEach(() => {
    tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'gizmosql-driver-lib-test-'));
    fs.writeFileSync(
      path.join(tmpRoot, 'driver-manifest.json'),
      JSON.stringify({ version: '9.9.9', sha256: {} })
    );
  });

  afterEach(() => {
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  });

  it('driverVersion reads the pinned version from driver-manifest.json', () => {
    expect(driverVersion(tmpRoot)).toBe('9.9.9');
  });

  it('cachedDriverPath points at drivers/<version>/libadbc_driver_gizmosql.<ext>', () => {
    const p = cachedDriverPath(tmpRoot, 'linux' as NodeJS.Platform, 'x64');
    expect(p).toBe(path.join(tmpRoot, 'drivers', '9.9.9', 'libadbc_driver_gizmosql.so'));
  });

  it('prefers GIZMOSQL_DRIVER_LIB when it points at an existing file', () => {
    const override = path.join(tmpRoot, 'custom.dylib');
    fs.writeFileSync(override, 'x');
    // Even with a cached copy present, the env var wins.
    const cached = cachedDriverPath(tmpRoot, 'darwin' as NodeJS.Platform, 'arm64');
    fs.mkdirSync(path.dirname(cached), { recursive: true });
    fs.writeFileSync(cached, 'y');
    expect(
      resolveDriverLib(tmpRoot, { GIZMOSQL_DRIVER_LIB: override }, 'darwin' as NodeJS.Platform, 'arm64')
    ).toBe(override);
  });

  it('throws when GIZMOSQL_DRIVER_LIB points at a missing file', () => {
    const missing = path.join(tmpRoot, 'nope.dylib');
    expect(() =>
      resolveDriverLib(tmpRoot, { GIZMOSQL_DRIVER_LIB: missing }, 'darwin' as NodeJS.Platform, 'arm64')
    ).toThrow(/GIZMOSQL_DRIVER_LIB is set to .*nope\.dylib.* no file exists/);
  });

  it('falls back to the download cache when the env var is unset', () => {
    const cached = cachedDriverPath(tmpRoot, 'linux' as NodeJS.Platform, 'arm64');
    fs.mkdirSync(path.dirname(cached), { recursive: true });
    fs.writeFileSync(cached, 'lib');
    expect(resolveDriverLib(tmpRoot, {}, 'linux' as NodeJS.Platform, 'arm64')).toBe(cached);
  });

  it('throws with remediation steps when nothing is found', () => {
    expect(() => resolveDriverLib(tmpRoot, {}, 'win32' as NodeJS.Platform, 'x64')).toThrow(
      /not found.*postinstall.*GIZMOSQL_DRIVER_LIB.*make -C gizmosql-adbc\/go lib/s
    );
  });
});
