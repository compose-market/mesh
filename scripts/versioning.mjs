import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

const RELEASE_VERSION_PATTERN = /^v?(\d+)\.(\d+)(?:\.(\d+))?$/;
const GITHUB_API_VERSION = "2022-11-28";

function parseVersionParts(input) {
  const raw = typeof input === "string" ? input.trim() : "";
  const match = RELEASE_VERSION_PATTERN.exec(raw);
  if (!match) {
    throw new Error(`Invalid release version "${input}". Expected x.y or x.y.z.`);
  }

  const [, major, minor, patch = "0"] = match;
  return {
    major: Number.parseInt(major, 10),
    minor: Number.parseInt(minor, 10),
    patch: Number.parseInt(patch, 10),
  };
}

function readJson(filePath) {
  return JSON.parse(readFileSync(filePath, "utf-8"));
}

function writeJson(filePath, value) {
  writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf-8");
}

function ensureVersionString(input) {
  return normalizeReleaseVersion(input);
}

export function normalizeReleaseVersion(input) {
  const { major, minor, patch } = parseVersionParts(input);
  return `${major}.${minor}.${patch}`;
}

export function compareVersions(left, right) {
  const a = parseVersionParts(left);
  const b = parseVersionParts(right);

  if (a.major !== b.major) return Math.sign(a.major - b.major);
  if (a.minor !== b.minor) return Math.sign(a.minor - b.minor);
  return Math.sign(a.patch - b.patch);
}

export function incrementReleaseVersion(input) {
  const { major, minor } = parseVersionParts(input);
  const nextMinor = minor + 1;
  const carry = Math.floor(nextMinor / 10);
  return `${major + carry}.${nextMinor % 10}.0`;
}

export function computeReleaseVersion({ packageVersion, publishedVersions }) {
  const normalizedPackageVersion = normalizeReleaseVersion(packageVersion);
  const normalizedPublishedVersions = Array.from(
    new Set(
      (publishedVersions || []).map((value) => normalizeReleaseVersion(value)),
    ),
  ).sort(compareVersions);
  const publishedVersion = normalizedPublishedVersions.at(-1) ?? null;

  const releaseVersion = !publishedVersion || compareVersions(normalizedPackageVersion, publishedVersion) > 0
    ? normalizedPackageVersion
    : incrementReleaseVersion(publishedVersion);

  return {
    packageVersion: normalizedPackageVersion,
    publishedVersion,
    releaseVersion,
    source: releaseVersion === normalizedPackageVersion ? "package" : "auto-bump",
    tag: `v${releaseVersion}`,
  };
}

export function readPackageVersion(rootDir = process.cwd()) {
  const packagePath = path.join(rootDir, "package.json");
  const pkg = readJson(packagePath);
  if (!pkg.version) {
    throw new Error(`Missing version in ${packagePath}`);
  }
  return ensureVersionString(pkg.version);
}

export function resolveBuildVersion(rootDir = process.cwd()) {
  if (process.env.COMPOSE_MESH_BUILD_VERSION) {
    return ensureVersionString(process.env.COMPOSE_MESH_BUILD_VERSION);
  }

  return readPackageVersion(rootDir);
}

export function syncProjectVersion({
  rootDir = process.cwd(),
  version,
  enableUpdaterArtifacts = Boolean(process.env.TAURI_SIGNING_PRIVATE_KEY),
} = {}) {
  const normalizedVersion = ensureVersionString(version ?? resolveBuildVersion(rootDir));
  const packagePath = path.join(rootDir, "package.json");
  const tauriConfPath = path.join(rootDir, "tauri", "tauri.conf.json");
  const cargoTomlPath = path.join(rootDir, "tauri", "Cargo.toml");

  const changes = [];

  if (existsSync(packagePath)) {
    const pkg = readJson(packagePath);
    if (pkg.version !== normalizedVersion) {
      pkg.version = normalizedVersion;
      writeJson(packagePath, pkg);
      changes.push(`package.json -> ${normalizedVersion}`);
    }
  }

  if (existsSync(tauriConfPath)) {
    const conf = readJson(tauriConfPath);
    let dirty = false;

    if (conf.version !== normalizedVersion) {
      conf.version = normalizedVersion;
      dirty = true;
    }

    if (enableUpdaterArtifacts) {
      if (conf.bundle?.createUpdaterArtifacts !== "v1Compatible") {
        conf.bundle = {
          ...conf.bundle,
          createUpdaterArtifacts: "v1Compatible",
        };
        dirty = true;
      }
    } else if (conf.bundle?.createUpdaterArtifacts) {
      delete conf.bundle.createUpdaterArtifacts;
      dirty = true;
    }

    if (dirty) {
      writeJson(tauriConfPath, conf);
      changes.push(`tauri.conf.json -> ${normalizedVersion}`);
    }
  }

  if (existsSync(cargoTomlPath)) {
    const cargo = readFileSync(cargoTomlPath, "utf-8");
    const patched = cargo.replace(
      /^version\s*=\s*"[^"]*"/m,
      `version = "${normalizedVersion}"`,
    );
    if (patched !== cargo) {
      writeFileSync(cargoTomlPath, patched, "utf-8");
      changes.push(`Cargo.toml -> ${normalizedVersion}`);
    }
  }

  return {
    version: normalizedVersion,
    changes,
  };
}

function parseResolveReleaseArgs(argv) {
  const args = {
    owner: process.env.GITHUB_REPOSITORY_OWNER || "compose-market",
    repo: process.env.GITHUB_REPOSITORY?.split("/")[1] || "mesh",
    rootDir: process.cwd(),
  };

  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    const next = argv[index + 1];

    if (current === "--owner" && next) {
      args.owner = next;
      index += 1;
      continue;
    }

    if (current === "--repo" && next) {
      args.repo = next;
      index += 1;
      continue;
    }

    if (current === "--root" && next) {
      args.rootDir = path.resolve(next);
      index += 1;
    }
  }

  return args;
}

async function fetchGitHubPage(url, token) {
  const response = await fetch(url, {
    headers: {
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": GITHUB_API_VERSION,
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`GitHub API request failed (${response.status}) for ${url}: ${body}`);
  }

  return response.json();
}

async function listReleases({ owner, repo, token }) {
  const releases = [];
  let page = 1;

  while (true) {
    const url = `https://api.github.com/repos/${owner}/${repo}/releases?per_page=100&page=${page}`;
    const batch = await fetchGitHubPage(url, token);
    releases.push(...batch);

    if (batch.length < 100) {
      return releases;
    }

    page += 1;
  }
}

function collectPublishedVersions(releases) {
  return releases
    .filter((release) => !release.draft)
    .map((release) => release.tag_name)
    .filter(Boolean);
}

function formatOutputLines(values) {
  return Object.entries(values)
    .map(([key, value]) => `${key}=${value}`)
    .join("\n");
}

async function runResolveReleaseCli(argv) {
  const { owner, repo, rootDir } = parseResolveReleaseArgs(argv);
  const token = process.env.GITHUB_TOKEN || process.env.GH_TOKEN || "";
  const releases = await listReleases({ owner, repo, token });
  const result = computeReleaseVersion({
    packageVersion: readPackageVersion(rootDir),
    publishedVersions: collectPublishedVersions(releases),
  });

  const outputs = {
    version: result.releaseVersion,
    tag: result.tag,
    package_version: result.packageVersion,
    published_version: result.publishedVersion ?? "",
    source: result.source,
    release_name: `Compose Mesh ${result.tag}`,
    repository: `${owner}/${repo}`,
  };

  const lines = formatOutputLines(outputs);
  if (process.env.GITHUB_OUTPUT) {
    appendFileSync(process.env.GITHUB_OUTPUT, `${lines}\n`, "utf-8");
  }

  console.log(lines);
}

async function runCli(argv) {
  const [command, ...rest] = argv;

  if (command === "resolve-release") {
    await runResolveReleaseCli(rest);
    return;
  }

  if (command === "sync-project") {
    const { version, changes } = syncProjectVersion({
      version: rest[0],
    });

    console.log(
      changes.length === 0
        ? `[version-sync] already aligned at ${version}`
        : changes.map((change) => `[version-sync] ${change}`).join("\n"),
    );
    return;
  }

  throw new Error(`Unknown versioning command "${command}"`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  runCli(process.argv.slice(2)).catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
