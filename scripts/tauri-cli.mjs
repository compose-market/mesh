import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { syncProjectVersion } from "./versioning.mjs";

/**
 * Sync version from package.json → tauri.conf.json + Cargo.toml.
 * package.json is the SINGLE SOURCE OF TRUTH for the app version.
 */
function syncVersion() {
  try {
    const { version, changes } = syncProjectVersion();
    if (changes.length === 0) {
      console.log(`[version-sync] already aligned at ${version}`);
      return;
    }

    for (const change of changes) {
      console.log(`[version-sync] ${change}`);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[version-sync] ${message}`);
    process.exit(1);
  }
}

function withCargoPath(env) {
  const nextEnv = { ...env };
  const homeDir = nextEnv.HOME || nextEnv.USERPROFILE;
  if (!homeDir) {
    return nextEnv;
  }

  const cargoBin = path.join(homeDir, ".cargo", "bin");
  const pathParts = (nextEnv.PATH || "").split(path.delimiter).filter(Boolean);

  if (!pathParts.includes(cargoBin)) {
    nextEnv.PATH = [cargoBin, ...pathParts].join(path.delimiter);
  }

  return nextEnv;
}

function hasCargo(env) {
  const check = spawnSync("cargo", ["--version"], {
    env,
    stdio: "ignore",
  });

  return check.status === 0;
}

function resolveCargoHint(env) {
  const homeDir = env.HOME || env.USERPROFILE;
  if (!homeDir) {
    return null;
  }

  const cargoPath = path.join(homeDir, ".cargo", "bin", process.platform === "win32" ? "cargo.exe" : "cargo");
  return existsSync(cargoPath) ? cargoPath : null;
}

function failMissingCargo(env) {
  const cargoHint = resolveCargoHint(env);
  const hintLines = cargoHint
    ? [
        `Detected cargo at ${cargoHint}, but it is not executable from this environment.`,
        "Verify permissions and shell startup files, then retry.",
      ]
    : [
        "Rust toolchain is not available.",
        "Install via rustup (https://rustup.rs), then retry `npm run tauri:build`.",
      ];

  const message = [
    "[tauri-cli] cargo is required by Tauri but was not found on PATH.",
    ...hintLines,
  ].join("\n");

  console.error(message);
  process.exit(1);
}

function runTauri() {
  const args = process.argv.slice(2);
  if (args[0] === "sync-version") {
    syncVersion();
    process.exit(0);
  }

  syncVersion();

  const tauriArgs = args.length > 0 ? args : ["build"];
  const env = withCargoPath(process.env);

  if (!hasCargo(env)) {
    failMissingCargo(env);
  }

  const localTauriBin = path.join(
    process.cwd(),
    "node_modules",
    ".bin",
    process.platform === "win32" ? "tauri.cmd" : "tauri",
  );
  const tauriCmd = existsSync(localTauriBin)
    ? localTauriBin
    : process.platform === "win32"
      ? "tauri.cmd"
      : "tauri";
  const result = spawnSync(tauriCmd, tauriArgs, {
    env,
    stdio: "inherit",
    shell: process.platform === "win32",
  });

  if (result.error) {
    console.error(`[tauri-cli] Failed to execute '${tauriCmd}': ${result.error.message}`);
    process.exit(typeof result.status === "number" ? result.status : 1);
  }

  process.exit(typeof result.status === "number" ? result.status : 0);
}

runTauri();
