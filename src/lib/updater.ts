export interface AvailableLocalUpdate {
  version: string;
  notes: string | null;
  publishedAt: string | null;
}

export interface LocalUpdateState {
  phase: "idle" | "checking" | "available" | "downloading" | "installing" | "error";
  enabled: boolean;
  currentVersion: string | null;
  available: AvailableLocalUpdate | null;
  checkedAt: number | null;
  error: string | null;
  downloadProgress: number | null;
}

export function createLocalUpdateState(): LocalUpdateState {
  return {
    phase: "idle",
    enabled: true,
    currentVersion: null,
    available: null,
    checkedAt: null,
    error: null,
    downloadProgress: null,
  };
}

export function setLocalUpdatePhase(
  current: LocalUpdateState,
  phase: LocalUpdateState["phase"],
): LocalUpdateState {
  return {
    ...current,
    phase,
    error: null,
  };
}

export function setLocalUpdateError(current: LocalUpdateState, message: string): LocalUpdateState {
  return {
    ...current,
    phase: "error",
    error: message,
  };
}

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

export async function checkForLocalUpdates(): Promise<LocalUpdateState> {
  if (!isTauriRuntime()) {
    return {
      ...createLocalUpdateState(),
      phase: "idle",
      enabled: false,
      checkedAt: Date.now(),
    };
  }

  try {
    const { check } = await import("@tauri-apps/plugin-updater");
    const update = await check();
    const checkedAt = Date.now();

    if (!update) {
      return {
        phase: "idle",
        enabled: true,
        currentVersion: null,
        available: null,
        checkedAt,
        error: null,
        downloadProgress: null,
      };
    }

    return {
      phase: "available",
      enabled: true,
      currentVersion: update.currentVersion,
      available: {
        version: update.version,
        notes: update.body ?? null,
        publishedAt: update.date ?? null,
      },
      checkedAt,
      error: null,
      downloadProgress: null,
    };
  } catch (err) {
    return {
      ...createLocalUpdateState(),
      phase: "error",
      checkedAt: Date.now(),
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

export async function installLocalUpdate(
  onProgress?: (percent: number) => void,
): Promise<void> {
  if (!isTauriRuntime()) {
    throw new Error("Local updater requires Tauri runtime");
  }

  const { check } = await import("@tauri-apps/plugin-updater");
  const { relaunch } = await import("@tauri-apps/plugin-process");

  const update = await check();
  if (!update) {
    throw new Error("No update available");
  }

  let downloaded = 0;
  let contentLength = 0;

  await update.downloadAndInstall((event) => {
    switch (event.event) {
      case "Started":
        contentLength = event.data.contentLength ?? 0;
        break;
      case "Progress":
        downloaded += event.data.chunkLength;
        if (contentLength > 0 && onProgress) {
          onProgress(Math.round((downloaded / contentLength) * 100));
        }
        break;
      case "Finished":
        onProgress?.(100);
        break;
    }
  });

  await relaunch();
}
