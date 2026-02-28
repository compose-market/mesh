import type { OsPermissionStatus } from "./types";

type MediaKind = "camera" | "microphone";

function hasMediaApi(): boolean {
  return typeof navigator !== "undefined" && !!navigator.mediaDevices?.getUserMedia;
}

export async function queryMediaPermission(kind: MediaKind): Promise<OsPermissionStatus> {
  if (!hasMediaApi()) {
    return "unsupported";
  }

  if (!navigator.permissions?.query) {
    return "unknown";
  }

  try {
    const result = await navigator.permissions.query({ name: kind as PermissionName });
    if (result.state === "granted") return "granted";
    if (result.state === "denied") return "denied";
    return "unknown";
  } catch {
    return "unknown";
  }
}

export async function requestMediaPermission(kind: MediaKind): Promise<OsPermissionStatus> {
  if (!hasMediaApi()) {
    return "unsupported";
  }

  try {
    const stream = await navigator.mediaDevices.getUserMedia(
      kind === "camera" ? { video: true, audio: false } : { audio: true, video: false },
    );
    for (const track of stream.getTracks()) {
      track.stop();
    }
    return "granted";
  } catch {
    const status = await queryMediaPermission(kind);
    return status === "unknown" ? "denied" : status;
  }
}
