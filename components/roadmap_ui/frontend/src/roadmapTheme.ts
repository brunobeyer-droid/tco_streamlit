import { safeString } from "./utils";

export const ROADMAP_LAYOUT = {
  LEFT_WIDTH: 260,
  LEFT_WIDTH_MIN: 240,
  LEFT_WIDTH_MAX: 520,
  ROW_HEIGHT: {
    comfortable: 52,
    compact: 44,
  },
  HEADER_HEIGHT: 84,
  BAR_RADIUS: 6,
  CHIP_HEIGHT: {
    comfortable: 24,
    compact: 20,
  },
  CHIP_GAP: {
    comfortable: 6,
    compact: 4,
  },
  ROW_PAD: {
    comfortable: 12,
    compact: 10,
  },
  ZOOM: {
    min: 0.7,
    max: 4.8,
  },
} as const;

export type EpicColor = {
  solid: string;
  soft: string;
  border: string;
  progress: string;
  textOnSolid: string;
  isNeutral: boolean;
};

function stableHash(seed: string): number {
  let hash = 0;
  for (let i = 0; i < seed.length; i += 1) {
    hash = ((hash << 5) - hash + seed.charCodeAt(i)) | 0;
  }
  return hash;
}

function isNeutralEpicToken(token: string): boolean {
  const t = safeString(token).toUpperCase();
  return t.includes("UNASSIGNED") || t.includes("OUT_OF_SCOPE") || t.includes("NO_EPIC") || t.includes("UNMAPPED");
}

function hslToLuminance(h: number, s: number, l: number): number {
  const sat = Math.max(0, Math.min(100, s)) / 100;
  const lig = Math.max(0, Math.min(100, l)) / 100;
  const c = (1 - Math.abs(2 * lig - 1)) * sat;
  const hp = ((h % 360) + 360) % 360 / 60;
  const x = c * (1 - Math.abs((hp % 2) - 1));
  let r = 0;
  let g = 0;
  let b = 0;
  if (hp >= 0 && hp < 1) {
    r = c;
    g = x;
  } else if (hp < 2) {
    r = x;
    g = c;
  } else if (hp < 3) {
    g = c;
    b = x;
  } else if (hp < 4) {
    g = x;
    b = c;
  } else if (hp < 5) {
    r = x;
    b = c;
  } else {
    r = c;
    b = x;
  }
  const m = lig - c / 2;
  const toLin = (v: number) => {
    const srgb = v + m;
    return srgb <= 0.04045 ? srgb / 12.92 : ((srgb + 0.055) / 1.055) ** 2.4;
  };
  const rl = toLin(r);
  const gl = toLin(g);
  const bl = toLin(b);
  return 0.2126 * rl + 0.7152 * gl + 0.0722 * bl;
}

export function epicColorFromId(epicIdRaw: string, epicTitleRaw = ""): EpicColor {
  const token = safeString(epicIdRaw || epicTitleRaw || "NO_EPIC");
  if (isNeutralEpicToken(token)) {
    return {
      solid: "hsl(214 11% 46%)",
      soft: "hsla(214, 11%, 46%, 0.2)",
      border: "hsla(214, 12%, 66%, 0.7)",
      progress: "linear-gradient(90deg, hsla(214, 15%, 65%, 0.95), hsla(214, 12%, 52%, 0.92))",
      textOnSolid: "rgba(2, 6, 23, 0.95)",
      isNeutral: true,
    };
  }

  const hash = stableHash(token);
  const hueStops = [8, 28, 46, 84, 116, 148, 178, 202, 224, 248, 276, 314, 336];
  const idx = Math.abs(hash) % hueStops.length;
  const hue = hueStops[idx];
  const sat = 30 + (Math.abs(hash >> 2) % 10);
  const light = 44 + (Math.abs(hash >> 5) % 7);
  const luminance = hslToLuminance(hue, sat, light);
  const textOnSolid = luminance > 0.38 ? "rgba(2, 6, 23, 0.95)" : "rgba(248, 250, 252, 0.97)";
  return {
    solid: `hsl(${hue} ${sat}% ${light}%)`,
    soft: `hsla(${hue}, ${sat}%, ${light}%, 0.16)`,
    border: `hsla(${hue}, ${sat}%, ${Math.min(72, light + 10)}%, 0.66)`,
    progress: `linear-gradient(90deg, hsla(${hue}, ${Math.min(48, sat + 8)}%, ${Math.min(64, light + 8)}%, 0.92), hsla(${hue}, ${Math.min(44, sat + 4)}%, ${Math.max(36, light - 8)}%, 0.9))`,
    textOnSolid,
    isNeutral: false,
  };
}

export function timelineDependencyVisibility(totalRows: number, totalLinks: number, mode: "auto" | "always"): boolean {
  if (mode === "always") return true;
  return totalRows <= 120 && totalLinks <= 300;
}
