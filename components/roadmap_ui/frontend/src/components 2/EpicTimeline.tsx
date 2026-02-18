import React, { useEffect, useMemo, useRef, useState } from "react";
import { RoadmapTimeline, RoadmapTimelineEpic, RoadmapTimelineFeature, RoadmapTimelineMilestone } from "../types";
import { clamp, escapeHtml, formatWhole, investmentMeta, safeString } from "../utils";
import { epicColorFromId, ROADMAP_LAYOUT, timelineDependencyVisibility } from "../roadmapTheme";

type TimelineProps = {
  timeline?: RoadmapTimeline | null;
  onShowTooltip: (html: string, e: React.MouseEvent) => void;
  onMoveTooltip: (e: React.MouseEvent) => void;
  onHideTooltip: () => void;
  onFeatureClick: (feature: RoadmapTimelineFeature) => void;
  onEpicClick: (epic: { id: string; title: string; businessValueSum?: number; featureCount?: number; progress?: number; epicUrl?: string }) => void;
  onMilestoneClick?: (milestone: { id: string }) => void;
  onMilestoneRailCreate?: (date: string) => void;
  selectedFeatureId?: string;
  selectedEpicId?: string;
  externalShowDependencies?: boolean;
  onShowDependenciesChange?: (value: boolean) => void;
  yearWindowStartOverride?: number;
  onYearWindowChange?: (value: number) => void;
  zoom?: number;
  timeframePreset?: "3m" | "6m" | "1y" | "fit" | "custom";
  showRoi?: boolean;
  sortBy?: "roi_bv" | "demand" | "name" | "progress" | "feature_count";
  sortDir?: "asc" | "desc";
  granularity?: "month" | "pi" | "sprint";
  onGranularityChange?: (value: "month" | "pi" | "sprint") => void;
  searchQuery?: string;
  showProgress?: boolean;
  dependencyLineMode?: "auto" | "always";
  onDependencyLineModeChange?: (value: "auto" | "always") => void;
  onDependencyClick?: (sourceId: string, targetId: string) => void;
  leftWidth?: number;
  onLeftWidthChange?: (value: number) => void;
  todayAnchorToken?: number;
  fitSelectionNonce?: number;
  timeWindowStart?: string;
  timeWindowEnd?: string;
  onRequestToday?: () => void;
  onRequestFitSelection?: () => void;
  featureFlags?: {
    selectionAutoscroll?: boolean;
    dependencyFocus?: boolean;
    milestoneCluster?: boolean;
    epicColorFinalize?: boolean;
    timeControlsPolish?: boolean;
    microPolish?: boolean;
  };
  focusMode?: boolean;
  density?: "comfortable" | "compact";
};

const bandHeaderHeight = 0;
const milestoneLaneTop: [string, string] = ["-3px", "10px"];
const timelineLeftMin = ROADMAP_LAYOUT.LEFT_WIDTH_MIN;
const timelineLeftMax = ROADMAP_LAYOUT.LEFT_WIDTH_MAX;

type TimelineRow =
  | { kind: "epic"; epicKey: string; epicId: string; title: string; progress: number; businessValueSum: number; featureCount: number; epicUrl?: string }
  | { kind: "feature"; epicKey: string; epicId: string; feature: RoadmapTimelineFeature };

type TimelineMilestonePoint = {
  id: string;
  label: string;
  date: string;
  type: "boundary" | "custom";
  tag?: string;
  sourceType?: string;
  epicId?: string;
  featureId?: string;
};

function normalizeIdToken(value: any): string {
  const raw = safeString(value ?? "");
  if (!raw) return "";
  // Normalize numeric tokens like "123.0" -> "123" to avoid key mismatches.
  if (/^-?\d+(\.0+)?$/.test(raw)) {
    const n = Number(raw);
    if (Number.isFinite(n)) return String(Math.trunc(n));
  }
  return raw;
}

function parseDate(value?: string | null): Date | null {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function fmtDate(value?: string | null): string {
  const d = parseDate(value);
  if (!d) return "—";
  return d.toISOString().slice(0, 10);
}

function milestoneTagClass(tagRaw?: string): string {
  const tag = safeString(tagRaw || "").trim().toLowerCase();
  if (!tag) return "";
  if (tag === "release") return "tag-release";
  if (tag === "risk") return "tag-risk";
  if (tag === "decision") return "tag-decision";
  if (tag === "dependency") return "tag-dependency";
  if (tag === "milestone") return "tag-milestone";
  return "tag-custom";
}

function timelineSortMultiplier(dir: "asc" | "desc"): number {
  return dir === "asc" ? 1 : -1;
}

function teamFlagLabel(value: string): string {
  const raw = safeString(value);
  if (!raw) return "";
  if (raw.length <= 10) return raw;
  const words = raw.split(/\s+/).filter(Boolean);
  if (words.length >= 2) {
    const initials = words.slice(0, 3).map((w) => w[0]?.toUpperCase() || "").join("");
    if (initials) return initials;
  }
  return `${raw.slice(0, 9)}…`;
}

function TimelineGlyph({ kind }: { kind: "epic" | "feature" | "application" }) {
  if (kind === "feature") {
    return (
      <svg viewBox="0 0 24 24" className="roadmap-timeline-glyph-svg" aria-hidden>
        <path fill="currentColor" d="M4 5h16v2H4zm0 6h16v2H4zm0 6h10v2H4z" />
      </svg>
    );
  }
  if (kind === "application") {
    return (
      <svg viewBox="0 0 24 24" className="roadmap-timeline-glyph-svg" aria-hidden>
        <path fill="currentColor" d="M3 3h8v8H3zm10 0h8v8h-8zM3 13h8v8H3zm10 0h8v8h-8z" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 24 24" className="roadmap-timeline-glyph-svg" aria-hidden>
      <path fill="currentColor" d="M10 3H3v7h7zm11 0h-7v7h7zM10 14H3v7h7zm4 0h2v3h-2zm0-8h2v4h-2zm-4 8h4v2h-4zm6-4h4v2h-4z" />
    </svg>
  );
}

export function EpicTimeline({
  timeline,
  onShowTooltip,
  onMoveTooltip,
  onHideTooltip,
  onFeatureClick,
  onEpicClick,
  onMilestoneClick,
  onMilestoneRailCreate,
  selectedFeatureId,
  selectedEpicId,
  externalShowDependencies,
  onShowDependenciesChange,
  yearWindowStartOverride,
  onYearWindowChange,
  zoom = 1,
  timeframePreset = "fit",
  showRoi = false,
  sortBy = "roi_bv",
  sortDir = "desc",
  granularity: granularityFromUi = "pi",
  onGranularityChange,
  searchQuery = "",
  showProgress = true,
  dependencyLineMode = "auto",
  onDependencyLineModeChange,
  onDependencyClick,
  leftWidth = 260,
  onLeftWidthChange,
  todayAnchorToken = 0,
  fitSelectionNonce = 0,
  timeWindowStart = "",
  timeWindowEnd = "",
  onRequestToday,
  onRequestFitSelection,
  featureFlags,
  focusMode = false,
  density = "comfortable",
}: TimelineProps) {
  const isCompactDensity = density === "compact";
  const rowHeight = isCompactDensity ? ROADMAP_LAYOUT.ROW_HEIGHT.compact : ROADMAP_LAYOUT.ROW_HEIGHT.comfortable;
  const showLabelMeta = !isCompactDensity;
  const showSecondaryBadges = !isCompactDensity;
  const epic = timeline?.epic || null;
  const featuresRaw = Array.isArray(timeline?.features) ? timeline!.features! : [];
  const piBands = Array.isArray(timeline?.piBands) ? timeline!.piBands! : [];
  const sprintBands = Array.isArray(timeline?.sprintBands) ? timeline!.sprintBands! : [];
  const links = Array.isArray(timeline?.links) ? timeline!.links! : [];
  const customMilestonesRaw = Array.isArray(timeline?.milestones) ? (timeline!.milestones as RoadmapTimelineMilestone[]) : [];
  const defaultGranularity = timeline?.defaultGranularity === "sprint" ? "sprint" : "pi";
  const hasSprintBands = sprintBands.length > 0;
  const [granularity, setGranularity] = useState<"pi" | "sprint">(granularityFromUi === "sprint" ? "sprint" : defaultGranularity);
  const [yearStartIdx, setYearStartIdx] = useState(0);
  const [isResizingLeftRail, setIsResizingLeftRail] = useState(false);
  const [viewportWidth, setViewportWidth] = useState(0);
  const viewportRef = useRef<HTMLDivElement>(null);
  const bodyScrollRef = useRef<HTMLDivElement>(null);
  const autoScrollKeyRef = useRef<string>("");
  const selectionScrollKeyRef = useRef<string>("");
  const granularityAnchorTsRef = useRef<number | null>(null);
  const zoomViewportAnchorRef = useRef<{ zoom: number; trackWidth: number } | null>(null);
  const resizeMoveRef = useRef<((event: PointerEvent | MouseEvent) => void) | null>(null);
  const resizeUpRef = useRef<((event: PointerEvent | MouseEvent) => void) | null>(null);
  const resizeMoveTypeRef = useRef<"pointermove" | "mousemove" | null>(null);
  const resizeUpTypeRef = useRef<"pointerup" | "mouseup" | null>(null);
  const panStateRef = useRef<{
    active: boolean;
    pointerId: number | null;
    startX: number;
    startY: number;
    startScrollLeft: number;
    startScrollTop: number;
  }>({
    active: false,
    pointerId: null,
    startX: 0,
    startY: 0,
    startScrollLeft: 0,
    startScrollTop: 0,
  });
  const [hoveredFeatureId, setHoveredFeatureId] = useState("");
  const [hoveredDependencyKey, setHoveredDependencyKey] = useState("");
  const [scrolledBody, setScrolledBody] = useState(false);
  const [isPanning, setIsPanning] = useState(false);
  const flagSelectionAutoscroll = Boolean(featureFlags?.selectionAutoscroll);
  const flagDependencyFocus = Boolean(featureFlags?.dependencyFocus);
  const flagMilestoneCluster = Boolean(featureFlags?.milestoneCluster);
  const flagTimeControlsPolish = Boolean(featureFlags?.timeControlsPolish);
  const flagEpicColorFinalize = Boolean(featureFlags?.epicColorFinalize);
  const flagMicroPolish = Boolean(featureFlags?.microPolish);

  const showDependencies = Boolean(externalShowDependencies);

  const handleShowDependenciesChange = (next: boolean) => {
    onShowDependenciesChange?.(next);
  };

  useEffect(() => {
    if (typeof yearWindowStartOverride === "number" && yearWindowStartOverride >= 0 && yearWindowStartOverride !== yearStartIdx) {
      setYearStartIdx(yearWindowStartOverride);
    }
  }, [yearWindowStartOverride, yearStartIdx]);

  useEffect(() => {
    const incoming = granularityFromUi === "sprint" ? "sprint" : "pi";
    if (incoming !== granularity) setGranularity(incoming);
  }, [granularityFromUi, granularity]);

  const [collapsedEpics, setCollapsedEpics] = useState<Record<string, boolean>>({});

  const features: RoadmapTimelineFeature[] = useMemo(() => {
    const fallbackEpicId = normalizeIdToken(epic?.id || timeline?.focusEpicId || "");
    const fallbackEpicTitle = safeString(epic?.title || "(Epic)");
    return featuresRaw.map((f) => ({
      ...f,
      epicId: normalizeIdToken((f as any).epicId || fallbackEpicId || "NO_EPIC"),
      epicTitle: safeString((f as any).epicTitle || fallbackEpicTitle || "(Epic)"),
      businessValue: Number((f as any).businessValue || 0),
    }));
  }, [featuresRaw, epic, timeline?.focusEpicId]);
  const searchNeedle = safeString(searchQuery).trim().toLowerCase();
  const featuresSearched = useMemo(() => {
    if (!searchNeedle) return features;
    return features.filter((f) => {
      const hay = [
        f.id,
        (f as any).epicId,
        f.title,
        (f as any).team,
        (f as any).program,
        (f as any).application,
        (f as any).epicTitle,
      ].map((v) => safeString(v).toLowerCase()).join(" ");
      return hay.includes(searchNeedle);
    });
  }, [features, searchNeedle]);

  const epicList: RoadmapTimelineEpic[] = useMemo(() => {
    const provided = Array.isArray((timeline as any)?.epics) ? ((timeline as any).epics as RoadmapTimelineEpic[]) : [];
    const searchedEpicIds = new Set(
      featuresSearched
        .map((f) => normalizeIdToken((f as any).epicId || ""))
        .filter(Boolean)
    );
    if (provided.length) {
      let out = provided.map((e) => ({
        id: normalizeIdToken(e.id || ""),
        title: safeString(e.title || "(Epic)"),
        progress: clamp(Number(e.progress || 0), 0, 1),
        businessValueSum: Number((e as any).businessValueSum || 0),
        featureCount: Number((e as any).featureCount || 0),
        epicUrl: safeString((e as any).epicUrl || ""),
      }));
      if (searchNeedle) {
        out = out.filter((e) => searchedEpicIds.has(safeString(e.id)));
      }
      return out;
    }
    const byId: Record<string, { title: string; progress: number; n: number; bv: number }> = {};
    for (const f of featuresSearched) {
      const id = safeString((f as any).epicId || "NO_EPIC");
      const title = safeString((f as any).epicTitle || "(Epic)");
      if (!byId[id]) byId[id] = { title, progress: 0, n: 0, bv: 0 };
      byId[id].progress += clamp(Number(f.progress || 0), 0, 1);
      byId[id].n += 1;
      byId[id].bv += Number((f as any).businessValue || 0);
    }
    return Object.entries(byId)
      .map(([id, v]) => ({
        id,
        title: v.title,
        progress: v.n > 0 ? clamp(v.progress / v.n, 0, 1) : 0,
        businessValueSum: Number(v.bv || 0),
        featureCount: Number(v.n || 0),
        epicUrl: "",
      }))
      .sort((a, b) => a.title.localeCompare(b.title));
  }, [timeline, featuresSearched, searchNeedle]);

  const epicEntries = useMemo(
    () => {
      const sorted = [...epicList];
      sorted.sort((a, b) => {
        const mult = timelineSortMultiplier(sortDir);
        if (sortBy === "roi_bv") {
          const cmp = (Number((a as any).businessValueSum || 0) - Number((b as any).businessValueSum || 0)) * mult;
          if (cmp !== 0) return cmp;
        } else if (sortBy === "progress") {
          const cmp = (Number(a.progress || 0) - Number(b.progress || 0)) * mult;
          if (cmp !== 0) return cmp;
        } else if (sortBy === "feature_count") {
          const ac = (featuresSearched || []).filter((f) => safeString((f as any).epicId || "") === safeString(a.id)).length;
          const bc = (featuresSearched || []).filter((f) => safeString((f as any).epicId || "") === safeString(b.id)).length;
          const cmp = (ac - bc) * mult;
          if (cmp !== 0) return cmp;
        }
        return a.title.localeCompare(b.title) * (sortBy === "name" ? mult : 1);
      });
      return sorted.map((e) => ({
        ...e,
        epicKey: `${normalizeIdToken(e.id || "")}||${safeString(e.title || "(Epic)")}`.trim(),
      }));
    },
    [epicList, featuresSearched, sortBy, sortDir]
  );

  const isMultiEpic = epicList.length > 1;
  const isApplicationGrouping = useMemo(
    () => epicEntries.length > 0 && epicEntries.every((e) => safeString(e.id).startsWith("APP::")),
    [epicEntries]
  );
  const groupNameLabel = isApplicationGrouping ? "Application" : "Epic";
  const groupTagLabel = isApplicationGrouping ? "Application" : "Epic";

  const epicColorMap = useMemo(() => {
    const out: Record<string, { solid: string; soft: string; border: string; progress: string }> = {};
    for (const e of epicEntries) {
      out[e.id] = epicColorFromId(e.id, e.title);
    }
    return out;
  }, [epicEntries]);

  const initializedGranularityRef = useRef(false);
  useEffect(() => {
    if (!initializedGranularityRef.current) {
      initializedGranularityRef.current = true;
      setGranularity(defaultGranularity);
      return;
    }
    if (granularity === "sprint" && !hasSprintBands) {
      setGranularity("pi");
      onGranularityChange?.("pi");
    }
  }, [defaultGranularity, hasSprintBands, granularity, onGranularityChange]);

  useEffect(() => {
    setCollapsedEpics((prev) => {
      const next: Record<string, boolean> = {};
      for (const e of epicEntries) {
        if (Object.prototype.hasOwnProperty.call(prev, e.epicKey)) {
          next[e.epicKey] = Boolean(prev[e.epicKey]);
        } else {
          next[e.epicKey] = true; // Default collapsed on first render for each epic.
        }
      }
      return next;
    });
  }, [epicEntries]);

  const activeBands = useMemo(() => {
    if (granularity === "sprint" && hasSprintBands) {
      return sprintBands.map((b) => ({
        id: String((b as any).sprintKey || ""),
        label: String((b as any).label || (b as any).sprintKey || ""),
        start: b.start || null,
        end: b.end || null,
      }));
    }
    return piBands.map((b) => ({
      id: String((b as any).piKey || ""),
      label: String((b as any).label || (b as any).piKey || ""),
      start: b.start || null,
      end: b.end || null,
    }));
  }, [granularity, hasSprintBands, piBands, sprintBands]);

  const yearsAvailable = useMemo(() => {
    const years = new Set<number>();
    for (const b of activeBands) {
      const s = parseDate(b.start || null);
      const e = parseDate(b.end || null);
      if (s) years.add(s.getFullYear());
      if (e) years.add(e.getFullYear());
    }
    return Array.from(years.values()).sort((a, b) => a - b);
  }, [activeBands]);

  const yearWindowSize = yearsAvailable.length <= 2 ? Math.max(1, yearsAvailable.length) : 2;

  useEffect(() => {
    if (!yearsAvailable.length) {
      if (yearStartIdx !== 0) setYearStartIdx(0);
      return;
    }
    const nowYear = new Date().getFullYear();
    const nowIdx = yearsAvailable.findIndex((y) => y >= nowYear);
    const preferred = nowIdx >= 0 ? nowIdx : Math.max(0, yearsAvailable.length - yearWindowSize);
    const maxStart = Math.max(0, yearsAvailable.length - yearWindowSize);
    const clamped = Math.max(0, Math.min(preferred, maxStart));
    if (yearStartIdx > maxStart || yearStartIdx < 0) {
      setYearStartIdx(clamped);
    }
  }, [yearsAvailable, yearWindowSize, yearStartIdx]);

  const yearWindow = useMemo(() => {
    if (!yearsAvailable.length) return null;
    const maxStart = Math.max(0, yearsAvailable.length - yearWindowSize);
    const startIdx = Math.max(0, Math.min(yearStartIdx, maxStart));
    const startYear = yearsAvailable[startIdx];
    const endYear = yearsAvailable[Math.min(yearsAvailable.length - 1, startIdx + yearWindowSize - 1)];
    return { startIdx, maxStart, startYear, endYear };
  }, [yearsAvailable, yearStartIdx, yearWindowSize]);

  useEffect(() => {
    if (!yearWindow) return;
    if (yearStartIdx !== yearWindow.startIdx) {
      setYearStartIdx(yearWindow.startIdx);
    }
  }, [yearWindow, yearStartIdx]);

  const yearWindowDates = useMemo(() => {
    if (!yearWindow) return null;
    return {
      min: new Date(Date.UTC(yearWindow.startYear, 0, 1)),
      max: new Date(Date.UTC(yearWindow.endYear, 11, 31)),
    };
  }, [yearWindow]);

  const presetWindowDates = useMemo(() => {
    const explicitStart = parseDate(timeWindowStart || null);
    const explicitEnd = parseDate(timeWindowEnd || null);
    if (flagTimeControlsPolish && explicitStart && explicitEnd && explicitEnd.getTime() >= explicitStart.getTime()) {
      return { min: explicitStart, max: explicitEnd };
    }
    const now = new Date();
    const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    const day = 86400000;
    if (timeframePreset === "3m") return { min: start, max: new Date(start.getTime() + 92 * day) };
    if (timeframePreset === "6m") return { min: start, max: new Date(start.getTime() + 183 * day) };
    if (timeframePreset === "1y") return { min: start, max: new Date(start.getTime() + 365 * day) };
    // Keep timeline default aligned with board behavior: focus "today forward".
    // Fallback to full-fit when that window would be empty.
    if (timeframePreset === "fit") {
      const window = { min: start, max: new Date(start.getTime() + 365 * day) };
      const minTs = window.min.getTime();
      const maxTs = window.max.getTime();
      const hasBands = activeBands.some((b) => {
        const s = parseDate(b.start || null);
        const e = parseDate(b.end || null);
        if (!s && !e) return false;
        const sTs = (s || e)!.getTime();
        const eTs = (e || s)!.getTime();
        return eTs >= minTs && sTs <= maxTs;
      });
      const hasFeatures = featuresSearched.some((f) => {
        const s = parseDate(f.start || null);
        const e = parseDate(f.end || null);
        if (!s && !e) return false;
        const sTs = (s || e)!.getTime();
        const eTs = (e || s)!.getTime();
        return eTs >= minTs && sTs <= maxTs;
      });
      if (hasBands || hasFeatures) return window;
      return null;
    }
    return yearWindowDates;
  }, [timeframePreset, yearWindowDates, activeBands, featuresSearched, timeWindowStart, timeWindowEnd, flagTimeControlsPolish]);

  const activeBandsInWindow = useMemo(() => {
    if (!presetWindowDates) return activeBands;
    const minTs = presetWindowDates.min.getTime();
    const maxTs = presetWindowDates.max.getTime();
    return activeBands.filter((b) => {
      const s = parseDate(b.start || null);
      const e = parseDate(b.end || null);
      if (!s && !e) return false;
      const sTs = (s || e)!.getTime();
      const eTs = (e || s)!.getTime();
      return eTs >= minTs && sTs <= maxTs;
    });
  }, [activeBands, presetWindowDates]);

  const featuresInWindow = useMemo(() => {
    if (!presetWindowDates) return featuresSearched;
    const minTs = presetWindowDates.min.getTime();
    const maxTs = presetWindowDates.max.getTime();
    return featuresSearched.filter((f) => {
      const s = parseDate(f.start || null);
      const e = parseDate(f.end || null);
      if (!s && !e) return false;
      const sTs = (s || e)!.getTime();
      const eTs = (e || s)!.getTime();
      return eTs >= minTs && sTs <= maxTs;
    });
  }, [featuresSearched, presetWindowDates]);

  const milestonePoints = useMemo(() => {
    const points: TimelineMilestonePoint[] = [];
    for (const ms of customMilestonesRaw) {
      const date = safeString(ms?.date || "");
      if (!date) continue;
      if (presetWindowDates) {
        const d = parseDate(date);
        if (!d) continue;
        if (d.getTime() < presetWindowDates.min.getTime() || d.getTime() > presetWindowDates.max.getTime()) continue;
      }
      const id = safeString(ms?.id || `${date}-${safeString(ms?.label || "Milestone")}`);
      const label = safeString(ms?.label || "Milestone");
      const tag = safeString((ms as any)?.tag || "");
      const sourceType = safeString((ms as any)?.sourceType || "");
      const epicId = safeString((ms as any)?.epicId || "");
      const featureId = safeString((ms as any)?.featureId || "");
      points.push({
        id: `custom-${id}`,
        label: tag ? `[${tag}] ${label}` : label,
        date,
        type: "custom",
        ...(tag ? { tag } : {}),
        ...(sourceType ? { sourceType } : {}),
        ...(epicId ? { epicId } : {}),
        ...(featureId ? { featureId } : {}),
      });
    }
    const dedup = new Map<string, TimelineMilestonePoint>();
    for (const p of points) {
      const key = `${p.type}::${p.date}::${safeString(p.label).toLowerCase()}`;
      if (!dedup.has(key)) dedup.set(key, p);
    }
    const sorted = Array.from(dedup.values()).sort((a, b) => {
      const ad = parseDate(a.date);
      const bd = parseDate(b.date);
      if (!ad || !bd) return 0;
      return ad.getTime() - bd.getTime();
    });
    return sorted;
  }, [customMilestonesRaw, presetWindowDates]);

  const dateRange = useMemo(() => {
    if (presetWindowDates) {
      const min = presetWindowDates.min;
      const max = presetWindowDates.max;
      if (max.getTime() <= min.getTime()) {
        return { min, max: new Date(min.getTime() + 86400000) };
      }
      return { min, max };
    }
    const allDates: Date[] = [];
    for (const f of featuresInWindow) {
      const s = parseDate(f.start || null);
      const e = parseDate(f.end || null);
      if (s) allDates.push(s);
      if (e) allDates.push(e);
    }
    for (const p of activeBandsInWindow) {
      const s = parseDate(p.start || null);
      const e = parseDate(p.end || null);
      if (s) allDates.push(s);
      if (e) allDates.push(e);
    }
    if (!allDates.length) return null;
    allDates.sort((a, b) => a.getTime() - b.getTime());
    const min = allDates[0];
    const max = allDates[allDates.length - 1];
    if (max.getTime() <= min.getTime()) {
      return { min, max: new Date(min.getTime() + 86400000) };
    }
    return { min, max };
  }, [presetWindowDates, featuresInWindow, activeBandsInWindow]);

  const toPct = (dateValue?: string | null) => {
    if (!dateRange) return 0;
    const d = parseDate(dateValue || null);
    if (!d) return 0;
    const span = dateRange.max.getTime() - dateRange.min.getTime();
    if (span <= 0) return 0;
    return clamp((d.getTime() - dateRange.min.getTime()) / span, 0, 1);
  };

  const todayPct = useMemo(() => {
    if (!dateRange) return null;
    const now = new Date();
    const today = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    if (today.getTime() < dateRange.min.getTime() || today.getTime() > dateRange.max.getTime()) return null;
    const span = dateRange.max.getTime() - dateRange.min.getTime();
    if (span <= 0) return null;
    return clamp((today.getTime() - dateRange.min.getTime()) / span, 0, 1);
  }, [dateRange]);

  const visibleMilestoneLabels = useMemo(() => {
    if (!milestonePoints.length || !dateRange) return { labels: [] as Array<{ id: string; leftPct: number; label: string; lane: 0 | 1 }>, hiddenCount: 0 };
    const density = milestonePoints.length;
    const spacingPct = isCompactDensity
      ? clamp(16 - density * 0.45, 6.2, 11.5)
      : clamp(14.5 - density * 0.48, 4.8, 10.2);
    const maxLabels = isCompactDensity
      ? (density <= 8 ? Math.min(density, 6) : density <= 16 ? 8 : 9)
      : (density <= 8 ? density : density <= 16 ? 10 : 12);
    let lastLeft = -999;
    const labels: Array<{ id: string; leftPct: number; label: string; lane: 0 | 1 }> = [];
    let hiddenCount = 0;
    for (const ms of milestonePoints) {
      const leftPct = toPct(ms.date) * 100;
      const compactLabel = safeString(ms.label || "Milestone").replace(/^\[[^\]]+\]\s*/, "").trim();
      const shortLabel = compactLabel.length > 18 ? `${compactLabel.slice(0, 18)}...` : compactLabel;
      if (labels.length < maxLabels && Math.abs(leftPct - lastLeft) >= spacingPct) {
        labels.push({ id: ms.id, leftPct, label: shortLabel || "Milestone", lane: (labels.length % 2 === 0 ? 0 : 1) });
        lastLeft = leftPct;
      } else {
        hiddenCount += 1;
      }
    }
    return { labels, hiddenCount };
  }, [milestonePoints, dateRange, isCompactDensity]);

  const milestoneClusters = useMemo(() => {
    if (!milestonePoints.length || !dateRange) return [] as Array<{ id: string; leftPct: number; points: TimelineMilestonePoint[] }>;
    const ordered = milestonePoints
      .map((ms) => ({ ...ms, leftPct: toPct(ms.date) * 100 }))
      .sort((a, b) => a.leftPct - b.leftPct);
    const clusters: Array<{ id: string; leftPct: number; points: TimelineMilestonePoint[] }> = [];
    const thresholdPct = flagMilestoneCluster ? 1.6 : 0.01;
    ordered.forEach((point) => {
      const last = clusters[clusters.length - 1];
      if (!last || Math.abs(point.leftPct - last.leftPct) > thresholdPct) {
        clusters.push({ id: point.id, leftPct: point.leftPct, points: [point] });
        return;
      }
      last.points.push(point);
      last.leftPct = last.points.reduce((acc, p) => acc + p.leftPct, 0) / last.points.length;
    });
    return clusters;
  }, [milestonePoints, dateRange, flagMilestoneCluster]);

  const epicWindows = useMemo(() => {
    const out: Record<string, { start: string; end: string }> = {};
    const byEpic: Record<string, { min?: Date; max?: Date }> = {};
    for (const f of featuresInWindow) {
      const epicId = safeString((f as any).epicId || "NO_EPIC");
      const s = parseDate(f.start || null);
      const e = parseDate(f.end || null);
      if (!byEpic[epicId]) byEpic[epicId] = {};
      if (s && (!byEpic[epicId].min || s.getTime() < byEpic[epicId].min!.getTime())) byEpic[epicId].min = s;
      if (e && (!byEpic[epicId].max || e.getTime() > byEpic[epicId].max!.getTime())) byEpic[epicId].max = e;
    }
    for (const [epicId, v] of Object.entries(byEpic)) {
      if (v.min && v.max) {
        out[epicId] = {
          start: v.min.toISOString().slice(0, 10),
          end: v.max.toISOString().slice(0, 10),
        };
      }
    }
    return out;
  }, [featuresInWindow]);

  const featureRowsByEpic = useMemo(() => {
    const out: Record<string, RoadmapTimelineFeature[]> = {};
    for (const f of featuresInWindow) {
      const eid = normalizeIdToken((f as any).epicId || "NO_EPIC");
      if (!out[eid]) out[eid] = [];
      out[eid].push(f);
    }
    for (const eid of Object.keys(out)) {
      out[eid] = out[eid].slice().sort((a, b) => {
        const mult = timelineSortMultiplier(sortDir);
        if (sortBy === "roi_bv") {
          const cmp = (Number((a as any).businessValue || 0) - Number((b as any).businessValue || 0)) * mult;
          if (cmp !== 0) return cmp;
        } else if (sortBy === "demand") {
          const cmp = (Number((a as any).fte || 0) - Number((b as any).fte || 0)) * mult;
          if (cmp !== 0) return cmp;
        } else if (sortBy === "progress") {
          const cmp = (Number(a.progress || 0) - Number(b.progress || 0)) * mult;
          if (cmp !== 0) return cmp;
        } else if (sortBy === "name" || sortBy === "feature_count") {
          const cmp = safeString(a.title).localeCompare(safeString(b.title)) * mult;
          if (cmp !== 0) return cmp;
        }
        const as = parseDate(a.start || null)?.getTime() || 0;
        const bs = parseDate(b.start || null)?.getTime() || 0;
        if (as !== bs) return as - bs;
        return safeString(a.title).localeCompare(safeString(b.title));
      });
    }
    return out;
  }, [featuresInWindow, sortBy, sortDir]);

  const rowEntries: TimelineRow[] = useMemo(() => {
    const rows: TimelineRow[] = [];
    for (const e of epicEntries) {
      rows.push({
        kind: "epic",
        epicKey: e.epicKey,
        epicId: e.id,
        title: e.title,
        progress: clamp(Number(e.progress || 0), 0, 1),
        businessValueSum: Number((e as any).businessValueSum || 0),
        featureCount: Number((e as any).featureCount || 0),
        epicUrl: safeString((e as any).epicUrl || ""),
      });
      if (!collapsedEpics[e.epicKey]) {
        let featRows = featureRowsByEpic[e.id] || [];
        if (!featRows.length) {
          // Fallback match by epic title in case id formats drift between payload segments.
          featRows = featuresInWindow.filter((f) => safeString((f as any).epicTitle || "").toLowerCase() === safeString(e.title).toLowerCase());
        }
        for (const f of featRows) rows.push({ kind: "feature", epicKey: e.epicKey, epicId: e.id, feature: f });
      }
    }
    return rows;
  }, [epicEntries, featureRowsByEpic, collapsedEpics, featuresInWindow]);
  const focusEpicId = useMemo(() => {
    if (!focusMode) return "";
    const directEpic = normalizeIdToken(selectedEpicId || "");
    if (directEpic) return directEpic;
    const selectedFeatureEpic = normalizeIdToken(
      (featuresInWindow.find((f) => safeString((f as any).id) === safeString(selectedFeatureId || "")) as any)?.epicId || ""
    );
    return selectedFeatureEpic;
  }, [focusMode, selectedEpicId, selectedFeatureId, featuresInWindow]);

  const featureCountByEpic = useMemo(() => {
    const out: Record<string, number> = {};
    for (const e of epicEntries) {
      let featRows = featureRowsByEpic[e.id] || [];
      if (!featRows.length) {
        featRows = featuresInWindow.filter((f) => safeString((f as any).epicTitle || "").toLowerCase() === safeString(e.title).toLowerCase());
      }
      out[e.epicKey] = featRows.length;
    }
    return out;
  }, [epicEntries, featureRowsByEpic, featuresInWindow]);

  const epicTeamFlags = useMemo(() => {
    const out: Record<string, { labels: string[]; overflow: number }> = {};
    for (const e of epicEntries) {
      let featRows = featureRowsByEpic[e.id] || [];
      if (!featRows.length) {
        featRows = featuresInWindow.filter((f) => safeString((f as any).epicTitle || "").toLowerCase() === safeString(e.title).toLowerCase());
      }
      const counts: Record<string, number> = {};
      for (const f of featRows) {
        const team = safeString((f as any).team || "");
        if (!team) continue;
        counts[team] = (counts[team] || 0) + 1;
      }
      const orderedTeams = Object.entries(counts)
        .sort((a, b) => {
          if (b[1] !== a[1]) return b[1] - a[1];
          return a[0].localeCompare(b[0]);
        })
        .map(([team]) => team);
      const visible = orderedTeams.slice(0, 2);
      out[e.epicKey] = {
        labels: visible,
        overflow: Math.max(0, orderedTeams.length - visible.length),
      };
    }
    return out;
  }, [epicEntries, featureRowsByEpic, featuresInWindow]);

  const totalRows = rowEntries.length;

  const idToRow = useMemo(() => {
    const out: Record<string, number> = {};
    rowEntries.forEach((r, idx) => {
      if (r.kind === "feature") out[String(r.feature.id)] = idx;
    });
    return out;
  }, [rowEntries]);

  const featureDepsById = useMemo(() => {
    const inbound: Record<string, number> = {};
    const outbound: Record<string, number> = {};
    links.forEach((ln) => {
      const source = safeString((ln as any).sourceId || "");
      const target = safeString((ln as any).targetId || "");
      if (!source || !target) return;
      outbound[source] = (outbound[source] || 0) + 1;
      inbound[target] = (inbound[target] || 0) + 1;
    });
    return { inbound, outbound };
  }, [links]);

  const headerTitle = isMultiEpic
    ? `${epicEntries.length} ${isApplicationGrouping ? "applications" : "epics"} in current selection`
    : safeString(epicEntries[0]?.title || epic?.title || "(Epic)");
  const headerProgress = isMultiEpic
    ? Math.round(
        clamp(
          epicEntries.reduce((acc, e) => acc + clamp(Number(e.progress || 0), 0, 1), 0) / Math.max(1, epicEntries.length),
          0,
          1
        ) * 100
      )
    : Math.round(clamp(Number(epicEntries[0]?.progress || epic?.progress || 0), 0, 1) * 100);

  const zoomLevel = clamp(Number(zoom || 1), 0.7, 4.8);
  const normalizedLeftWidth = Math.max(timelineLeftMin, Math.min(timelineLeftMax, Number(leftWidth || 260)));
  const viewportTrackWidthPx = Math.max(360, (viewportWidth > 0 ? viewportWidth : 1040) - normalizedLeftWidth);
  const trackMinWidthPx = Math.max(360, Math.round(viewportTrackWidthPx * zoomLevel));
  const timelineMinWidthPx = Math.max(normalizedLeftWidth + 360, normalizedLeftWidth + trackMinWidthPx);
  const canRenderDependencies = showDependencies && timelineDependencyVisibility(totalRows, links.length, dependencyLineMode);
  const linkedFeatureIds = useMemo(() => {
    if (!hoveredFeatureId) return new Set<string>();
    const out = new Set<string>([hoveredFeatureId]);
    links.forEach((ln) => {
      const source = safeString(ln.sourceId);
      const target = safeString(ln.targetId);
      if (source === hoveredFeatureId) out.add(target);
      if (target === hoveredFeatureId) out.add(source);
    });
    return out;
  }, [hoveredFeatureId, links]);

  const changeYearWindow = (delta: number, maxStart: number) => {
    setYearStartIdx((prev) => {
      const next = Math.max(0, Math.min(maxStart, prev + delta));
      if (next !== prev) onYearWindowChange?.(next);
      return next;
    });
  };

  const captureGranularityAnchor = () => {
    if (!flagTimeControlsPolish) return;
    const viewportEl = viewportRef.current;
    if (!viewportEl || !dateRange) return;
    const span = dateRange.max.getTime() - dateRange.min.getTime();
    if (span <= 0) return;
    const centerPct = (viewportEl.scrollLeft + viewportEl.clientWidth * 0.5) / Math.max(1, trackMinWidthPx);
    granularityAnchorTsRef.current = dateRange.min.getTime() + clamp(centerPct, 0, 1) * span;
  };

  useEffect(() => {
    const viewportEl = viewportRef.current;
    if (!viewportEl) return;
    const observer = new ResizeObserver((entries) => {
      const width = entries[0]?.contentRect?.width;
      if (typeof width === "number" && Number.isFinite(width)) {
        setViewportWidth(Math.max(0, Math.round(width)));
      }
    });
    observer.observe(viewportEl);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    return () => {
      if (resizeMoveRef.current && resizeMoveTypeRef.current) window.removeEventListener(resizeMoveTypeRef.current, resizeMoveRef.current as EventListener);
      if (resizeUpRef.current && resizeUpTypeRef.current) window.removeEventListener(resizeUpTypeRef.current, resizeUpRef.current as EventListener);
      resizeMoveRef.current = null;
      resizeUpRef.current = null;
      resizeMoveTypeRef.current = null;
      resizeUpTypeRef.current = null;
    };
  }, []);

  useEffect(() => {
    const viewportEl = viewportRef.current;
    if (!viewportEl || todayPct === null) return;
    const key = [
      timeframePreset,
      String(yearWindow?.startIdx ?? -1),
      dateRange?.min?.toISOString() || "",
      dateRange?.max?.toISOString() || "",
      String(todayAnchorToken || 0),
    ].join("|");
    if (selectionScrollKeyRef.current === key) return;
    selectionScrollKeyRef.current = key;
    // Anchor timeline so Today line starts at the left track boundary (forward-focused view).
    const targetLeft = (todayPct * trackMinWidthPx) - 8;
    const maxLeft = Math.max(0, viewportEl.scrollWidth - viewportEl.clientWidth);
    const nextLeft = Math.max(0, Math.min(maxLeft, targetLeft));
    window.requestAnimationFrame(() => {
      viewportEl.scrollLeft = nextLeft;
    });
  }, [todayPct, timeframePreset, yearWindow?.startIdx, dateRange?.min, dateRange?.max, todayAnchorToken]);

  useEffect(() => {
    const viewportEl = viewportRef.current;
    const prev = zoomViewportAnchorRef.current;
    zoomViewportAnchorRef.current = { zoom: zoomLevel, trackWidth: trackMinWidthPx };
    if (!viewportEl || !prev) return;
    if (Math.abs(prev.zoom - zoomLevel) < 0.0001) return;
    const centerPct = clamp((viewportEl.scrollLeft + viewportEl.clientWidth * 0.5) / Math.max(1, prev.trackWidth), 0, 1);
    window.requestAnimationFrame(() => {
      const maxLeft = Math.max(0, viewportEl.scrollWidth - viewportEl.clientWidth);
      const nextLeft = centerPct * trackMinWidthPx - viewportEl.clientWidth * 0.5;
      viewportEl.scrollLeft = Math.max(0, Math.min(maxLeft, nextLeft));
    });
  }, [zoomLevel, trackMinWidthPx]);

  useEffect(() => {
    if (!flagTimeControlsPolish) return;
    const anchorTs = granularityAnchorTsRef.current;
    const viewportEl = viewportRef.current;
    if (!viewportEl || !dateRange || anchorTs === null) return;
    const span = dateRange.max.getTime() - dateRange.min.getTime();
    if (span <= 0) {
      granularityAnchorTsRef.current = null;
      return;
    }
    const pct = clamp((anchorTs - dateRange.min.getTime()) / span, 0, 1);
    const nextLeft = pct * trackMinWidthPx - viewportEl.clientWidth * 0.5;
    const maxLeft = Math.max(0, viewportEl.scrollWidth - viewportEl.clientWidth);
    viewportEl.scrollTo({ left: Math.max(0, Math.min(maxLeft, nextLeft)), behavior: "smooth" });
    granularityAnchorTsRef.current = null;
  }, [granularity, dateRange?.min, dateRange?.max, trackMinWidthPx, flagTimeControlsPolish]);

  useEffect(() => {
    if (!flagSelectionAutoscroll) return;
    const viewportEl = viewportRef.current;
    const bodyScrollEl = bodyScrollRef.current;
    if (!viewportEl || !bodyScrollEl || !dateRange || !rowEntries.length) return;
    const targetFeatureId = safeString(selectedFeatureId || "");
    const targetEpicId = safeString(selectedEpicId || "");
    let targetRowIdx = -1;
    let targetDate = "";
    if (targetFeatureId) {
      targetRowIdx = rowEntries.findIndex((r) => r.kind === "feature" && safeString((r.feature as any).id) === targetFeatureId);
      const feature = rowEntries[targetRowIdx] && rowEntries[targetRowIdx].kind === "feature" ? rowEntries[targetRowIdx].feature : null;
      targetDate = safeString((feature as any)?.start || (feature as any)?.end || "");
    } else if (targetEpicId) {
      targetRowIdx = rowEntries.findIndex((r) => r.kind === "epic" && safeString((r as any).epicId) === targetEpicId);
      const win = epicWindows[targetEpicId];
      targetDate = safeString(win?.start || win?.end || "");
    }
    if (targetRowIdx < 0) return;
    const key = `sel:${targetFeatureId}|${targetEpicId}|${targetRowIdx}|${targetDate}|${fitSelectionNonce}`;
    if (autoScrollKeyRef.current === key) return;
    autoScrollKeyRef.current = key;
    const y = Math.max(0, targetRowIdx * rowHeight - rowHeight * 2.5);
    let x = viewportEl.scrollLeft;
    if (targetDate) {
      const pct = toPct(targetDate);
      const targetLeft = pct * trackMinWidthPx - Math.max(80, normalizedLeftWidth * 0.2);
      const maxLeft = Math.max(0, viewportEl.scrollWidth - viewportEl.clientWidth);
      x = Math.max(0, Math.min(maxLeft, targetLeft));
    }
    viewportEl.scrollTo({ left: x, behavior: "smooth" });
    bodyScrollEl.scrollTo({ top: y, behavior: "smooth" });
  }, [
    flagSelectionAutoscroll,
    selectedFeatureId,
    selectedEpicId,
    rowEntries,
    fitSelectionNonce,
    dateRange,
    epicWindows,
    trackMinWidthPx,
    normalizedLeftWidth,
  ]);

  const startLeftRailResize = (startX: number, moveType: "pointermove" | "mousemove", upType: "pointerup" | "mouseup") => {
    if (!onLeftWidthChange) return;
    const startWidth = Math.max(timelineLeftMin, Math.min(timelineLeftMax, Number(leftWidth || 260)));
    setIsResizingLeftRail(true);

    if (resizeMoveRef.current && resizeMoveTypeRef.current) {
      window.removeEventListener(resizeMoveTypeRef.current, resizeMoveRef.current as EventListener);
    }
    if (resizeUpRef.current && resizeUpTypeRef.current) {
      window.removeEventListener(resizeUpTypeRef.current, resizeUpRef.current as EventListener);
    }

    const onMove = (moveEvent: PointerEvent | MouseEvent) => {
      const delta = moveEvent.clientX - startX;
      const nextWidth = Math.round(Math.max(timelineLeftMin, Math.min(timelineLeftMax, startWidth + delta)));
      onLeftWidthChange(nextWidth);
    };
    const onUp = () => {
      setIsResizingLeftRail(false);
      if (resizeMoveRef.current && resizeMoveTypeRef.current) {
        window.removeEventListener(resizeMoveTypeRef.current, resizeMoveRef.current as EventListener);
      }
      if (resizeUpRef.current && resizeUpTypeRef.current) {
        window.removeEventListener(resizeUpTypeRef.current, resizeUpRef.current as EventListener);
      }
      resizeMoveRef.current = null;
      resizeUpRef.current = null;
      resizeMoveTypeRef.current = null;
      resizeUpTypeRef.current = null;
    };

    resizeMoveRef.current = onMove;
    resizeUpRef.current = onUp;
    resizeMoveTypeRef.current = moveType;
    resizeUpTypeRef.current = upType;
    window.addEventListener(moveType, onMove as EventListener);
    window.addEventListener(upType, onUp as EventListener, { once: true });
  };

  const handleLeftRailResizeStart = (event: React.PointerEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    startLeftRailResize(event.clientX, "pointermove", "pointerup");
  };

  const handleLeftRailResizeMouseStart = (event: React.MouseEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    startLeftRailResize(event.clientX, "mousemove", "mouseup");
  };

  const canStartViewportPan = (target: EventTarget | null) => {
    const el = target as HTMLElement | null;
    if (!el) return false;
    if (
      el.closest(
        [
          "button",
          "a",
          "input",
          "select",
          "textarea",
          "label",
          "[role='button']",
          ".roadmap-timeline-bar",
          ".roadmap-timeline-milestone-point",
          ".roadmap-timeline-expand-btn",
          ".roadmap-timeline-label-row-epic-click",
          ".roadmap-timeline-resize-handle",
          ".roadmap-timeline-deps-toggle",
          ".roadmap-timeline-mode-select",
        ].join(",")
      )
    ) return false;
    return true;
  };

  const stopViewportPan = () => {
    const viewportEl = viewportRef.current;
    const pan = panStateRef.current;
    if (viewportEl && pan.pointerId !== null && typeof viewportEl.releasePointerCapture === "function") {
      try {
        viewportEl.releasePointerCapture(pan.pointerId);
      } catch {
        // no-op: pointer may already be released by browser
      }
    }
    panStateRef.current = {
      active: false,
      pointerId: null,
      startX: 0,
      startY: 0,
      startScrollLeft: 0,
      startScrollTop: 0,
    };
    setIsPanning(false);
  };

  const handleViewportPointerDown = (event: React.PointerEvent<HTMLDivElement>) => {
    if (event.button !== 0 || isResizingLeftRail) return;
    if (!canStartViewportPan(event.target)) return;
    const viewportEl = viewportRef.current;
    if (!viewportEl) return;
    panStateRef.current = {
      active: true,
      pointerId: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      startScrollLeft: viewportEl.scrollLeft || 0,
      startScrollTop: bodyScrollRef.current?.scrollTop || 0,
    };
    if (typeof viewportEl.setPointerCapture === "function") viewportEl.setPointerCapture(event.pointerId);
    setIsPanning(true);
    event.preventDefault();
  };

  const handleViewportPointerMove = (event: React.PointerEvent<HTMLDivElement>) => {
    const pan = panStateRef.current;
    if (!pan.active) return;
    const viewportEl = viewportRef.current;
    if (!viewportEl) return;
    const dx = event.clientX - pan.startX;
    const dy = event.clientY - pan.startY;
    viewportEl.scrollLeft = pan.startScrollLeft - dx;
    if (bodyScrollRef.current) {
      bodyScrollRef.current.scrollTop = Math.max(0, pan.startScrollTop - dy);
    }
    event.preventDefault();
  };

  const handleViewportPointerUp = () => stopViewportPan();
  const handleViewportPointerCancel = () => stopViewportPan();

  useEffect(() => {
    return () => stopViewportPan();
  }, []);

  const hasRows = rowEntries.length > 0 && Boolean(dateRange);
  const emptyMessage = useMemo(() => {
    if (!hasRows && searchNeedle) return "No results for current search.";
    if (!hasRows && !featuresRaw.length) return "No data in scope.";
    if (!hasRows) return "No timeline rows available for the current timeframe.";
    return "";
  }, [hasRows, searchNeedle, featuresRaw.length]);

  return (
    <div
      className={`roadmap-timeline-shell${flagMicroPolish ? " is-polished" : ""}${isCompactDensity ? " is-compact" : ""}`}
      style={{ ["--rm-timeline-row-h" as any]: `${rowHeight}px` } as React.CSSProperties}
    >
      <div className={`roadmap-timeline-top${scrolledBody ? " is-scrolled" : ""}`}>
        <div className="roadmap-timeline-top-left">
          <div className="roadmap-timeline-epic">{headerTitle}</div>
          <div className="roadmap-timeline-epic-progress">{headerProgress}%</div>
        </div>
        <div className="roadmap-timeline-top-right">
          <div className="roadmap-timeline-controls">
            {timeframePreset === "custom" && yearWindow ? (
              <div className="roadmap-timeline-granularity">
                <button
                  className="roadmap-timeline-granularity-btn"
                  onClick={() => changeYearWindow(-1, yearWindow.maxStart)}
                  disabled={yearWindow.startIdx <= 0}
                  title="Previous year window"
                >
                  ◀
                </button>
                <button className="roadmap-timeline-granularity-btn is-active" type="button" style={{ cursor: "default" }}>
                  {yearWindow.startYear === yearWindow.endYear
                    ? `${yearWindow.startYear}`
                    : `${yearWindow.startYear} - ${yearWindow.endYear}`}
                </button>
                <button
                  className="roadmap-timeline-granularity-btn"
                  onClick={() => changeYearWindow(1, yearWindow.maxStart)}
                  disabled={yearWindow.startIdx >= yearWindow.maxStart}
                  title="Next year window"
                >
                  ▶
                </button>
              </div>
            ) : null}
            <div className="roadmap-timeline-granularity">
              <button className={`roadmap-timeline-granularity-btn ${granularity === "pi" ? "is-active" : ""}`} onClick={() => { captureGranularityAnchor(); setGranularity("pi"); onGranularityChange?.("pi"); }}>
                PI
              </button>
              <button
                className={`roadmap-timeline-granularity-btn ${granularity === "sprint" ? "is-active" : ""}`}
                onClick={() => { captureGranularityAnchor(); setGranularity("sprint"); onGranularityChange?.("sprint"); }}
                disabled={!hasSprintBands}
                title={hasSprintBands ? "Sprint bands" : "Sprint bands unavailable for current data"}
              >
                Sprint
              </button>
            </div>
            <div className="roadmap-timeline-granularity">
              <button
                className="roadmap-timeline-granularity-btn"
                onClick={() =>
                  setCollapsedEpics((prev) => {
                    const next = { ...prev };
                    for (const e of epicEntries) next[e.epicKey] = false;
                    return next;
                  })
                }
                title="Expand all epics"
              >
                Expand all
              </button>
              <button
                className="roadmap-timeline-granularity-btn"
                onClick={() =>
                  setCollapsedEpics((prev) => {
                    const next = { ...prev };
                    for (const e of epicEntries) next[e.epicKey] = true;
                    return next;
                  })
                }
                title="Collapse all epics"
              >
                Collapse all
              </button>
            </div>
            <label className="roadmap-timeline-deps-toggle" title="Show dependency lines between feature bars">
              <input type="checkbox" checked={showDependencies} onChange={(e) => handleShowDependenciesChange(Boolean(e.target.checked))} />
              <span>Dependencies</span>
            </label>
            <select className="roadmap-timeline-mode-select" value={dependencyLineMode} onChange={(e) => onDependencyLineModeChange?.(e.target.value as any)}>
              <option value="auto">Links: Auto</option>
              <option value="always">Links: Always</option>
            </select>
            {showDependencies && !canRenderDependencies ? <span className="roadmap-timeline-density-note">Links hidden due to density</span> : null}
            {flagTimeControlsPolish ? (
              <>
                <button className="roadmap-timeline-granularity-btn" type="button" onClick={() => onRequestToday?.()}>
                  Today
                </button>
                <button className="roadmap-timeline-granularity-btn" type="button" onClick={() => onRequestFitSelection?.()}>
                  Fit Sel
                </button>
              </>
            ) : null}
          </div>
        </div>
      </div>
      <div
        className={`roadmap-timeline-viewport${isPanning ? " is-panning" : ""}`}
        ref={viewportRef}
        onPointerDown={handleViewportPointerDown}
        onPointerMove={handleViewportPointerMove}
        onPointerUp={handleViewportPointerUp}
        onPointerCancel={handleViewportPointerCancel}
      >
        {!hasRows ? <div className="roadmap-timeline-empty">{emptyMessage}</div> : null}
        {!hasRows ? <div className="roadmap-timeline-skeleton" /> : null}
        <div className="roadmap-timeline-content" style={{ minWidth: `${timelineMinWidthPx}px` }}>
          <div className="roadmap-timeline-milestones" style={{ gridTemplateColumns: `var(--rm-left-width) minmax(${trackMinWidthPx}px, 1fr)` }}>
              <div className="roadmap-timeline-milestones-label">Milestones</div>
              <div
                className="roadmap-timeline-milestones-track"
                onClick={(e) => {
                  if (!onMilestoneRailCreate || !dateRange) return;
                  const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect();
                  const x = Math.max(0, Math.min(rect.width, e.clientX - rect.left));
                  const pct = rect.width > 0 ? x / rect.width : 0;
                  const minTs = dateRange.min.getTime();
                  const maxTs = dateRange.max.getTime();
                  const ts = minTs + pct * Math.max(1, maxTs - minTs);
                  const d = new Date(ts).toISOString().slice(0, 10);
                  onMilestoneRailCreate(d);
                }}
                title={onMilestoneRailCreate ? "Click to create milestone at date" : ""}
              >
                <div className="roadmap-timeline-milestones-line" />
                <div className="roadmap-timeline-milestone-labels">
                  {visibleMilestoneLabels.labels.map((ms) => (
                    <span
                      key={`ms-label-${ms.id}`}
                      className="roadmap-timeline-milestone-label"
                      style={{ left: `${ms.leftPct}%`, top: ms.lane === 0 ? milestoneLaneTop[0] : milestoneLaneTop[1] }}
                    >
                      {ms.label}
                    </span>
                  ))}
                  {visibleMilestoneLabels.hiddenCount > 0 ? (
                    <span className="roadmap-timeline-milestone-overflow">+{visibleMilestoneLabels.hiddenCount}</span>
                  ) : null}
                </div>
                {milestonePoints.length === 0 ? <span className="roadmap-timeline-milestone-empty">No milestones in current window</span> : null}
                {milestoneClusters.map((cluster) => {
                  const left = `${cluster.leftPct}%`;
                  const ms = cluster.points[0];
                  const isCluster = cluster.points.length > 1;
                  const title = isCluster ? `+${cluster.points.length} milestones` : safeString(ms.label);
                  const summaryList = cluster.points.slice(0, 6).map((p) => `• ${escapeHtml(p.label)} (${escapeHtml(fmtDate(p.date))})`).join("<br/>");
                  return (
                    <button
                      key={`ms-${cluster.id}`}
                      className={`roadmap-timeline-milestone-point ${ms.type === "custom" ? "is-custom" : ""} ${milestoneTagClass(ms.tag)}`}
                      style={{ left }}
                      onMouseEnter={(e) =>
                        onShowTooltip(
                          `
                          <div style='font-weight:700;margin-bottom:6px;'>${escapeHtml(title)}</div>
                          <div style='opacity:0.85;'>${escapeHtml(fmtDate(ms.date))}</div>
                          <div style='opacity:0.75;'>${escapeHtml(ms.type === "custom" ? "Custom milestone" : (granularity === "sprint" ? "Sprint boundary" : "PI boundary"))}</div>
                          ${isCluster ? `<div style='opacity:0.78;margin-top:4px;'>${summaryList}</div>` : ""}
                          `,
                          e
                        )
                      }
                      onMouseMove={onMoveTooltip}
                      onMouseLeave={onHideTooltip}
                      onClick={(e) => {
                        e.stopPropagation();
                        if (isCluster) {
                          onMilestoneClick?.({ id: safeString(ms.id).replace(/^custom-/, "") });
                          return;
                        }
                        onMilestoneClick?.({ id: safeString(ms.id).replace(/^custom-/, "") });
                      }}
                      type="button"
                    >
                      {isCluster ? <span className="roadmap-timeline-milestone-cluster">+{cluster.points.length - 1}</span> : null}
                    </button>
                  );
                })}
              </div>
          </div>
          <div className="roadmap-timeline-bands-row" style={{ gridTemplateColumns: `var(--rm-left-width) minmax(${trackMinWidthPx}px, 1fr)` }}>
            <div className="roadmap-timeline-bands-label">{granularity === "sprint" ? "Sprint" : "PI"}</div>
            <div className="roadmap-timeline-bands-track">
              {activeBandsInWindow.map((band) => {
                const left = `${toPct(band.start) * 100}%`;
                const right = `${toPct(band.end) * 100}%`;
                const widthPct = Math.max(1, (parseFloat(right) - parseFloat(left)) || 1);
                return (
                  <div
                    key={`band-row-${band.id}`}
                    className="roadmap-timeline-band"
                    style={{ left, width: `${widthPct}%` }}
                  >
                    <span>{safeString(band.label)}</span>
                  </div>
                );
              })}
            </div>
          </div>
          <div
            className={`roadmap-timeline-body${isResizingLeftRail ? " is-resizing" : ""}`}
            ref={bodyScrollRef}
            onScroll={(e) => setScrolledBody((e.currentTarget.scrollTop || 0) > 0)}
            style={{ gridTemplateColumns: `var(--rm-left-width) minmax(${trackMinWidthPx}px, 1fr)` }}
          >
            <div className="roadmap-timeline-label-col">
          {rowEntries.map((row, idx) => {
            if (row.kind === "epic") {
              const expanded = !collapsedEpics[row.epicKey];
              const featCount = Number(featureCountByEpic[row.epicKey] || 0);
              const isSelEpic = safeString(selectedEpicId || "") === safeString(row.epicId);
              const epicColors = epicColorMap[safeString(row.epicId)] || epicColorFromId(safeString(row.epicId), safeString(row.title));
              const teamFlags = epicTeamFlags[row.epicKey] || { labels: [], overflow: 0 };
              return (
                <div
                  className={`roadmap-timeline-label-row roadmap-timeline-label-row-epic roadmap-timeline-label-row-epic-click ${isSelEpic ? "is-selected" : ""}${focusEpicId && safeString(row.epicId) !== safeString(focusEpicId) ? " is-dimmed" : ""}`}
                  key={`label-epic-${row.epicId}-${idx}`}
                  onClick={() => setCollapsedEpics((prev) => ({ ...prev, [row.epicKey]: expanded }))}
                  style={{ ["--rm-epic-soft" as any]: epicColors.soft, ["--rm-epic-border" as any]: epicColors.border, height: `${rowHeight}px` } as React.CSSProperties}
                >
                  <button
                    className={`roadmap-timeline-expand-btn ${expanded ? "is-expanded" : "is-collapsed"}`}
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      setCollapsedEpics((prev) => ({ ...prev, [row.epicKey]: expanded }));
                    }}
                  >
                    {expanded ? "▾" : "▸"}
                  </button>
                  <span className="roadmap-timeline-label-dot" style={{ background: epicColors.solid }} />
                  <div className="roadmap-timeline-label-main">
                    <span className="roadmap-timeline-label-title">{safeString(row.title)}</span>
                    {showLabelMeta ? (
                      <span className="roadmap-timeline-label-meta">
                        {groupNameLabel} · {featCount}
                        {showRoi ? ` · BV ${formatWhole(Number((row as any).businessValueSum || 0))}` : ""}
                      </span>
                    ) : null}
                  </div>
                  {showLabelMeta && teamFlags.labels.length ? (
                    <span className="roadmap-timeline-team-flags">
                      {teamFlags.labels.map((team) => (
                        <span key={`${row.epicKey}-${team}`} className="roadmap-timeline-team-flag">
                          {teamFlagLabel(team)}
                        </span>
                      ))}
                      {teamFlags.overflow > 0 ? (
                        <span className="roadmap-timeline-team-flag is-overflow">
                          +{teamFlags.overflow}
                        </span>
                      ) : null}
                    </span>
                  ) : null}
                </div>
              );
            }
            const isSelFeature = safeString(selectedFeatureId || "") === safeString(row.feature.id);
            const fId = safeString(row.feature.id);
            const inboundCount = Number(featureDepsById.inbound[fId] || 0);
            const outboundCount = Number(featureDepsById.outbound[fId] || 0);
            const invest = investmentMeta((row.feature as any).investmentDimension);
            const featureColors = epicColorMap[safeString(row.epicId)] || epicColorFromId(safeString(row.epicId), safeString(row.feature.epicTitle || ""));
            return (
              <div
                className={`roadmap-timeline-label-row ${isSelFeature ? "is-selected" : ""}${flagDependencyFocus && hoveredFeatureId && !linkedFeatureIds.has(fId) ? " is-dimmed" : ""}${focusEpicId && safeString(row.epicId) !== safeString(focusEpicId) ? " is-dimmed" : ""}`}
                key={`label-feature-${row.feature.id}-${idx}`}
                style={{ ["--rm-epic-soft" as any]: featureColors.soft, height: `${rowHeight}px` } as React.CSSProperties}
              >
                <span className={`roadmap-investment-chip is-${invest.tone}`} title={`Investment: ${invest.label}`}>
                  <span aria-hidden>{invest.icon}</span>
                  {invest.code ? <span>{invest.code}</span> : null}
                </span>
                <span className="roadmap-timeline-label-title">{safeString(row.feature.title)}</span>
                {showLabelMeta ? (
                  <span className="roadmap-timeline-label-meta">
                    {safeString(row.feature.team || "")}
                    {showRoi ? ` · BV ${formatWhole(Number((row.feature as any).businessValue || 0))}` : ""}
                  </span>
                ) : null}
                {showDependencies && showLabelMeta ? (
                  <span className="roadmap-timeline-dep-counts">
                    <span className="roadmap-timeline-dep-count">↗ {outboundCount}</span>
                    <span className="roadmap-timeline-dep-count">↘ {inboundCount}</span>
                  </span>
                ) : null}
              </div>
            );
          })}
            </div>
            <div
              className={`roadmap-timeline-resize-handle${isResizingLeftRail ? " is-active" : ""}`}
              role="separator"
              aria-orientation="vertical"
              aria-label="Resize epic column"
              title="Drag to resize epic column"
              onPointerDown={handleLeftRailResizeStart}
              onMouseDown={handleLeftRailResizeMouseStart}
            />
            <div className="roadmap-timeline-track-col" style={{ height: `${Math.max(1, totalRows) * rowHeight}px` }}>
          {canRenderDependencies ? (
            <svg
              className="roadmap-timeline-links"
              width="100%"
              height={Math.max(1, totalRows) * rowHeight}
              style={{ top: `0px`, height: `${Math.max(1, totalRows) * rowHeight}px` }}
            >
              <defs>
                <marker id="rmTimelineArrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
                  <path d="M0,0 L8,4 L0,8 z" fill="var(--rm-link-strong)" />
                </marker>
              </defs>
              {links.map((ln, idx) => {
                const s = idToRow[String(ln.sourceId)];
                const t = idToRow[String(ln.targetId)];
                if (s === undefined || t === undefined) return null;
                const source = rowEntries[s];
                const target = rowEntries[t];
                if (source.kind !== "feature" || target.kind !== "feature") return null;
                const x1 = toPct(source.feature.end) * 100;
                const x2 = toPct(target.feature.start) * 100;
                const y1 = s * rowHeight + rowHeight / 2;
                const y2 = t * rowHeight + rowHeight / 2;
                const mid = (x1 + x2) / 2;
                const d = `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`;
                const sourceId = safeString(ln.sourceId);
                const targetId = safeString(ln.targetId);
                const depKey = `${sourceId}::${targetId}`;
                const isConnected = hoveredFeatureId && (hoveredFeatureId === sourceId || hoveredFeatureId === targetId);
                const sourceEpicId = safeString((source as any).epicId || "");
                const targetEpicId = safeString((target as any).epicId || "");
                const outsideFocus = focusEpicId && sourceEpicId !== safeString(focusEpicId) && targetEpicId !== safeString(focusEpicId);
                const isDimmed = (flagDependencyFocus && hoveredFeatureId && !isConnected) || Boolean(outsideFocus);
                const isFocused = hoveredDependencyKey === depKey || isConnected;
                return (
                  <path
                    key={`ln-${idx}-${ln.sourceId}-${ln.targetId}`}
                    d={d}
                    className={`roadmap-timeline-link-path${isFocused ? " is-focused" : ""}${isDimmed ? " is-dimmed" : ""}`}
                    markerEnd="url(#rmTimelineArrow)"
                    onMouseEnter={() => setHoveredDependencyKey(depKey)}
                    onMouseLeave={() => setHoveredDependencyKey("")}
                    onClick={() => onDependencyClick?.(sourceId, targetId)}
                  />
                );
              })}
            </svg>
          ) : null}

          <div className="roadmap-timeline-grid-overlay">
            {activeBandsInWindow.map((band) => (
              <span
                key={`grid-${band.id}`}
                className="roadmap-timeline-gridline"
                style={{ left: `${toPct(band.start) * 100}%` }}
              />
            ))}
          </div>

          {todayPct !== null ? (
            <div className="roadmap-timeline-today-line" style={{ left: `${todayPct * 100}%` }}>
              <span className="roadmap-timeline-today-dot" />
            </div>
          ) : null}

          {rowEntries.map((row, idx) => {
            if (row.kind === "epic") {
              const window = epicWindows[row.epicId];
              const progress = clamp(Number(row.progress || 0), 0, 1);
              const isSelEpic = safeString(selectedEpicId || "") === safeString(row.epicId);
              const epicColors = epicColorMap[safeString(row.epicId)] || epicColorFromId(safeString(row.epicId), safeString(row.title));
              return (
                <div
                  className={`roadmap-timeline-row roadmap-timeline-row-epic ${isSelEpic ? "is-selected" : ""}${focusEpicId && safeString(row.epicId) !== safeString(focusEpicId) ? " is-dimmed" : ""}`}
                  style={{ top: `${idx * rowHeight}px`, height: `${rowHeight}px` }}
                  key={`bar-epic-${row.epicId}-${idx}`}
                >
                  {window ? (
                <div
                  className={`roadmap-timeline-bar roadmap-timeline-bar-epic ${isSelEpic ? "is-selected" : ""}${flagEpicColorFinalize && epicColors.isNeutral ? " is-neutral" : ""}`}
                  style={{
                        left: `${toPct(window.start) * 100}%`,
                        width: `${Math.max(1.2, (toPct(window.end) - toPct(window.start)) * 100)}%`,
                        ["--rm-epic-solid" as any]: epicColors.solid,
                        ["--rm-epic-soft" as any]: epicColors.soft,
                        ["--rm-epic-border" as any]: epicColors.border,
                        ["--rm-epic-progress" as any]: epicColors.progress,
                        ["--rm-epic-text" as any]: epicColors.textOnSolid,
                      } as React.CSSProperties}
                      onMouseEnter={(e) =>
                        onShowTooltip(
                          `<div class='rm-tip'>
                            <div class='rm-tip-title'>${escapeHtml(groupTagLabel)}: ${escapeHtml(safeString(row.title))}</div>
                            <div class='rm-tip-row'><span>#${escapeHtml(safeString(row.epicId))}</span><strong>${Math.round(progress * 100)}%</strong></div>
                            <div class='rm-tip-row'><span>${escapeHtml(fmtDate(window.start))}</span><strong>${escapeHtml(fmtDate(window.end))}</strong></div>
                            ${showRoi ? `<div class='rm-tip-row'><span>Business Value</span><strong>${formatWhole(Number((row as any).businessValueSum || 0))}</strong></div>` : ""}
                          </div>`,
                          e
                        )
                      }
                      onMouseMove={onMoveTooltip}
                      onMouseLeave={onHideTooltip}
                      onClick={() =>
                        onEpicClick({
                          id: safeString(row.epicId),
                          title: safeString(row.title),
                          businessValueSum: Number((row as any).businessValueSum || 0),
                          featureCount: Number((row as any).featureCount || 0),
                          progress: progress,
                          epicUrl: safeString((row as any).epicUrl || ""),
                        })
                      }
                    >
                      {showProgress ? <span className="roadmap-timeline-bar-progress roadmap-timeline-bar-progress-epic" style={{ width: `${progress * 100}%` }} /> : null}
                      <span className="roadmap-timeline-bar-main">
                        <span className="roadmap-timeline-bar-title">
                          <span className="roadmap-timeline-glyph" aria-hidden>
                            <TimelineGlyph kind={isApplicationGrouping ? "application" : "epic"} />
                          </span>
                          {safeString(row.title)}
                        </span>
                        <span className="roadmap-timeline-bar-badges">
                          {showProgress ? <span className="roadmap-timeline-bar-badge">{Math.round(progress * 100)}%</span> : null}
                          {showSecondaryBadges && showRoi ? <span className="roadmap-timeline-bar-badge">BV {formatWhole(Number((row as any).businessValueSum || 0))}</span> : null}
                        </span>
                      </span>
                    </div>
                  ) : null}
                </div>
              );
            }

            const f = row.feature;
            const left = `${toPct(f.start) * 100}%`;
            const right = `${toPct(f.end) * 100}%`;
            const widthPct = Math.max(1.2, (parseFloat(right) - parseFloat(left)) || 1.2);
            const progress = clamp(Number(f.progress || 0), 0, 1);
            const invest = investmentMeta((f as any).investmentDimension);
            const featureColors = epicColorMap[safeString(row.epicId)] || epicColorFromId(safeString(row.epicId), safeString(row.feature.epicTitle || ""));
            const tip = `<div class='rm-tip'>
              <div class='rm-tip-title'>Feature: ${escapeHtml(safeString(f.title))}</div>
              <div class='rm-tip-row'><span>#${escapeHtml(safeString(f.id))}</span><strong>${Math.round(progress * 100)}%</strong></div>
              <div class='rm-tip-row'><span>${escapeHtml(fmtDate(f.start))}</span><strong>${escapeHtml(fmtDate(f.end))}</strong></div>
              <div class='rm-tip-row'><span>${escapeHtml(safeString(f.team || ""))}</span><strong>${escapeHtml(safeString((f as any).program || ""))}</strong></div>
              <div class='rm-tip-row'><span>Investment</span><strong>${escapeHtml(invest.label)}</strong></div>
              ${showRoi ? `<div class='rm-tip-row'><span>Business Value</span><strong>${formatWhole(Number((f as any).businessValue || 0))}</strong></div>` : ""}
            </div>`;

            const isSelFeature = safeString(selectedFeatureId || "") === safeString(f.id);
            const isLinked = !hoveredFeatureId || linkedFeatureIds.has(safeString(f.id));
            const outboundCount = Number(featureDepsById.outbound[safeString(f.id)] || 0);
            const inboundCount = Number(featureDepsById.inbound[safeString(f.id)] || 0);
            return (
              <div
                className={`roadmap-timeline-row ${isSelFeature ? "is-selected" : ""}${flagDependencyFocus && hoveredFeatureId && !isLinked ? " is-dimmed" : ""}${flagDependencyFocus && hoveredFeatureId && isLinked ? " is-focus" : ""}${focusEpicId && safeString(row.epicId) !== safeString(focusEpicId) ? " is-dimmed" : ""}`}
                style={{ top: `${idx * rowHeight}px`, height: `${rowHeight}px` }}
                key={`bar-feature-${f.id}-${idx}`}
              >
                <div
                  className={`roadmap-timeline-bar ${isSelFeature ? "is-selected" : ""}${flagEpicColorFinalize && featureColors.isNeutral ? " is-neutral" : ""}`}
                  style={{
                    left,
                    width: `${widthPct}%`,
                    ["--rm-epic-solid" as any]: featureColors.solid,
                    ["--rm-epic-soft" as any]: featureColors.soft,
                    ["--rm-epic-border" as any]: featureColors.border,
                    ["--rm-epic-progress" as any]: featureColors.progress,
                    ["--rm-epic-text" as any]: featureColors.textOnSolid,
                  } as React.CSSProperties}
                  onMouseEnter={(e) => onShowTooltip(tip, e)}
                  onMouseMove={onMoveTooltip}
                  onMouseLeave={onHideTooltip}
                  onMouseOver={() => setHoveredFeatureId(safeString(f.id))}
                  onMouseOut={() => setHoveredFeatureId("")}
                  onClick={() => onFeatureClick(f)}
                >
                  {showProgress ? <span className="roadmap-timeline-bar-progress" style={{ width: `${progress * 100}%` }} /> : null}
                  <span className="roadmap-timeline-bar-main">
                    <span className="roadmap-timeline-bar-title">
                      <span className="roadmap-timeline-glyph" aria-hidden>
                        <TimelineGlyph kind="feature" />
                      </span>
                      <span className={`roadmap-investment-chip is-${invest.tone}`} title={`Investment: ${invest.label}`}>
                        <span aria-hidden>{invest.icon}</span>
                        {invest.code ? <span>{invest.code}</span> : null}
                      </span>
                      {safeString(f.title)}
                    </span>
                    <span className="roadmap-timeline-bar-badges">
                      {showSecondaryBadges && showDependencies ? <span className="roadmap-timeline-dep-count">↗ {outboundCount}</span> : null}
                      {showSecondaryBadges && showDependencies ? <span className="roadmap-timeline-dep-count">↘ {inboundCount}</span> : null}
                      {showProgress ? <span className="roadmap-timeline-bar-badge">{Math.round(progress * 100)}%</span> : null}
                      {showSecondaryBadges && showRoi ? <span className="roadmap-timeline-bar-badge">BV {formatWhole(Number((f as any).businessValue || 0))}</span> : null}
                    </span>
                  </span>
                </div>
              </div>
            );
          })}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
