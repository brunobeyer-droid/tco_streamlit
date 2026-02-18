import React, { useMemo, useRef, useState } from "react";
import { Streamlit } from "streamlit-component-lib";
import { toPng } from "html-to-image";
import { createPortal } from "react-dom";
import { RoadmapApp, RoadmapCell, RoadmapData, RoadmapEventEnvelope, RoadmapItem, RoadmapTimelineMilestone, RoadmapUiState } from "./types";
import { clamp, escapeHtml, safeString } from "./utils";
import { ROADMAP_LAYOUT } from "./roadmapTheme";
import { BoardHeader } from "./components/BoardHeader";
import { Grid } from "./components/Grid";
import { EpicTimeline } from "./components/EpicTimeline";
import { RoadmapDrawer } from "./components/RoadmapDrawer";

const headerHeight = ROADMAP_LAYOUT.HEADER_HEIGHT;
const leftWidth = ROADMAP_LAYOUT.LEFT_WIDTH;
const minPiWidth = 180;
const timelineLeftMin = ROADMAP_LAYOUT.LEFT_WIDTH_MIN;
const timelineLeftMax = ROADMAP_LAYOUT.LEFT_WIDTH_MAX;
const baseChipHeight = ROADMAP_LAYOUT.CHIP_HEIGHT.comfortable;
const compactChipHeight = ROADMAP_LAYOUT.CHIP_HEIGHT.compact;
const baseGap = ROADMAP_LAYOUT.CHIP_GAP.comfortable;
const compactGap = ROADMAP_LAYOUT.CHIP_GAP.compact;
const basePad = ROADMAP_LAYOUT.ROW_PAD.comfortable;
const compactPad = ROADMAP_LAYOUT.ROW_PAD.compact;
const windowSizeFallback = 4;
const compactToolbarMaxWidthPx = 1200;
const zoomMin = ROADMAP_LAYOUT.ZOOM.min;
const zoomMax = ROADMAP_LAYOUT.ZOOM.max;
const tooltipOffsetPx = 16;
const tooltipMarginPx = 8;
const sortableControls = ["roi_bv", "demand", "name", "progress", "feature_count"] as const;
const sortDirections = ["asc", "desc"] as const;

type TooltipState = {
  visible: boolean;
  x: number;
  y: number;
  html: string;
};

type SelectionState =
  | { type: "feature"; featureId: string; appId: string; piId: string }
  | { type: "cell"; appId: string; piId: string }
  | null;

type SortBy = (typeof sortableControls)[number];
type SortDir = (typeof sortDirections)[number];

function normalizeSortBy(value: any): SortBy {
  return sortableControls.includes(value as SortBy) ? (value as SortBy) : "roi_bv";
}

function normalizeSortDir(value: any): SortDir {
  return sortDirections.includes(value as SortDir) ? (value as SortDir) : "desc";
}

function normalizeTimelineLeftWidth(value: any): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return leftWidth;
  return Math.round(clamp(parsed, timelineLeftMin, timelineLeftMax));
}

function sortMultiplier(dir: SortDir): number {
  return dir === "asc" ? 1 : -1;
}

function compareNumber(a: number, b: number, dir: SortDir): number {
  return (a - b) * sortMultiplier(dir);
}

function compareText(a: string, b: string, dir: SortDir): number {
  return a.localeCompare(b) * sortMultiplier(dir);
}

function normalizeIdToken(value: any): string {
  const raw = safeString(value || "");
  if (!raw) return "";
  if (/^-?\d+(\.0+)?$/.test(raw)) {
    const n = Number(raw);
    if (Number.isFinite(n)) return String(Math.trunc(n));
  }
  return raw;
}

function normalizeProgressRatio(value: any): number | null {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  if (n > 1 && n <= 100) return clamp(n / 100, 0, 1);
  return clamp(n, 0, 1);
}

function resolveFeatureUrl(featureId: any, featureUrlRaw: any, epicUrlRaw: any): string {
  const featureUrl = safeString(featureUrlRaw || "").trim();
  if (featureUrl && !["none", "nan", "null"].includes(featureUrl.toLowerCase())) return featureUrl;
  const featureIdNorm = normalizeIdToken(featureId);
  const epicUrl = safeString(epicUrlRaw || "").trim();
  if (!featureIdNorm || !epicUrl) return "";
  const match = epicUrl.match(/^((?:https:\/\/dev\.azure\.com\/[^/]+\/[^/?#]+))\/_workitems\/edit\/[^/?#]+/i);
  if (!match) return "";
  return `${match[1]}/_workitems/edit/${featureIdNorm}`;
}

function eventId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function isCompactToolbarViewport(): boolean {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") return false;
  return window.matchMedia(`(max-width: ${compactToolbarMaxWidthPx}px)`).matches;
}

export function RoadmapBoard({ data: rawData, height }: { data: RoadmapData; height: number }) {
  const data = (rawData as RoadmapData) || {};
  const meta = data.meta || {};
  const pis = Array.isArray(data.pis) ? data.pis : [];
  const sourceApps = Array.isArray(data.apps) ? data.apps : [];
  const sourceCells = data.cells && typeof data.cells === "object" ? data.cells : {};
  const timeline = data.timeline || null;
  const hasTimeline = Boolean(timeline?.epic && Array.isArray(timeline?.features) && timeline.features!.length > 0);
  const capabilities = meta.capabilities || {};
  const featureFlags = (meta as any).feature_flags || {};
  const flagSelectionAutoscroll = Boolean((featureFlags as any).roadmap_flag_selection_autoscroll_v3 ?? false);
  const flagDependencyFocus = Boolean((featureFlags as any).roadmap_flag_dependency_focus_v3 ?? false);
  const flagMilestoneCluster = Boolean((featureFlags as any).roadmap_flag_milestone_cluster_v3 ?? false);
  const flagEpicColorFinalize = Boolean((featureFlags as any).roadmap_flag_epic_color_finalize_v3 ?? false);
  const flagTimeControlsPolish = Boolean((featureFlags as any).roadmap_flag_time_controls_polish_v3 ?? false);
  const flagMicroPolish = Boolean((featureFlags as any).roadmap_flag_micro_polish_v3 ?? false);
  const flagMilestoneFeedback = Boolean((featureFlags as any).roadmap_flag_milestone_feedback_v4 ?? false);
  const canGroupBy = (capabilities as any).configureGroupBy !== false;
  const canSort = (capabilities as any).configureSort !== false;
  const canShowRoi = (capabilities as any).showRoi !== false;
  const windowMeta = meta.window || {};
  const windowSize = windowMeta.size ?? windowSizeFallback;
  const uiDefaults = (meta as any).ui_defaults || {};
  const initialUi: RoadmapUiState = {
    view: uiDefaults.view === "timeline" ? "timeline" : "board",
    density: uiDefaults.density === "compact" ? "compact" : "comfortable",
    timeframePreset: uiDefaults.timeframePreset === "3m" || uiDefaults.timeframePreset === "6m" || uiDefaults.timeframePreset === "1y" || uiDefaults.timeframePreset === "custom" ? uiDefaults.timeframePreset : "fit",
    granularity: uiDefaults.granularity === "month" || uiDefaults.granularity === "sprint" ? uiDefaults.granularity : "pi",
    zoom: clamp(Number(uiDefaults.zoom || 1), zoomMin, zoomMax),
    timeWindowStart: safeString(uiDefaults.timeWindowStart || ""),
    timeWindowEnd: safeString(uiDefaults.timeWindowEnd || ""),
    fitSelectionNonce: Number(uiDefaults.fitSelectionNonce || 0),
    todayAnchorToken: Number(uiDefaults.todayAnchorToken || 0),
    hoveredFeatureId: safeString(uiDefaults.hoveredFeatureId || ""),
    dependencyFocus: (uiDefaults.dependencyFocus && typeof uiDefaults.dependencyFocus === "object")
      ? {
          sourceId: safeString((uiDefaults.dependencyFocus as any).sourceId || ""),
          targetId: safeString((uiDefaults.dependencyFocus as any).targetId || ""),
        }
      : null,
    timelineLeftWidth: normalizeTimelineLeftWidth(uiDefaults.timelineLeftWidth ?? leftWidth),
    groupBy: (String(meta.rows_by || "").toLowerCase().includes("epic") ? "epics" : "applications"),
    showDependencies: uiDefaults.showDependencies === true,
    showProgress: uiDefaults.showProgress !== false,
    showRoi: Boolean(uiDefaults.showRoi ?? false),
    dependencyLineMode: uiDefaults.dependencyLineMode === "always" ? "always" : "auto",
    searchQuery: safeString(uiDefaults.searchQuery || ""),
    sortBy: normalizeSortBy(uiDefaults.sortBy),
    sortDir: normalizeSortDir(uiDefaults.sortDir),
    selection: { kind: "none" },
    drawer: "closed",
  };

  const rootRef = useRef<HTMLDivElement>(null);
  const toolbarRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLDivElement>(null);
  const timelineExportRef = useRef<HTMLDivElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const tooltipAnchorRef = useRef<{ x: number; y: number }>({ x: 0, y: 0 });
  const boardPanRef = useRef<{
    active: boolean;
    startX: number;
    startScrollLeft: number;
    moved: boolean;
  }>({ active: false, startX: 0, startScrollLeft: 0, moved: false });
  const boardPanSuppressClickRef = useRef(false);
  const boardMouseMoveRef = useRef<((e: MouseEvent) => void) | null>(null);
  const boardMouseUpRef = useRef<((e: MouseEvent) => void) | null>(null);

  const [hoveredApp, setHoveredApp] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<TooltipState>({ visible: false, x: 0, y: 0, html: "" });
  const [ui, setUi] = useState<RoadmapUiState>(initialUi);
  const [windowStart, setWindowStart] = useState(0);
  const [selection, setSelection] = useState<SelectionState>(null);
  const [moreOpen, setMoreOpen] = useState(false);
  const [uiError, setUiError] = useState<string | null>(null);
  const [containerWidth, setContainerWidth] = useState(0);
  const [boardOffsetTop, setBoardOffsetTop] = useState(0);
  const [windowAnim, setWindowAnim] = useState<"slide-left" | "slide-right" | null>(null);
  const [timelineYearWindowStart, setTimelineYearWindowStart] = useState<number | null>(null);
  const [timelineSelectedFeatureId, setTimelineSelectedFeatureId] = useState<string>("");
  const [timelineSelectedEpicId, setTimelineSelectedEpicId] = useState<string>("");
  const [timelineTodayAnchorToken, setTimelineTodayAnchorToken] = useState(0);
  const [isCompactToolbar, setIsCompactToolbar] = useState<boolean>(isCompactToolbarViewport);
  const [focusMode, setFocusMode] = useState(false);
  const [isBoardPanning, setIsBoardPanning] = useState(false);
  const [milestoneDraftDate, setMilestoneDraftDate] = useState("");
  const [timelineSelectedEpicSnapshot, setTimelineSelectedEpicSnapshot] = useState<{
    id: string;
    title: string;
    businessValueSum?: number;
    featureCount?: number;
    progress?: number;
    epicUrl?: string;
  } | null>(null);
  const [draftMilestones, setDraftMilestones] = useState<RoadmapTimelineMilestone[]>([]);
  const [baselineMilestones, setBaselineMilestones] = useState<RoadmapTimelineMilestone[]>([]);
  const [milestoneSyncMsg, setMilestoneSyncMsg] = useState<string>("");
  const [milestoneLastSaveResult, setMilestoneLastSaveResult] = useState<{
    created: number;
    deleted: number;
    failed: number;
    message: string;
  } | null>(null);
  const [milestoneItemStatus, setMilestoneItemStatus] = useState<Record<string, "saving" | "saved" | "failed">>({});
  const [milestoneUndoDelete, setMilestoneUndoDelete] = useState<{ id: string; label: string; date: string } | null>(null);
  const milestonePendingRef = useRef<null | "save">(null);
  const milestonePendingBatchRef = useRef<{ creates: number; deletes: number }>({ creates: 0, deletes: 0 });
  const milestonePendingSavedDraftRef = useRef<RoadmapTimelineMilestone[]>([]);
  const milestoneMsgTimerRef = useRef<number | null>(null);
  const milestoneItemStatusTimerRef = useRef<number | null>(null);
  const lastWindowStart = useRef<number | null>(null);

  React.useEffect(() => {
    Streamlit.setFrameHeight(height + 20);
  }, [height]);

  const emit = React.useCallback((type: RoadmapEventEnvelope["type"], payload?: Record<string, unknown>) => {
    Streamlit.setComponentValue({
      type,
      requestId: eventId(),
      payload: payload || {},
    } as RoadmapEventEnvelope);
  }, []);

  React.useEffect(() => {
    if (!scrollRef.current) return;
    const observer = new ResizeObserver((entries) => {
      const width = entries[0]?.contentRect?.width;
      if (typeof width === "number") setContainerWidth(width);
    });
    observer.observe(scrollRef.current);
    return () => observer.disconnect();
  }, []);

  React.useEffect(() => {
    if (ui.view === "timeline" && !hasTimeline) {
      setUi((prev) => ({ ...prev, view: "board" }));
    }
  }, [ui.view, hasTimeline]);

  React.useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return;
    const mql = window.matchMedia(`(max-width: ${compactToolbarMaxWidthPx}px)`);
    const onChange = (event: MediaQueryListEvent) => {
      setIsCompactToolbar(event.matches);
    };
    setIsCompactToolbar(mql.matches);
    if (typeof mql.addEventListener === "function") {
      mql.addEventListener("change", onChange);
      return () => mql.removeEventListener("change", onChange);
    }
    mql.onchange = onChange;
    return () => {
      mql.onchange = null;
    };
  }, []);

  const serverMilestones = useMemo(
    () => (Array.isArray(timeline?.milestones) ? (timeline!.milestones as RoadmapTimelineMilestone[]) : []),
    [timeline]
  );
  const serverMilestoneSignature = useMemo(
    () => JSON.stringify(serverMilestones.map((m) => [safeString(m.id), safeString(m.date), safeString(m.label)])),
    [serverMilestones]
  );
  React.useEffect(() => {
    setBaselineMilestones(serverMilestones);
    setDraftMilestones(serverMilestones);
    setMilestoneUndoDelete(null);
    if (milestonePendingRef.current) {
      if (milestoneMsgTimerRef.current) {
        window.clearTimeout(milestoneMsgTimerRef.current);
        milestoneMsgTimerRef.current = null;
      }
      milestonePendingRef.current = null;
      const c = Number(milestonePendingBatchRef.current.creates || 0);
      const d = Number(milestonePendingBatchRef.current.deletes || 0);
      setMilestoneLastSaveResult({
        created: c,
        deleted: d,
        failed: 0,
        message: `Saved: ${c} created, ${d} deleted`,
      });
      setMilestoneSyncMsg(`Saved: ${c} created, ${d} deleted`);
      if (milestoneItemStatusTimerRef.current) window.clearTimeout(milestoneItemStatusTimerRef.current);
      milestoneItemStatusTimerRef.current = window.setTimeout(() => {
        setMilestoneItemStatus({});
      }, 2600);
    }
  }, [serverMilestoneSignature, serverMilestones]); // eslint-disable-line react-hooks/exhaustive-deps

  React.useEffect(() => {
    const msStatus = (meta as any)?.status?.milestones;
    if (!flagMilestoneFeedback || !msStatus || typeof msStatus !== "object") return;
    if (milestoneMsgTimerRef.current) {
      window.clearTimeout(milestoneMsgTimerRef.current);
      milestoneMsgTimerRef.current = null;
    }
    const created = Number((msStatus as any).created || 0);
    const deleted = Number((msStatus as any).deleted || 0);
    const failed = Number((msStatus as any).failed || 0);
    const message = safeString((msStatus as any).message || "") || `Saved: ${created} created, ${deleted} deleted`;
    setMilestoneLastSaveResult({ created, deleted, failed, message });
    if (message) setMilestoneSyncMsg(message);
  }, [meta, flagMilestoneFeedback]);

  React.useEffect(() => {
    return () => {
      if (milestoneMsgTimerRef.current) window.clearTimeout(milestoneMsgTimerRef.current);
      if (milestoneItemStatusTimerRef.current) window.clearTimeout(milestoneItemStatusTimerRef.current);
    };
  }, []);

  React.useEffect(() => {
    const groupFromMeta = String(meta.rows_by || "").toLowerCase().includes("epic") ? "epics" : "applications";
    setUi((prev) => (prev.groupBy === groupFromMeta ? prev : { ...prev, groupBy: groupFromMeta as any }));
  }, [meta.rows_by]);

  React.useEffect(() => {
    const toolbarEl = toolbarRef.current;
    if (!toolbarEl) return;
    const updateOffset = () => {
      const toolbarHeight = toolbarEl?.offsetHeight ?? 0;
      setBoardOffsetTop(toolbarHeight);
    };
    updateOffset();
    const obs = new ResizeObserver(updateOffset);
    obs.observe(toolbarEl);
    return () => obs.disconnect();
  }, []);

  const currentPiInfo = useMemo(() => {
    const today = new Date();
    const today0 = new Date(today.getFullYear(), today.getMonth(), today.getDate());
    const parseDate = (val?: string | number | null) => {
      if (val === null || val === undefined || val === "") return null;
      if (typeof val === "number") {
        const d = new Date(val);
        return Number.isNaN(d.getTime()) ? null : d;
      }
      const d = new Date(val);
      return Number.isNaN(d.getTime()) ? null : d;
    };
    let currentPiId: string | null = null;
    let anyDates = false;
    let closestIndex = -1;
    let closestDiff = Number.POSITIVE_INFINITY;
    pis.forEach((pi, idx) => {
      const start =
        parseDate(pi.start_date) ||
        parseDate((pi as any).PI_START_DATE) ||
        parseDate((pi as any).start_date) ||
        parseDate(pi.start_ts) ||
        parseDate((pi as any).start);
      const end =
        parseDate(pi.end_date) ||
        parseDate((pi as any).PI_END_DATE) ||
        parseDate((pi as any).end_date) ||
        parseDate(pi.end_ts) ||
        parseDate((pi as any).end);
      if (start && end && end.getTime() > start.getTime()) {
        anyDates = true;
        if (today0 >= start && today0 <= end) {
          currentPiId = pi.id;
        }
        const diff = Math.abs(today0.getTime() - start.getTime());
        if (diff < closestDiff) {
          closestDiff = diff;
          closestIndex = idx;
        }
      }
    });
    return { currentPiId, allDatesMissing: !anyDates, closestIndex, today0 };
  }, [pis]);

  const initialWindowIndex = useMemo(() => {
    if (currentPiInfo.currentPiId) {
      const idx = pis.findIndex((pi) => pi.id === currentPiInfo.currentPiId);
      if (idx >= 0) return idx;
    }
    if (currentPiInfo.closestIndex >= 0) return currentPiInfo.closestIndex;
    if (!windowMeta.startPiId) return 0;
    const idx = pis.findIndex((pi) => pi.id === windowMeta.startPiId);
    return idx >= 0 ? idx : 0;
  }, [pis, windowMeta.startPiId, currentPiInfo]);

  const windowInitialized = useRef(false);
  React.useEffect(() => {
    if (windowInitialized.current || !pis.length) return;
    const start = Math.min(Math.max(0, initialWindowIndex), Math.max(0, pis.length - windowSize));
    setWindowStart(start);
    windowInitialized.current = true;
  }, [pis.length, initialWindowIndex, windowSize]);

  React.useEffect(() => {
    windowInitialized.current = false;
  }, [pis.length]);

  React.useEffect(() => {
    if (lastWindowStart.current === null) {
      lastWindowStart.current = windowStart;
      return;
    }
    const prev = lastWindowStart.current;
    if (windowStart === prev) return;
    setWindowAnim(windowStart > prev ? "slide-left" : "slide-right");
    lastWindowStart.current = windowStart;
    const t = window.setTimeout(() => setWindowAnim(null), 260);
    return () => window.clearTimeout(t);
  }, [windowStart]);

  // Keep PI window navigation local in the frontend to avoid full Streamlit reruns.

  React.useEffect(() => {
    if (!selection) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setSelection(null);
        setTimelineSelectedFeatureId("");
        setTimelineSelectedEpicId("");
        setUi((prev) => ({ ...prev, drawer: "closed", selection: { kind: "none" } }));
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [selection]);

  React.useEffect(() => {
    const sel = (meta as any)?.selection;
    if (!sel || typeof sel !== "object") return;
    const mode = safeString((sel as any).mode || "").toLowerCase();
    const appId = safeString((sel as any).appId || "");
    const piId = safeString((sel as any).piId || "");
    const featureId = safeString((sel as any).featureId || "");
    if (mode === "feature" && appId && piId && featureId) {
      setSelection({ type: "feature", appId, piId, featureId });
      setUi((prev) => ({ ...prev, selection: { kind: "feature", featureId, appId, piId, epicId: safeString((sel as any).epicId || "") } }));
      return;
    }
    if (mode === "cell" && appId && piId) {
      setSelection({ type: "cell", appId, piId });
      setUi((prev) => ({ ...prev, selection: { kind: "cell", appId, piId } }));
    }
  }, [meta]);

  const selectedTimelineFeatureId = useMemo(() => {
    if (timelineSelectedFeatureId) return timelineSelectedFeatureId;
    if (selection?.type === "feature") return safeString(selection.featureId || "");
    const sel = (meta as any)?.selection;
    return safeString((sel as any)?.featureId || "");
  }, [timelineSelectedFeatureId, selection, meta]);

  const selectedTimelineEpicId = useMemo(() => {
    if (timelineSelectedEpicId) return timelineSelectedEpicId;
    if (selection?.type === "feature") {
      const tf = (timeline?.features || []).find((f: any) => safeString((f as any)?.id) === safeString(selection.featureId));
      return safeString((tf as any)?.epicId || "");
    }
    const sel = (meta as any)?.selection;
    return safeString((sel as any)?.epicId || "");
  }, [timelineSelectedEpicId, selection, timeline, meta]);

  const timelineTodayYearWindowStart = useMemo(() => {
    const bands = [
      ...((timeline?.piBands as any[]) || []),
      ...((timeline?.sprintBands as any[]) || []),
    ];
    const years = new Set<number>();
    bands.forEach((b: any) => {
      const s = new Date((b as any)?.start || "");
      const e = new Date((b as any)?.end || "");
      if (!Number.isNaN(s.getTime())) years.add(s.getUTCFullYear());
      if (!Number.isNaN(e.getTime())) years.add(e.getUTCFullYear());
    });
    const sorted = Array.from(years.values()).sort((a, b) => a - b);
    if (!sorted.length) return 0;
    const nowYear = new Date().getUTCFullYear();
    const idx = sorted.findIndex((y) => y >= nowYear);
    const windowSizeYears = sorted.length <= 2 ? Math.max(1, sorted.length) : 2;
    const maxStart = Math.max(0, sorted.length - windowSizeYears);
    const start = idx >= 0 ? idx : maxStart;
    return Math.max(0, Math.min(maxStart, start));
  }, [timeline]);

  React.useEffect(() => {
    if (!moreOpen) return;
    const onClick = (e: MouseEvent) => {
      const target = e.target as HTMLElement;
      if (!target.closest(".roadmap-more")) setMoreOpen(false);
    };
    window.addEventListener("click", onClick);
    return () => window.removeEventListener("click", onClick);
  }, [moreOpen]);

  const hasPoints = useMemo(() => {
    for (const cell of Object.values(sourceCells)) {
      if (!cell || !Array.isArray(cell.items)) continue;
      if (cell.items.some((item) => Number(item.points || 0) > 0)) return true;
    }
    return true;
  }, [sourceCells]);
  const pointsLabel = hasPoints ? "Story Points" : "Feature Count";
  const compact = ui.density === "compact";
  const activeView = ui.view;
  const timelineLeftWidth = normalizeTimelineLeftWidth(ui.timelineLeftWidth ?? leftWidth);
  const boardLeftWidth = timelineLeftWidth;

  const effectiveWindowSize = useMemo(() => {
    if (ui.timeframePreset === "3m") return Math.min(Math.max(2, 1), Math.max(1, pis.length));
    if (ui.timeframePreset === "6m") return Math.min(3, Math.max(1, pis.length));
    if (ui.timeframePreset === "1y") return Math.min(6, Math.max(1, pis.length));
    if (ui.timeframePreset === "fit") return Math.min(windowSize, Math.max(1, pis.length));
    return Math.min(windowSize, Math.max(1, pis.length));
  }, [ui.timeframePreset, windowSize, pis.length]);

  const todayWindowStartIndex = useMemo(() => {
    if (!pis.length) return 0;
    let idx = -1;
    if (currentPiInfo.currentPiId) {
      idx = pis.findIndex((pi) => pi.id === currentPiInfo.currentPiId);
    }
    if (idx < 0 && currentPiInfo.closestIndex >= 0) {
      idx = currentPiInfo.closestIndex;
    }
    if (idx < 0) {
      idx = initialWindowIndex >= 0 ? initialWindowIndex : 0;
    }
    const maxStart = Math.max(0, pis.length - effectiveWindowSize);
    return Math.max(0, Math.min(maxStart, idx));
  }, [pis, currentPiInfo.currentPiId, currentPiInfo.closestIndex, initialWindowIndex, effectiveWindowSize]);

  const visiblePis = useMemo(() => pis.slice(windowStart, windowStart + effectiveWindowSize), [pis, windowStart, effectiveWindowSize]);
  React.useEffect(() => {
    setWindowStart((prev) => Math.min(Math.max(0, pis.length - effectiveWindowSize), Math.max(0, prev)));
  }, [pis.length, effectiveWindowSize]);

  const currentPiVisible = useMemo(() => {
    if (currentPiInfo.allDatesMissing) return true;
    if (!currentPiInfo.currentPiId) return false;
    return visiblePis.some((pi) => pi.id === currentPiInfo.currentPiId);
  }, [visiblePis, currentPiInfo]);

  const piMeta = useMemo(() => {
    const parseDate = (val?: string | number | null) => {
      if (val === null || val === undefined || val === "") return null;
      if (typeof val === "number") {
        const d = new Date(val);
        return Number.isNaN(d.getTime()) ? null : d;
      }
      const d = new Date(val);
      return Number.isNaN(d.getTime()) ? null : d;
    };
    const meta: Record<
      string,
      {
        isCurrent: boolean;
        progress: number;
        hasDates: boolean;
        isPast: boolean;
        start?: Date;
        end?: Date;
        elapsedDays?: number;
        totalDays?: number;
        mode: "calendar" | "fallback";
      }
    > = {};
    let anyWithDates = false;
    visiblePis.forEach((pi) => {
      const start =
        parseDate(pi.start_date) ||
        parseDate((pi as any).PI_START_DATE) ||
        parseDate((pi as any).start_date) ||
        parseDate(pi.start_ts) ||
        parseDate((pi as any).start);
      const end =
        parseDate(pi.end_date) ||
        parseDate((pi as any).PI_END_DATE) ||
        parseDate((pi as any).end_date) ||
        parseDate(pi.end_ts) ||
        parseDate((pi as any).end);
      if (start && end && end.getTime() > start.getTime()) {
        anyWithDates = true;
        const totalDays = Math.max(1, Math.round((end.getTime() - start.getTime()) / 86400000));
        const elapsedDays = Math.round((currentPiInfo.today0.getTime() - start.getTime()) / 86400000);
        const progress = Math.min(1, Math.max(0, elapsedDays / totalDays));
        const isCurrent = currentPiInfo.currentPiId === pi.id && currentPiVisible;
        const isPast = end.getTime() < currentPiInfo.today0.getTime();
        meta[pi.id] = {
          isCurrent,
          progress,
          hasDates: true,
          isPast,
          start,
          end,
          elapsedDays,
          totalDays,
          mode: "calendar",
        };
      }
    });
    if (!anyWithDates && visiblePis.length > 0) {
      meta[visiblePis[0].id] = {
        isCurrent: true,
        progress: 0.5,
        hasDates: false,
        isPast: false,
        mode: "fallback",
      };
    }
    visiblePis.forEach((pi) => {
      if (!meta[pi.id]) {
        meta[pi.id] = { isCurrent: false, progress: 0.5, hasDates: false, isPast: false, mode: "fallback" };
      }
    });
    return meta;
  }, [visiblePis, currentPiInfo, currentPiVisible]);

  const currentPiIndex = useMemo(() => {
    if (currentPiInfo.allDatesMissing) return -1;
    if (!currentPiVisible) return -1;
    return visiblePis.findIndex((pi) => pi.id === currentPiInfo.currentPiId);
  }, [visiblePis, currentPiInfo, currentPiVisible]);

  const sourceGroupBy = useMemo(
    () => (String(meta.rows_by || meta.row_mode || "").toLowerCase().includes("epic") ? "epics" : "applications"),
    [meta.rows_by, meta.row_mode]
  );

  const groupedBoardData = useMemo(() => {
    if (ui.groupBy === sourceGroupBy) {
      return { apps: sourceApps, cells: sourceCells };
    }

    const derivedCells: Record<string, RoadmapCell> = {};
    const rowSeen = new Set<string>();
    const rowOrder: string[] = [];
    const rowLabelById: Record<string, string> = {};
    const rowFeatureIds: Record<string, Set<string>> = {};
    const rowBvSum: Record<string, number> = {};
    const rowDemandSum: Record<string, number> = {};
    const rowProgressSum: Record<string, number> = {};
    const rowProgressN: Record<string, number> = {};
    const rowEpicId: Record<string, string> = {};
    const rowEpicState: Record<string, string> = {};

    const ensureRow = (rowIdRaw: string, labelRaw: string) => {
      const rowId = safeString(rowIdRaw || "");
      const label = safeString(labelRaw || rowId || "");
      if (!rowId) return "";
      if (!rowSeen.has(rowId)) {
        rowSeen.add(rowId);
        rowOrder.push(rowId);
        rowLabelById[rowId] = label || rowId;
        rowFeatureIds[rowId] = new Set<string>();
        rowBvSum[rowId] = 0;
        rowDemandSum[rowId] = 0;
        rowProgressSum[rowId] = 0;
        rowProgressN[rowId] = 0;
      }
      return rowId;
    };

    Object.entries(sourceCells).forEach(([cellKey, cell]) => {
      if (!cell || !Array.isArray(cell.items)) return;
      const parts = String(cellKey).split("|||");
      const piId = safeString(parts[1] || "");
      if (!piId) return;
      (cell.items || []).forEach((item) => {
        const epicId = safeString(item.epic_id || "");
        const epicTitle = safeString(item.epic_title || "");
        const appName = safeString(item.application || "");
        const rowId =
          ui.groupBy === "epics"
            ? safeString(epicTitle || (epicId ? `Epic ${epicId}` : "(No Epic)"))
            : safeString(appName || "(Unmapped Application)");
        const label = rowId;
        const normalizedRowId = ensureRow(rowId, label);
        if (!normalizedRowId) return;
        const derivedKey = `${normalizedRowId}|||${piId}`;
        if (!derivedCells[derivedKey]) {
          derivedCells[derivedKey] = { items: [], remainder: 0 };
        }
        derivedCells[derivedKey].items.push(item);

        const fid = safeString(item.id || "");
        if (fid) rowFeatureIds[normalizedRowId].add(fid);
        const bv = Number(item.business_value || 0);
        if (Number.isFinite(bv)) rowBvSum[normalizedRowId] += bv;
        const fte = Number(item.fte || 0);
        if (Number.isFinite(fte)) rowDemandSum[normalizedRowId] += fte;
        const progress = Number(item.progress_pct || 0);
        if (Number.isFinite(progress)) {
          rowProgressSum[normalizedRowId] += progress;
          rowProgressN[normalizedRowId] += 1;
        }
        if (ui.groupBy === "epics") {
          if (!rowEpicId[normalizedRowId] && epicId) rowEpicId[normalizedRowId] = epicId;
          if (!rowEpicState[normalizedRowId] && safeString(item.epic_state || "")) rowEpicState[normalizedRowId] = safeString(item.epic_state || "");
        }
      });
    });

    const derivedApps: RoadmapApp[] = rowOrder.map((rowId) => {
      const featureCount = rowFeatureIds[rowId]?.size || 0;
      const avgProgress = rowProgressN[rowId] > 0 ? rowProgressSum[rowId] / rowProgressN[rowId] : 0;
      const out: RoadmapApp = {
        id: rowId,
        label: rowLabelById[rowId] || rowId,
        item_count: featureCount,
        bv_sum: Number(rowBvSum[rowId] || 0),
        demand_fte_sum: Number(rowDemandSum[rowId] || 0),
        feature_count: featureCount,
        avg_progress: Number(avgProgress || 0),
      };
      if (ui.groupBy === "epics") {
        out.epic_id = safeString(rowEpicId[rowId] || "");
        (out as any).epic_state = safeString(rowEpicState[rowId] || "");
      }
      return out;
    });

    return { apps: derivedApps, cells: derivedCells };
  }, [ui.groupBy, sourceGroupBy, sourceApps, sourceCells]);

  const apps = groupedBoardData.apps;
  const cells = groupedBoardData.cells;
  const rowHeaderLabel = ui.groupBy === "epics" ? "Epics" : "Applications";

  const zoomedPiWidth = useMemo(
    () => Math.max(96, Math.round(minPiWidth * clamp(ui.zoom, zoomMin, zoomMax))),
    [ui.zoom]
  );
  const gridTemplateColumns = useMemo(
    () => `${boardLeftWidth}px repeat(${visiblePis.length}, minmax(${zoomedPiWidth}px, 1fr))`,
    [visiblePis.length, boardLeftWidth, zoomedPiWidth]
  );
  const canvasMinWidth = useMemo(
    () => boardLeftWidth + visiblePis.length * zoomedPiWidth,
    [visiblePis.length, zoomedPiWidth, boardLeftWidth]
  );
  const needsHorizontalScroll = useMemo(
    () => canvasMinWidth > containerWidth + 1,
    [canvasMinWidth, containerWidth]
  );
  const columnWidth = useMemo(() => {
    if (!visiblePis.length) return zoomedPiWidth;
    if (needsHorizontalScroll) return zoomedPiWidth;
    return Math.max(zoomedPiWidth, (containerWidth - boardLeftWidth) / visiblePis.length);
  }, [visiblePis.length, zoomedPiWidth, needsHorizontalScroll, containerWidth, boardLeftWidth]);

  const rowMetrics = useMemo(() => {
    const out: Record<string, { bv: number; demand: number; count: number; progress: number; progressN: number; seeded: boolean }> = {};
    apps.forEach((app) => {
      const hasPayloadMetrics =
        (app as any).bv_sum !== undefined
        || (app as any).demand_fte_sum !== undefined
        || (app as any).feature_count !== undefined
        || (app as any).avg_progress !== undefined;
      out[app.id] = {
        bv: Number((app as any).bv_sum || 0),
        demand: Number((app as any).demand_fte_sum || 0),
        count: Number((app as any).feature_count || app.item_count || 0),
        progress: Number((app as any).avg_progress || 0),
        progressN: Number((app as any).feature_count || app.item_count || 0),
        seeded: hasPayloadMetrics,
      };
    });
    Object.entries(cells).forEach(([key, cell]) => {
      const [appId] = key.split("|||");
      if (!appId || !cell) return;
      if (!out[appId]) {
        out[appId] = { bv: 0, demand: 0, count: 0, progress: 0, progressN: 0, seeded: false };
      }
      if (out[appId].seeded) {
        return;
      }
      (cell.items || []).forEach((item) => {
        const bv = Number(item.business_value || 0);
        const fte = Number(item.fte || 0);
        const progress = Number(item.progress_pct || 0);
        if (Number.isFinite(bv)) out[appId].bv += bv;
        if (Number.isFinite(fte)) out[appId].demand += fte;
        if (Number.isFinite(progress)) {
          out[appId].progress += progress;
          out[appId].progressN += 1;
        }
      });
    });
    return out;
  }, [apps, cells]);

  const sortedApps = useMemo(() => {
    const dir = ui.sortDir;
    const mode = ui.sortBy;
    const items = [...apps];
    items.sort((a, b) => {
      const ma = rowMetrics[a.id] || { bv: 0, demand: 0, count: 0, progress: 0, progressN: 0, seeded: false };
      const mb = rowMetrics[b.id] || { bv: 0, demand: 0, count: 0, progress: 0, progressN: 0, seeded: false };
      let cmp = 0;
      if (mode === "roi_bv") cmp = compareNumber(ma.bv, mb.bv, dir);
      else if (mode === "demand") cmp = compareNumber(ma.demand, mb.demand, dir);
      else if (mode === "feature_count") cmp = compareNumber(ma.count, mb.count, dir);
      else if (mode === "progress") {
        const ap = ma.progressN > 0 ? ma.progress / ma.progressN : 0;
        const bp = mb.progressN > 0 ? mb.progress / mb.progressN : 0;
        cmp = compareNumber(ap, bp, dir);
      } else cmp = compareText(safeString(a.label), safeString(b.label), dir);
      if (cmp !== 0) return cmp;
      return safeString(a.label).localeCompare(safeString(b.label));
    });
    return items;
  }, [apps, rowMetrics, ui.sortBy, ui.sortDir]);

  const viewCells = useMemo(() => {
    const dir = ui.sortDir;
    const mode = ui.sortBy;
    const out: Record<string, RoadmapCell> = {};
    Object.entries(cells).forEach(([key, cell]) => {
      if (!cell || !Array.isArray(cell.items)) {
        out[key] = cell as RoadmapCell;
        return;
      }
      const items = [...cell.items];
      items.sort((a, b) => {
        let cmp = 0;
        if (mode === "roi_bv") cmp = compareNumber(Number(a.business_value || 0), Number(b.business_value || 0), dir);
        else if (mode === "demand") cmp = compareNumber(Number(a.fte || 0), Number(b.fte || 0), dir);
        else if (mode === "progress") cmp = compareNumber(Number(a.progress_pct || 0), Number(b.progress_pct || 0), dir);
        else cmp = compareText(safeString(a.title), safeString(b.title), dir);
        if (cmp !== 0) return cmp;
        return safeString(a.title).localeCompare(safeString(b.title));
      });
      out[key] = { ...cell, items };
    });
    return out;
  }, [cells, ui.sortBy, ui.sortDir]);

  const query = safeString(ui.searchQuery || "").trim().toLowerCase();
  const itemMatchesQuery = React.useCallback(
    (item: RoadmapItem) => {
      if (!query) return true;
      const hay = [
        item.title,
        item.shortTitle,
        item.id,
        item.team,
        item.program,
        item.application,
        item.epic_title,
        item.epic_id,
      ]
        .map((v) => safeString(v).toLowerCase())
        .join(" ");
      return hay.includes(query);
    },
    [query]
  );

  const filteredCells = useMemo(() => {
    if (!query) return viewCells;
    const out: Record<string, RoadmapCell> = {};
    Object.entries(viewCells).forEach(([key, cell]) => {
      if (!cell || !Array.isArray(cell.items)) {
        out[key] = cell as RoadmapCell;
        return;
      }
      const items = (cell.items || []).filter((it) => itemMatchesQuery(it));
      out[key] = {
        ...cell,
        items,
        remainder: 0,
      };
    });
    return out;
  }, [viewCells, query, itemMatchesQuery]);

  const visibleApps = useMemo(() => {
    if (!query) return sortedApps;
    return sortedApps.filter((app) => {
      return visiblePis.some((pi) => {
        const cell = filteredCells[`${app.id}|||${pi.id}`];
        return Boolean(cell && Array.isArray(cell.items) && cell.items.length > 0);
      });
    });
  }, [sortedApps, visiblePis, filteredCells, query]);

  const maxItemStats = useMemo(() => {
    let maxFte = 0;
    let maxPoints = 0;
    Object.values(filteredCells).forEach((cell) => {
      (cell.items || []).forEach((item) => {
        maxFte = Math.max(maxFte, Number(item.fte || 0));
        maxPoints = Math.max(maxPoints, Number(item.points || 0));
      });
    });
    return { maxFte, maxPoints };
  }, [filteredCells]);

  const rowHeights = useMemo(() => {
    const map: Record<string, number> = {};
    visibleApps.forEach((app) => {
      let maxChips = 1;
      visiblePis.forEach((pi) => {
        const cell = filteredCells[`${app.id}|||${pi.id}`];
        const compactLimit = compact ? 3 : undefined;
        const items = cell ? cell.items : [];
        const displayCount = compactLimit ? Math.min(items.length, compactLimit) : items.length;
        const remainder = cell ? cell.remainder + Math.max(items.length - displayCount, 0) : 0;
        const chipCount = displayCount + (remainder > 0 ? 1 : 0);
        maxChips = Math.max(maxChips, chipCount || 1);
      });
      map[app.id] = maxChips;
    });
    return map;
  }, [visibleApps, visiblePis, filteredCells, compact]);

  const showTooltip = (html: string, e: React.MouseEvent) => {
    tooltipAnchorRef.current = { x: e.clientX, y: e.clientY };
    const next = resolveTooltipPosition(e.clientX, e.clientY, tooltipRef.current);
    setTooltip({ visible: true, x: next.x, y: next.y, html });
  };
  const moveTooltip = (e: React.MouseEvent) => {
    if (!tooltip.visible) return;
    tooltipAnchorRef.current = { x: e.clientX, y: e.clientY };
    const next = resolveTooltipPosition(e.clientX, e.clientY, tooltipRef.current);
    setTooltip((prev) => ({ ...prev, x: next.x, y: next.y }));
  };
  const hideTooltip = () => {
    if (!tooltip.visible) return;
    setTooltip((prev) => ({ ...prev, visible: false }));
  };

  const canStartBoardPan = (target: EventTarget | null) => {
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
          ".roadmap-drawer",
        ].join(",")
      )
    ) return false;
    return true;
  };

  const stopBoardPan = () => {
    const host = scrollRef.current;
    if (boardMouseMoveRef.current) {
      window.removeEventListener("mousemove", boardMouseMoveRef.current);
      boardMouseMoveRef.current = null;
    }
    if (boardMouseUpRef.current) {
      window.removeEventListener("mouseup", boardMouseUpRef.current);
      boardMouseUpRef.current = null;
    }
    boardPanRef.current = { active: false, startX: 0, startScrollLeft: 0, moved: false };
    setIsBoardPanning(false);
  };

  const handleBoardMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (activeView !== "board" || e.button !== 0 || !needsHorizontalScroll) return;
    if (!canStartBoardPan(e.target)) return;
    const host = scrollRef.current;
    if (!host) return;
    boardPanRef.current = {
      active: true,
      startX: e.clientX,
      startScrollLeft: host.scrollLeft || 0,
      moved: false,
    };
    const onMove = (ev: MouseEvent) => {
      const st = boardPanRef.current;
      if (!st.active || activeView !== "board") return;
      const dx = ev.clientX - st.startX;
      if (!st.moved && Math.abs(dx) < 4) return;
      if (!st.moved) {
        st.moved = true;
        setIsBoardPanning(true);
      }
      host.scrollLeft = st.startScrollLeft - dx;
      boardPanSuppressClickRef.current = true;
      ev.preventDefault();
    };
    const onUp = () => {
      const moved = boardPanRef.current.moved;
      stopBoardPan();
      if (moved) {
        window.setTimeout(() => {
          boardPanSuppressClickRef.current = false;
        }, 0);
      }
    };
    boardMouseMoveRef.current = onMove;
    boardMouseUpRef.current = onUp;
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp, { once: true });
    e.preventDefault();
  };
  const handleBoardClickCapture = (e: React.MouseEvent<HTMLDivElement>) => {
    if (!boardPanSuppressClickRef.current) return;
    e.preventDefault();
    e.stopPropagation();
  };

  React.useEffect(() => () => stopBoardPan(), []);

  function resolveTooltipPosition(clientX: number, clientY: number, el?: HTMLDivElement | null) {
    if (typeof window === "undefined") return { x: clientX + tooltipOffsetPx, y: clientY + tooltipOffsetPx };
    const w = Number(el?.offsetWidth || 300);
    const h = Number(el?.offsetHeight || 140);
    const vw = Number(window.innerWidth || 0);
    const vh = Number(window.innerHeight || 0);

    let x = clientX + tooltipOffsetPx;
    let y = clientY + tooltipOffsetPx;
    if (x + w + tooltipMarginPx > vw) x = clientX - w - tooltipOffsetPx;
    if (y + h + tooltipMarginPx > vh) y = clientY - h - tooltipOffsetPx;
    x = Math.max(tooltipMarginPx, Math.min(vw - w - tooltipMarginPx, x));
    y = Math.max(tooltipMarginPx, Math.min(vh - h - tooltipMarginPx, y));
    return { x, y };
  }

  React.useEffect(() => {
    if (!tooltip.visible) return;
    const { x, y } = tooltipAnchorRef.current;
    const next = resolveTooltipPosition(x, y, tooltipRef.current);
    setTooltip((prev) => ((prev.x === next.x && prev.y === next.y) ? prev : { ...prev, x: next.x, y: next.y }));
  }, [tooltip.visible, tooltip.html]);

  const closeDrawerSelection = React.useCallback(() => {
    setSelection(null);
    setTimelineSelectedFeatureId("");
    setTimelineSelectedEpicId("");
    setTimelineSelectedEpicSnapshot(null);
    setUi((prev) => ({ ...prev, drawer: "closed", selection: { kind: "none" } }));
  }, []);

  const handleFeatureClick = (item: RoadmapItem, appId: string, piId: string, _e: React.MouseEvent) => {
    const nextFeatureId = safeString(item.id);
    const sameFeatureOpen =
      ui.drawer === "details"
      && (
        (selection?.type === "feature" && safeString(selection.featureId) === nextFeatureId)
        || (ui.selection.kind === "feature" && safeString(ui.selection.featureId) === nextFeatureId)
      );
    if (sameFeatureOpen) {
      closeDrawerSelection();
      return;
    }
    setSelection({ type: "feature", featureId: item.id, appId, piId });
    setUi((prev) => ({ ...prev, drawer: "details", selection: { kind: "feature", featureId: item.id, appId, piId, epicId: safeString(item.epic_id || "") } }));
    setTimelineSelectedFeatureId(safeString(item.id));
    setTimelineSelectedEpicId(safeString(item.epic_id || ""));
    setTimelineSelectedEpicSnapshot(null);
  };

  const handleCellClick = (appId: string, piId: string) => {
    const sameCellOpen =
      ui.drawer === "details"
      && (
        (selection?.type === "cell" && safeString(selection.appId) === safeString(appId) && safeString(selection.piId) === safeString(piId))
        || (ui.selection.kind === "cell" && safeString(ui.selection.appId) === safeString(appId) && safeString(ui.selection.piId) === safeString(piId))
      );
    if (sameCellOpen) {
      closeDrawerSelection();
      return;
    }
    setSelection({ type: "cell", appId, piId });
    setUi((prev) => ({ ...prev, drawer: "details", selection: { kind: "cell", appId, piId } }));
  };

  const handleMoreClick = (appId: string, piId: string) => {
    handleCellClick(appId, piId);
  };

  const handleTimelineFeatureClick = (feature: any) => {
    const featureId = safeString(feature?.id || "");
    if (!featureId) return;
    const sameFeatureOpen =
      ui.drawer === "details"
      && (
        (selection?.type === "feature" && safeString(selection.featureId) === featureId)
        || (ui.selection.kind === "feature" && safeString(ui.selection.featureId) === featureId)
      );
    if (sameFeatureOpen) {
      closeDrawerSelection();
      return;
    }
    const piId = safeString(feature?.piKey || "");
    setSelection({ type: "feature", featureId, appId: "", piId });
    setUi((prev) => ({ ...prev, drawer: "details", selection: { kind: "feature", featureId, appId: "", piId, epicId: safeString(feature?.epicId || "") } }));
    setTimelineSelectedFeatureId(featureId);
    setTimelineSelectedEpicId(safeString(feature?.epicId || ""));
    setTimelineSelectedEpicSnapshot(null);
  };

  const handleTimelineEpicClick = (epic: { id: string; title: string; businessValueSum?: number; featureCount?: number; progress?: number; epicUrl?: string }) => {
    const epicId = safeString(epic?.id || "");
    if (!epicId) return;
    const sameEpicOpen =
      ui.drawer === "details"
      && (
        (ui.selection.kind === "epic" && safeString(ui.selection.epicId) === epicId)
        || safeString(timelineSelectedEpicId) === epicId
      );
    if (sameEpicOpen) {
      closeDrawerSelection();
      return;
    }
    setSelection(null);
    setTimelineSelectedEpicId(epicId);
    setTimelineSelectedFeatureId("");
    setTimelineSelectedEpicSnapshot({
      id: epicId,
      title: safeString(epic.title),
      businessValueSum: Number(epic.businessValueSum || 0),
      featureCount: Number(epic.featureCount || 0),
      progress: Number(epic.progress || 0),
      epicUrl: safeString(epic.epicUrl || ""),
    });
    setUi((prev) => ({ ...prev, drawer: "details", selection: { kind: "epic", epicId, title: safeString(epic.title) } }));
  };

  const applyUiPatch = (patch: Partial<RoadmapUiState>) => {
    setUi((prev) => {
      let changed = false;
      const next: RoadmapUiState = { ...prev };
      (Object.keys(patch) as Array<keyof RoadmapUiState>).forEach((k) => {
        const v = patch[k];
        if (v !== undefined && prev[k] !== v) {
          (next as any)[k] = v;
          changed = true;
        }
      });
      if (!changed) return prev;
      return next;
    });
  };

  const handleMilestoneCreate = (payload: { title: string; date: string; tag: string }) => {
    if (milestonePendingRef.current) return;
    const tempId = `tmp-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    const sel = ui.selection;
    const optimistic: RoadmapTimelineMilestone = {
      id: tempId,
      label: payload.title,
      date: payload.date,
      type: "custom",
      tag: payload.tag,
      sourceType: sel.kind === "feature" ? "FEATURE" : sel.kind === "epic" ? "EPIC" : "MANUAL",
      featureId: sel.kind === "feature" ? safeString(sel.featureId || "") : undefined,
      epicId: sel.kind === "epic" ? safeString(sel.epicId || "") : (sel.kind === "feature" ? safeString(sel.epicId || "") : undefined),
    };
    setDraftMilestones((prev) => ([...(prev || []), optimistic]));
    setMilestoneUndoDelete(null);
    setMilestoneSyncMsg("Milestone added to draft");
    setMilestoneLastSaveResult(null);
    setMilestoneDraftDate("");
  };

  const handleMilestoneDelete = (milestoneId: string) => {
    if (milestonePendingRef.current) return;
    const deleted = (draftMilestones || []).find((m) => safeString(m.id) === safeString(milestoneId));
    setDraftMilestones((prev) => (prev || []).filter((m) => safeString(m.id) !== safeString(milestoneId)));
    setMilestoneItemStatus((prev) => {
      const next = { ...prev };
      delete next[safeString(milestoneId)];
      return next;
    });
    if (deleted) {
      setMilestoneUndoDelete({
        id: safeString(deleted.id),
        label: safeString(deleted.label || "Milestone"),
        date: safeString(deleted.date || ""),
      });
    }
    setMilestoneSyncMsg("Milestone removed from draft");
    setMilestoneLastSaveResult(null);
  };

  const milestoneDirty = useMemo(() => {
    const norm = (items: RoadmapTimelineMilestone[]) =>
      JSON.stringify(
        (items || [])
          .map((m) => [safeString(m.id), safeString(m.label), safeString(m.date), safeString((m as any).tag || "")])
          .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
      );
    return norm(draftMilestones || []) !== norm(baselineMilestones || []);
  }, [draftMilestones, baselineMilestones]);

  const milestoneDraftSummary = useMemo(() => {
    const serverIdSet = new Set((baselineMilestones || []).map((m) => safeString(m.id)).filter(Boolean));
    const draftIdSet = new Set((draftMilestones || []).map((m) => safeString(m.id)).filter(Boolean));
    const creates = (draftMilestones || []).filter((m) => {
      const id = safeString(m.id);
      return id.startsWith("tmp-") || !serverIdSet.has(id);
    }).length;
    const deletes = (baselineMilestones || []).filter((m) => !draftIdSet.has(safeString(m.id))).length;
    return { creates, deletes, dirty: milestoneDirty };
  }, [draftMilestones, baselineMilestones, milestoneDirty]);

  const milestoneLastSyncAt = useMemo(() => {
    const candidates = (serverMilestones || [])
      .map((m: any) => safeString(m?.updatedAt || m?.UPDATED_AT || m?.updated_at || ""))
      .filter(Boolean)
      .sort();
    return candidates.length ? candidates[candidates.length - 1] : "";
  }, [serverMilestones]);

  const handleMilestoneSaveChanges = () => {
    if (milestonePendingRef.current || !milestoneDirty) return;
    const serverIdSet = new Set((baselineMilestones || []).map((m) => safeString(m.id)).filter(Boolean));
    const draftIdSet = new Set((draftMilestones || []).map((m) => safeString(m.id)).filter(Boolean));
    const creates = (draftMilestones || [])
      .filter((m) => {
        const id = safeString(m.id);
        return id.startsWith("tmp-") || !serverIdSet.has(id);
      })
      .map((m) => ({
        id: safeString(m.id || ""),
        title: safeString(m.label || ""),
        date: safeString(m.date || ""),
        tag: safeString((m as any).tag || "Milestone") || "Milestone",
        pi_key: "",
        epic_id: safeString((m as any).epicId || ""),
        feature_id: safeString((m as any).featureId || ""),
        source_type: safeString((m as any).sourceType || "MANUAL") || "MANUAL",
      }))
      .filter((m) => m.title && m.date);
    const deletes = (baselineMilestones || [])
      .map((m) => safeString(m.id))
      .filter((id) => id && !draftIdSet.has(id));
    const pendingCreateIds = creates.map((m) => safeString((m as any).id || "")).filter(Boolean);
    milestonePendingSavedDraftRef.current = [...(draftMilestones || [])];
    milestonePendingBatchRef.current = { creates: creates.length, deletes: deletes.length };
    milestonePendingRef.current = "save";
    setMilestoneUndoDelete(null);
    setMilestoneItemStatus((prev) => {
      const next = { ...prev };
      pendingCreateIds.forEach((id) => {
        next[id] = "saving";
      });
      return next;
    });
    setMilestoneSyncMsg("Saving milestone changes...");
    setMilestoneLastSaveResult(null);
    if (milestoneMsgTimerRef.current) window.clearTimeout(milestoneMsgTimerRef.current);
    milestoneMsgTimerRef.current = window.setTimeout(() => {
      const c = Number(milestonePendingBatchRef.current.creates || 0);
      const d = Number(milestonePendingBatchRef.current.deletes || 0);
      milestonePendingRef.current = null;
      setBaselineMilestones([...(milestonePendingSavedDraftRef.current || [])]);
      setMilestoneItemStatus((prev) => {
        const next = { ...prev };
        pendingCreateIds.forEach((id) => {
          next[id] = "saved";
        });
        return next;
      });
      if (milestoneItemStatusTimerRef.current) window.clearTimeout(milestoneItemStatusTimerRef.current);
      milestoneItemStatusTimerRef.current = window.setTimeout(() => {
        setMilestoneItemStatus({});
      }, 2600);
      setMilestoneLastSaveResult({
        created: c,
        deleted: d,
        failed: 0,
        message: `Saved: ${c} created, ${d} deleted`,
      });
      setMilestoneSyncMsg(`Saved: ${c} created, ${d} deleted`);
    }, 6000);
    emit("milestone_batch_apply", {
      creates,
      deletes,
    });
  };

  const handleMilestoneDiscardChanges = () => {
    if (milestonePendingRef.current) return;
    setDraftMilestones(baselineMilestones);
    setMilestoneUndoDelete(null);
    setMilestoneItemStatus({});
    setMilestoneSyncMsg("Draft discarded");
    setMilestoneLastSaveResult(null);
  };

  const handleMilestoneUndoDelete = () => {
    if (milestonePendingRef.current || !milestoneUndoDelete) return;
    setDraftMilestones((prev) => {
      const exists = (prev || []).some((m) => safeString(m.id) === safeString(milestoneUndoDelete.id));
      if (exists) return prev || [];
      const restored = {
        id: safeString(milestoneUndoDelete.id),
        label: safeString(milestoneUndoDelete.label),
        date: safeString(milestoneUndoDelete.date),
        type: "custom" as const,
      };
      return [...(prev || []), restored].sort((a, b) => safeString(a.date).localeCompare(safeString(b.date)));
    });
    setMilestoneUndoDelete(null);
    setMilestoneSyncMsg("Delete undone");
  };

  const timelineForView = useMemo(() => {
    if (!timeline) return timeline;
    return { ...timeline, milestones: draftMilestones };
  }, [timeline, draftMilestones]);

  const groupedTimelineForView = useMemo(() => {
    if (!timelineForView || ui.groupBy !== "applications") return timelineForView;
    const sourceFeatures = Array.isArray((timelineForView as any).features) ? ((timelineForView as any).features as any[]) : [];
    const appKey = (v: any) => {
      const app = safeString((v as any)?.application || "");
      return app || "(Unmapped Application)";
    };
    const mappedFeatures = sourceFeatures.map((f) => {
      const app = appKey(f);
      return {
        ...f,
        epicId: `APP::${app}`,
        epicTitle: app,
      };
    });
    const grouped = new Map<string, { title: string; progressSum: number; progressN: number; bv: number; count: number }>();
    mappedFeatures.forEach((f) => {
      const id = safeString((f as any).epicId || "");
      const title = safeString((f as any).epicTitle || "");
      if (!id) return;
      const entry = grouped.get(id) || { title, progressSum: 0, progressN: 0, bv: 0, count: 0 };
      const p = Number((f as any).progress || 0);
      if (Number.isFinite(p)) {
        entry.progressSum += Math.max(0, Math.min(1, p));
        entry.progressN += 1;
      }
      const bv = Number((f as any).businessValue || 0);
      if (Number.isFinite(bv)) entry.bv += bv;
      entry.count += 1;
      grouped.set(id, entry);
    });
    const appEpics = Array.from(grouped.entries()).map(([id, v]) => ({
      id,
      title: v.title,
      progress: v.progressN > 0 ? (v.progressSum / v.progressN) : 0,
      businessValueSum: v.bv,
      featureCount: v.count,
      epicUrl: "",
    }));
    return {
      ...timelineForView,
      epics: appEpics,
      features: mappedFeatures,
      focusEpicId: safeString(appEpics[0]?.id || ""),
      epic: appEpics[0] || null,
    } as any;
  }, [timelineForView, ui.groupBy]);

  const dataForDrawer = useMemo(() => {
    if (!groupedTimelineForView) return data;
    return { ...data, timeline: groupedTimelineForView };
  }, [data, groupedTimelineForView]);

  const handleExportCsv = () => {
    try {
      setUiError(null);
      const rows: string[] = [];
      rows.push(
        ["PI", "App", "Feature ID", "Title", "State", "Status", "Points", "Derived FTE", "Business Value", "Team", "Program"].join(",")
      );
      visiblePis.forEach((pi) => {
        sortedApps.forEach((app) => {
          const cell = viewCells[`${app.id}|||${pi.id}`];
          if (!cell) return;
          cell.items.forEach((item) => {
            rows.push(
              [
                `"${pi.label}"`,
                `"${app.label}"`,
                `"${item.id}"`,
                `"${(item.title || "").replace(/\"/g, '""')}"`,
                `"${(item.state || "").replace(/\"/g, '""')}"`,
                `"${item.statusBucket}"`,
                `${Number(item.points || 0)}`,
                `${Number(item.fte || 0)}`,
                `${Number(item.business_value || 0)}`,
                `"${(item.team || "").replace(/\"/g, '""')}"`,
                `"${(item.program || "").replace(/\"/g, '""')}"`,
              ].join(",")
            );
          });
        });
      });
      const blob = new Blob([rows.join("\n")], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = "roadmap_window.csv";
      link.click();
      URL.revokeObjectURL(url);
      setMoreOpen(false);
    } catch (err: any) {
      setUiError(`Export CSV failed. ${err?.message || ""}`.trim());
    }
  };

  const handleExportPng = async () => {
    try {
      setUiError(null);
      setMoreOpen(false);
      await new Promise<void>((resolve) => window.requestAnimationFrame(() => resolve()));
      const exportNode = activeView === "timeline"
        ? (timelineExportRef.current || rootRef.current)
        : (canvasRef.current || rootRef.current);
      if (!exportNode) {
        throw new Error("No roadmap surface available for export.");
      }
      const nodeBg = window.getComputedStyle(exportNode).backgroundColor || "";
      const rootBg = rootRef.current ? window.getComputedStyle(rootRef.current).backgroundColor || "" : "";
      const bg = (nodeBg && nodeBg !== "rgba(0, 0, 0, 0)" && nodeBg !== "transparent")
        ? nodeBg
        : ((rootBg && rootBg !== "rgba(0, 0, 0, 0)" && rootBg !== "transparent") ? rootBg : "#0b1220");
      const rootStyles = rootRef.current ? window.getComputedStyle(rootRef.current) : null;
      const docStyles = window.getComputedStyle(document.documentElement);
      const exportVarKeys = [
        "--st-sidebar-background-color",
        "--st-background-color",
        "--rm-sidebar-bg",
        "--rm-surface",
        "--rm-surface-2",
        "--rm-surface-3",
        "--rm-surface-4",
        "--rm-border",
        "--rm-text",
        "--rm-muted",
        "--rm-header-bg",
        "--rm-header-border",
        "--rm-rail-bg",
        "--rm-rail-bg-alt",
        "--rm-rail-pi-bg",
        "--rm-rail-ms-bg",
      ] as const;
      const exportStyleVars: Record<string, string> = {};
      exportVarKeys.forEach((key) => {
        const fromRoot = rootStyles?.getPropertyValue(key)?.trim() || "";
        const fromDoc = docStyles.getPropertyValue(key)?.trim() || "";
        const value = fromRoot || fromDoc;
        if (value) exportStyleVars[key] = value;
      });
      const dataUrl = await toPng(exportNode, {
        cacheBust: true,
        pixelRatio: 2,
        backgroundColor: bg,
        style: exportStyleVars as any,
      });
      const link = document.createElement("a");
      link.href = dataUrl;
      link.download = "roadmap_window.png";
      link.click();
    } catch (err: any) {
      setUiError(`Export PNG failed. ${err?.message || ""}`.trim());
    }
  };

  const selectedFeature = useMemo(() => {
    const featureId = safeString(
      selection?.type === "feature"
        ? selection.featureId
        : (ui.selection.kind === "feature" ? ui.selection.featureId : "")
    );
    if (!featureId) return null;
    const fromBoard = Object.values(cells)
      .flatMap((c) => c?.items || [])
      .find((item) => safeString(item.id) === featureId) || null;
    if (fromBoard) {
      const resolvedFeatureUrl = resolveFeatureUrl((fromBoard as any).id, (fromBoard as any).feature_url, (fromBoard as any).epic_url);
      if (!resolvedFeatureUrl || resolvedFeatureUrl === safeString((fromBoard as any).feature_url || "").trim()) return fromBoard;
      return {
        ...fromBoard,
        feature_url: resolvedFeatureUrl,
      } as RoadmapItem;
    }
    const tf = ((groupedTimelineForView as any)?.features || []).find((f: any) => safeString((f as any)?.id) === featureId);
    if (!tf) return null;
    const resolvedFeatureUrl = resolveFeatureUrl((tf as any).id, (tf as any).featureUrl, (tf as any).epicUrl);
    return {
      id: safeString((tf as any).id),
      title: safeString((tf as any).title),
      shortTitle: safeString((tf as any).title),
      statusBucket: "planned",
      state: safeString((tf as any).state || ""),
      points: Number((tf as any).points || 0),
      fte: Number((tf as any).fte || 0),
      team: safeString((tf as any).team || ""),
      program: safeString((tf as any).program || ""),
      application: safeString((tf as any).application || ""),
      epic_id: safeString((tf as any).epicId || ""),
      epic_title: safeString((tf as any).epicTitle || ""),
      progress_pct: Number((tf as any).progress || 0),
      progress_total_sp: 0,
      progress_proposed_sp: 0,
      progress_inprogress_sp: 0,
      progress_completed_sp: 0,
      business_value: Number((tf as any).businessValue || 0),
      feature_url: resolvedFeatureUrl,
      epic_url: safeString((tf as any).epicUrl || ""),
    } as RoadmapItem;
  }, [selection, ui.selection, cells, groupedTimelineForView]);

  const selectedCellItems = useMemo(() => {
    if (!selection || selection.type !== "cell") return [];
    const cell = filteredCells[`${selection.appId}|||${selection.piId}`];
    return cell?.items || [];
  }, [selection, filteredCells]);
  const selectedCellTitle = useMemo(() => {
    if (!selection || selection.type !== "cell") return "";
    const appLabel = safeString(sortedApps.find((app) => app.id === selection.appId)?.label || selection.appId);
    const piLabel = safeString(pis.find((pi) => pi.id === selection.piId)?.label || selection.piId);
    return `${appLabel} · ${piLabel}`;
  }, [selection, sortedApps, pis]);

  const selectedEpic = useMemo(() => {
    const epicId = normalizeIdToken(timelineSelectedEpicId || (ui.selection.kind === "epic" ? ui.selection.epicId : ""));
    if (!epicId) return null;
    const epics = Array.isArray((groupedTimelineForView as any)?.epics) ? ((groupedTimelineForView as any).epics as any[]) : [];
    const match = epics.find((e) => normalizeIdToken((e as any)?.id || "") === epicId);
    const timelineFeatures = Array.isArray((groupedTimelineForView as any)?.features) ? ((groupedTimelineForView as any).features as any[]) : [];
    const featuresForEpic = timelineFeatures.filter((f) => normalizeIdToken((f as any)?.epicId || "") === epicId);
    const fallbackProgressFromFeatures = featuresForEpic.length
      ? (
        featuresForEpic
          .map((f) => normalizeProgressRatio((f as any)?.progress))
          .filter((v): v is number => v !== null)
          .reduce((acc, v) => acc + v, 0) / Math.max(1, featuresForEpic.length)
      )
      : null;
    const progress =
      normalizeProgressRatio((match as any)?.progress)
      ?? normalizeProgressRatio((timelineSelectedEpicSnapshot as any)?.progress)
      ?? normalizeProgressRatio(fallbackProgressFromFeatures)
      ?? 0;
    const businessValueSum =
      Number((match as any)?.businessValueSum || 0)
      || Number((timelineSelectedEpicSnapshot as any)?.businessValueSum || 0)
      || featuresForEpic.reduce((acc, f) => acc + (Number((f as any)?.businessValue || 0) || 0), 0);
    const featureCount =
      Number((match as any)?.featureCount || 0)
      || Number((timelineSelectedEpicSnapshot as any)?.featureCount || 0)
      || featuresForEpic.length;
    return {
      id: epicId,
      title: safeString((match as any)?.title || (timelineSelectedEpicSnapshot as any)?.title || ""),
      businessValueSum,
      featureCount,
      progress,
      epicUrl: safeString((match as any)?.epicUrl || (timelineSelectedEpicSnapshot as any)?.epicUrl || ""),
    };
  }, [timelineSelectedEpicId, timelineSelectedEpicSnapshot, groupedTimelineForView, ui.selection]);
  const focusAppId = useMemo(() => {
    if (!focusMode) return "";
    if (selection?.type === "cell") return safeString(selection.appId || "");
    if (selection?.type === "feature" && safeString(selection.appId || "")) return safeString(selection.appId || "");
    if (ui.selection.kind === "cell") return safeString(ui.selection.appId || "");
    if (ui.selection.kind === "feature" && safeString(ui.selection.appId || "")) return safeString(ui.selection.appId || "");
    if (ui.groupBy === "epics") {
      const selectedEpicIdNorm = safeString(
        selectedEpic?.id
        || timelineSelectedEpicId
        || (ui.selection.kind === "epic" ? ui.selection.epicId : "")
        || (ui.selection.kind === "feature" ? ui.selection.epicId : "")
      );
      if (selectedEpicIdNorm) {
        const byEpicId = visibleApps.find((a) => safeString((a as any).epic_id || "") === selectedEpicIdNorm);
        if (byEpicId) return safeString(byEpicId.id);
      }
      const byEpicTitle = safeString(selectedEpic?.title || (ui.selection.kind === "epic" ? ui.selection.title : ""));
      if (byEpicTitle) {
        const byLabel = visibleApps.find((a) => safeString(a.label) === byEpicTitle);
        if (byLabel) return safeString(byLabel.id);
      }
    }
    return "";
  }, [focusMode, selection, ui.selection, ui.groupBy, selectedEpic, timelineSelectedEpicId, visibleApps]);

  const selectedDependency = useMemo(() => {
    if (ui.selection.kind !== "dependency") return null;
    const sourceId = safeString(ui.selection.sourceId);
    const targetId = safeString(ui.selection.targetId);
    const features = Array.isArray((groupedTimelineForView as any)?.features) ? ((groupedTimelineForView as any).features as any[]) : [];
    const source = features.find((f) => safeString((f as any).id) === sourceId);
    const target = features.find((f) => safeString((f as any).id) === targetId);
    const links = Array.isArray((groupedTimelineForView as any)?.links) ? ((groupedTimelineForView as any).links as any[]) : [];
    const inboundIds = links
      .filter((ln) => safeString((ln as any).targetId) === sourceId)
      .map((ln) => safeString((ln as any).sourceId));
    const outboundIds = links
      .filter((ln) => safeString((ln as any).sourceId) === sourceId)
      .map((ln) => safeString((ln as any).targetId));
    const titleById = new Map<string, string>();
    features.forEach((f) => titleById.set(safeString((f as any).id), safeString((f as any).title)));
    return {
      sourceId,
      targetId,
      sourceTitle: safeString((source as any)?.title || sourceId),
      targetTitle: safeString((target as any)?.title || targetId),
      inbound: inboundIds.map((id) => titleById.get(id) || id).filter(Boolean),
      outbound: outboundIds.map((id) => titleById.get(id) || id).filter(Boolean),
    };
  }, [ui.selection, groupedTimelineForView]);

  const hiddenSelection = useMemo(() => {
    if (!query) return { hidden: false, reason: "" };
    if (ui.selection.kind === "feature") {
      const featureId = safeString(ui.selection.featureId || "");
      if (!featureId) return { hidden: false, reason: "" };
      const inBoard = Object.values(filteredCells).some((cell) => (cell?.items || []).some((item) => safeString(item.id) === featureId));
      if (!inBoard) return { hidden: true, reason: "Selected item hidden by current filter." };
    }
    if (ui.selection.kind === "epic") {
      const epicId = safeString(ui.selection.epicId || "");
      if (!epicId) return { hidden: false, reason: "" };
      const timelineFeatures = Array.isArray((groupedTimelineForView as any)?.features) ? ((groupedTimelineForView as any).features as any[]) : [];
      const hasVisibleEpic = timelineFeatures.some((f) => {
        if (safeString((f as any).epicId) !== epicId) return false;
        return itemMatchesQuery({
          id: safeString((f as any).id),
          title: safeString((f as any).title),
          shortTitle: safeString((f as any).title),
          statusBucket: "planned",
          state: safeString((f as any).state || ""),
          points: Number((f as any).points || 0),
          fte: Number((f as any).fte || 0),
          team: safeString((f as any).team || ""),
          program: safeString((f as any).program || ""),
          application: safeString((f as any).application || ""),
          epic_id: safeString((f as any).epicId || ""),
          epic_title: safeString((f as any).epicTitle || ""),
          progress_pct: Number((f as any).progress || 0),
          progress_total_sp: 0,
          progress_proposed_sp: 0,
          progress_inprogress_sp: 0,
          progress_completed_sp: 0,
          business_value: Number((f as any).businessValue || 0),
          feature_url: "",
          epic_url: "",
        } as RoadmapItem);
      });
      if (!hasVisibleEpic) return { hidden: true, reason: "Selected item hidden by current filter." };
    }
    return { hidden: false, reason: "" };
  }, [query, ui.selection, filteredCells, groupedTimelineForView, itemMatchesQuery]);

  const fitSelection = React.useCallback(() => {
    let nextStart = "";
    let nextEnd = "";
    if (ui.selection.kind === "feature") {
      const featureId = safeString(ui.selection.featureId || "");
      const timelineFeatures = Array.isArray((groupedTimelineForView as any)?.features) ? ((groupedTimelineForView as any).features as any[]) : [];
      const target = timelineFeatures.find((f) => safeString((f as any).id) === featureId);
      if (target) {
        nextStart = safeString((target as any).start || "");
        nextEnd = safeString((target as any).end || "");
      } else {
        const selPi = safeString(ui.selection.piId || "");
        if (selPi) {
          const idx = pis.findIndex((pi) => safeString(pi.id) === selPi);
          if (idx >= 0) {
            const maxStart = Math.max(0, pis.length - effectiveWindowSize);
            setWindowStart(Math.max(0, Math.min(maxStart, idx)));
          }
        }
      }
      applyUiPatch({
        timeframePreset: "fit",
        fitSelectionNonce: Number(ui.fitSelectionNonce || 0) + 1,
        timeWindowStart: nextStart || ui.timeWindowStart || "",
        timeWindowEnd: nextEnd || ui.timeWindowEnd || "",
      });
      return;
    }
    if (ui.selection.kind === "epic") {
      const epicId = safeString(ui.selection.epicId || "");
      const timelineFeatures = Array.isArray((groupedTimelineForView as any)?.features) ? ((groupedTimelineForView as any).features as any[]) : [];
      const forEpic = timelineFeatures.filter((f) => safeString((f as any).epicId) === epicId);
      const starts = forEpic.map((f) => safeString((f as any).start || "")).filter(Boolean).sort();
      const ends = forEpic.map((f) => safeString((f as any).end || "")).filter(Boolean).sort();
      nextStart = starts[0] || "";
      nextEnd = ends[ends.length - 1] || "";
    }
    applyUiPatch({
      timeframePreset: "fit",
      fitSelectionNonce: Number(ui.fitSelectionNonce || 0) + 1,
      timeWindowStart: nextStart || ui.timeWindowStart || "",
      timeWindowEnd: nextEnd || ui.timeWindowEnd || "",
    });
    setTimelineTodayAnchorToken((prev) => prev + 1);
  }, [ui.selection, ui.fitSelectionNonce, ui.timeWindowStart, ui.timeWindowEnd, pis, effectiveWindowSize, groupedTimelineForView]);

  const resetView = () => {
    setMoreOpen(false);
    setWindowStart(0);
    setTimelineYearWindowStart(null);
    setFocusMode(false);
    applyUiPatch({
      density: "comfortable",
      timeframePreset: "fit",
      granularity: "pi",
      zoom: 1,
      timeWindowStart: "",
      timeWindowEnd: "",
      searchQuery: "",
      showDependencies: false,
      showProgress: true,
      dependencyLineMode: "auto",
      sortBy: "roi_bv",
      sortDir: "desc",
    });
  };

  const revealSelection = () => {
    setMoreOpen(false);
    applyUiPatch({ searchQuery: "" });
    if (ui.selection.kind === "feature" || ui.selection.kind === "epic") {
      applyUiPatch({ view: "timeline", drawer: "details" });
      fitSelection();
    }
  };

  const applyDisplayPreset = (preset: "planning" | "execution" | "dependency") => {
    if (preset === "planning") {
      applyUiPatch({
        density: "comfortable",
        showDependencies: false,
        showProgress: false,
        showRoi: true,
        sortBy: "roi_bv",
        sortDir: "desc",
        dependencyLineMode: "auto",
      });
    } else if (preset === "execution") {
      applyUiPatch({
        density: "comfortable",
        showDependencies: true,
        showProgress: true,
        showRoi: false,
        sortBy: "progress",
        sortDir: "desc",
        dependencyLineMode: "auto",
      });
    } else {
      applyUiPatch({
        density: "comfortable",
        showDependencies: true,
        showProgress: true,
        showRoi: false,
        sortBy: "demand",
        sortDir: "desc",
        dependencyLineMode: "always",
      });
    }
  };

  if (!Array.isArray(pis) || !Array.isArray(apps)) {
    return (
      <div className="roadmap-shell rm-root" style={{ height }} ref={rootRef}>
        <div className="roadmap-error">Invalid payload: missing PI or app lists.</div>
      </div>
    );
  }

  if (!pis.length || !apps.length) {
    return (
      <div className="roadmap-shell rm-root" style={{ height }} ref={rootRef}>
        <div className="roadmap-empty">No roadmap data available.</div>
      </div>
    );
  }

  const chipHeight = compact ? compactChipHeight : baseChipHeight;
  const gap = compact ? compactGap : baseGap;
  const pad = compact ? compactPad : basePad;
  const canFocusSelection = Boolean(
    safeString(focusAppId || "")
    || safeString(timelineSelectedEpicId || "")
    || ui.selection.kind === "epic"
    || ui.selection.kind === "feature"
    || ui.selection.kind === "cell"
  );
  const maxWindowStart = Math.max(0, pis.length - effectiveWindowSize);
  const canWindowPrev = windowStart > 0;
  const canWindowNext = windowStart < maxWindowStart;

  return (
    <div
      className={`roadmap-shell rm-root ${compact ? "is-compact" : ""} ${isCompactToolbar ? "is-compact-toolbar" : ""}`}
      style={
        {
          height,
          "--board-min-width": `${canvasMinWidth}px`,
          "--rm-left-width": `${activeView === "timeline" ? timelineLeftWidth : boardLeftWidth}px`,
          ...(meta.theme?.sidebar_bg ? { "--rm-sidebar-bg": meta.theme.sidebar_bg } : {}),
          "--rm-header-text": "var(--rm-text)",
          "--board-offset-top": `${boardOffsetTop}px`,
        } as React.CSSProperties
      }
      ref={rootRef}
    >
      <div className="roadmap-toolbar" ref={toolbarRef}>
        <div className="roadmap-title">Roadmap Board</div>
        <div className="roadmap-toolbar-center">
          <div className="roadmap-view-tabs">
            <button className={`roadmap-tab ${activeView === "board" ? "is-active" : ""}`} onClick={() => applyUiPatch({ view: "board" })}>
              Board
            </button>
            {hasTimeline ? (
              <button className={`roadmap-tab ${activeView === "timeline" ? "is-active" : ""}`} onClick={() => applyUiPatch({ view: "timeline" })}>
                Timeline
              </button>
            ) : null}
          </div>
          <select
            className="roadmap-toolbar-select"
            value={ui.timeframePreset}
            onChange={(e) => applyUiPatch({ timeframePreset: e.target.value as any })}
            title="Timeframe"
          >
            <option value="3m">3M</option>
            <option value="6m">6M</option>
            <option value="1y">1Y</option>
            <option value="fit">Fit</option>
            <option value="custom">Custom</option>
          </select>
          {activeView === "board" ? (
            <div className="roadmap-window-arrows" aria-label="Window navigation">
              <button
                className="roadmap-icon-button roadmap-window-arrow"
                title="Previous window"
                onClick={() => setWindowStart((prev) => Math.max(0, prev - 1))}
                disabled={!canWindowPrev}
              >
                ‹
              </button>
              <button
                className="roadmap-icon-button roadmap-window-arrow"
                title="Next window"
                onClick={() => setWindowStart((prev) => Math.min(maxWindowStart, prev + 1))}
                disabled={!canWindowNext}
              >
                ›
              </button>
            </div>
          ) : null}
          <button
            className="roadmap-button"
            onClick={() => {
              setWindowStart(todayWindowStartIndex);
              setTimelineYearWindowStart(timelineTodayYearWindowStart);
              setTimelineTodayAnchorToken((prev) => prev + 1);
              applyUiPatch({ todayAnchorToken: Number(ui.todayAnchorToken || 0) + 1 });
              if (scrollRef.current) scrollRef.current.scrollLeft = 0;
            }}
          >
            Today
          </button>
        </div>
        <div className="roadmap-toolbar-actions">
          <input
            type="text"
            className="roadmap-toolbar-select roadmap-search-input"
            placeholder="Search"
            value={ui.searchQuery}
            onChange={(e) => applyUiPatch({ searchQuery: e.target.value })}
            autoComplete="off"
            autoCorrect="off"
            autoCapitalize="none"
            spellCheck={false}
          />
          <button
            className="roadmap-button"
            title="Customize view settings"
            onClick={() => {
              setMoreOpen(false);
              if (ui.drawer === "configure") {
                closeDrawerSelection();
              } else {
                applyUiPatch({ drawer: "configure" });
              }
            }}
          >
            Customize
          </button>
          <div className="roadmap-more">
            <button
              className="roadmap-button"
              onClick={(e) => {
                e.stopPropagation();
                setUiError(null);
                setMoreOpen((prev) => !prev);
              }}
            >
              More
            </button>
            {moreOpen ? (
              <div className="roadmap-export-menu" onClick={(e) => e.stopPropagation()}>
                <button className="roadmap-button roadmap-export-item" onClick={() => { fitSelection(); setMoreOpen(false); }}>
                  Fit Selection
                </button>
                <button className="roadmap-button roadmap-export-item" onClick={() => { applyUiPatch({ drawer: "milestones" }); setMoreOpen(false); }}>
                  Milestones
                </button>
                <button
                  className="roadmap-button roadmap-export-item"
                  disabled={!canFocusSelection}
                  onClick={() => {
                    setFocusMode((prev) => !prev);
                    setMoreOpen(false);
                  }}
                >
                  {focusMode ? "Focus: On" : "Focus: Off"}
                </button>
                <button className="roadmap-button roadmap-export-item" onClick={resetView}>
                  Reset View
                </button>
                <button className="roadmap-button roadmap-export-item" onClick={handleExportCsv}>
                  Export CSV (window)
                </button>
                <button className="roadmap-button roadmap-export-item" onClick={handleExportPng}>
                  Export PNG snapshot
                </button>
              </div>
            ) : null}
          </div>
        </div>
      </div>
      <div className="roadmap-status-chips">
        <button className="roadmap-active-filter-chip" onClick={() => applyUiPatch({ drawer: "configure" })}>
          {ui.showDependencies ? "Dependencies: On" : "Dependencies: Off"}
        </button>
        <button className="roadmap-active-filter-chip" onClick={() => applyUiPatch({ drawer: "configure" })}>
          {ui.showProgress ? "Progress: On" : "Progress: Off"}
        </button>
        {canShowRoi ? (
          <button className="roadmap-active-filter-chip" onClick={() => applyUiPatch({ drawer: "configure" })}>
            {ui.showRoi ? "ROI: On" : "ROI: Off"}
          </button>
        ) : null}
        <button className="roadmap-active-filter-chip" onClick={() => applyUiPatch({ drawer: "milestones" })}>
          {milestoneDirty ? `Milestones Draft: +${milestoneDraftSummary.creates}/-${milestoneDraftSummary.deletes}` : "Milestones: Synced"}
        </button>
      </div>
      {uiError ? <div className="roadmap-error">{uiError}</div> : null}
      {/* Status legend intentionally removed; each item has an inline status badge. */}
      {safeString((meta as any)?.status?.text || "") ? <div className="roadmap-note">{safeString((meta as any)?.status?.text || "")}</div> : null}
      {milestoneSyncMsg && !flagMilestoneFeedback ? <div className="roadmap-note">{milestoneSyncMsg}</div> : null}
      {hiddenSelection.hidden ? (
        <div className="roadmap-selection-notice">
          <span>{hiddenSelection.reason}</span>
          <button className="roadmap-button" onClick={revealSelection}>Reveal selection</button>
        </div>
      ) : null}
      <div className="roadmap-main-surface">
        <div
          className={`roadmap-scroll${isBoardPanning ? " is-panning" : ""}`}
          ref={scrollRef}
          style={{
            overflowX: activeView === "timeline" ? "hidden" : (needsHorizontalScroll ? "auto" : "hidden"),
            overflowY: "hidden",
            cursor: activeView === "board" && needsHorizontalScroll ? (isBoardPanning ? "grabbing" : "grab") : "default",
          }}
          onMouseDown={handleBoardMouseDown}
          onClickCapture={handleBoardClickCapture}
        >
          <div className={`roadmap-view-frame ${activeView === "timeline" ? "is-timeline" : "is-board"}`}>
            {activeView === "timeline" && hasTimeline ? (
              <div className="roadmap-export-target" ref={timelineExportRef}>
                <EpicTimeline
                  timeline={groupedTimelineForView}
                  onShowTooltip={showTooltip}
                  onMoveTooltip={moveTooltip}
                  onHideTooltip={hideTooltip}
                  onFeatureClick={handleTimelineFeatureClick}
                  onEpicClick={handleTimelineEpicClick}
                  onMilestoneClick={(milestone) => {
                    applyUiPatch({ drawer: "milestones", selection: { kind: "milestone", milestoneId: safeString(milestone.id) } as any });
                  }}
                  onMilestoneRailCreate={(date) => {
                    setMilestoneDraftDate(date);
                    applyUiPatch({ drawer: "milestones" });
                  }}
                  selectedFeatureId={selectedTimelineFeatureId}
                  selectedEpicId={selectedTimelineEpicId}
                  externalShowDependencies={ui.showDependencies}
                  onShowDependenciesChange={(value) => {
                    if (value !== ui.showDependencies) applyUiPatch({ showDependencies: value });
                  }}
                  yearWindowStartOverride={timelineYearWindowStart ?? undefined}
                  onYearWindowChange={(value) => {
                    setTimelineYearWindowStart(value);
                  }}
                  zoom={ui.zoom}
                  timeframePreset={ui.timeframePreset}
                  showRoi={ui.showRoi}
                  sortBy={ui.sortBy}
                  sortDir={ui.sortDir}
                  granularity={ui.granularity}
                  onGranularityChange={(value) => applyUiPatch({ granularity: value })}
                  searchQuery={ui.searchQuery}
                  showProgress={ui.showProgress}
                  dependencyLineMode={ui.dependencyLineMode}
                  onDependencyLineModeChange={(value) => applyUiPatch({ dependencyLineMode: value })}
                  density={ui.density}
                  onDependencyClick={(sourceId, targetId) => {
                    emit("selection_change", { mode: "dependency", source_id: sourceId, target_id: targetId });
                    applyUiPatch({
                      drawer: "details",
                      selection: { kind: "dependency", sourceId, targetId } as any,
                    });
                  }}
                  leftWidth={timelineLeftWidth}
                  onLeftWidthChange={(value) => applyUiPatch({ timelineLeftWidth: normalizeTimelineLeftWidth(value) })}
                  todayAnchorToken={timelineTodayAnchorToken}
                  fitSelectionNonce={Number(ui.fitSelectionNonce || 0)}
                  timeWindowStart={safeString(ui.timeWindowStart || "")}
                  timeWindowEnd={safeString(ui.timeWindowEnd || "")}
                  featureFlags={{
                    selectionAutoscroll: flagSelectionAutoscroll,
                    dependencyFocus: flagDependencyFocus,
                    milestoneCluster: flagMilestoneCluster,
                    epicColorFinalize: flagEpicColorFinalize,
                    timeControlsPolish: flagTimeControlsPolish,
                    microPolish: flagMicroPolish,
                  }}
                  focusMode={focusMode}
                  onRequestToday={() => {
                    setTimelineYearWindowStart(timelineTodayYearWindowStart);
                    setTimelineTodayAnchorToken((prev) => prev + 1);
                    applyUiPatch({ todayAnchorToken: Number(ui.todayAnchorToken || 0) + 1 });
                  }}
                  onRequestFitSelection={fitSelection}
                />
              </div>
            ) : (
              <div
                  className={`roadmap-canvas${windowAnim ? ` ${windowAnim}` : ""}`}
                  ref={canvasRef}
                  style={{ ["--rm-header-height" as any]: `${headerHeight}px` }}
                >
                <BoardHeader
                  pis={visiblePis}
                  gridTemplateColumns={gridTemplateColumns}
                  headerHeight={headerHeight}
                  rowHeaderLabel={rowHeaderLabel}
                  piMeta={piMeta}
                />
                <div className="roadmap-board-y-scroll">
                  <Grid
                  apps={visibleApps}
                  pis={visiblePis}
                  cells={filteredCells}
                    gridTemplateColumns={gridTemplateColumns}
                    rowHeights={rowHeights}
                    compact={compact}
                    pointsLabel={pointsLabel}
                    hasPoints={hasPoints}
                    showRoi={ui.showRoi}
                    showProgress={ui.showProgress}
                    maxPoints={maxItemStats.maxPoints}
                    maxFte={maxItemStats.maxFte}
                    onCellClick={handleCellClick}
                    onFeatureClick={handleFeatureClick}
                    onMoreClick={handleMoreClick}
                    onShowTooltip={showTooltip}
                    onMoveTooltip={moveTooltip}
                    onHideTooltip={hideTooltip}
                    selectedFeatureId={
                      selection?.type === "feature"
                        ? safeString(selection.featureId)
                        : ui.selection.kind === "feature"
                          ? safeString(ui.selection.featureId)
                          : ""
                    }
                    onHoverApp={setHoveredApp}
                    hoveredApp={hoveredApp}
                    rowPad={pad}
                    chipHeight={chipHeight}
                    gap={gap}
                  pastPiIds={new Set(visiblePis.filter((pi) => piMeta[pi.id]?.isPast).map((pi) => pi.id))}
                  focusedAppId={focusAppId}
                />
                </div>
                {currentPiIndex >= 0 && piMeta[visiblePis[currentPiIndex]?.id]?.hasDates ? (
                  <div
                    className="roadmap-today-line"
                    style={{
                      left: `${boardLeftWidth + currentPiIndex * columnWidth + (piMeta[visiblePis[currentPiIndex]?.id]?.progress ?? 0.5) * columnWidth}px`,
                    }}
                  >
                    <span className="roadmap-today-line-dot" />
                  </div>
                ) : null}
                {query && visibleApps.length === 0 ? <div className="roadmap-empty">No matches for search query.</div> : null}
              </div>
            )}
          </div>
        </div>
        <RoadmapDrawer
          data={dataForDrawer}
          ui={ui}
          selectedFeature={selectedFeature}
          selectedEpic={selectedEpic}
          selectedCellItems={selectedCellItems}
          selectedCellTitle={selectedCellTitle}
          selectedDependency={selectedDependency}
          onSelectFeatureFromCell={(featureId) => {
            if (!selection || selection.type !== "cell") return;
            const feature = selectedCellItems.find((it) => safeString(it.id) === safeString(featureId));
            if (!feature) return;
            setSelection({ type: "feature", featureId: safeString(feature.id), appId: selection.appId, piId: selection.piId });
            setUi((prev) => ({
              ...prev,
              drawer: "details",
              selection: {
                kind: "feature",
                featureId: safeString(feature.id),
                appId: selection.appId,
                piId: selection.piId,
                epicId: safeString(feature.epic_id || ""),
              },
            }));
            setTimelineSelectedFeatureId(safeString(feature.id));
            setTimelineSelectedEpicId(safeString(feature.epic_id || ""));
          }}
          onClose={closeDrawerSelection}
          onOpenTab={(tab) => applyUiPatch({ drawer: tab })}
          onUiPatch={applyUiPatch}
          onApplyDisplayPreset={applyDisplayPreset}
          onMilestoneCreate={handleMilestoneCreate}
          onMilestoneDelete={handleMilestoneDelete}
          onMilestoneSaveChanges={handleMilestoneSaveChanges}
          onMilestoneDiscardChanges={handleMilestoneDiscardChanges}
          milestoneDirty={milestoneDirty}
          milestoneBusy={Boolean(milestonePendingRef.current)}
          draftMilestoneDate={milestoneDraftDate}
          milestoneDraftSummary={milestoneDraftSummary}
          milestoneLastSaveResult={flagMilestoneFeedback ? milestoneLastSaveResult : null}
          milestoneLastSyncAt={milestoneLastSyncAt}
          milestoneItemStatus={milestoneItemStatus}
          milestoneUndoDelete={milestoneUndoDelete}
          onMilestoneUndoDelete={handleMilestoneUndoDelete}
        />
      </div>

      {tooltip.visible && typeof document !== "undefined"
        ? createPortal(
            <div ref={tooltipRef} className="roadmap-tooltip" style={{ top: tooltip.y, left: tooltip.x }} dangerouslySetInnerHTML={{ __html: tooltip.html }} />,
            document.body
          )
        : null}

    </div>
  );
}
