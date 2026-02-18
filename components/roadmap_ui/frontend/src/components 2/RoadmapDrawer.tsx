import React from "react";
import { RoadmapData, RoadmapItem, RoadmapUiState } from "../types";
import { investmentMeta, safeString, sanitizeExternalUrl } from "../utils";

type DrawerProps = {
  data: RoadmapData;
  ui: RoadmapUiState;
  selectedFeature: RoadmapItem | null;
  selectedEpic: { id: string; title: string; businessValueSum?: number; featureCount?: number; progress?: number; epicUrl?: string } | null;
  selectedCellItems?: RoadmapItem[];
  selectedCellTitle?: string;
  selectedDependency?: {
    sourceId: string;
    targetId: string;
    sourceTitle: string;
    targetTitle: string;
    inbound: string[];
    outbound: string[];
  } | null;
  onSelectFeatureFromCell?: (featureId: string) => void;
  onClose: () => void;
  onOpenTab: (tab: "details" | "configure" | "milestones") => void;
  onUiPatch: (patch: Partial<RoadmapUiState>) => void;
  onApplyDisplayPreset?: (preset: "planning" | "execution" | "dependency") => void;
  onMilestoneCreate: (payload: { title: string; date: string; tag: string }) => void;
  onMilestoneDelete: (milestoneId: string) => void;
  onMilestoneSaveChanges: () => void;
  onMilestoneDiscardChanges: () => void;
  milestoneDirty?: boolean;
  milestoneBusy?: boolean;
  draftMilestoneDate?: string;
  milestoneDraftSummary?: { creates: number; deletes: number; dirty: boolean };
  milestoneLastSaveResult?: { created: number; deleted: number; failed: number; message: string } | null;
  milestoneLastSyncAt?: string;
  milestoneItemStatus?: Record<string, "saving" | "saved" | "failed">;
  milestoneUndoDelete?: { id: string; label: string; date: string } | null;
  onMilestoneUndoDelete?: () => void;
};

function milestonesInTimeline(data: RoadmapData) {
  const list = Array.isArray(data.timeline?.milestones) ? data.timeline!.milestones! : [];
  return list.filter((m) => safeString((m as any).type || "custom") === "custom");
}

type DetailsTab = "overview" | "signals" | "legend";

export function RoadmapDrawer({
  data,
  ui,
  selectedFeature,
  selectedEpic,
  selectedCellItems = [],
  selectedCellTitle = "",
  selectedDependency = null,
  onSelectFeatureFromCell,
  onClose,
  onOpenTab,
  onUiPatch,
  onApplyDisplayPreset,
  onMilestoneCreate,
  onMilestoneDelete,
  onMilestoneSaveChanges,
  onMilestoneDiscardChanges,
  milestoneDirty = false,
  milestoneBusy = false,
  draftMilestoneDate = "",
  milestoneDraftSummary,
  milestoneLastSaveResult = null,
  milestoneLastSyncAt = "",
  milestoneItemStatus = {},
  milestoneUndoDelete = null,
  onMilestoneUndoDelete,
}: DrawerProps) {
  const caps = data.meta?.capabilities || {};
  const canCreate = caps.milestoneCreate !== false;
  const canDelete = caps.milestoneDelete !== false;
  const canGroupBy = caps.configureGroupBy !== false;
  const canSort = caps.configureSort !== false;
  const canShowRoi = caps.showRoi !== false;
  const milestones = milestonesInTimeline(data);
  const [title, setTitle] = React.useState("");
  const [date, setDate] = React.useState("");
  const [tag, setTag] = React.useState("Milestone");
  const [detailsTab, setDetailsTab] = React.useState<DetailsTab>("overview");
  const [legendFocusOpen, setLegendFocusOpen] = React.useState(false);
  const [copyStatus, setCopyStatus] = React.useState<"" | "copied" | "failed">("");
  const [guardAction, setGuardAction] = React.useState<null | { kind: "close" } | { kind: "tab"; tab: "details" | "configure" | "milestones" }>(null);

  React.useEffect(() => {
    const v = safeString(draftMilestoneDate || "");
    if (!v) return;
    setDate(v);
  }, [draftMilestoneDate]);

  React.useEffect(() => {
    if (!guardAction) return;
    if (milestoneDirty || milestoneBusy) return;
    if (guardAction.kind === "close") onClose();
    if (guardAction.kind === "tab") onOpenTab(guardAction.tab);
    setGuardAction(null);
  }, [guardAction, milestoneDirty, milestoneBusy, onClose, onOpenTab]);

  React.useEffect(() => {
    if (ui.drawer !== "details") return;
    if (ui.selection.kind === "none") setDetailsTab("legend");
    else setDetailsTab("overview");
  }, [ui.drawer, ui.selection.kind]);

  React.useEffect(() => {
    if (!legendFocusOpen) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setLegendFocusOpen(false);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [legendFocusOpen]);

  const selectedEpicSignal = (() => {
    if (!selectedEpic) return null;
    const apps = Array.isArray(data.apps) ? data.apps : [];
    const epicId = safeString(selectedEpic.id || "");
    const epicTitle = safeString(selectedEpic.title || "").toLowerCase();
    const byEpicId = apps.find((a: any) => safeString(a?.epic_id || "") === epicId);
    if (byEpicId) return byEpicId as any;
    const byRowId = apps.find((a: any) => safeString(a?.id || "").toLowerCase() === epicTitle);
    if (byRowId) return byRowId as any;
    const byLabel = apps.find((a: any) => safeString(a?.label || "").toLowerCase() === epicTitle);
    return (byLabel || null) as any;
  })();
  const signalCfg: any = (data.meta as any)?.signal_config || {};
  const velRef = Number.isFinite(Number(signalCfg?.velocity_ref_pts_per_pi)) ? Number(signalCfg.velocity_ref_pts_per_pi) : 65;
  const velUp = Number.isFinite(Number(signalCfg?.velocity_up_ratio)) ? Number(signalCfg.velocity_up_ratio) : 1.15;
  const velDown = Number.isFinite(Number(signalCfg?.velocity_down_ratio)) ? Number(signalCfg.velocity_down_ratio) : 0.9;
  const trendDown = Number.isFinite(Number(signalCfg?.trend_down_threshold)) ? Number(signalCfg.trend_down_threshold) : -0.2;
  const trendUp = Number.isFinite(Number(signalCfg?.trend_up_threshold)) ? Number(signalCfg.trend_up_threshold) : 0.2;
  const trendVolCv = Number.isFinite(Number(signalCfg?.trend_volatile_cv)) ? Number(signalCfg.trend_volatile_cv) : 0.45;
  const trendStableCv = Number.isFinite(Number(signalCfg?.trend_stable_cv)) ? Number(signalCfg.trend_stable_cv) : 0.25;
  const confHighMin = Number.isFinite(Number(signalCfg?.confidence_high_min)) ? Number(signalCfg.confidence_high_min) : 75;
  const confMediumMin = Number.isFinite(Number(signalCfg?.confidence_medium_min)) ? Number(signalCfg.confidence_medium_min) : 55;
  const burnHigh = Number.isFinite(Number(signalCfg?.burn_high_util)) ? Number(signalCfg.burn_high_util) : 1.1;
  const burnTight = Number.isFinite(Number(signalCfg?.burn_tight_util)) ? Number(signalCfg.burn_tight_util) : 0.95;
  const burnMedium = Number.isFinite(Number(signalCfg?.burn_medium_util)) ? Number(signalCfg.burn_medium_util) : 0.9;
  const modelRulesText = [
    "NEXT Roadmap Signal Guide (Advisory, Non-Financial)",
    `Vel (Delivery Throughput): compares PI delivery velocity to ${velRef.toFixed(0)} pts/PI baseline. Up when ratio >= ${velUp.toFixed(2)}; down when ratio <= ${velDown.toFixed(2)}.`,
    `Trend (Delivery Momentum): reads recent non-zero PI series (completed SP, fallback points). Down when trend <= ${(trendDown * 100).toFixed(0)}%; up when trend >= ${(trendUp * 100).toFixed(0)}%; volatile when CV >= ${trendVolCv.toFixed(2)}; stable when CV <= ${trendStableCv.toFixed(2)}.`,
    `Conf (Plan Confidence): feature score = coverage (40) + date completeness (20) + story signal (20) + mapping quality (20). Row confidence = weighted feature average + trend adjustment (stable +5, up +3, volatile -6, down -10, low-data -8). Bands: high >= ${confHighMin.toFixed(0)}, medium >= ${confMediumMin.toFixed(0)}, else low.`,
    `Risk (Near-Term Execution Risk): next-2-PI utilization = demand/capacity. High when util >= ${burnHigh.toFixed(2)} or tight util >= ${burnTight.toFixed(2)} with weak confidence/trend. Medium when util >= ${burnMedium.toFixed(2)} or medium confidence/downtrend/volatile; else low.`,
    "Canonical financial totals remain in canonical cost pipeline.",
  ].join("\n");
  const handleCopyModelRules = async () => {
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(modelRulesText);
      } else {
        const ta = document.createElement("textarea");
        ta.value = modelRulesText;
        ta.style.position = "fixed";
        ta.style.opacity = "0";
        document.body.appendChild(ta);
        ta.focus();
        ta.select();
        document.execCommand("copy");
        document.body.removeChild(ta);
      }
      setCopyStatus("copied");
    } catch {
      setCopyStatus("failed");
    }
    window.setTimeout(() => setCopyStatus(""), 1800);
  };

  if (ui.drawer === "closed") return null;

  const featureProgressPct = Number.isFinite(Number(selectedFeature?.progress_pct))
    ? Math.round(Math.max(0, Math.min(1, Number(selectedFeature?.progress_pct || 0))) * 100)
    : null;
  const epicProgressPct = Number.isFinite(Number(selectedEpic?.progress))
    ? Math.round(Math.max(0, Math.min(1, Number(selectedEpic?.progress || 0))) * 100)
    : null;
  const selectedFeatureUrl = sanitizeExternalUrl(selectedFeature?.feature_url);
  const selectedFeatureEpicUrl = sanitizeExternalUrl(selectedFeature?.epic_url);
  const selectedEpicUrl = sanitizeExternalUrl(selectedEpic?.epicUrl);
  const selectedFeatureInvestment = investmentMeta((selectedFeature as any)?.investment_dimension || "");
  const contextTitle = selectedFeature
    ? safeString(selectedFeature.title || "Feature")
    : selectedEpic
      ? safeString(selectedEpic.title || "Epic")
      : ui.selection.kind === "cell"
        ? safeString(selectedCellTitle || "Cell")
        : ui.selection.kind === "dependency" && selectedDependency
          ? "Dependency"
          : "Roadmap";
  const contextMeta = selectedFeature
    ? `Feature #${safeString(selectedFeature.id)}`
    : selectedEpic
      ? `Epic #${safeString(selectedEpic.id)}`
      : ui.selection.kind === "cell"
        ? `${selectedCellItems.length} feature(s)`
        : ui.selection.kind === "dependency" && selectedDependency
          ? `#${safeString(selectedDependency.sourceId)} -> #${safeString(selectedDependency.targetId)}`
          : "Select an item";
  const draftCreates = Number(milestoneDraftSummary?.creates || 0);
  const draftDeletes = Number(milestoneDraftSummary?.deletes || 0);
  const showGuard = Boolean(guardAction && milestoneDirty && !milestoneBusy && ui.drawer === "milestones");
  const requestClose = () => {
    if (ui.drawer === "milestones" && milestoneDirty && !milestoneBusy) {
      setGuardAction({ kind: "close" });
      return;
    }
    onClose();
  };
  const requestTab = (tab: "details" | "configure" | "milestones") => {
    if (tab === ui.drawer) return;
    if (ui.drawer === "milestones" && milestoneDirty && !milestoneBusy && tab !== "milestones") {
      setGuardAction({ kind: "tab", tab });
      return;
    }
    onOpenTab(tab);
  };

  return (
    <aside className="roadmap-drawer">
      <div className="roadmap-drawer-header">
        <div className="roadmap-drawer-tabs">
          <button className={`roadmap-tab roadmap-drawer-tab ${ui.drawer === "details" ? "is-active" : ""}`} onClick={() => requestTab("details")}>
            <span className="roadmap-drawer-tab-icon">◧</span>
            <span className="roadmap-drawer-tab-label">Details</span>
          </button>
          <button className={`roadmap-tab roadmap-drawer-tab ${ui.drawer === "configure" ? "is-active" : ""}`} onClick={() => requestTab("configure")}>
            <span className="roadmap-drawer-tab-icon">⚙</span>
            <span className="roadmap-drawer-tab-label">Config</span>
          </button>
          <button className={`roadmap-tab roadmap-drawer-tab ${ui.drawer === "milestones" ? "is-active" : ""}`} onClick={() => requestTab("milestones")}>
            <span className="roadmap-drawer-tab-icon">◈</span>
            <span className="roadmap-drawer-tab-label">Milestones</span>
          </button>
        </div>
        <button className="roadmap-icon-button" onClick={requestClose}>✕</button>
      </div>

      <div className="roadmap-drawer-body">
        <div className="roadmap-drawer-context">
          <div className="roadmap-drawer-context-title" title={contextTitle}>{contextTitle}</div>
          <div className="roadmap-drawer-context-meta">{contextMeta}</div>
        </div>
        {ui.drawer === "details" ? (
          <div className="roadmap-drawer-section roadmap-drawer-panel">
            <div className="roadmap-drawer-subtabs" role="tablist" aria-label="Details sections">
              <button className={`roadmap-drawer-subtab ${detailsTab === "overview" ? "is-active" : ""}`} onClick={() => setDetailsTab("overview")}>Overview</button>
              <button className={`roadmap-drawer-subtab ${detailsTab === "signals" ? "is-active" : ""}`} onClick={() => setDetailsTab("signals")}>Signals</button>
              <button className={`roadmap-drawer-subtab ${detailsTab === "legend" ? "is-active" : ""}`} onClick={() => setDetailsTab("legend")}>Legend</button>
            </div>
            {detailsTab === "overview" ? (
              selectedFeature ? (
              <>
                <div className="roadmap-drawer-title">{selectedFeature.title}</div>
                <div className="roadmap-drawer-kv">Feature #{selectedFeature.id}</div>
                <div className="roadmap-drawer-kv">Epic: {selectedFeature.epic_title || "—"}{selectedFeature.epic_id ? ` (#${selectedFeature.epic_id})` : ""}</div>
                <div className="roadmap-drawer-kv">State: {selectedFeature.state || "—"}</div>
                <div className="roadmap-drawer-kv">Progress: {featureProgressPct !== null ? `${featureProgressPct}%` : "—"}</div>
                <div className="roadmap-drawer-kv">Demand: {Number.isFinite(Number(selectedFeature.fte)) ? Number(selectedFeature.fte).toFixed(2) : "—"} FTE</div>
                <div className="roadmap-drawer-kv">Points: {Number.isFinite(Number(selectedFeature.points)) ? Number(selectedFeature.points).toLocaleString() : "—"}</div>
                <div className="roadmap-drawer-kv">Program: {selectedFeature.program || "—"}</div>
                <div className="roadmap-drawer-kv">Team: {selectedFeature.team || "—"}</div>
                <div className="roadmap-drawer-kv">Application: {selectedFeature.application || "—"}</div>
                <div className="roadmap-drawer-kv">Investment Dimension: {selectedFeatureInvestment.label}</div>
                <div className="roadmap-drawer-kv">
                  Business Value: {Number.isFinite(Number(selectedFeature.business_value)) && Number(selectedFeature.business_value) > 0 ? Number(selectedFeature.business_value).toLocaleString() : "—"}
                </div>
                {selectedFeatureUrl ? <a className="roadmap-button" href={selectedFeatureUrl} target="_blank" rel="noreferrer">Open Feature in ADO</a> : null}
                {selectedFeatureEpicUrl ? <a className="roadmap-button" href={selectedFeatureEpicUrl} target="_blank" rel="noreferrer">Open Epic in ADO</a> : null}
              </>
            ) : selectedEpic ? (
              <>
                <div className="roadmap-drawer-title">{selectedEpic.title || "Epic"}</div>
                <div className="roadmap-drawer-kv">Epic #{selectedEpic.id}</div>
                <div className="roadmap-drawer-kv">Progress: {epicProgressPct !== null ? `${epicProgressPct}%` : "—"}</div>
                <div className="roadmap-drawer-kv">Features in scope: {Number.isFinite(Number(selectedEpic.featureCount)) ? Number(selectedEpic.featureCount).toLocaleString() : "—"}</div>
                <div className="roadmap-drawer-kv">
                  Epic Business Value: {Number.isFinite(Number(selectedEpic.businessValueSum)) && Number(selectedEpic.businessValueSum) > 0 ? Number(selectedEpic.businessValueSum).toLocaleString() : "—"}
                </div>
                {selectedEpicUrl ? <a className="roadmap-button" href={selectedEpicUrl} target="_blank" rel="noreferrer">Open Epic in ADO</a> : null}
              </>
            ) : ui.selection.kind === "cell" ? (
              <>
                <div className="roadmap-drawer-title">{selectedCellTitle || "Cell details"}</div>
                <div className="roadmap-drawer-kv">{selectedCellItems.length} feature(s)</div>
                <div className="roadmap-drawer-list">
                  {selectedCellItems.length ? (
                    selectedCellItems.map((item) => (
                      <button
                        key={safeString(item.id)}
                        className="roadmap-drawer-list-item"
                        onClick={() => onSelectFeatureFromCell?.(safeString(item.id))}
                      >
                        <div>
                          <div>{safeString(item.title)}</div>
                          <div className="roadmap-drawer-kv">
                            Feature #{safeString(item.id)} · {safeString(item.state || "—")}
                            {ui.showRoi ? ` · BV ${Number.isFinite(Number(item.business_value)) && Number(item.business_value) > 0 ? Number(item.business_value).toLocaleString() : "—"}` : ""}
                          </div>
                        </div>
                      </button>
                    ))
                  ) : (
                    <div className="roadmap-drawer-kv">No items in this cell.</div>
                  )}
                </div>
              </>
            ) : ui.selection.kind === "dependency" && selectedDependency ? (
              <>
                <div className="roadmap-drawer-title">Dependency</div>
                <div className="roadmap-drawer-kv">From: {selectedDependency.sourceTitle} (#{selectedDependency.sourceId})</div>
                <div className="roadmap-drawer-kv">To: {selectedDependency.targetTitle} (#{selectedDependency.targetId})</div>
                <div className="roadmap-drawer-kv">Inbound ({selectedDependency.inbound.length})</div>
                <div className="roadmap-drawer-list">
                  {selectedDependency.inbound.length ? selectedDependency.inbound.map((v) => (
                    <div key={`in-${v}`} className="roadmap-drawer-kv">↘ {v}</div>
                  )) : <div className="roadmap-drawer-kv">—</div>}
                </div>
                <div className="roadmap-drawer-kv">Outbound ({selectedDependency.outbound.length})</div>
                <div className="roadmap-drawer-list">
                  {selectedDependency.outbound.length ? selectedDependency.outbound.map((v) => (
                    <div key={`out-${v}`} className="roadmap-drawer-kv">↗ {v}</div>
                  )) : <div className="roadmap-drawer-kv">—</div>}
                </div>
              </>
            ) : (
              <div className="roadmap-drawer-kv">Select a feature, epic, or milestone.</div>
            )
            ) : null}
            {detailsTab === "signals" ? (
              <>
                <div className="roadmap-drawer-title">Signal Summary</div>
                {selectedFeature ? (
                  <>
                    <div className="roadmap-drawer-kv">Feature #{selectedFeature.id}</div>
                    <div className="roadmap-signal-list">
                      <div
                        className={`roadmap-signal-line is-confidence is-${safeString((selectedFeature as any).feature_confidence_band || "low").toLowerCase()}`}
                        title="Feature confidence signal (advisory)"
                      >
                        <span className="roadmap-signal-icon">◐</span>
                        <span className="roadmap-signal-key">Conf</span>
                        <span className="roadmap-signal-value">
                          {safeString((selectedFeature as any).feature_confidence_band || "—")} ({Number.isFinite(Number((selectedFeature as any).feature_confidence_score)) ? Number((selectedFeature as any).feature_confidence_score).toFixed(0) : "—"})
                        </span>
                      </div>
                    </div>
                    <div className="roadmap-drawer-kv roadmap-drawer-note">Feature-level confidence is advisory and non-financial.</div>
                  </>
                ) : selectedEpic ? (
                  <>
                    {(() => {
                      const trend = safeString((selectedEpicSignal as any)?.delivery_trend || "low_data").toLowerCase();
                      const confBand = safeString((selectedEpicSignal as any)?.confidence_band || "low").toLowerCase();
                      const risk = safeString((selectedEpicSignal as any)?.burn_risk_2pi || "medium").toLowerCase();
                      return (
                        <>
                    <div className="roadmap-drawer-kv">Epic #{safeString(selectedEpic.id || "—")}</div>
                    <div className="roadmap-signal-list">
                      <div className={`roadmap-signal-line is-trend is-${trend}`} title="Delivery trend based on recent PI series">
                        <span className="roadmap-signal-icon">↗</span>
                        <span className="roadmap-signal-key">Trend</span>
                        <span className="roadmap-signal-value">{safeString((selectedEpicSignal as any)?.delivery_trend || "low_data")}</span>
                      </div>
                      <div className={`roadmap-signal-line is-confidence is-${confBand}`} title="Aggregated confidence score and band">
                        <span className="roadmap-signal-icon">●</span>
                        <span className="roadmap-signal-key">Conf</span>
                        <span className="roadmap-signal-value">{safeString((selectedEpicSignal as any)?.confidence_band || "—")} ({Number.isFinite(Number((selectedEpicSignal as any)?.confidence_score)) ? Number((selectedEpicSignal as any).confidence_score).toFixed(0) : "—"})</span>
                      </div>
                      <div className={`roadmap-signal-line is-risk is-${risk}`} title="Burn risk for next 2 PI envelope">
                        <span className="roadmap-signal-icon">!</span>
                        <span className="roadmap-signal-key">Risk</span>
                        <span className="roadmap-signal-value">{safeString((selectedEpicSignal as any)?.burn_risk_2pi || "—")}</span>
                      </div>
                      <div className="roadmap-signal-line" title="Number of low-confidence features in scope">
                        <span className="roadmap-signal-icon">#</span>
                        <span className="roadmap-signal-key">Low Conf</span>
                        <span className="roadmap-signal-value">{Number((selectedEpicSignal as any)?.confidence_low_feature_count || 0)}</span>
                      </div>
                    </div>
                    <div className="roadmap-drawer-kv roadmap-drawer-note">Trend/Conf/Risk are advisory and non-financial.</div>
                        </>
                      );
                    })()}
                  </>
                ) : ui.selection.kind === "cell" ? (
                  <>
                    <div className="roadmap-signal-list">
                      <div className="roadmap-signal-line" title="Features currently in selected cell">
                        <span className="roadmap-signal-icon">#</span>
                        <span className="roadmap-signal-key">Features</span>
                        <span className="roadmap-signal-value">{selectedCellItems.length}</span>
                      </div>
                      <div className="roadmap-signal-line" title="Low-confidence features in selected cell">
                        <span className="roadmap-signal-icon">○</span>
                        <span className="roadmap-signal-key">Low Conf</span>
                        <span className="roadmap-signal-value">{selectedCellItems.filter((i: any) => safeString(i?.feature_confidence_band || "").toLowerCase() === "low").length}</span>
                      </div>
                    </div>
                    <div className="roadmap-drawer-kv roadmap-drawer-note">Use this as triage signal, not financial commitment.</div>
                  </>
                ) : ui.selection.kind === "dependency" && selectedDependency ? (
                  <>
                    <div className="roadmap-signal-list">
                      <div className="roadmap-signal-line" title="Inbound links to selected dependency">
                        <span className="roadmap-signal-icon">↘</span>
                        <span className="roadmap-signal-key">Inbound</span>
                        <span className="roadmap-signal-value">{selectedDependency.inbound.length}</span>
                      </div>
                      <div className="roadmap-signal-line" title="Outbound links from selected dependency">
                        <span className="roadmap-signal-icon">↗</span>
                        <span className="roadmap-signal-key">Outbound</span>
                        <span className="roadmap-signal-value">{selectedDependency.outbound.length}</span>
                      </div>
                    </div>
                    <div className="roadmap-drawer-kv roadmap-drawer-note">Dependency load supports delivery-risk interpretation.</div>
                  </>
                ) : (
                  <div className="roadmap-drawer-kv">Select an item to inspect signals.</div>
                )}
              </>
            ) : null}
            {detailsTab === "legend" ? (
              <>
                <div className="roadmap-drawer-title">Icons and Flags</div>
                <div className="roadmap-drawer-actions-inline">
                  <button className="roadmap-button" onClick={() => setLegendFocusOpen(true)}>Expand explanation</button>
                  <button className="roadmap-button" onClick={handleCopyModelRules}>Copy model rules</button>
                </div>
                {copyStatus ? (
                  <div className={`roadmap-drawer-kv ${copyStatus === "copied" ? "roadmap-copy-status-ok" : "roadmap-copy-status-fail"}`}>
                    {copyStatus === "copied" ? "Signal guide copied." : "Could not copy signal guide."}
                  </div>
                ) : null}
                <div className="roadmap-drawer-kv">Vel ↑ / → / ↓: higher / around / lower delivery throughput vs baseline.</div>
                <div className="roadmap-drawer-kv">Trend ↗ / ~ / ↘ / ≈ / ·: improving / stable / declining / volatile / low data momentum.</div>
                <div className="roadmap-drawer-kv">Conf ● / ◐ / ○: high / medium / low plan confidence.</div>
                <div className="roadmap-drawer-kv">Risk ! / • / ✓: high / medium / low near-term execution risk (next 2 PI).</div>
                <div className="roadmap-drawer-kv">Dep ⛓: dependency pressure (blocked + blocking load).</div>
                <div className="roadmap-drawer-kv">Investment ◉ PH / ✦ NO / ▣ OP: Product Health / New Opportunities / Operating.</div>

                <div className="roadmap-drawer-title">How NEXT Scores This</div>
                <div className="roadmap-drawer-kv">
                  Vel (throughput): compares PI delivery velocity against {velRef.toFixed(0)} pts/PI.
                  Up when ratio ≥ {velUp.toFixed(2)}, down when ratio ≤ {velDown.toFixed(2)}.
                </div>
                <div className="roadmap-drawer-kv">
                  Trend (momentum): uses recent non-zero PI series (completed SP, fallback points).
                  Down if trend ≤ {(trendDown * 100).toFixed(0)}%, up if trend ≥ {(trendUp * 100).toFixed(0)}%.
                  Volatile if CV ≥ {trendVolCv.toFixed(2)}, stable if CV ≤ {trendStableCv.toFixed(2)}.
                </div>
                <div className="roadmap-drawer-kv">
                  Conf (plan confidence): feature score = coverage (40) + date completeness (20) + story signal (20) + mapping quality (20).
                  Row confidence = weighted feature average + trend adjustment (stable +5, up +3, volatile -6, down -10, low-data -8).
                  Bands: high ≥ {confHighMin.toFixed(0)}, medium ≥ {confMediumMin.toFixed(0)}, else low.
                </div>
                <div className="roadmap-drawer-kv">
                  Risk (execution): next 2 PI utilization = demand/capacity.
                  High if util ≥ {burnHigh.toFixed(2)} or tight util ≥ {burnTight.toFixed(2)} with weak confidence/trend.
                  Medium if util ≥ {burnMedium.toFixed(2)} or medium confidence/downtrend/volatile.
                </div>
                <div className="roadmap-drawer-kv roadmap-drawer-note">Trend, confidence, and risk are advisory signals for planning conversations (non-financial). Canonical financial totals remain in canonical cost pipeline.</div>
              </>
            ) : null}
          </div>
        ) : null}

        {ui.drawer === "configure" ? (
          <div className="roadmap-drawer-section roadmap-drawer-panel">
            <div className="roadmap-drawer-title">Display Presets</div>
            <div className="roadmap-drawer-actions-inline">
              <button className="roadmap-button" onClick={() => onApplyDisplayPreset?.("planning")}>Planning</button>
              <button className="roadmap-button" onClick={() => onApplyDisplayPreset?.("execution")}>Execution</button>
              <button className="roadmap-button" onClick={() => onApplyDisplayPreset?.("dependency")}>Dependency</button>
            </div>
            <div className="roadmap-drawer-title">Basic</div>
            <label className="roadmap-drawer-field">
              <span>View</span>
              <select value={ui.view} onChange={(e) => onUiPatch({ view: e.target.value as any })}>
                <option value="board">Board</option>
                <option value="timeline">Timeline</option>
              </select>
            </label>
            <label className="roadmap-drawer-field">
              <span>Density</span>
              <select value={ui.density} onChange={(e) => onUiPatch({ density: e.target.value as any })}>
                <option value="comfortable">Comfortable</option>
                <option value="compact">Compact</option>
              </select>
            </label>
            <label className="roadmap-drawer-field">
              <span>Zoom</span>
              <input type="range" min={0.5} max={1.8} step={0.1} value={ui.zoom} onChange={(e) => onUiPatch({ zoom: Number(e.target.value) })} />
            </label>
            {canShowRoi ? (
              <label className="roadmap-drawer-field-inline">
                <input type="checkbox" checked={ui.showRoi} onChange={(e) => onUiPatch({ showRoi: e.target.checked })} />
                <span>Show ROI (BV)</span>
              </label>
            ) : null}
            {canGroupBy ? (
              <label className="roadmap-drawer-field">
                <span>Rows by</span>
                <select value={ui.groupBy} onChange={(e) => onUiPatch({ groupBy: e.target.value as any })}>
                  <option value="applications">Applications</option>
                  <option value="epics">Epics</option>
                </select>
              </label>
            ) : null}
            {canSort ? (
              <label className="roadmap-drawer-field">
                <span>Sort by</span>
                <select value={ui.sortBy} onChange={(e) => onUiPatch({ sortBy: e.target.value as any })}>
                  <option value="roi_bv">ROI (Business Value)</option>
                  <option value="demand">Demand</option>
                  <option value="name">Name</option>
                  <option value="progress">Progress</option>
                  <option value="feature_count">Feature Count</option>
                </select>
              </label>
            ) : null}
            {canSort ? (
              <label className="roadmap-drawer-field">
                <span>Sort direction</span>
                <select value={ui.sortDir} onChange={(e) => onUiPatch({ sortDir: e.target.value as any })}>
                  <option value="desc">Descending</option>
                  <option value="asc">Ascending</option>
                </select>
              </label>
            ) : null}
            <details className="roadmap-drawer-advanced">
              <summary className="roadmap-drawer-advanced-toggle">Advanced controls</summary>
              <div className="roadmap-drawer-section">
                <label className="roadmap-drawer-field-inline">
                  <input type="checkbox" checked={ui.showDependencies} onChange={(e) => onUiPatch({ showDependencies: e.target.checked })} />
                  <span>Show dependency lines</span>
                </label>
                <label className="roadmap-drawer-field-inline">
                  <input type="checkbox" checked={ui.showProgress} onChange={(e) => onUiPatch({ showProgress: e.target.checked })} />
                  <span>Show progress badges</span>
                </label>
                <label className="roadmap-drawer-field">
                  <span>Dependency density mode</span>
                  <select value={ui.dependencyLineMode} onChange={(e) => onUiPatch({ dependencyLineMode: e.target.value as any })}>
                    <option value="auto">Auto-hide at high density</option>
                    <option value="always">Always show</option>
                  </select>
                </label>
              </div>
            </details>
          </div>
        ) : null}

        {ui.drawer === "milestones" ? (
          <div className="roadmap-drawer-section roadmap-drawer-panel">
            {(milestoneDirty || milestoneBusy) ? (
              <div className="roadmap-drawer-status roadmap-drawer-status-draft" role="status" aria-live="polite">
                <strong>Draft changes pending</strong>
                <span>+{draftCreates} creates · -{draftDeletes} deletes</span>
              </div>
            ) : null}
            {!milestoneDirty && !milestoneBusy ? (
              <div className="roadmap-drawer-status roadmap-drawer-status-draft" role="status" aria-live="polite">
                <strong>No pending milestone draft</strong>
                <span>All timeline milestones are in sync.</span>
              </div>
            ) : null}
            {milestoneLastSaveResult ? (
              <div className={`roadmap-drawer-status ${milestoneLastSaveResult.failed > 0 ? "roadmap-drawer-status-error" : "roadmap-drawer-status-ok"}`} role="status" aria-live="polite">
                <strong>{milestoneLastSaveResult.failed > 0 ? "Failed" : "Saved"}</strong>
                <span>
                  {milestoneLastSaveResult.message}
                  {milestoneLastSaveResult.failed > 0 ? ` · failed ${milestoneLastSaveResult.failed}` : ""}
                </span>
              </div>
            ) : null}
            {showGuard ? (
              <div className="roadmap-drawer-status roadmap-drawer-status-guard" role="alert">
                <strong>Discard draft?</strong>
                <span>You have unsaved milestone changes.</span>
                <div className="roadmap-drawer-actions-inline">
                  <button
                    className="roadmap-button"
                    onClick={() => {
                      const next = guardAction;
                      onMilestoneDiscardChanges();
                      setGuardAction(null);
                      if (!next) return;
                      if (next.kind === "close") onClose();
                      if (next.kind === "tab") onOpenTab(next.tab);
                    }}
                  >
                    Discard draft
                  </button>
                  <button className="roadmap-icon-button" onClick={() => setGuardAction(null)}>Stay</button>
                </div>
              </div>
            ) : null}
            {milestoneUndoDelete ? (
              <div className="roadmap-drawer-status roadmap-drawer-status-guard" role="status" aria-live="polite">
                <strong>Deleted from draft</strong>
                <span>{safeString(milestoneUndoDelete.label || "Milestone")} · {safeString(milestoneUndoDelete.date || "")}</span>
                <div className="roadmap-drawer-actions-inline">
                  <button className="roadmap-button" disabled={milestoneBusy} onClick={() => onMilestoneUndoDelete?.()}>Undo delete</button>
                </div>
              </div>
            ) : null}
            <div className="roadmap-drawer-title">Current Window</div>
            {milestoneLastSyncAt ? <div className="roadmap-drawer-kv">Last successful sync: {milestoneLastSyncAt}</div> : null}
            <div className="roadmap-drawer-list">
              {milestones.map((ms) => (
                <div key={safeString(ms.id)} className="roadmap-drawer-list-item">
                  <div>
                    <div>{safeString(ms.label || "Milestone")}</div>
                    <div className="roadmap-drawer-kv">{safeString(ms.date)}</div>
                  </div>
                  <div className="roadmap-drawer-actions-inline">
                    {safeString(milestoneItemStatus[safeString(ms.id)]) ? (
                      <span className={`roadmap-ms-item-state is-${safeString(milestoneItemStatus[safeString(ms.id)])}`}>
                        {safeString(milestoneItemStatus[safeString(ms.id)])}
                      </span>
                    ) : null}
                  {canDelete ? <button className="roadmap-icon-button" disabled={milestoneBusy} onClick={() => onMilestoneDelete(safeString(ms.id))}>Delete</button> : null}
                  </div>
                </div>
              ))}
              {!milestones.length ? <div className="roadmap-drawer-kv">No milestones in current scope.</div> : null}
            </div>
            {canCreate ? (
              <div className="roadmap-drawer-form">
                <input placeholder="Title" value={title} onChange={(e) => setTitle(e.target.value)} />
                <input type="date" value={date} onChange={(e) => setDate(e.target.value)} />
                <select value={tag} onChange={(e) => setTag(e.target.value)}>
                  <option>Milestone</option>
                  <option>Release</option>
                  <option>Risk</option>
                  <option>Decision</option>
                  <option>Dependency</option>
                </select>
                <button
                  className="roadmap-button"
                  disabled={!title.trim() || !date || milestoneBusy}
                  onClick={() => {
                    onMilestoneCreate({ title: title.trim(), date, tag });
                    setTitle("");
                    setDate("");
                    setTag("Milestone");
                  }}
                >
                  Add marker
                </button>
                {milestoneBusy ? (
                  <div className="roadmap-inline-saving" role="status" aria-live="polite">
                    <span className="roadmap-inline-saving-spinner" aria-hidden />
                    <span>Saving milestone changes...</span>
                  </div>
                ) : null}
                <div className="roadmap-drawer-sticky-actions">
                  <div className="roadmap-drawer-actions-inline">
                  <button
                    className="roadmap-button"
                    disabled={!milestoneDirty || milestoneBusy}
                    onClick={onMilestoneSaveChanges}
                    aria-keyshortcuts="Enter"
                  >
                    Save changes
                  </button>
                  <button
                    className="roadmap-icon-button"
                    disabled={!milestoneDirty || milestoneBusy}
                    onClick={onMilestoneDiscardChanges}
                    aria-keyshortcuts="Alt+Enter"
                  >
                    Discard
                  </button>
                  </div>
                </div>
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
      {legendFocusOpen ? (
        <div className="roadmap-legend-focus-backdrop" onClick={() => setLegendFocusOpen(false)}>
          <div className="roadmap-legend-focus-modal" onClick={(e) => e.stopPropagation()}>
            <div className="roadmap-legend-focus-header">
              <div className="roadmap-drawer-title">Signal Guide - Expanded View</div>
              <div className="roadmap-drawer-actions-inline">
                <button className="roadmap-button" onClick={handleCopyModelRules}>Copy model rules</button>
                <button className="roadmap-icon-button" onClick={() => setLegendFocusOpen(false)}>✕</button>
              </div>
            </div>
            <div className="roadmap-legend-focus-body">
              <div className="roadmap-drawer-kv">Vel ↑ / → / ↓: higher / around / lower delivery throughput vs baseline.</div>
              <div className="roadmap-drawer-kv">
                Vel (throughput) compares PI delivery velocity against {velRef.toFixed(0)} pts/PI.
                Up when ratio ≥ {velUp.toFixed(2)}, down when ratio ≤ {velDown.toFixed(2)}.
              </div>
              <div className="roadmap-drawer-kv">Trend ↗ / ~ / ↘ / ≈ / ·: improving / stable / declining / volatile / low data momentum.</div>
              <div className="roadmap-drawer-kv">
                Trend (momentum) uses recent non-zero PI series (completed SP, fallback points).
                Down if trend ≤ {(trendDown * 100).toFixed(0)}%, up if trend ≥ {(trendUp * 100).toFixed(0)}%.
                Volatile if CV ≥ {trendVolCv.toFixed(2)}, stable if CV ≤ {trendStableCv.toFixed(2)}.
              </div>
              <div className="roadmap-drawer-kv">Conf ● / ◐ / ○: high / medium / low plan confidence.</div>
              <div className="roadmap-drawer-kv">
                Confidence = feature score (coverage 40 + date completeness 20 + story signal 20 + mapping quality 20),
                then row score = weighted feature average + trend adjustment (stable +5, up +3, volatile -6, down -10, low-data -8).
                Bands: high ≥ {confHighMin.toFixed(0)}, medium ≥ {confMediumMin.toFixed(0)}, else low.
              </div>
              <div className="roadmap-drawer-kv">Risk ! / • / ✓: high / medium / low near-term execution risk (next 2 PI).</div>
              <div className="roadmap-drawer-kv">
                Risk uses next-2-PI utilization = demand/capacity.
                High if util ≥ {burnHigh.toFixed(2)} or tight util ≥ {burnTight.toFixed(2)} with weak confidence/trend.
                Medium if util ≥ {burnMedium.toFixed(2)} or medium confidence/downtrend/volatile.
              </div>
              <div className="roadmap-drawer-kv">Investment ◉ PH / ✦ NO / ▣ OP: Product Health / New Opportunities / Operating.</div>
              <div className="roadmap-drawer-kv">Dep ⛓: dependency pressure (blocked + blocking load).</div>
              <div className="roadmap-drawer-kv roadmap-drawer-note">Trend, confidence, and risk are advisory signals for planning conversations (non-financial). Canonical financial totals remain in canonical cost pipeline.</div>
            </div>
          </div>
        </div>
      ) : null}
    </aside>
  );
}
