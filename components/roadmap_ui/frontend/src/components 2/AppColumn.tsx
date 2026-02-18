import React from "react";
import { RoadmapApp } from "../types";
import { epicColorFromId } from "../roadmapTheme";
import { clamp, formatWhole, safeString } from "../utils";

type AppColumnProps = {
  app: RoadmapApp;
  isEven: boolean;
  groupBy: "applications" | "epics";
  compact: boolean;
  onEpicClick?: (app: RoadmapApp) => void;
};

function normalizeProgress(raw: any): number {
  const n = Number(raw);
  if (!Number.isFinite(n)) return 0;
  if (n > 1 && n <= 100) return clamp(n / 100, 0, 1);
  return clamp(n, 0, 1);
}

export function AppColumn({ app, isEven, groupBy, compact, onEpicClick }: AppColumnProps) {
  const itemCount = Number(app.item_count || 0);
  const featureCount = Number((app as any).feature_count ?? itemCount);
  const progressPct = Math.round(normalizeProgress((app as any).avg_progress || 0) * 100);
  const demandFte = Number((app as any).demand_fte_sum || 0);
  const bv = Number((app as any).bv_sum || 0);
  const deliveryTrend = safeString((app as any).delivery_trend || (app as any).velocity_signal || "low_data").toLowerCase();
  const confidenceScoreRaw = Number((app as any).confidence_score ?? 0);
  const confidenceScore = Number.isFinite(confidenceScoreRaw) ? Math.max(0, Math.min(100, Math.round(confidenceScoreRaw))) : 0;
  const confidenceBand = safeString((app as any).confidence_band || "low").toLowerCase();
  const confidenceLowPctRaw = Number((app as any).confidence_low_feature_pct ?? 0);
  const confidenceLowPct = Number.isFinite(confidenceLowPctRaw) ? Math.max(0, Math.min(1, confidenceLowPctRaw)) : 0;
  const burnRisk = safeString((app as any).burn_risk_2pi || "medium").toLowerCase();
  const burnRiskReason = safeString((app as any).burn_risk_reason || "Monitor next 2 PI");
  const depBand = safeString((app as any).dependency_impact_band || "low").toLowerCase();
  const depScoreRaw = Number((app as any).dependency_impact_score ?? 0);
  const depScore = Number.isFinite(depScoreRaw) ? Math.max(0, Math.min(100, Math.round(depScoreRaw))) : 0;
  const depBlocked = Number((app as any).dependency_impact_blocked ?? 0);
  const depBlocking = Number((app as any).dependency_impact_blocking ?? 0);
  const hasDependencyImpact =
    (app as any).dependency_impact_score !== undefined
    && (Number.isFinite(depScore) && (depScore > 0 || depBlocked > 0 || depBlocking > 0));
  const epicId = safeString((app as any).epic_id || "");
  const epicColors = epicColorFromId(epicId || safeString(app.label || ""), safeString(app.label || ""));
  const isEpicMode = groupBy === "epics";
  const subLabel = groupBy === "epics"
    ? (epicId ? `Epic #${epicId}` : "Epic")
    : "Application";
  const accentStyle = isEpicMode
    ? { background: epicColors.solid, boxShadow: `0 0 0 1px ${epicColors.border}` }
    : { background: "var(--rm-accent)", boxShadow: "0 0 0 1px color-mix(in srgb, var(--rm-accent) 55%, var(--rm-border))" };
  const clickable = isEpicMode && typeof onEpicClick === "function";
  const trendLabel =
    deliveryTrend === "stable" ? "Trend Stable"
    : deliveryTrend === "downtrend" ? "Trend Down"
    : deliveryTrend === "volatile" ? "Trend Volatile"
    : deliveryTrend === "uptrend" ? "Trend Up"
    : "Trend Low Data";
  const trendSymbol =
    deliveryTrend === "stable" ? "~"
    : deliveryTrend === "downtrend" ? "↘"
    : deliveryTrend === "volatile" ? "≈"
    : deliveryTrend === "uptrend" ? "↗"
    : "·";
  const confidenceLabel = confidenceBand === "high" ? "High" : confidenceBand === "medium" ? "Medium" : "Low";
  const confidenceSymbol = confidenceBand === "high" ? "●" : confidenceBand === "medium" ? "◐" : "○";
  const burnRiskSymbol = burnRisk === "high" ? "!" : burnRisk === "low" ? "✓" : "•";
  const depSymbol = depBand === "high" ? "⛓" : depBand === "medium" ? "⛓" : "⛓";
  const handleClick = () => {
    if (!clickable) return;
    onEpicClick?.(app);
  };
  return (
    <div className={`roadmap-cell roadmap-app ${isEven ? "is-even" : "is-odd"}`}>
      <div
        className={`roadmap-app-card ${compact ? "is-compact" : ""} ${clickable ? "is-clickable" : ""}`}
        role={clickable ? "button" : undefined}
        tabIndex={clickable ? 0 : undefined}
        onClick={handleClick}
        onKeyDown={(e) => {
          if (!clickable) return;
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onEpicClick?.(app);
          }
        }}
      >
        <span className="roadmap-app-accent-dot" style={accentStyle} />
        <div className="roadmap-app-title-wrap">
          <div className="roadmap-app-title-row">
            <div className="roadmap-app-title">{app.label}</div>
            <span className={`roadmap-app-type-chip ${isEpicMode ? "is-epic" : "is-app"}`}>
              {isEpicMode ? "EPIC" : "APP"}
            </span>
          </div>
          <div className="roadmap-app-sub">{subLabel}</div>
        </div>
        <div className="roadmap-app-badges">
          <span
            className={`roadmap-app-signal roadmap-app-signal-trend is-${deliveryTrend}`}
            title={`${trendLabel} (advisory signal)`}
          >
            {trendSymbol}
          </span>
          <span
            className={`roadmap-app-signal roadmap-app-signal-confidence is-${confidenceBand}`}
            title={`Confidence ${confidenceLabel} (${confidenceScore}) · Low features ${(confidenceLowPct * 100).toFixed(0)}%`}
          >
            {confidenceSymbol}
          </span>
          <span
            className={`roadmap-app-signal roadmap-app-signal-risk is-${burnRisk}`}
            title={`Burn risk next 2 PI: ${safeString(burnRisk || "medium")} · ${burnRiskReason}`}
          >
            {burnRiskSymbol}
          </span>
          {hasDependencyImpact ? (
            <span
              className={`roadmap-app-signal roadmap-app-signal-dep is-${depBand}`}
              title={`Dependency impact ${depBand || "low"} (${depScore}) · blocked ${Number.isFinite(depBlocked) ? depBlocked : 0} · blocking ${Number.isFinite(depBlocking) ? depBlocking : 0}`}
            >
              {depSymbol}
            </span>
          ) : null}
          <span className="roadmap-app-badge">{progressPct}%</span>
          <span className="roadmap-app-badge">BV {formatWhole(bv)}</span>
          <span className="roadmap-app-badge">{formatWhole(featureCount)} feats</span>
          {isEpicMode ? <span className="roadmap-app-badge">{demandFte.toFixed(1)} FTE</span> : null}
        </div>
        <div className="roadmap-app-progress-track" aria-label={`Progress ${progressPct}%`}>
          <span
            className="roadmap-app-progress-fill"
            style={{
              width: `${progressPct}%`,
              background: isEpicMode ? epicColors.progress : "linear-gradient(90deg, color-mix(in srgb, var(--rm-accent) 70%, white 12%), var(--rm-accent-strong))",
            }}
          />
        </div>
      </div>
    </div>
  );
}
