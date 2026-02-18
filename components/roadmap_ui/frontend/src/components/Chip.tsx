import React from "react";
import { RoadmapItem } from "../types";
import { clamp, escapeHtml, formatWhole, investmentMeta, safeString } from "../utils";
import { epicColorFromId } from "../roadmapTheme";

type ChipProps = {
  item: RoadmapItem;
  intensity: number;
  pointsLabel: string;
  showRoi: boolean;
  showProgress: boolean;
  isSelected?: boolean;
  onClick: (e: React.MouseEvent) => void;
  onShowTooltip: (html: string, e: React.MouseEvent) => void;
  onMoveTooltip: (e: React.MouseEvent) => void;
  onHideTooltip: () => void;
};

export function Chip({
  item,
  intensity,
  pointsLabel,
  showRoi,
  showProgress,
  isSelected = false,
  onClick,
  onShowTooltip,
  onMoveTooltip,
  onHideTooltip,
}: ChipProps) {
  const bucket = (item.statusBucket || "planned").toLowerCase();
  const token =
    bucket === "active"
      ? "inprogress"
      : bucket === "validation"
        ? "validation"
      : bucket === "done"
        ? "done"
        : "planned";
  const progressPct = clamp(Number(item.progress_pct || 0), 0, 1);
  const hasProgress = Number(item.progress_total_sp || 0) > 0;
  const epicColors = epicColorFromId(safeString(item.epic_id || ""), safeString(item.epic_title || ""));
  const isUnmapped = epicColors.isNeutral || safeString(item.mapping_status || "").toUpperCase().includes("OUT_OF_SCOPE");
  const style = {
    "--chip-border": epicColors.border || `var(--rm-chip-border-${token})`,
    "--chip-fill-base": epicColors.soft || `var(--rm-chip-fill-base-${token})`,
    "--chip-fill-value": epicColors.progress || `var(--rm-chip-fill-value-${token})`,
    "--chip-solid": epicColors.solid,
    "--chip-text": epicColors.textOnSolid,
  } as React.CSSProperties;
  const progressText = hasProgress ? `${Math.round(progressPct * 100)}%` : "—";
  const businessValue = Number(item.business_value);
  const hasBusinessValue = Number.isFinite(businessValue) && businessValue > 0;
  const invest = investmentMeta((item as any).investment_dimension);
  const tip = `
    <div style='font-weight:700;margin-bottom:6px;'>${escapeHtml(safeString(item.title))}</div>
    <div style='opacity:0.85;'>Feature #${escapeHtml(safeString(item.id || ""))}</div>
    <div style='opacity:0.85;'>${escapeHtml(safeString(item.state || "—"))} · ${escapeHtml(progressText)}</div>
    <div style='opacity:0.85;'>${escapeHtml(pointsLabel)} ${formatWhole(item.points)} · ${escapeHtml(safeString(item.team || "—"))}</div>
    <div style='opacity:0.85;'>Investment: ${escapeHtml(invest.label)}</div>
    ${showRoi ? `<div style='opacity:0.85;'>Business Value: ${hasBusinessValue ? formatWhole(businessValue) : "—"}</div>` : ""}
  `;
  return (
    <div
      className={`roadmap-pill ${isUnmapped ? "is-neutral" : ""}${isSelected ? " is-selected" : ""}`}
      style={style}
      onClick={onClick}
      onMouseEnter={(e) => onShowTooltip(tip, e)}
      onMouseMove={onMoveTooltip}
      onMouseLeave={onHideTooltip}
    >
      <span className="roadmap-pill-fill-layer" aria-hidden>
        <span className="roadmap-pill-fill-base" />
        {hasProgress ? (
          <span
            className="roadmap-pill-fill-value"
            style={{ width: `${Math.round(progressPct * 100)}%` }}
          />
        ) : null}
      </span>
      <div className="roadmap-pill-main">
        <span className="roadmap-pill-epic-dot" />
        <span className={`roadmap-investment-chip is-${invest.tone}`} title={`Investment: ${invest.label}`}>
          <span aria-hidden>{invest.icon}</span>
          {invest.code ? <span>{invest.code}</span> : null}
        </span>
        <span className="roadmap-pill-text">{item.shortTitle || item.title}</span>
        <span className={`roadmap-status roadmap-status-${token}`}>{bucket}</span>
        {showProgress ? <span className="roadmap-pill-progress">{progressText}</span> : null}
        {showRoi ? <span className="roadmap-pill-roi">{hasBusinessValue ? `BV ${formatWhole(businessValue)}` : "BV —"}</span> : null}
      </div>
    </div>
  );
}
