import React from "react";
import { RoadmapCell, RoadmapItem } from "../types";
import { clamp } from "../utils";
import { Chip } from "./Chip";

type CellProps = {
  appId: string;
  piId: string;
  cell?: RoadmapCell;
  pointsLabel: string;
  compact: boolean;
  showRoi: boolean;
  showProgress: boolean;
  onCellClick: () => void;
  onFeatureClick: (item: RoadmapItem, e: React.MouseEvent) => void;
  onMoreClick: () => void;
  onShowTooltip: (html: string, e: React.MouseEvent) => void;
  onMoveTooltip: (e: React.MouseEvent) => void;
  onHideTooltip: () => void;
  selectedFeatureId?: string;
  maxPoints: number;
  maxFte: number;
  hasPoints: boolean;
  isPast?: boolean;
};

export function Cell({
  cell,
  pointsLabel,
  compact,
  showRoi,
  showProgress,
  onCellClick,
  onFeatureClick,
  onMoreClick,
  onShowTooltip,
  onMoveTooltip,
  onHideTooltip,
  selectedFeatureId,
  maxPoints,
  maxFte,
  hasPoints,
  isPast,
}: CellProps) {
  if (!cell || cell.items.length === 0) {
    return <div className={`roadmap-cell is-empty${isPast ? " is-past" : ""}`} onClick={onCellClick} />;
  }

  const compactLimit = 3;
  const displayItems = compact ? cell.items.slice(0, compactLimit) : cell.items;
  const remainder = cell.remainder + Math.max(cell.items.length - displayItems.length, 0);

  return (
    <div className={`roadmap-cell${isPast ? " is-past" : ""}`} onClick={onCellClick}>
      <div className="roadmap-cell-content" onClick={(e) => e.stopPropagation()}>
        <div className="roadmap-pill-stack">
          {displayItems.map((item) => {
            const base = hasPoints ? Number(item.points || 0) : Number(item.fte || 0);
            const maxBase = hasPoints ? maxPoints : maxFte;
            const intensity = clamp(maxBase ? base / maxBase : 0.4, 0.35, 1);
            return (
              <Chip
                key={item.id}
                item={item}
                intensity={intensity}
                pointsLabel={pointsLabel}
                showRoi={showRoi}
                showProgress={showProgress}
                isSelected={String(item.id) === String(selectedFeatureId || "")}
                onClick={(e) => onFeatureClick(item, e)}
                onShowTooltip={onShowTooltip}
                onMoveTooltip={onMoveTooltip}
                onHideTooltip={onHideTooltip}
              />
            );
          })}
          {remainder > 0 ? (
            <button
              className="roadmap-more-chip"
              onClick={(e) => {
                e.stopPropagation();
                onMoreClick();
              }}
            >
              +{remainder} more
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}
