import React from "react";
import { RoadmapApp, RoadmapCell, RoadmapItem, RoadmapPi } from "../types";
import { AppColumn } from "./AppColumn";
import { Cell } from "./Cell";
import { safeString } from "../utils";

type GridProps = {
  apps: RoadmapApp[];
  pis: RoadmapPi[];
  cells: Record<string, RoadmapCell>;
  gridTemplateColumns: string;
  rowHeights: Record<string, number>;
  compact: boolean;
  pointsLabel: string;
  hasPoints: boolean;
  showRoi: boolean;
  showProgress: boolean;
  maxPoints: number;
  maxFte: number;
  onCellClick: (appId: string, piId: string) => void;
  onFeatureClick: (item: RoadmapItem, appId: string, piId: string, e: React.MouseEvent) => void;
  onMoreClick: (appId: string, piId: string) => void;
  onShowTooltip: (html: string, e: React.MouseEvent) => void;
  onMoveTooltip: (e: React.MouseEvent) => void;
  onHideTooltip: () => void;
  selectedFeatureId?: string;
  onHoverApp: (appId: string | null) => void;
  hoveredApp: string | null;
  rowPad: number;
  chipHeight: number;
  gap: number;
  pastPiIds: Set<string>;
  focusedAppId?: string;
  groupBy: "applications" | "epics";
  onEpicClick?: (app: RoadmapApp) => void;
};

export function Grid({
  apps,
  pis,
  cells,
  gridTemplateColumns,
  rowHeights,
  compact,
  pointsLabel,
  hasPoints,
  showRoi,
  showProgress,
  maxPoints,
  maxFte,
  onCellClick,
  onFeatureClick,
  onMoreClick,
  onShowTooltip,
  onMoveTooltip,
  onHideTooltip,
  selectedFeatureId,
  onHoverApp,
  hoveredApp,
  rowPad,
  chipHeight,
  gap,
  pastPiIds,
  focusedAppId = "",
  groupBy,
  onEpicClick,
}: GridProps) {
  return (
    <div className="roadmap-body">
      {apps.map((app, rowIndex) => {
        const maxItems = rowHeights[app.id] || 1;
        const rowHeight = rowPad * 2 + maxItems * chipHeight + Math.max(0, maxItems - 1) * gap;
        const activeFocus = safeString(focusedAppId || hoveredApp || "");
        const isDimmed = Boolean(activeFocus) && activeFocus !== app.id;
        return (
          <div
            key={app.id}
            className={`roadmap-row ${isDimmed ? "is-dimmed" : ""} ${hoveredApp === app.id ? "is-focus" : ""} ${rowIndex % 2 === 0 ? "is-even" : "is-odd"}`}
            style={{ gridTemplateColumns, minHeight: rowHeight }}
            onMouseEnter={() => onHoverApp(app.id)}
            onMouseLeave={() => onHoverApp(null)}
          >
            <AppColumn app={app} isEven={rowIndex % 2 === 0} groupBy={groupBy} compact={compact} onEpicClick={onEpicClick} />
            {pis.map((pi) => {
              const cellKey = `${app.id}|||${pi.id}`;
              const cell = cells[cellKey];
              const isPast = pastPiIds.has(pi.id);
              return (
                <Cell
                  key={pi.id}
                  appId={app.id}
                  piId={pi.id}
                  cell={cell}
                  pointsLabel={pointsLabel}
                  compact={compact}
                  isPast={isPast}
                  showRoi={showRoi}
                  showProgress={showProgress}
                  onCellClick={() => onCellClick(app.id, pi.id)}
                  onFeatureClick={(item, e) => onFeatureClick(item, app.id, pi.id, e)}
                  onMoreClick={() => onMoreClick(app.id, pi.id)}
                  onShowTooltip={onShowTooltip}
                  onMoveTooltip={onMoveTooltip}
                  onHideTooltip={onHideTooltip}
                  selectedFeatureId={selectedFeatureId}
                  maxPoints={maxPoints}
                  maxFte={maxFte}
                  hasPoints={hasPoints}
                />
              );
            })}
          </div>
        );
      })}
    </div>
  );
}
