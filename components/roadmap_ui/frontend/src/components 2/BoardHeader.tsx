import React from "react";
import { RoadmapPi } from "../types";
import { formatNumber } from "../utils";

type BoardHeaderProps = {
  pis: RoadmapPi[];
  gridTemplateColumns: string;
  headerHeight: number;
  rowHeaderLabel: string;
  signalConfig?: {
    velocity_ref_pts_per_pi?: number;
    velocity_up_ratio?: number;
    velocity_down_ratio?: number;
  } | null;
  piMeta: Record<
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
  >;
};

function toFiniteNumber(value: unknown): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }
  const raw = String(value ?? "").trim();
  if (!raw) return 0;
  // Accept localized separators from backend/debug payloads.
  const normalized = raw.replace(/\s/g, "").replace(/,/g, "");
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function BoardHeader({ pis, gridTemplateColumns, headerHeight, rowHeaderLabel, signalConfig, piMeta }: BoardHeaderProps) {
  const velocityBaselineRef = Math.max(1, toFiniteNumber(signalConfig?.velocity_ref_pts_per_pi ?? 65));
  const velocityUpRatio = Math.max(1, toFiniteNumber(signalConfig?.velocity_up_ratio ?? 1.15));
  const velocityDownRatio = Math.max(0.01, toFiniteNumber(signalConfig?.velocity_down_ratio ?? 0.9));
  return (
    <div className="roadmap-row roadmap-header" style={{ gridTemplateColumns, minHeight: headerHeight }}>
      <div className="roadmap-header-cell roadmap-header-app">{rowHeaderLabel}</div>
      {pis.map((pi) => {
        const demand = toFiniteNumber(pi.demand_fte);
        const capacity = toFiniteNumber(pi.capacity_fte);
        const deliveryVelocity = toFiniteNumber(pi.delivery_velocity_pts_per_pi);
        const hasDeliveryVelocity = deliveryVelocity > 0;
        const velocityRatio = hasDeliveryVelocity ? deliveryVelocity / Math.max(velocityBaselineRef, 1) : 1;
        const velocityArrow = !hasDeliveryVelocity
          ? "—"
          : velocityRatio >= velocityUpRatio
            ? "↑"
            : velocityRatio <= velocityDownRatio
              ? "↓"
              : "→";
        const velocityTrendClass = !hasDeliveryVelocity
          ? "is-stable"
          : velocityRatio >= velocityUpRatio
            ? "is-up"
            : velocityRatio <= velocityDownRatio
              ? "is-down"
              : "is-stable";
        const hasCapacity = capacity > 0;
        const rawRatio = hasCapacity ? demand / capacity : 0;
        const ratio = Math.min(1, Math.max(0, rawRatio));
        const fillClass =
          rawRatio >= 1 ? "is-red" : rawRatio >= 0.85 ? "is-yellow" : "is-green";
        const barClass = hasCapacity ? fillClass : "is-disabled";
        const meta = piMeta[pi.id] || {
          isCurrent: false,
          progress: 0.5,
          hasDates: false,
          isPast: false,
          mode: "fallback",
        };
        const headerClass = [
          "roadmap-header-cell",
          meta.isCurrent ? "is-current" : "",
          meta.isPast ? "is-past" : "",
        ]
          .filter(Boolean)
          .join(" ");
        return (
          <div key={pi.id} className={headerClass}>
            <div className="roadmap-header-title">{pi.label}</div>
            {meta.isCurrent ? (
              <span className="roadmap-header-today">
                {meta.hasDates ? "TODAY" : "CURRENT"}
              </span>
            ) : null}
            <div className="roadmap-header-meta">
              <span className="roadmap-header-pill roadmap-header-capacity">
                {hasCapacity
                  ? `Capacity ${formatNumber(capacity)} FTE`
                  : "Capacity —"}
              </span>
              <span className={`roadmap-header-velocity-indicator ${velocityTrendClass}`}>
                {hasDeliveryVelocity
                  ? (
                    <span title={`Velocity ${formatNumber(deliveryVelocity)} pts/PI (ref ${velocityBaselineRef})`}>
                      {velocityArrow}
                    </span>
                  )
                  : "—"}
              </span>
              <span className="roadmap-header-pill roadmap-header-demand">Demand {formatNumber(demand)} FTE</span>
            </div>
            <div className={`roadmap-header-bar ${barClass}`}>
              <span className={`roadmap-header-bar-fill ${fillClass}`} style={{ width: `${ratio * 100}%` }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}
