import React, { useMemo, useRef, useState } from "react";
import { Streamlit } from "streamlit-component-lib";

type RoadmapItem = {
  id: string;
  title: string;
  shortTitle: string;
  statusBucket: string;
  state: string;
  points: number;
  fte: number;
  team: string;
  program: string;
  epic_id?: string;
  epic_title?: string;
};

type RoadmapData = {
  meta: {
    max_items_per_cell?: number;
  };
  pis: Array<{ id: string; label: string; demand_fte?: number; capacity_fte?: number | null }>;
  apps: Array<{ id: string; label: string; item_count: number }>;
  cells: Record<
    string,
    {
      remainder: number;
      items: RoadmapItem[];
    }
  >;
};

const palette: Record<string, string> = {
  done: "var(--rm-chip-done)",
  active: "var(--rm-chip-inprogress)",
  planned: "var(--rm-chip-planned)",
};

const accent: Record<string, string> = {
  done: "var(--rm-chip-border-done)",
  active: "var(--rm-chip-border-inprogress)",
  planned: "var(--rm-chip-border-planned)",
};

const textColor: Record<string, string> = {
  done: "var(--rm-chip-text-done)",
  active: "var(--rm-chip-text-inprogress)",
  planned: "var(--rm-chip-text-planned)",
};

const headerHeight = 44;
const leftWidth = 220;
const minColWidth = 260;
const unmappedProgram = "⚠ Unmapped / Needs Mapping";

export function RoadmapGrid({ data: rawData, height }: { data: RoadmapData; height: number }) {
  const data = (rawData as any) ?? {};
  const pis = Array.isArray(data.pis) ? data.pis : [];
  const apps = Array.isArray(data.apps) ? data.apps : [];
  const cells = data.cells && typeof data.cells === "object" ? data.cells : {};

  const [compact, setCompact] = useState(false);
  const [collapsedPrograms, setCollapsedPrograms] = useState<Record<string, boolean>>({});
  const rootRef = useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    if (!rootRef.current) return;
    const update = () => {
      if (!rootRef.current) return;
      const h = Math.min(1400, rootRef.current.scrollHeight + 20);
      Streamlit.setFrameHeight(h);
    };
    update();
    const observer = new ResizeObserver(update);
    observer.observe(rootRef.current);
    return () => observer.disconnect();
  }, [compact, pis.length, apps.length, collapsedPrograms]);

  if (!pis.length || !apps.length) {
    return (
      <div className="roadmap-shell rm-root" style={{ height }} ref={rootRef}>
        <div className="roadmap-empty">No roadmap data available.</div>
      </div>
    );
  }

  const gridTemplateColumns = useMemo(() => {
    return `${leftWidth}px repeat(${pis.length}, minmax(${minColWidth}px, 1fr))`;
  }, [pis.length]);

  const { appProgramMap, programGroups, programStats } = useMemo(() => {
    const appProgramCounts: Record<string, Record<string, number>> = {};
    const programTotals: Record<
      string,
      { fte: number; count: number; done: number; active: number; planned: number; features: Set<string> }
    > = {};

    Object.entries(cells).forEach(([key, cell]) => {
      const [appId] = key.split("|||");
      if (!appId) return;
      if (!appProgramCounts[appId]) appProgramCounts[appId] = {};
      (cell.items || []).forEach((item) => {
        const program = item.program || "(No Program)";
        appProgramCounts[appId][program] = (appProgramCounts[appId][program] || 0) + 1;
        if (!programTotals[program]) {
          programTotals[program] = {
            fte: 0,
            count: 0,
            done: 0,
            active: 0,
            planned: 0,
            features: new Set(),
          };
        }
        programTotals[program].fte += Number(item.fte || 0);
        if (item.id) programTotals[program].features.add(item.id);
        programTotals[program].count += 1;
        const bucket = (item.bucket || "planned").toLowerCase();
        if (bucket === "done") programTotals[program].done += 1;
        else if (bucket === "active") programTotals[program].active += 1;
        else programTotals[program].planned += 1;
      });
    });

    const appProgramMapLocal: Record<string, string> = {};
    apps.forEach((app) => {
      const label = app.label || "";
      if (label.includes("Unmapped")) {
        appProgramMapLocal[app.id] = unmappedProgram;
        return;
      }
      const counts = appProgramCounts[app.id] || {};
      let best = "(No Program)";
      let bestCount = 0;
      Object.entries(counts).forEach(([program, count]) => {
        if (count > bestCount) {
          best = program;
          bestCount = count;
        }
      });
      appProgramMapLocal[app.id] = best;
    });

    const programOrder: string[] = [];
    apps.forEach((app) => {
      const program = appProgramMapLocal[app.id] || "(No Program)";
      if (!programOrder.includes(program)) programOrder.push(program);
    });
    if (programOrder.includes(unmappedProgram)) {
      const filtered = programOrder.filter((p) => p !== unmappedProgram);
      filtered.push(unmappedProgram);
      programOrder.splice(0, programOrder.length, ...filtered);
    }

    const groups: Array<{ program: string; apps: typeof apps }> = [];
    programOrder.forEach((program) => {
      const groupApps = apps.filter((app) => appProgramMapLocal[app.id] === program);
      if (groupApps.length) {
        groups.push({ program, apps: groupApps });
      }
    });

    return {
      appProgramMap: appProgramMapLocal,
      programGroups: groups,
      programStats: programTotals,
    };
  }, [apps, cells]);

  const handleClick = (item: any, appId: string, piId: string) => {
    Streamlit.setComponentValue({
      type: "feature_click",
      feature_id: item.id,
      feature_title: String(item?.title || ""),
      epic_id: String(item?.epic_id || ""),
      epic_title: String(item?.epic_title || ""),
      app_id: appId,
      pi_id: piId,
    });
  };

  const chipHeight = compact ? 18 : 22;
  const gap = compact ? 4 : 6;
  const pad = compact ? 8 : 12;

  const setAllCollapsed = (collapsed: boolean) => {
    const next: Record<string, boolean> = {};
    programGroups.forEach((group) => {
      next[group.program] = collapsed;
    });
    setCollapsedPrograms(next);
  };

  return (
    <div className={`roadmap-shell rm-root ${compact ? "roadmap-compact" : ""}`} style={{ height }} ref={rootRef}>
      <div className="roadmap-toolbar">
        <div className="roadmap-title">Roadmap Board</div>
        <div className="roadmap-toolbar-actions">
          <button className="roadmap-button" onClick={() => setAllCollapsed(true)}>
            Collapse all
          </button>
          <button className="roadmap-button" onClick={() => setAllCollapsed(false)}>
            Expand all
          </button>
          <label className="roadmap-toggle">
            <input
              type="checkbox"
              checked={compact}
              onChange={(e) => setCompact(e.target.checked)}
            />
            <span>{compact ? "Compact" : "Comfortable"}</span>
          </label>
        </div>
      </div>
      <div className="roadmap-scroll" style={{ height }}>
        <div className="roadmap-canvas">
          <div
            className="roadmap-row roadmap-header"
            style={{ gridTemplateColumns, height: headerHeight }}
          >
            <div className="roadmap-header-cell roadmap-header-app">Application</div>
            {pis.map((pi) => (
              <div key={pi.id} className="roadmap-header-cell">
                <div className="roadmap-header-title">{pi.label}</div>
                <div className="roadmap-header-pill">{(pi.fte_total ?? 0).toFixed(1)} FTE</div>
              </div>
            ))}
          </div>
          <div className="roadmap-body">
            {programGroups.map((group) => {
              const stats = programStats[group.program];
              const totalFte = stats ? stats.fte : 0;
              const featureCount = stats ? stats.features.size : 0;
              const done = stats ? stats.done : 0;
              const active = stats ? stats.active : 0;

              const isCollapsed = collapsedPrograms[group.program] || false;
              return (
                <div key={group.program} className="roadmap-program-block">
                  <div
                    className="roadmap-program-header"
                    style={{ gridTemplateColumns, top: headerHeight + 8 }}
                    onClick={() =>
                      setCollapsedPrograms((prev) => ({
                        ...prev,
                        [group.program]: !isCollapsed,
                      }))
                    }
                  >
                    <div className="roadmap-program-title">
                      {group.program} <span className="roadmap-program-count">({group.apps.length})</span>
                    </div>
                    <div className="roadmap-program-pills">
                      <span className="roadmap-program-pill">{totalFte.toFixed(1)} FTE</span>
                      <span className="roadmap-program-pill">{featureCount} features</span>
                      <span className="roadmap-program-pill">{active} active</span>
                      <span className="roadmap-program-pill">{done} done</span>
                    </div>
                    <div className="roadmap-program-toggle">{isCollapsed ? "▸" : "▾"}</div>
                  </div>
                  {!isCollapsed &&
                    group.apps.map((app) => {
                      const maxItemsInRow = pis.reduce((acc, pi) => {
                        const cell = cells[`${app.id}|||${pi.id}`];
                        const chipCount = cell
                          ? (cell.items?.length || 0) + (cell.remainder > 0 ? 1 : 0)
                          : 0;
                        return Math.max(acc, chipCount);
                      }, 1);
                      const rowHeight =
                        pad * 2 +
                        maxItemsInRow * chipHeight +
                        Math.max(0, maxItemsInRow - 1) * gap;

                      return (
                        <div
                          key={app.id}
                          className="roadmap-row"
                          style={{ gridTemplateColumns, minHeight: rowHeight }}
                          title={appProgramMap[app.id] ? appProgramMap[app.id] : ""}
                        >
                          <div className="roadmap-cell roadmap-app" title={app.label}>
                            {app.label}
                          </div>
                          {pis.map((pi) => {
                            const cellKey = `${app.id}|||${pi.id}`;
                            const cell = cells[cellKey];
                            const cellTitle = cell
                              ? `Features: ${cell.count} | Points: ${cell.points} | FTE: ${cell.fte}`
                              : "";
                            return (
                              <div key={pi.id} className="roadmap-cell" title={cellTitle}>
                                {cell ? (
                                  <div className="roadmap-pill-stack">
                                    {cell.items.map((item) => {
                                      const bucket = (item.statusBucket || "planned").toLowerCase();
                                      const style = {
                                        "--chip-bg": palette[bucket] || palette.planned,
                                        "--chip-border": accent[bucket] || accent.planned,
                                        "--chip-text": textColor[bucket] || textColor.planned,
                                        "--chip-shadow-opacity": "0.22",
                                      } as React.CSSProperties;
                                      const tip = `${item.title}\nState: ${item.state}\nPoints: ${item.points}\nFTE: ${item.fte}\nTeam: ${item.team}\nProgram: ${item.program}`;
                                      return (
                                        <div
                                          key={item.id}
                                          className="roadmap-pill"
                                          style={style}
                                          title={tip}
                                          onClick={() => handleClick(item, app.id, pi.id)}
                                        >
                                          <span className="roadmap-pill-accent" />
                                          <span className="roadmap-pill-text">{item.shortTitle || item.title}</span>
                                        </div>
                                      );
                                    })}
                                    {cell.remainder > 0 ? (
                                      <div className="roadmap-badge">+{cell.remainder} more</div>
                                    ) : null}
                                  </div>
                                ) : null}
                              </div>
                            );
                          })}
                        </div>
                      );
                    })}
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
