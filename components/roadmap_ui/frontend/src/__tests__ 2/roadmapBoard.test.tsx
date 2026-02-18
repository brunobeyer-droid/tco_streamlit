/* @vitest-environment jsdom */
import React from "react";
import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";
import { RoadmapBoard } from "../RoadmapBoard";

const { setComponentValue, toPngMock } = vi.hoisted(() => ({ setComponentValue: vi.fn(), toPngMock: vi.fn() }));

vi.mock("streamlit-component-lib", () => ({
  Streamlit: {
    setFrameHeight: vi.fn(),
    setComponentValue,
  },
}));
vi.mock("html-to-image", () => ({
  toPng: toPngMock,
}));

class ResizeObserverMock {
  observe() {}
  disconnect() {}
}

let compactToolbarMode = false;

function setCompactToolbarMode(next: boolean) {
  compactToolbarMode = next;
}

function installMatchMediaMock() {
  vi.stubGlobal(
    "matchMedia",
    vi.fn().mockImplementation((query: string) => {
      const isCompactQuery = query.includes("(max-width: 1200px)");
      const listeners = new Set<(event: MediaQueryListEvent) => void>();
      return {
        matches: isCompactQuery ? compactToolbarMode : false,
        media: query,
        onchange: null,
        addEventListener: (_type: string, listener: (event: MediaQueryListEvent) => void) => {
          listeners.add(listener);
        },
        removeEventListener: (_type: string, listener: (event: MediaQueryListEvent) => void) => {
          listeners.delete(listener);
        },
        addListener: (listener: (event: MediaQueryListEvent) => void) => {
          listeners.add(listener);
        },
        removeListener: (listener: (event: MediaQueryListEvent) => void) => {
          listeners.delete(listener);
        },
        dispatchEvent: (event: Event) => {
          listeners.forEach((listener) => listener(event as MediaQueryListEvent));
          return true;
        },
      };
    })
  );
}

beforeEach(() => {
  setComponentValue.mockClear();
  toPngMock.mockReset();
  toPngMock.mockResolvedValue("data:image/png;base64,abc");
  setCompactToolbarMode(false);
  vi.stubGlobal("ResizeObserver", ResizeObserverMock);
  installMatchMediaMock();
  const store = new Map<string, string>();
  Object.defineProperty(window, "localStorage", {
    value: {
      getItem: (k: string) => (store.has(k) ? store.get(k)! : null),
      setItem: (k: string, v: string) => {
        store.set(k, String(v));
      },
      removeItem: (k: string) => {
        store.delete(k);
      },
      clear: () => {
        store.clear();
      },
      key: (idx: number) => Array.from(store.keys())[idx] || null,
      get length() {
        return store.size;
      },
    },
    configurable: true,
  });
});

afterEach(() => {
  cleanup();
});

const baseData = {
  meta: {
    row_mode: "apps",
    rows_by: "Applications",
    labels: {
      completed: "Completed",
      inProgress: "In Progress",
      validation: "Validation",
      planned: "Planned",
    },
    capabilities: {
      milestoneCreate: true,
      milestoneDelete: true,
      configureGroupBy: true,
    },
  },
  pis: [{ id: "2026 I1", label: "2026 I1", demand_fte: 1, capacity_fte: 2 }],
  apps: [{ id: "App A", label: "App A", item_count: 1 }],
  cells: {
    "App A|||2026 I1": {
      remainder: 0,
      items: [
        {
          id: "1001",
          title: "Feature A",
          shortTitle: "Feature A",
          statusBucket: "planned",
          state: "New",
          points: 8,
          fte: 0.2,
          business_value: 55,
          team: "Team A",
          program: "Program A",
        },
      ],
    },
  },
  timeline: {
    epic: { id: "2001", title: "Epic A", progress: 0.2 },
    features: [{ id: "1001", title: "Feature A", epicId: "2001", epicTitle: "Epic A", start: "2026-01-01", end: "2026-01-20", businessValue: 55, team: "Team A", program: "Program A", application: "App A" }],
    epics: [{ id: "2001", title: "Epic A", progress: 0.2, businessValueSum: 55 }],
    piBands: [{ piKey: "2026 I1", label: "2026 I1", start: "2026-01-01", end: "2026-01-30" }],
    links: [],
    milestones: [],
  },
};

describe("RoadmapBoard", () => {
  it("opens details drawer on feature click", () => {
    render(<RoadmapBoard data={baseData as any} height={820} />);
    fireEvent.click(screen.getByText("Feature A"));
    expect(screen.getByText("Details")).toBeInTheDocument();
    expect(screen.getByText("Feature #1001")).toBeInTheDocument();
  });

  it("shows feature ADO link when feature_url is missing but epic_url is present", () => {
    const data = {
      ...baseData,
      cells: {
        "App A|||2026 I1": {
          remainder: 0,
          items: [
            {
              ...(baseData as any).cells["App A|||2026 I1"].items[0],
              feature_url: "",
              epic_url: "https://dev.azure.com/org1/proj1/_workitems/edit/2001",
            },
          ],
        },
      },
    };
    render(<RoadmapBoard data={data as any} height={820} />);
    fireEvent.click(screen.getByText("Feature A"));
    const link = screen.getByRole("link", { name: "Open Feature in ADO" });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute("href", "https://dev.azure.com/org1/proj1/_workitems/edit/1001");
  });

  it("applies configure state in-memory", () => {
    render(<RoadmapBoard data={baseData as any} height={820} />);
    fireEvent.click(screen.getByText("Feature A"));
    fireEvent.click(screen.getByRole("button", { name: "Configure" }));
    fireEvent.change(screen.getByDisplayValue("Comfortable"), { target: { value: "compact" } });
    fireEvent.change(screen.getByDisplayValue("ROI (Business Value)"), { target: { value: "name" } });
    fireEvent.change(screen.getByDisplayValue("Descending"), { target: { value: "asc" } });
    expect(screen.getByDisplayValue("Compact")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Name")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Ascending")).toBeInTheDocument();
  });

  it("emits milestone_batch_apply from drawer save", () => {
    render(<RoadmapBoard data={baseData as any} height={820} />);
    fireEvent.click(screen.getByText("Feature A"));
    fireEvent.click(screen.getByRole("button", { name: "Milestones" }));
    fireEvent.change(screen.getByPlaceholderText("Title"), { target: { value: "Release R1" } });
    fireEvent.change(document.querySelector('input[type="date"]') as HTMLInputElement, { target: { value: "2026-03-01" } });
    fireEvent.click(screen.getByText("Add marker"));
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
    expect(setComponentValue).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "milestone_batch_apply",
      })
    );
  });

  it("shows milestone draft summary and structured save result banner", () => {
    const withFlag = {
      ...baseData,
      meta: {
        ...(baseData as any).meta,
        feature_flags: {
          roadmap_flag_milestone_feedback_v4: true,
        },
      },
    };
    const { rerender } = render(<RoadmapBoard data={withFlag as any} height={820} />);
    fireEvent.click(screen.getByText("Feature A"));
    fireEvent.click(screen.getByRole("button", { name: "Milestones" }));
    fireEvent.change(screen.getByPlaceholderText("Title"), { target: { value: "Release R1" } });
    fireEvent.change(document.querySelector('input[type="date"]') as HTMLInputElement, { target: { value: "2026-03-01" } });
    fireEvent.click(screen.getByText("Add marker"));
    expect(screen.getByText("Draft changes pending")).toBeInTheDocument();
    expect(screen.getByText("+1 creates · -0 deletes")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
    const synced = {
      ...withFlag,
      timeline: {
        ...(withFlag as any).timeline,
        milestones: [{ id: "9001", label: "Release R1", date: "2026-03-01", type: "custom" }],
      },
    };
    rerender(<RoadmapBoard data={synced as any} height={820} />);
    expect(screen.getAllByText("Saved: 1 created, 0 deleted").length).toBeGreaterThan(0);
  });

  it("shows hidden-selection notice and reveal action when search hides selected feature", () => {
    render(<RoadmapBoard data={baseData as any} height={820} />);
    fireEvent.click(screen.getByText("Feature A"));
    fireEvent.change(screen.getByPlaceholderText("Search in loaded items"), { target: { value: "does-not-match" } });
    expect(screen.getByText("Selected item hidden by current filter.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Reveal selection" }));
    expect((screen.getByPlaceholderText("Search in loaded items") as HTMLInputElement).value).toBe("");
  });

  it("renders separate milestones and bands rails in timeline view", () => {
    const { container } = render(<RoadmapBoard data={baseData as any} height={820} />);
    fireEvent.click(screen.getByRole("button", { name: "Timeline" }));
    expect(container.querySelector(".roadmap-timeline-milestones")).toBeInTheDocument();
    expect(container.querySelector(".roadmap-timeline-bands-row")).toBeInTheDocument();
  });

  it("shows BV badges when ROI toggle is enabled", () => {
    const { container } = render(<RoadmapBoard data={baseData as any} height={820} />);
    fireEvent.click(screen.getByLabelText("Show ROI (BV)"));
    expect(container.querySelector(".roadmap-pill-roi")).toBeInTheDocument();
  });

  it("uses compact toolbar mode at <=1200px and opens configure controls from Controls button", () => {
    setCompactToolbarMode(true);
    render(<RoadmapBoard data={baseData as any} height={820} />);
    expect(screen.getByText("Controls")).toBeInTheDocument();
    expect(screen.getByText("Reset")).toBeInTheDocument();
    expect(screen.queryByTitle("Sort rows and items by")).not.toBeInTheDocument();
    expect(screen.queryByText("Compact")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Open configure controls" }));
    expect(screen.getByRole("button", { name: "Configure" })).toHaveClass("is-active");
  });

  it("keeps non-compact toolbar controls visible above compact breakpoint", () => {
    setCompactToolbarMode(false);
    render(<RoadmapBoard data={baseData as any} height={820} />);
    expect(screen.queryByRole("button", { name: "Controls" })).not.toBeInTheDocument();
    expect(screen.getByTitle("Sort rows and items by")).toBeInTheDocument();
    expect(screen.getByText("Compact")).toBeInTheDocument();
    expect(screen.getByText("Reset view")).toBeInTheDocument();
  });

  it("allows ROI toggle from configure drawer in compact toolbar mode", () => {
    setCompactToolbarMode(true);
    const { container } = render(<RoadmapBoard data={baseData as any} height={820} />);
    fireEvent.click(screen.getByRole("button", { name: "Open configure controls" }));
    fireEvent.click(screen.getByLabelText("Show ROI (BV)"));
    expect(container.querySelector(".roadmap-pill-roi")).toBeInTheDocument();
  });

  it("uses epic terminology in timeline and hides epic BV when ROI toggle is off", () => {
    const { container } = render(<RoadmapBoard data={{ ...baseData, meta: { ...(baseData as any).meta, rows_by: "Epics" } } as any} height={820} />);
    fireEvent.click(screen.getByRole("button", { name: "Timeline" }));
    expect(container.textContent || "").not.toMatch(/Initiative/i);
    expect(container.textContent || "").toMatch(/Epic · 1/);
    expect(container.textContent || "").not.toMatch(/BV 55/);
  });

  it("renders team flags in epic rows", () => {
    const { container } = render(<RoadmapBoard data={{ ...baseData, meta: { ...(baseData as any).meta, rows_by: "Epics" } } as any} height={820} />);
    fireEvent.click(screen.getByRole("button", { name: "Timeline" }));
    const flag = container.querySelector(".roadmap-timeline-team-flag");
    expect(flag).toBeInTheDocument();
    expect(flag?.textContent).toContain("Team A");
  });

  it("renders dependency count badges in timeline", () => {
    const data = {
      ...baseData,
      timeline: {
        ...(baseData as any).timeline,
        features: [
          { id: "1001", title: "Feature A", epicId: "2001", epicTitle: "Epic A", start: "2026-01-01", end: "2026-01-10", businessValue: 10, team: "Team A", program: "Program A", application: "App A" },
          { id: "1002", title: "Feature B", epicId: "2001", epicTitle: "Epic A", start: "2026-01-06", end: "2026-01-20", businessValue: 15, team: "Team A", program: "Program A", application: "App A" },
        ],
        links: [{ sourceId: "1001", targetId: "1002" }],
      },
      meta: {
        ...(baseData as any).meta,
        feature_flags: { roadmap_flag_dependency_focus_v3: true },
      },
    };
    const { container } = render(<RoadmapBoard data={data as any} height={820} />);
    fireEvent.click(screen.getByRole("button", { name: "Timeline" }));
    fireEvent.click(screen.getByRole("button", { name: "Expand all" }));
    const depsToggle = screen.getByRole("checkbox", { name: /Dependencies/i });
    expect(depsToggle).not.toBeChecked();
    fireEvent.click(depsToggle);
    expect(container.textContent || "").toMatch(/↗ 1/);
    expect(container.textContent || "").toMatch(/↘ 1/);
  });

  it("resizes timeline left rail without local persistence", async () => {
    render(<RoadmapBoard data={{ ...baseData, meta: { ...(baseData as any).meta, rows_by: "Epics" } } as any} height={820} />);
    fireEvent.click(screen.getByRole("button", { name: "Timeline" }));
    const resizer = screen.getByLabelText("Resize epic column");
    fireEvent.mouseDown(resizer, { clientX: 260 });
    fireEvent.mouseMove(window, { clientX: 360 });
    fireEvent.mouseUp(window);
    await waitFor(() => {
      const key = Array.from({ length: window.localStorage.length })
        .map((_, i) => window.localStorage.key(i))
        .find((k) => (k || "").startsWith("roadmap.ui."));
      expect(key).toBeFalsy();
    });
  });

  it("switches group by locally without emitting view_change", () => {
    render(<RoadmapBoard data={baseData as any} height={820} />);
    fireEvent.click(screen.getByRole("button", { name: "Epics" }));
    expect(screen.getByRole("button", { name: "Epics" })).toHaveClass("is-active");
    expect(setComponentValue).not.toHaveBeenCalledWith(expect.objectContaining({ type: "view_change" }));
  });

  it("defaults to Epics when payload rows_by is Epics", () => {
    render(<RoadmapBoard data={{ ...baseData, meta: { ...(baseData as any).meta, rows_by: "Epics" } } as any} height={820} />);
    expect(screen.getByRole("button", { name: "Epics" })).toHaveClass("is-active");
  });

  it("applies non-gray capacity bar status classes in board header", () => {
    const { container } = render(<RoadmapBoard data={baseData as any} height={820} />);
    const bar = container.querySelector(".roadmap-header-bar");
    expect(bar).toBeInTheDocument();
    expect(bar).toHaveClass("is-green");
  });

  it("today button recenters board window to today's PI when current PI is out of view", () => {
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date("2026-01-15T12:00:00Z"));
      const pis = [
        { id: "2026 I1", label: "2026 I1", start_date: "2026-01-01", end_date: "2026-01-31", demand_fte: 1, capacity_fte: 2 },
        { id: "2026 I2", label: "2026 I2", start_date: "2026-02-01", end_date: "2026-02-28", demand_fte: 1, capacity_fte: 2 },
        { id: "2026 I3", label: "2026 I3", start_date: "2026-03-01", end_date: "2026-03-31", demand_fte: 1, capacity_fte: 2 },
        { id: "2026 I4", label: "2026 I4", start_date: "2026-04-01", end_date: "2026-04-30", demand_fte: 1, capacity_fte: 2 },
        { id: "2027 I1", label: "2027 I1", start_date: "2027-01-01", end_date: "2027-01-31", demand_fte: 1, capacity_fte: 2 },
      ];
      const cells: Record<string, any> = {};
      pis.forEach((pi, idx) => {
        cells[`App A|||${pi.id}`] = {
          remainder: 0,
          items: [
            {
              id: `100${idx + 1}`,
              title: `Feature ${idx + 1}`,
              shortTitle: `Feature ${idx + 1}`,
              statusBucket: "planned",
              state: "New",
              points: 8,
              fte: 0.2,
              business_value: 10 + idx,
              team: "Team A",
              program: "Program A",
            },
          ],
        };
      });
      const data = {
        ...baseData,
        pis,
        apps: [{ id: "App A", label: "App A", item_count: 5 }],
        cells,
      };

      render(<RoadmapBoard data={data as any} height={820} />);
      expect(screen.getByText(/Window:\s*2026 I1/)).toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: "Next ▶" }));
      expect(screen.getByText(/Window:\s*2026 I2/)).toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: "Today" }));
      expect(screen.getByText(/Window:\s*2026 I1/)).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });

  it("exports PNG snapshot in board view", async () => {
    render(<RoadmapBoard data={baseData as any} height={820} />);
    fireEvent.click(screen.getByRole("button", { name: "Export" }));
    fireEvent.click(screen.getByRole("button", { name: "Export PNG snapshot" }));
    await waitFor(() => {
      expect(toPngMock).toHaveBeenCalledTimes(1);
    });
  });

  it("exports PNG snapshot in timeline view", async () => {
    render(<RoadmapBoard data={{ ...baseData, meta: { ...(baseData as any).meta, rows_by: "Epics" } } as any} height={820} />);
    fireEvent.click(screen.getByRole("button", { name: "Timeline" }));
    fireEvent.click(screen.getByRole("button", { name: "Export" }));
    fireEvent.click(screen.getByRole("button", { name: "Export PNG snapshot" }));
    await waitFor(() => {
      expect(toPngMock).toHaveBeenCalledTimes(1);
    });
  });
});
