export type RoadmapItem = {
  id: string;
  title: string;
  shortTitle: string;
  statusBucket: string;
  state: string;
  points: number;
  fte: number;
  team: string;
  program: string;
  application?: string;
  mapping_status?: string;
  feature_url?: string;
  epic_id?: string;
  epic_title?: string;
  epic_state?: string;
  epic_url?: string;
  progress_pct?: number;
  progress_total_sp?: number;
  progress_proposed_sp?: number;
  progress_inprogress_sp?: number;
  progress_completed_sp?: number;
  epic_progress_pct?: number;
  epic_feature_count_total?: number;
  epic_feature_count_with_stories?: number;
  epic_feature_count_with_sp?: number;
  business_value?: number | null;
};

export type RoadmapCell = {
  remainder: number;
  items: RoadmapItem[];
};

export type RoadmapPi = {
  id: string;
  label: string;
  demand_fte?: number;
  capacity_fte?: number | null;
  start_date?: string;
  end_date?: string;
  start_ts?: string;
  end_ts?: string;
};

export type RoadmapApp = {
  id: string;
  label: string;
  item_count: number;
  epic_id?: string;
  bv_sum?: number;
  demand_fte_sum?: number;
  feature_count?: number;
  avg_progress?: number;
};

export type RoadmapData = {
  meta?: {
    window?: { startPiId?: string | null; size?: number };
    row_mode?: string;
    rows_by?: string;
    labels?: {
      completed?: string;
      inProgress?: string;
      validation?: string;
      planned?: string;
    };
    theme?: { sidebar_bg?: string };
    ui_defaults?: {
      view?: "board" | "timeline";
      density?: "comfortable" | "compact";
      timeframePreset?: "3m" | "6m" | "1y" | "fit" | "custom";
      granularity?: "month" | "pi" | "sprint";
      zoom?: number;
      timeWindowStart?: string;
      timeWindowEnd?: string;
      fitSelectionNonce?: number;
      todayAnchorToken?: number;
      hoveredFeatureId?: string;
      dependencyFocus?: { sourceId: string; targetId: string } | null;
      timelineLeftWidth?: number;
      showDependencies?: boolean;
      showProgress?: boolean;
      dependencyLineMode?: "auto" | "always";
      searchQuery?: string;
      drawer?: "closed" | "details" | "configure" | "milestones";
      showRoi?: boolean;
      sortBy?: "roi_bv" | "demand" | "name" | "progress" | "feature_count";
      sortDir?: "asc" | "desc";
    };
    capabilities?: {
      milestoneCreate?: boolean;
      milestoneDelete?: boolean;
      configureGroupBy?: boolean;
      configureSort?: boolean;
      showRoi?: boolean;
    };
    status?: {
      text?: string;
    };
  };
  pis?: RoadmapPi[];
  apps?: RoadmapApp[];
  cells?: Record<string, RoadmapCell>;
  timeline?: RoadmapTimeline | null;
};

export type RoadmapSelection =
  | { kind: "none" }
  | { kind: "feature"; featureId: string; appId?: string; piId?: string; epicId?: string }
  | { kind: "epic"; epicId: string; title?: string }
  | { kind: "milestone"; milestoneId: string }
  | { kind: "cell"; appId: string; piId: string }
  | { kind: "dependency"; sourceId: string; targetId: string };

export type RoadmapUiState = {
  view: "board" | "timeline";
  density: "comfortable" | "compact";
  timeframePreset: "3m" | "6m" | "1y" | "fit" | "custom";
  granularity: "month" | "pi" | "sprint";
  zoom: number;
  timeWindowStart?: string;
  timeWindowEnd?: string;
  fitSelectionNonce?: number;
  todayAnchorToken?: number;
  hoveredFeatureId?: string;
  dependencyFocus?: { sourceId: string; targetId: string } | null;
  timelineLeftWidth?: number;
  groupBy: "applications" | "epics";
  showDependencies: boolean;
  showProgress: boolean;
  showRoi: boolean;
  dependencyLineMode: "auto" | "always";
  searchQuery: string;
  sortBy: "roi_bv" | "demand" | "name" | "progress" | "feature_count";
  sortDir: "asc" | "desc";
  milestoneDraftSummary?: {
    creates: number;
    deletes: number;
    dirty: boolean;
  };
  milestoneLastSaveResult?: {
    created: number;
    deleted: number;
    failed: number;
    message: string;
  };
  selection: RoadmapSelection;
  drawer: "closed" | "details" | "configure" | "milestones";
};

export type RoadmapEventEnvelope = {
  type:
    | "selection_change"
    | "view_change"
    | "drawer_open"
    | "milestone_create"
    | "milestone_delete"
    | "milestone_batch_apply"
    | "window_change"
    | "ui_error"
    | "frontend_ready";
  requestId: string;
  payload?: Record<string, unknown>;
};

export type RoadmapTimelineEpic = {
  id: string;
  title: string;
  progress: number;
  businessValueSum?: number;
  featureCount?: number;
  epicUrl?: string;
};

export type RoadmapTimelineFeature = {
  id: string;
  title: string;
  epicId?: string;
  epicTitle?: string;
  state?: string;
  start?: string | null;
  end?: string | null;
  progress?: number;
  points?: number;
  fte?: number;
  team?: string;
  program?: string;
  application?: string;
  piKey?: string;
  sprintKey?: string;
  dateSource?: string;
  businessValue?: number;
  featureUrl?: string;
  epicUrl?: string;
};

export type RoadmapTimelinePiBand = {
  piKey: string;
  label: string;
  start?: string | null;
  end?: string | null;
};

export type RoadmapTimelineSprintBand = {
  sprintKey: string;
  label: string;
  start?: string | null;
  end?: string | null;
};

export type RoadmapTimelineLink = {
  sourceId: string;
  targetId: string;
  type?: string;
};

export type RoadmapTimelineMilestone = {
  id: string;
  label: string;
  date: string;
  type?: "custom" | "boundary";
  epicId?: string | null;
  featureId?: string | null;
  tag?: string | null;
  sourceType?: string | null;
};

export type RoadmapTimeline = {
  epic?: RoadmapTimelineEpic | null;
  epics?: RoadmapTimelineEpic[];
  focusEpicId?: string;
  features?: RoadmapTimelineFeature[];
  piBands?: RoadmapTimelinePiBand[];
  sprintBands?: RoadmapTimelineSprintBand[];
  defaultGranularity?: "pi" | "sprint";
  links?: RoadmapTimelineLink[];
  milestones?: RoadmapTimelineMilestone[];
};
