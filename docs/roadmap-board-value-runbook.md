# Runbook - Roadmap Board Value Extensions (Velocity + Decision Signals)

## 1. Objective

Increase decision quality in `pages/2_Roadmap.py` and the Roadmap board UI by adding operational signals on top of canonical demand/capacity data (without changing canonical financial logic).

Principles:
- Keep canonical costs and Derived FTE ownership unchanged.
- Add transparent indicators (explainable rules, visible basis).
- Prefer incremental rollout with debug visibility.

---

## 2. Current State

Already available:
- Capacity vs demand at PI scope.
- Derived FTE model with velocity-aware forecast pipeline.
- Board and timeline views with Epic/Application grouping.

Gap:
- Users see "what" (load) but limited signal on "confidence" and "risk trajectory."

---

## 3. Implemented in this cycle

### 3.1 Delivery Trend Signal (board row-level)

Status: Implemented.

What:
- Row-level delivery trend badge (`Trend Stable`, `Trend Down`, `Trend Volatile`, `Trend Up`, `Trend Low Data`) in board left rail.

How:
- Built from PI series per row (Epic/Application) using:
  - primary metric: `FEATURE_PROGRESS_COMPLETED_SP`
  - fallback metric: `POINTS`
- Classification rule:
  - `Downtrend`: recent trend <= -20%
  - `Volatile`: coefficient of variation >= 0.45
  - `Uptrend`: recent trend >= +20%
  - `Stable`: low variation / no strong trend
  - `Low Data`: insufficient non-zero history

Notes:
- Signal is advisory and non-financial.
- Canonical demand/cost math remains unchanged.
- Naming intentionally avoids conflict with team velocity baseline (`EFFECTIVE_BASELINE_POINTS`).

### 3.2 Confidence Score (board row-level)

Status: Implemented.

What:
- Row-level confidence badge (`Conf <score> High/Medium/Low`) in board left rail.

How:
- Deterministic score (0-100), non-financial:
  - PI coverage ratio contribution.
  - PI date completeness contribution.
  - Feature volume sufficiency contribution.
  - Delivery trend quality contribution.
- Banding:
  - `High`: >= 75
  - `Medium`: 55-74
  - `Low`: < 55

Explainability:
- Added debug panel with confidence inputs and breakdown per row.

### 3.3 Hybrid Confidence (feature + epic/app)

Status: Implemented.

What:
- Confidence now uses a hybrid model:
  - feature-level confidence score/band
  - aggregated epic/app confidence score for board badges
- Drawer (Epic details) now shows `Top low-confidence features` for focused mitigation.

How:
- Feature confidence is computed from coverage/date/story-signal/mapping quality.
- Epic/App confidence is weighted aggregation (feature confidence) with trend adjustment.

### 3.4 Burn Risk Next 2 PI (advisory)

Status: Implemented.

What:
- Added row-level `Burn Risk` signal for next 2 PI lookahead (`high`, `medium`, `low`).
- Exposed as compact symbol in board left rail and as reason in tooltip.

How:
- Uses next-2-PI demand vs capacity envelope plus confidence/trend context.
- Emits concise advisory reason (`Demand above capacity`, etc.).

### 3.5 Signal Legend in Drawer

Status: Implemented.

What:
- Added compact legend in `Drawer > Details` for `Vel`, `Trend`, `Conf`, and `Risk`.
- Keeps symbols self-explanatory without adding board noise.

### 3.6 Dependency Impact (board row-level)

Status: Implemented.

What:
- Added compact dependency impact symbol in board left rail.
- Tooltip explains impact score and blocked/blocking counts.

How:
- Built from timeline dependency links for features in each row scope.
- Produces `dependency_impact_score` + impact band (`high`, `medium`, `low`).

### 3.7 Signal Threshold Config (no frontend hardcode)

Status: Implemented.

What:
- Velocity/trend/confidence/burn thresholds are now centralized in backend and sent in payload `meta.signal_config`.
- Board header `Vel` arrows now read thresholds from payload, not hardcoded values.

Default values:
- `velocity_ref_pts_per_pi=65`
- `velocity_up_ratio=1.15`
- `velocity_down_ratio=0.90`
- `trend_down_threshold=-0.20`, `trend_up_threshold=0.20`
- `trend_volatile_cv=0.45`, `trend_stable_cv=0.25`
- `confidence_high_min=75`, `confidence_medium_min=55`
- `burn_high_util=1.10`, `burn_tight_util=0.95`, `burn_medium_util=0.90`

Env override keys:
- `TCO_ROADMAP_SIGNAL_VEL_REF`
- `TCO_ROADMAP_SIGNAL_VEL_UP_RATIO`
- `TCO_ROADMAP_SIGNAL_VEL_DOWN_RATIO`
- `TCO_ROADMAP_SIGNAL_TREND_DOWN`
- `TCO_ROADMAP_SIGNAL_TREND_UP`
- `TCO_ROADMAP_SIGNAL_TREND_VOLATILE_CV`
- `TCO_ROADMAP_SIGNAL_TREND_STABLE_CV`
- `TCO_ROADMAP_SIGNAL_CONF_HIGH_MIN`
- `TCO_ROADMAP_SIGNAL_CONF_MEDIUM_MIN`
- `TCO_ROADMAP_SIGNAL_BURN_HIGH_UTIL`
- `TCO_ROADMAP_SIGNAL_BURN_TIGHT_UTIL`
- `TCO_ROADMAP_SIGNAL_BURN_MEDIUM_UTIL`

---

## 4. Next Value Increments

### 4.1 Delivery Velocity by PI in Header (snapshot-only)

Goal:
- Show `Delivery Velocity (pts/PI)` in board header beside `Capacity vs Demand` for faster portfolio steering.

Display:
- Per-PI metric in header chip/column meta:
  - `Capacity FTE`
  - `Demand FTE`
  - `Delivery Velocity (pts/PI)` (same concept as Welcome baseline)

Data source (performance requirement):
- Use `TCO_TEAM_VELOCITY_SNAPSHOT` (or snapshot-backed view only).
- Do **not** query raw/real-time velocity computation paths on page render.

Scope behavior:
- Respect active filters (Program/Team/Application) and PI window.
- For multi-team scope, aggregate velocity as weighted average by `PI_POINTS_DONE` (same approach used in Welcome snapshot logic).

Non-functional:
- Must remain fast for portfolio scopes; snapshot reads only.
- If snapshot is missing for scope/PI, show `N/A` with graceful fallback (no heavy recompute).

### 4.2 Confidence / Reliability Score

Goal:
- Show confidence per row/PI for plan reliability.

Candidate inputs:
- Date completeness.
- SWAG/Derived FTE readiness.
- Dependency concentration/open critical links.
- Velocity stability.

Output:
- Score 0-100 + band (`High`, `Medium`, `Low`) with tooltip breakdown.

### 4.3 What-if Fast Simulation (UI-only)

Goal:
- Let users test sensitivity without mutating canonical data.

Controls:
- Capacity multiplier (`-10% .. +10%`).
- Velocity multiplier (`-10% .. +10%`).

Output:
- Over/under capacity delta by PI and row.

### 4.4 Burn Risk Forward (2 PI lookahead)

Goal:
- Surface near-term delivery burn risk (not only current PI).

Output:
- `At Risk Next 2 PI` flag with concise reason (`demand trend + low confidence`, etc.).

### 4.5 Dependency Load with Impact

Goal:
- Move from dependency count to impact relevance.

Output:
- `Blocked Features`, `Blocking Features`, `Critical Chain` badge and sidebar summary.

### 4.6 Delta vs Baseline Snapshot

Goal:
- Weekly governance view of movement.

Output:
- `Delta Demand FTE`, `Delta Capacity FTE`, `Delta Risk` vs previous snapshot in scope.

### 4.7 Signal Legend (simple)

Goal:
- Make board symbols/flags self-explanatory for fast executive reads.

Scope:
- Add compact legend for:
  - `Vel` arrow symbols (`↑`, `→`, `↓`)
  - `Trend` badge states
  - `Conf` band (`High`, `Medium`, `Low`)

Placement options:
- Preferred: sidebar/drawer help block (always reachable, low visual noise).
- Alternative: small "Legend" popover in board header.

Content style:
- One-line meaning per symbol/flag, plain language.
- Must state "advisory/non-financial" for trend/confidence signals.

### 4.8 Sidebar UX Restructure (high-impact)

Goal:
- Significantly improve sidebar clarity by reducing mixed content and creating predictable information zones.

Proposed approach:
- Replace current mixed single-flow sidebar with tabs (or segmented sections) to separate concerns.
- Recommended tabs:
  - `Details`: selected Epic/Feature context, key fields, links.
  - `Signals`: concise signal interpretation (`Vel`, `Trend`, `Conf`, `Risk`) with current values.
  - `Legend`: clear one-line explanation for each icon/symbol/color used in board and sidebar.
  - `Actions` (optional): quick actions (fit selection, open timeline context, milestone shortcuts).

Legend content standard:
- Keep each item in one short line: `Symbol + Name + Meaning`.
- Include explicit advisory note for non-financial signals.
- Use same icon/color tokens as board to avoid interpretation mismatch.

UX acceptance criteria:
- User can identify meaning of any icon in <= 5 seconds.
- Sidebar content density reduced (no long card stacks in default view).
- Primary details remain visible without scrolling on laptop viewport.
- No regression in selection behavior (feature/epic click + toggle close).

Status update:
- Phase 1 implemented:
  - Added `Details` sub-tabs inside drawer: `Overview`, `Signals`, `Legend`.
  - Moved icon explanations to dedicated `Legend` tab.
  - Added compact `Signals` tab to reduce mixed content in `Overview`.
- Phase 2 implemented:
  - Top drawer tabs switched to compact icon+label mode to free vertical/horizontal space.
  - `Legend` now includes "How NEXT Calculates" for transparency:
    - Velocity reference and up/down thresholds.
    - Trend rule and volatility CV thresholds.
    - Confidence formula (feature components + trend adjustment) and band cutoffs.
    - Burn risk next-2-PI utilization thresholds.
- Next:
  - Validate wording with business users and simplify terms if needed.
  - Optional: add a "Copy model rules" action for governance docs.

---

## 5. Data Contract Extensions (recommended)

For board payload:
- `pis[]` optional fields: `delivery_velocity_pts_per_pi`, `delivery_velocity_source` (expected `SNAPSHOT`)
- `apps[]` optional non-canonical fields: `delivery_trend`, `confidence_score`, `burn_risk_2pi`, `dependency_impact_score`, `delta_demand_fte`, `delta_capacity_fte`

Rules:
- Keep fields optional and backwards-compatible.
- Keep computational logic in page/backend, not duplicated across React components.

---

## 6. Rollout Plan

Phase A (done):
- Delivery Trend badge.

Phase B (done):
- Delivery Velocity by PI in header (snapshot-only).

Phase C (done):
- Confidence Score + diagnostics panel.

Phase D (done):
- Burn Risk 2 PI + dependency impact implemented.

Phase E:
- What-if controls + snapshot deltas.

Gating:
- Add per-feature-flag toggles (default off in prod, staged enablement).
- Include debug expander with raw inputs for each signal.

---

## 7. Validation Checklist

- No changes to canonical cost functions behavior.
- Board/timeline filters still match scoped rows.
- Signal labels deterministic for same input scope.
- Header delivery velocity comes from snapshot source only.
- UI remains responsive for large portfolios.
- Debug diagnostics available for explainability.

---

## 8. Suggested KPIs

- Planning confidence adoption rate (views with score consulted).
- Reduction of late PI scope churn after signal rollout.
- Precision of `Burn Risk` alerts vs realized slip.
- Ratio of "low_data" rows over time (data quality progress).
