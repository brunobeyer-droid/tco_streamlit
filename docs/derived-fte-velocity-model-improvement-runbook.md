# Runbook - Derived FTE Velocity Model Improvement (Trend-Based)

## 1. Objective

Define and operationalize a safer, more realistic velocity-driven model for `Derived FTE` in future PIs, replacing pure last-known carry-forward with controlled trend smoothing.

This runbook is focused on model evolution only. It does **not** change canonical cost ownership or UI page responsibilities.

---

## 2. Current Behavior (Baseline)

Current snapshot behavior is:

1. Resolve team baseline from the latest valid completed PI in `TCO_TEAM_VELOCITY_SNAPSHOT`.
2. Carry that baseline forward into future PIs until a newer completed PI exists.

Example:
- If `ERNE WEST` latest valid baseline is `95 pts/PI` (from `2025 PI4`), future PIs in `2026` show `95` until `2026 PI1` closes and snapshot refresh updates baseline.

This is stable and fast, but can lag reality when team throughput changes quickly.

---

## 3. Target Model

Move from **single-point carry-forward** to **snapshot trend baseline**:

1. Use recent history window per team (default last 3 completed PIs).
2. Compute a weighted baseline with recency bias.
3. Apply bounded trend adjustment.
4. Emit a confidence score and source label.
5. Keep hard fail-open/fallback rules for availability.

### 3.1 Formula (default proposal)

For each team:

1. Recent completed PI velocities:
   - `v_t` (most recent), `v_t-1`, `v_t-2`
2. Weighted moving average:
   - `v_wma = 0.5*v_t + 0.3*v_t-1 + 0.2*v_t-2`
3. Trend factor:
   - `trend = clamp((v_t - v_t-1) / max(v_t-1, eps), -0.15, +0.15)`
4. Final baseline:
   - `v_final = v_wma * (1 + alpha*trend)` where `alpha=0.5` by default
5. Guardrails:
   - `v_final = clamp(v_final, floor_pts, ceil_pts)` (defaults: `45..130`)

If fewer than 3 valid points exist:
- 2 points: use `0.65/0.35` weighted blend.
- 1 point: use last-known carry-forward.
- 0 points: use global fallback baseline (current legacy behavior).

### 3.2 Confidence

Compute confidence per team snapshot row:

Inputs:
- sample size (1/2/3+ PIs)
- volatility (CV of recent velocities)
- recency gap (how old is latest completed PI)

Output:
- `HIGH`, `MEDIUM`, `LOW`
- numeric score `0..100`

Confidence is advisory and must be transparent in diagnostics.

---

## 4. Data Contract Changes

Extend snapshot artifacts (no heavy runtime recompute on page render):

### 4.1 `TCO_TEAM_VELOCITY_SNAPSHOT` additions

Add columns:

- `BASELINE_METHOD` (`LAST_PI`, `WMA_TREND`, `GLOBAL_FALLBACK`)
- `BASELINE_CONFIDENCE_BAND` (`HIGH`, `MEDIUM`, `LOW`)
- `BASELINE_CONFIDENCE_SCORE` (`FLOAT`)
- `BASELINE_HISTORY_COUNT` (`INT`)
- `BASELINE_TREND_PCT` (`FLOAT`)
- `BASELINE_WINDOW_LABEL` (e.g. `LAST_3_PI`)

### 4.2 Optional detail table

Optional table for auditability:

- `TCO_TEAM_VELOCITY_MODEL_AUDIT`
- One row per team refresh with intermediate values (`v_t`, `v_t-1`, `v_t-2`, wma, trend, final, reason/fallback).

---

## 5. Refresh Pipeline Rules

Model runs only in controlled refresh paths:

1. After ADO sync (`TCO_REFRESH_SNAPSHOTS_AFTER_ADO_SYNC=1`)
2. After offline rebuild (`TCO_REFRESH_SNAPSHOTS_AFTER_OFFLINE_REBUILD=1`)
3. Manual/admin refresh scripts

Must **not** run in page-request path.

---

## 6. Config Flags

Add/standardize these model flags:

- `TCO_VELOCITY_MODEL_MODE=LAST_PI|WMA_TREND`
- `TCO_VELOCITY_MODEL_WINDOW_PI=3`
- `TCO_VELOCITY_MODEL_TREND_ALPHA=0.5`
- `TCO_VELOCITY_MODEL_TREND_CLAMP=0.15`
- `TCO_VELOCITY_MODEL_FLOOR_PTS=45`
- `TCO_VELOCITY_MODEL_CEIL_PTS=130`
- `TCO_VELOCITY_MODEL_MIN_SAMPLE=2`

Keep existing reliability/perf controls:

- `TCO_VELOCITY_SNAPSHOT_AUTO_REFRESH=0`
- `TCO_VELOCITY_ALLOW_STALE_SNAPSHOT=1`
- `TCO_COST_QUERY_CIRCUIT_SECONDS=30`

---

## 7. Rollout Plan

### P5.1 - Offline Backtest (no behavior change)

1. Implement model in refresh script/procedure in shadow mode.
2. Store both:
   - current baseline (`LAST_PI`)
   - candidate baseline (`WMA_TREND`)
3. Produce backtest report by team/program/year.

Primary benchmark:
- `ERNE WEST`, full `2025`, then compare projected quality in `2026 PI1+`.

### P5.2 - Canary Enablement

1. Enable `WMA_TREND` for selected portfolio(s)/program(s) only.
2. Monitor:
   - forecast error vs realized throughput
   - timeout rate
   - fallback rate
   - confidence distribution

### P5.3 - Broad Enablement

1. Promote `WMA_TREND` to default where metrics are stable.
2. Keep explicit override for legacy `LAST_PI`.

---

## 8. Validation Metrics

Track by team/program and PI:

1. `MAPE` between forecasted demand/capacity balance and realized outcome proxy.
2. `Bias` (systematic over/under-forecast).
3. `Fallback ratio` (`GLOBAL_FALLBACK` and `LAST_PI` usage while in trend mode).
4. Confidence quality:
   - high-confidence rows should show lower forecast error than low-confidence rows.

Acceptance targets (initial):

1. No regression in timeout/error rates.
2. >= 10% reduction in forecast bias for canary teams.
3. Fallback ratio under agreed threshold (e.g. < 20%).

---

## 9. UI/Diagnostics Requirements

### 9.1 Settings (ADO/Advanced)

Show:
- `Derived FTE total (SWAG)`
- `Derived FTE total (Velocity)`
- model method and confidence summary

No expensive real-time velocity recomputation in interactive page loads.

### 9.2 Welcome KPI

Delivery Baseline card should display:
- baseline value
- source method (`LAST_PI` or `WMA_TREND`)
- confidence band

When unavailable:
- show explicit `N/A` with reason (no silent fallback text).

---

## 10. Risk Controls

1. Hard timeout protection remains active (`circuit` behavior).
2. Snapshot-first reads remain mandatory.
3. Any heavy diagnostics stay on-demand only.
4. Trend mode must fail open to stable baseline (`LAST_PI` then global fallback).

---

## 11. Implementation Checklist

1. Add model flags and defaults.
2. Extend snapshot schema for method/confidence metadata.
3. Implement trend baseline in snapshot refresh path.
4. Add shadow/backtest report script.
5. Add Settings diagnostics for method/confidence.
6. Add Welcome baseline metadata display.
7. Execute canary rollout and collect metrics.
8. Decide default mode per portfolio based on evidence.

---

## 12. Decision Record Template

For each portfolio/program:

- Scope:
- Mode selected (`LAST_PI` or `WMA_TREND`):
- Effective date:
- Backtest summary:
- Timeout/error impact:
- Fallback ratio:
- Confidence calibration result:
- Approver:

