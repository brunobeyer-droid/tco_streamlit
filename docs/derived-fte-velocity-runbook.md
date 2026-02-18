# Runbook Completo - Modelo Derived FTE + Velocity (P0 -> P7)

## 1. Context
- Repository: `/Users/brunobeyer/Documents/GitHub/NEXT`
- Main objective: make forecast (App Group -> Team -> Program -> Portfolio) reliable, fast, and stable using a configurable demand driver (`SWAG` legacy or `SNAPSHOT_VELOCITY`) without breaking canonical architecture.
- Golden rule: canonical cost logic remains in `core/canonical_costs.py`.

## 2. Execution Rules
- Do not move cost logic to UI pages.
- Keep changes minimal and scoped.
- No broad refactors.
- Keep UI and code text/comments in English.
- Always validate both performance and financial parity.

---

## 3. Phases Overview
1. P0 - Read stability and availability safeguards.
2. P1 - Configurable Derived FTE driver for Forecast.
3. P2 - Canonical projected SQL optimization.
4. P2.1 - Database hardening (indexes + execution plan stability).
5. P3 - Expected demand snapshotization.
6. P4 - Model fidelity observability.
7. P5 - Portfolio/profile governance.
8. P6 - Release and operations robustness.
9. P7 - Production flags and environment baseline.

---

## 4. P0 - Read Stability and Availability (Completed)

### Objective
- Prevent page stalls/timeouts in key flows.
- Prioritize `TCO_TEAM_VELOCITY_SNAPSHOT` over expensive runtime paths.

### Implemented
1. Snapshot-first velocity baseline loading with fail-open behavior.
2. Snapshot staleness checks using `DATA_VERSION`.
3. Optional stale-snapshot usage to preserve availability.
4. Scope-level circuit breaker to avoid query retry storms after timeout.

### Main files
- `core/ado_recon.py`
- `core/data.py`
- `pages/0_Welcome.py`

### Acceptance (P0)
- Pages do not crash on transient DB latency.
- Placeholder/warning appears instead of fatal failure.
- Velocity baseline avoids heavy fallback cascades whenever possible.

---

## 5. P1 - Configurable Derived FTE Driver (Completed)

### Objective
- Support projected demand calculation via `SWAG` or `SNAPSHOT_VELOCITY` per ADO profile.
- Control row-level fallback when snapshot rows are missing.

### Implemented
1. Profile/env-driven forecast settings:
   - `forecast.derived_fte_driver`
   - `forecast.velocity_row_fallback`
2. Canonical projected SQL supports:
   - `SWAG` legacy path.
   - `SNAPSHOT_VELOCITY` path using `TCO_TEAM_VELOCITY_SNAPSHOT`.
3. ADO Profile Editor UI controls in Settings.
4. Snapshot table readiness guard in canonical path.
5. Defaults/normalization updates and test coverage.

### Main files
- `core/canonical_costs.py`
- `core/ado_profile.py`
- `pages/10_Settings.py`
- `db/mssql_backend.py`
- `tests/test_canonical_costs_projected_driver.py`

### Acceptance (P1)
- Driver mode can be switched per profile without page redesign.
- Projected/Expected respects active driver mode.
- Row-level fallback behavior matches profile setting.

---

## 6. P2 - Canonical Projected SQL Optimization (Completed)

### Objective
- Reduce projected query cost without changing financial logic.

### Implemented
1. Pushdown of `program/team` filters at demand extraction stage.
2. Snapshot mode optimization:
   - Aggregate SWAG demand first (`src`).
   - Apply velocity baseline after aggregation.
3. Snapshot CTE filtered by `year/pi`.
4. SQL parameter ordering corrected for safe bind consistency.
5. Remaining non-English UI text fixed in settings.

### Main files
- `core/canonical_costs.py`
- `pages/10_Settings.py`
- `tests/test_canonical_costs_projected_driver.py`

### Validation evidence
- Targeted tests passed (including projected driver and velocity-related tests).

### Acceptance (P2)
- Lower latency for narrow Team/Program scopes.
- No canonical financial regression.

---

## 7. P2.1 - Database Hardening (Partially Completed)

### Objective
- Eliminate Team/Program timeout behavior under real workload.
- Keep pages available with fail-open paths while root-cause SQL tuning continues.

### Technical scope
1. Profile true SQL bottlenecks (no assumptions).
2. Add minimal, safe, idempotent indexes.
3. Validate execution plan and p95 latency improvement.
4. Confirm strict financial parity before/after.

### Deliverables
1. SQL diagnostics script (slowest queries, CPU, reads).
2. SQL index script (idempotent).
3. Before/after report (duration, reads, CPU).
4. Financial parity report on canonical outputs.

### Initial index candidates
1. `TCO_TEAM_VELOCITY_SNAPSHOT (YEAR, PI, TEAMID) INCLUDE (EFFECTIVE_BASELINE_POINTS)`.
2. Additional indexes only after profiling view base tables used by projected canonical joins.

### Implemented in this cycle
1. Query-throttle/circuit behavior validated in runtime (`TCO_COST_QUERY_CIRCUIT_SECONDS`).
2. Insights fail-open guards added for:
   - cost model fetch path,
   - Explorer rows path,
   - capacity-demand path.
3. Data Quality scope isolation and timeout guardrails to avoid cross-page filter lock and retry storms.
4. Capacity demand now reads snapshot-first (`TCO_PROJECTED_DEMAND_SNAPSHOT`) and only falls back to Explorer when needed.

### Acceptance (P2.1)
1. Welcome/Insights/Budget load without timeout on Team/Program filters.
2. SQL CPU no longer stays elevated after navigation.
3. Baseline/Projected canonical totals remain equal (except rounding noise).

### Remaining risks
1. Over-indexing increases write/sync overhead.
2. Incorrect index assumptions can shift bottlenecks.
3. Heavy direct scans on `VW_TCO_FEATURE_DEMAND` can still timeout in ad-hoc diagnostics if snapshots are bypassed.

### Mitigation
1. Apply minimal indexes first.
2. Measure every change step.
3. Keep rollback scripts (`DROP INDEX`) ready.

---

## 8. P3 - Expected Demand Snapshotization (In Progress)

### Objective
- Remove expensive demand computation from interactive page load.

### Implemented
1. Snapshot table and refresh path in place:
   - `TCO_PROJECTED_DEMAND_SNAPSHOT`
   - `scripts/refresh_projected_demand_snapshot.sh`
2. Snapshot-first reads active in canonical projected demand path.
3. Snapshot-first read added for capacity-demand data path (`core/capacity_data.py`) with Explorer fallback.

### Next implementation
1. Materialize expected demand by `data_version` at grain:
   - `year, pi, program, team, group`.
2. Canonical read path becomes snapshot-first with controlled fallback.

### Deliverables
1. Snapshot table + refresh routine.
2. Canonical read integration.
3. Coverage metric for snapshot freshness/completeness.

### Acceptance (P3)
1. Significant latency reduction in filtered scopes.
2. No change in canonical financial totals.

---

## 9. P4 - Model Fidelity Observability (In Progress)

### Objective
- Make model quality transparent for Product Owners.

### Implementation scope
1. KPIs:
   - `% demand rows from velocity snapshot`
   - `% rows using global fallback`
   - `% rows excluded in strict mode`
2. Expose in Welcome/Settings diagnostics.
3. Benchmark validation against known team history (`ERNE WEST`, program `R&M`, full 2025).

### Deliverables
1. Coverage cards.
2. Threshold warnings for excessive fallback.
3. SQL validation pack:
   - `scripts/p4_0_model_fidelity_overview.sql`
   - `scripts/p4_1_team_velocity_benchmark.sql`
   - `scripts/check_delivery_baseline_team.sql`
   - `scripts/p4_2_team_calibration_diagnostics.sql`
   - `scripts/p4_2_team_calibration_snapshot_fast.sql`
   - `scripts/p4_3_program_team_fidelity_fast.sql`

### Execution order (P4)
1. Run `scripts/p4_0_model_fidelity_overview.sql` for `2025..2026`.
2. Run `scripts/check_delivery_baseline_team.sql` for `R&M / ERNE WEST` and confirm Welcome KPI parity.
3. Run `scripts/p4_1_team_velocity_benchmark.sql` for full `2025` and inspect PI-level deltas (`SWAG` vs `Velocity`).
4. Run `scripts/p4_2_team_calibration_diagnostics.sql` for `R&M / ERNE WEST` (`2025`) to validate baseline concentration and historical calibration behavior.
5. Record:
   - snapshot coverage percentage,
   - global fallback percentage,
   - Derived FTE delta distribution,
   - baseline source mix,
   - team baseline concentration (distinct baseline values and weighted baseline by done points).

### Acceptance (P4)
1. Teams can explain forecast behavior changes with evidence.
2. Reliability posture is visible by scope.
3. ERNE WEST benchmark reproduces Welcome Delivery Baseline and PI-level behavior without ambiguity.
4. Historical ERNE WEST calibration explains whether 65/95 concentration is expected data behavior or a model gap.

### Current evidence (MOIT / NEXT_LOCAL, executed on 2026-02-17)
1. ERNE WEST (R&M), 2026:
   - `DERIVED_FTE_SWAG_TOTAL = 48.28`
   - `DERIVED_FTE_VELOCITY_SNAPSHOT_TOTAL = 41.65`
   - `DELTA = -6.63`
2. ERNE WEST (R&M), 2025-2026:
   - `SWAG_POINTS_TOTAL = 4528.00`
   - `DERIVED_FTE_SWAG_TOTAL = 69.66`
   - `DERIVED_FTE_VELOCITY_SNAPSHOT_TOTAL = 60.16`
   - `DELTA = -9.50`
3. Baseline concentration for ERNE WEST in the validated window:
   - distinct baseline points observed: `65` and `95`
4. Program-wide team ranking (R&M, 2025-2026) available via:
   - `scripts/p4_3_program_team_fidelity_fast.sql`
   - ranks teams by absolute velocity-vs-SWAG delta for calibration priority.

---

## 10. P5 - Portfolio/Profile Governance (Planned)

### Objective
- Enable controlled adoption by portfolio with clear policy.

### Proposed implementation
1. Profile-level policy modes:
   - `SWAG`
   - `SNAPSHOT_VELOCITY` with fallback
   - `SNAPSHOT_VELOCITY` strict
2. Progressive rollout path:
   - DEV -> HML -> PROD per portfolio.

### Deliverables
1. Policy matrix per profile.
2. Activation checklist by environment.

### Acceptance (P5)
1. Portfolio-level control without cross-portfolio side effects.
2. Predictable rollout governance.

---

## 11. P6 - Release and Operations Robustness (Planned)

### Objective
- Prevent future performance and financial regressions.

### Proposed implementation
1. Regression suites:
   - Performance (latency/SLO by page and scope).
   - Financial parity (canonical outputs by scenario/dimension).
2. Operational guardrails:
   - Timeout telemetry,
   - incident playbook,
   - release gate checklist.

### Deliverables
1. Pre-release validation pipeline.
2. Incident runbook.
3. Model health dashboard/checkpoints.

### Acceptance (P6)
1. Releases blocked on failed performance or parity checks.
2. Faster diagnosis/recovery in production incidents.

---

## 12. P7 - Production Flags and Environment Baseline (Final Phase)

### Objective
- Standardize production behavior for performance, reliability, and forecast consistency.
- Avoid hidden config drift between DEV and PROD.

### Production flags/env vars (recommended baseline)

| Name | PROD value | Why |
|---|---:|---|
| `TCO_VELOCITY_SNAPSHOT_AUTO_REFRESH` | `0` | Prevent expensive inline snapshot rebuilds on user page load. |
| `TCO_VELOCITY_ALLOW_STALE_SNAPSHOT` | `1` | Prefer availability (read stale snapshot) over runtime fallback storms. |
| `TCO_PROJECTED_DEMAND_SNAPSHOT_ENABLED` | `1` | Keep projected demand snapshot path active. |
| `TCO_PROJECTED_DEMAND_SNAPSHOT_AUTO_REFRESH` | `0` | Avoid expensive inline refresh in interactive requests. |
| `TCO_PROJECTED_DEMAND_SNAPSHOT_STRICT_SCOPE` | `1` | Force scoped reads for Team/Program queries, reducing heavy scans. |
| `TCO_PROJECTED_VELOCITY_FAILOPEN_TO_SWAG` | `1` | Keep projected path available if snapshot coverage is incomplete. |
| `TCO_EXPLORER_VELOCITY_FROM_SNAPSHOT` | `1` | Ensure velocity-derived FTE in diagnostics comes from snapshot logic. |
| `TCO_COST_QUERY_CIRCUIT_SECONDS` | `30` | Throttle repeated timeout retries for the same scope. |
| `TCO_WELCOME_INLINE_FIDELITY` | `0` | Keep Welcome fast; run fidelity only on-demand in Settings/Data Quality. |
| `TCO_DQ_LIGHT_FILTER_OPTIONS` | `1` | Keep Data Quality filter bootstrapping lightweight. |
| `TCO_REFRESH_SNAPSHOTS_AFTER_ADO_SYNC` | `1` | Refresh snapshots right after successful ADO sync. |
| `TCO_REFRESH_SNAPSHOTS_AFTER_OFFLINE_REBUILD` | `1` | Keep offline restore/rebuild consistent with snapshot-first reads. |

### Override flags (use only in controlled diagnostics)

| Name | Default policy | Notes |
|---|---:|---|
| `TCO_PROJECTED_FTE_DRIVER` | `unset` | Prefer ADO Profile setting per portfolio. Use env only for emergency/global override. |
| `TCO_PROJECTED_VELOCITY_ROW_FALLBACK` | `unset` | Prefer ADO Profile setting. Set only for temporary global override tests. |

### Deployment notes (PROD)
1. Set these values as platform environment variables (Azure App Service / Container env), not only in `secrets.toml`.
2. Keep snapshot refreshes in scheduled/sync workflows, not in page-request paths.
3. After changing flags, recycle app instances and validate:
   - Welcome Team/Program scope loads without timeout.
   - Data Quality opens without loop in `Loading scope and filters...`.
   - Settings on-demand velocity fidelity runs within acceptable latency.
4. Use the repo checker to validate effective runtime values:
   - `scripts/check_runtime_flags.py --secrets .streamlit/secrets.toml`
5. Use this baseline template when creating platform env vars:
   - `scripts/prod_flags.env.example`

### Acceptance (P7)
1. Stable page load under Team/Program scopes.
2. No repeated timeout storms for identical scope queries.
3. Forecast demand remains snapshot-first with controlled fail-open behavior.

---

## 13. Handoff Prompt for Another Codex Account
```text
Continue in /Users/brunobeyer/Documents/GitHub/NEXT.
Use this phase runbook:
- P0/P1/P2 completed.
- P2.1/P3/P4 in execution and validation.
- Keep P7 production flag baseline enforced.

Constraints:
- Keep cost logic canonical in core/canonical_costs.py.
- No cost math in UI pages.
- Minimal scoped changes only.
- UI/code text in English.

P2.1 tasks:
1) SQL profiling of projected path under Team/Program filters.
2) Propose and apply minimal idempotent indexes.
3) Validate before/after latency, reads, CPU.
4) Validate canonical financial parity across scopes and scenarios.
5) Deliver scripts + evidence + rollback notes.

Acceptance:
- No timeout on Welcome/Insights/Budget filtered views.
- No sustained SQL CPU spikes after navigation.
- No financial regression.
```

---

## 14. Operational Execution (Recommended)

### Daily/Per-sync flow
1. Run ADO sync (or offline rebuild in DEV restore scenario).
2. Refresh snapshots:
   - `scripts/refresh_velocity_snapshot.sh <YEAR>`
   - `scripts/refresh_projected_demand_snapshot.sh <YEAR> 1 0`
3. Validate model quickly (fast diagnostics):
   - `scripts/p4_2_team_calibration_snapshot_fast.sql` (team benchmark)
   - `scripts/p4_3_program_team_fidelity_fast.sql` (program-wide ranking)
4. Spot-check UI:
   - Welcome: Delivery Baseline KPI
   - Insights: KPI strip and Cost vs Value vs Demand
   - Settings: on-demand velocity diagnostics
5. Run runtime flag checker:
   - `scripts/check_runtime_flags.py --secrets .streamlit/secrets.toml`

### Post-deploy / post-restart pre-warm (recommended)
1. Open the app once with Portfolio scope.
2. Navigate to Insights and wait for first full load (`step 4/5`) to complete.
3. Keep the page open until KPI strip and core charts render.
4. This first run primes cache/plan state; subsequent users/scopes should load faster.
5. Repeat after app recycle, major sync, or environment restart.

### Notes
1. Fast diagnostics are snapshot-based and designed to avoid `VW_TCO_FEATURE_DEMAND` timeout pressure.
2. If snapshots are stale/incomplete, refresh first before interpreting deltas.
