GLOBAL GUARDRAILS (STRICT) — APPLY TO ALL OUTPUTS

Mode:
- Offline only. Do NOT call external services (no Azure DevOps, no web, no cloud APIs).
- Use local production dumps/snapshots only (e.g., data/, snapshots/, fixtures/).

Language Policy (STRICT):
- All code, identifiers, filenames, variables, functions, classes, comments, docstrings, and technical outputs MUST be written in English only.
- Portuguese may be used only for conversational explanations outside code, never inside generated code or code suggestions.

Safety Policy:
- Do NOT modify code automatically unless explicitly requested.
- Prefer minimal diffs. Avoid large rewrites. Preserve existing behavior unless the goal explicitly requires change.
- Always provide a rollback path and list impacted files.

Architecture Boundaries (STRICT):
- UX Layer (pages/, UI components) must NOT contain business rules or domain logic.
- Domain Layer owns ADO/NEXT logic, derived FTE, velocity, onboarding logic, and data shaping for KPIs.
- Cost Layer owns canonical cost model transformations (invoices, vendors, app instances/groups, NWF categories).
- Prevent cross-layer coupling and circular imports. UI should call domain services, domain may call cost services, never the reverse.

Canonical Cost Model Protection (STRICT):
- Do NOT change cost model logic.
- Invoices remain tied to application instance IDs.
- Default instance behavior must remain consistent (if required by current design).
- Vendor mappings and BASE pool classification must not change unexpectedly.
- Additional Costs remain program-level.
- Avoid joins that duplicate costs (no unintended row multiplication).

Access Control & Scope (STRICT):
- Do NOT break access control, user permissions, or portfolio scoping.
- portfolio_key must always be enforced.
- Team/program filters must be applied consistently across Welcome/Dashboard/Insights.
- Prevent cross-portfolio data leakage.
- No fallback to None/empty portfolio state during page computations.

Liquidfill Widget Protection (STRICT — NON-NEGOTIABLE):
- The liquidfill widget must remain on the same row as the Plan vs Forecast header.
- It must remain in the second column.
- It must not clip under any layout/container change.
- Do not redesign or re-implement the liquidfill logic. Preserve behavior and positioning.

Performance Guardrails:
- Do not introduce expensive repeated dataframe transformations across pages.
- Prefer shared domain services for reusable transformations.
- Identify memoization/pre-aggregation opportunities without changing results.
- Any optimization must preserve outputs and filters.

Reporting:
- All automations/skills must output a concise markdown report under automation/reports/.
- Reports must include PASS/FAIL (when applicable), risks (LOW/MED/HIGH), impacted files, and suggested minimal next steps.

---
name: next-performance-sentinel-offline
description: Run an offline-only performance sentinel for NEXT core page data pipelines using local dumps under data/, snapshots/, and fixtures/. Use when diagnosing slow Welcome/Dashboard/Insights data flows, identifying repeated dataframe transformations, expensive groupby/merge chains, missing caching opportunities, and producing memoization/pre-aggregation recommendations without changing business logic.
---

# Next Performance Sentinel Offline

Run a deterministic, read-only profiler for core NEXT page pipelines and emit a markdown report with optimization recommendations.

## Workflow

1. Run the sentinel script from repository root:

```bash
.venv/bin/python automation/skills/next-performance-sentinel-offline/scripts/run_performance_sentinel.py \
  --repo-root /Users/brunobeyer/Documents/GitHub/NEXT
```

2. Optionally focus on specific pages:

```bash
.venv/bin/python automation/skills/next-performance-sentinel-offline/scripts/run_performance_sentinel.py \
  --repo-root /Users/brunobeyer/Documents/GitHub/NEXT \
  --pages pages/0_Welcome.py pages/1_Dashboard.py
```

3. Read the report:

`automation/reports/performance_sentinel.md`

## Rules

- Run offline only.
- Use local dumps only (`data/`, `snapshots/`, `fixtures/`).
- Do not call Azure DevOps.
- Do not call external APIs.
- Do not auto-edit product logic.
- Surface opportunities through diagnostics and recommendations.

## Output Contract

The report must include:
- Pipeline simulation scope and local dump footprint.
- Approximate execution cost ranking by function.
- Repeated dataframe transformation patterns.
- Expensive groupby/merge chain findings.
- Missing caching opportunity candidates.
- Memoization and pre-aggregation suggestions.

## References

- Heuristic scoring details: `references/heuristics.md`
