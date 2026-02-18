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
name: next-snapshot-contract-checker-offline
description: Detect schema contract drift between local snapshots and generate a migration-focused markdown report without modifying app code. Use when comparing the newest vs previous snapshot in `data/` or `snapshots/`, especially for removed/renamed columns, type changes, row growth anomalies, and risk checks around ADO feature dumps and Apptio NWF/category columns.
---

# NEXT Snapshot Contract Checker Offline

Run an offline contract check between the newest and previous local snapshots.
Generate a report only; never edit product code.

## Workflow

1. Run the checker from repo root:
```bash
.venv/bin/python automation/skills/next-snapshot-contract-checker-offline/scripts/snapshot_contract_checker.py
```

2. Optional tuning:
```bash
.venv/bin/python automation/skills/next-snapshot-contract-checker-offline/scripts/snapshot_contract_checker.py \
  --sample-size 400 \
  --growth-threshold 1.7
```

3. Read report:
- `automation/reports/snapshot_contract_check.md`

## Detection Rules

The checker compares newest snapshot candidate vs previous candidate under `data/` and `snapshots/`.

It detects:
- Removed columns
- Potential renamed columns (name similarity + type hint)
- Data type changes
- Abnormal row growth (default `>= 2.0x` and absolute delta `>= 100`)

It highlights risk areas:
- ADO feature dump patterns in table names (`ado`, `azure devops`, `feature`)
- Apptio NWF/category column patterns (`nwf`, `apptio`, `category`)

## Output Contract

Always write exactly one markdown report:
- `automation/reports/snapshot_contract_check.md`

Always include migration suggestions as guidance only:
- No SQL migrations
- No code edits
- No schema mutations

## Notes

- Operate offline only, using local files.
- If fewer than two snapshot candidates are available, write a report that states the blocker.
- Prefer deterministic checks; do not infer business logic beyond schema and growth signals.
