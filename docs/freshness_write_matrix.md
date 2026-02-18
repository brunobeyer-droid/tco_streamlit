# Freshness Write Matrix

Scope audited: `pages/0_Welcome.py`, `pages/1_Dashboard.py`, `pages/1_Insights.py`, `pages/2_Roadmap.py`, `pages/2_Invoices.py`, `pages/2_Contracts.py`, `pages/Budget.py`, `pages/3_Programs.py`, `pages/4_Teams.py`, `pages/5_Vendors.py`, `pages/6_Applications.py`, `pages/7_Rates.py`, `pages/10_Settings.py`, `pages/11_Data_Quality.py`, `pages/99_Admin.py`.

Classification legend:
- `portfolio_data_write`: must trigger global freshness invalidation (DATA_VERSION + cache epoch).
- `control_db_write`: must not invalidate portfolio freshness.
- `local_ui_state_write`: no DB freshness impact.

## Matrix

| Page | Write entrypoints | Current refresh hook behavior | Gaps found | Final intended behavior |
|---|---|---|---|---|
| `pages/0_Welcome.py` | none (read-only for costs/KPIs) | n/a | none | read-only; no write invalidation required |
| `pages/1_Dashboard.py` | none (read-only charts/KPIs) | n/a | none | read-only; no write invalidation required |
| `pages/1_Insights.py` | none (read-only analytics) | n/a | none | read-only; no write invalidation required |
| `pages/2_Roadmap.py` | milestone CRUD (`portfolio_data_write`) | `post_write_refresh(...)` present | `bump_version` was implicit/omitted | explicit `post_write_refresh(..., bump_version=True)` on all milestone write paths |
| `pages/2_Invoices.py` | invoice/notes/attachments CRUD (`portfolio_data_write`) | explicit `post_write_refresh(..., bump_version=True)` | none | keep explicit bump true |
| `pages/2_Contracts.py` | contract CRUD (`portfolio_data_write`) | `post_write_refresh(...)` present | `bump_version` omitted | explicit `post_write_refresh(..., bump_version=True)` for create/edit/delete |
| `pages/Budget.py` | none (events disabled; read-only usage) | n/a | none | read-only; no write invalidation required |
| `pages/3_Programs.py` | program/composition/program-additional writes (`portfolio_data_write`) | `_run_post_save_refresh(... bump_version=True default)` | none | keep explicit portfolio invalidation via wrapper |
| `pages/4_Teams.py` | team/headcount/composition/MSP writes (`portfolio_data_write`) | `_run_post_save_refresh(... bump_version=True default)` | none | keep explicit portfolio invalidation via wrapper |
| `pages/5_Vendors.py` | vendor CRUD/orphan remap (`portfolio_data_write`) | `post_write_refresh(...)` present | one save path omitted `bump_version` | explicit `post_write_refresh(..., bump_version=True)` for all vendor writes |
| `pages/6_Applications.py` | apps/groups/mappings CRUD (`portfolio_data_write`) | `_run_post_save_refresh(... bump_version=True default)` | none | keep explicit portfolio invalidation via wrapper |
| `pages/7_Rates.py` | rates/MSP assignments writes (`portfolio_data_write`) | helper called `post_write_refresh(...)` | helper omitted explicit `bump_version` | helper now enforces `bump_version=True` |
| `pages/10_Settings.py` | mixed writes: portfolio settings/mappings (`portfolio_data_write`) + control automation (`control_db_write`) | `_settings_post_write_refresh(...)` used with explicit choices | copy suggested refresh as routine in some places | keep explicit bump policy by write type; update UX copy to repair-only pipeline |
| `pages/11_Data_Quality.py` | none (diagnostics read-only) | n/a | none | read-only; no write invalidation required |
| `pages/99_Admin.py` | admin maintenance, pipeline, cleanup, control/admin ops (mixed) | `_admin_post_write_refresh(...)` used; manual bump where required | refresh-pipeline messaging could imply routine use | keep mixed write policy and reposition pipeline as repair/recompute tool |

## Global contract

1. Any `portfolio_data_write` must invalidate globally:
   - bump DATA_VERSION (directly or via write wrapper/safety-net), and
   - bump session `_cache_epoch` via refresh helper.
2. Any `control_db_write` must **not** bump active portfolio DATA_VERSION.
3. Direct DML via `execute(...)` is protected by a safety-net bump for portfolio DB writes.
4. Schema/bootstrap DDL (`CREATE/ALTER/DROP`) is excluded from safety-net bumps to avoid false churn.
