# NEXT Core Smoke Test (Offline)

- Overall: **FAIL**
- Repo: `/Users/brunobeyer/Documents/GitHub/NEXT`

## 1) Local snapshot/dump discovery
- Status: **FAIL**
- No candidate snapshot/dump folders found under `data/`, `snapshots/`, or `fixtures/`.

## 2) Invariant checks
- [PASS] **INV-001** - Portfolio scoping and team/program filters are enforced.
  - Details: Selected pytest scope checks passed.
  - References: `tests/test_scope.py::test_derive_user_scope_uses_membership_scope_first`, `tests/test_scope_master_pages.py::test_programs_scope_filter_hides_out_of_scope_programs_for_contributor`, `welcome/state.py::derive_user_scope`, `core/authorization.py::filter_df_by_scope`
- [PASS] **INV-002** - No cross-portfolio leakage through scoped cache keys.
  - Details: Cache isolation check passed in smoke suite.
  - References: `tests/test_cache_isolation.py::test_cost_model_scope_key_is_portfolio_isolated`, `core/cost_model.py::_scope_key`
- [PASS] **INV-003** - Welcome/Dashboard/Insights core pipeline modules import offline.
  - Details: import ok: welcome.layout
import ok: core.data
import ok: visuals.cost_anatomy
import ok: core.insights_cost_scope
  - References: `welcome/layout.py`, `core/data.py`, `visuals/cost_anatomy.py`, `core/insights_cost_scope.py`
- [PASS] **INV-004** - No empty/None portfolio fallback in active portfolio DB resolution.
  - Details: get_active_portfolio_db_cfg() -> cfg=None, err='no_active_portfolio'
  - References: `core/portfolio_runtime.py::get_active_portfolio_db_cfg`
- [FAIL] **INV-005** - Local snapshot/dump folder exists for fully offline runs.
  - Details: No snapshot/dump candidate folder was found.
  - References: `data/`, `snapshots/`, `fixtures/`

## 4) Summary
- 1 invariant check(s) failed.
- Review failing checks and references above.
