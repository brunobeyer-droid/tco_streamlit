# Core Smoke Test (Offline)

- PASS / FAIL: FAIL
- Portfolio scope: `(not set)`
- Program filters: `(none)`
- Team filters: `(none)`

## Files involved
- `pages/0_Welcome.py`
- `pages/1_Dashboard.py`
- `pages/1_Insights.py`
- Local datasets: none found

## Functions involved
- `fetch_capacity_demand_pi`
- `fetch_filter_options`
- `get_app_group_costs`
- `get_feature_costs`
- `get_msp_costs`
- `get_pi_costs`
- `get_program_nwf_actuals_monthly`
- `get_program_nwf_actuals_monthly_breakdown`
- `get_unassigned_breakdown`
- `load_cost_model`
- `load_explorer_feature_rows`

## Invariant checks
- FAIL | local datasets available: No readable local dump files found in data/, snapshots/, fixtures/.

## Dataset stats
- No datasets loaded.

## Suggested minimal fix
- local datasets available: Place at least one CSV/Parquet/JSON dump in data/, snapshots/, or fixtures/.
