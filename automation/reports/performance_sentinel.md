# NEXT Performance Sentinel (Offline)

Generated: 2026-02-17 21:39:09

## Execution Mode

- Offline-only: Yes
- External APIs called: No
- Azure DevOps called: No
- App logic modified: No

## Pipeline Simulation Scope

- `pages/0_Welcome.py`
- `pages/1_Dashboard.py`
- `pages/1_Insights.py`
- Detected core function call targets: 63
- Analyzed function definitions resolved: 62

## Local Dump Footprint

- Approximate total local rows/events: 0

## Approximate Cost by Function

| Rank | Function | Approx ms | Score | groupby | merge | Chains | Cache |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | `core.insights_cost_scope.build_initiative_feature_detail_df` | 508.2 | 121.0 | 9 | 2 | 29 | No |
| 2 | `core.canonical_costs.get_cost_lines` | 483.0 | 115.0 | 7 | 0 | 109 | No |
| 3 | `core.portfolio_signals.build_delivery_signals` | 428.4 | 102.0 | 7 | 4 | 28 | No |
| 4 | `core.capacity_data.fetch_capacity_demand_pi` | 273.0 | 65.0 | 2 | 2 | 53 | Yes |
| 5 | `core.ado_recon.load_explorer_feature_rows` | 247.8 | 59.0 | 0 | 0 | 31 | Yes |
| 6 | `core.canonical_costs.get_pi_costs` | 218.4 | 52.0 | 3 | 0 | 21 | No |
| 7 | `core.ado_recon.load_velocity_fidelity_metrics` | 176.4 | 42.0 | 3 | 1 | 16 | Yes |
| 8 | `core.nwf_program.get_program_nwf_actuals_monthly_breakdown` | 117.6 | 28.0 | 0 | 0 | 33 | No |
| 9 | `core.data.fetch_ado_features` | 96.6 | 23.0 | 0 | 0 | 5 | Yes |
| 10 | `core.program_strategic_maturity.calculate_program_strategic_maturity` | 96.6 | 23.0 | 0 | 3 | 4 | No |
| 11 | `core.velocity_baseline.select_velocity_snapshot` | 88.2 | 21.0 | 0 | 0 | 27 | No |
| 12 | `core.program_maturity.calculate_program_maturity` | 75.6 | 18.0 | 0 | 0 | 5 | No |
| 13 | `core.canonical_costs.get_unassigned_breakdown` | 67.2 | 16.0 | 1 | 0 | 10 | No |
| 14 | `core.portfolio_signals.build_financial_signals` | 67.2 | 16.0 | 1 | 0 | 9 | No |
| 15 | `core.data.fetch_pi_calendar_resolved_dates` | 54.6 | 13.0 | 0 | 0 | 5 | Yes |
| 16 | `core.ui.render_headline_with_help` | 54.6 | 13.0 | 0 | 0 | 2 | No |
| 17 | `core.data.attach_pi_period_columns` | 50.4 | 12.0 | 0 | 1 | 15 | No |
| 18 | `core.insights_cost_scope.build_initiative_plot_df` | 50.4 | 12.0 | 1 | 0 | 12 | No |
| 19 | `core.invoices_alerts.summarize_alerts` | 50.4 | 12.0 | 0 | 0 | 8 | No |
| 20 | `core.nwf_program.get_program_nwf_actuals_monthly` | 46.2 | 11.0 | 0 | 0 | 9 | No |

## Repeated DataFrame Transformation Patterns

- `to_numeric -> fillna`
  - core.insights_cost_scope.build_initiative_feature_detail_df x3
  - core.canonical_costs.get_cost_lines x11
  - core.portfolio_signals.build_delivery_signals x3
  - core.capacity_data.fetch_capacity_demand_pi x6
- `to_numeric -> fillna -> astype`
  - core.insights_cost_scope.build_initiative_feature_detail_df x2
  - core.canonical_costs.get_cost_lines x7
  - core.ado_recon.load_explorer_feature_rows x3
  - core.canonical_costs.get_pi_costs x3
- `join`
  - core.canonical_costs.get_cost_lines x2
  - core.ado_recon.load_explorer_feature_rows x6
  - core.canonical_costs.get_pi_costs x2
  - core.nwf_program.get_program_nwf_actuals_monthly_breakdown x2
- `get -> fillna -> astype`
  - core.canonical_costs.get_cost_lines x24
  - core.ado_recon.load_velocity_fidelity_metrics x2
  - core.nwf_program.get_program_nwf_actuals_monthly_breakdown x11
  - core.velocity_baseline.select_velocity_snapshot x4
- `get -> fillna`
  - core.canonical_costs.get_cost_lines x24
  - core.ado_recon.load_velocity_fidelity_metrics x2
  - core.nwf_program.get_program_nwf_actuals_monthly_breakdown x11
  - core.velocity_baseline.select_velocity_snapshot x4
- `to_numeric -> astype`
  - core.insights_cost_scope.build_initiative_feature_detail_df x2
  - core.capacity_data.fetch_capacity_demand_pi x12
  - core.ado_recon.load_explorer_feature_rows x2
  - core.data.attach_pi_period_columns x3
- `apply`
  - core.insights_cost_scope.build_initiative_feature_detail_df x4
  - core.canonical_costs.get_cost_lines x2
  - core.capacity_data.fetch_capacity_demand_pi x3
  - core.ado_recon.load_explorer_feature_rows x2
- `groupby`
  - core.insights_cost_scope.build_initiative_feature_detail_df x5
  - core.canonical_costs.get_cost_lines x3
  - core.portfolio_signals.build_delivery_signals x7
  - core.capacity_data.fetch_capacity_demand_pi x2
- `fillna -> astype`
  - core.canonical_costs.get_cost_lines x5
  - core.portfolio_signals.build_delivery_signals x5
  - core.velocity_baseline.select_velocity_snapshot x3
  - core.portfolio_signals.build_financial_signals x3
- `sum -> reset_index`
  - core.portfolio_signals.build_delivery_signals x2
  - core.capacity_data.fetch_capacity_demand_pi x2
  - core.canonical_costs.get_pi_costs x3

## Expensive GroupBy/Merge Chain Findings

- `core.insights_cost_scope.build_initiative_feature_detail_df`: expensive_chains=1, groupby=9, merge=2
- `core.portfolio_signals.build_delivery_signals`: expensive_chains=1, groupby=7, merge=4
- `core.capacity_data.fetch_capacity_demand_pi`: expensive_chains=1, groupby=2, merge=2
- `core.ado_recon.load_velocity_fidelity_metrics`: expensive_chains=1, groupby=3, merge=1

## Missing Caching Opportunities

- `core.insights_cost_scope.build_initiative_feature_detail_df` (score=121.0, approx_ms=508.2) - no cache decorator detected.
- `core.canonical_costs.get_cost_lines` (score=115.0, approx_ms=483.0) - no cache decorator detected.
- `core.portfolio_signals.build_delivery_signals` (score=102.0, approx_ms=428.4) - no cache decorator detected.
- `core.canonical_costs.get_pi_costs` (score=52.0, approx_ms=218.4) - no cache decorator detected.
- `core.nwf_program.get_program_nwf_actuals_monthly_breakdown` (score=28.0, approx_ms=117.6) - no cache decorator detected.

## Suggested Memoization Candidates

- `core.insights_cost_scope.build_initiative_feature_detail_df` (groupby, reset_index, agg) - add argument-keyed memoization for repeated page filters.
- `core.canonical_costs.get_cost_lines` (groupby, agg, reset_index) - add argument-keyed memoization for repeated page filters.
- `core.portfolio_signals.build_delivery_signals` (groupby, sort_values, reset_index) - add argument-keyed memoization for repeated page filters.
- `core.canonical_costs.get_pi_costs` (reset_index, groupby, join) - add argument-keyed memoization for repeated page filters.
- `core.nwf_program.get_program_nwf_actuals_monthly_breakdown` (join) - add argument-keyed memoization for repeated page filters.
- `core.program_strategic_maturity.calculate_program_strategic_maturity` (merge, sort_values, reset_index) - add argument-keyed memoization for repeated page filters.
- `core.velocity_baseline.select_velocity_snapshot` (general dataframe work) - add argument-keyed memoization for repeated page filters.
- `core.program_maturity.calculate_program_maturity` (apply, sort_values, reset_index) - add argument-keyed memoization for repeated page filters.

## Suggested Pre-Aggregation Points

- `core.insights_cost_scope.build_initiative_feature_detail_df` - pre-aggregate repeated groupby outputs before merge fan-out (groupby=9, merge=2).
- `core.canonical_costs.get_cost_lines` - pre-aggregate repeated groupby outputs before merge fan-out (groupby=7, merge=0).
- `core.portfolio_signals.build_delivery_signals` - pre-aggregate repeated groupby outputs before merge fan-out (groupby=7, merge=4).
- `core.capacity_data.fetch_capacity_demand_pi` - pre-aggregate repeated groupby outputs before merge fan-out (groupby=2, merge=2).
- `core.canonical_costs.get_pi_costs` - pre-aggregate repeated groupby outputs before merge fan-out (groupby=3, merge=0).
- `core.ado_recon.load_velocity_fidelity_metrics` - pre-aggregate repeated groupby outputs before merge fan-out (groupby=3, merge=1).
- `core.program_strategic_maturity.calculate_program_strategic_maturity` - pre-aggregate repeated groupby outputs before merge fan-out (groupby=0, merge=3).

## Notes

- Cost values are approximate ranking heuristics, not benchmark timings.
- Report is intentionally diagnostic-only; no source code changes are applied.
