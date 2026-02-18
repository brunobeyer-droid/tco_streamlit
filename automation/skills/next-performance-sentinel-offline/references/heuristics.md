# Heuristic Model

This skill is intentionally offline and read-only. It estimates relative performance risk from AST patterns and local dump scale.

## Inputs

- Page entry files: default `pages/0_Welcome.py`, `pages/1_Dashboard.py`, `pages/1_Insights.py` (if present)
- Imported `core.*` call sites in those pages
- Local dump footprint from `data/`, `snapshots/`, `fixtures/`

## Cost Weights

- `merge`: 6
- `groupby`: 5
- `join`: 5
- `pivot_table`: 6
- `apply`: 4
- `agg`: 2
- `transform`: 3
- `sort_values`: 2
- `reset_index`: 1
- `assign`: 1

Approximate function score:

`base = sum(operation_count * weight)`

`chain_bonus = long_dataframe_chains * 2`

`repeat_bonus = repeated_chains * 3`

`raw_score = base + chain_bonus + repeat_bonus`

`scaled_score = raw_score * (1 + log10(max(local_rows, 1)) / 4)`

Estimated milliseconds are derived from scaled score for ranking only and are not benchmark timings.

## Detection Rules

- Repeated dataframe transformation:
  - same normalized method-chain appears at least twice
- Expensive chain:
  - chain contains `groupby` and `merge`, or chain length >= 5 with at least one heavy op
- Missing caching opportunity:
  - scaled score >= threshold and function lacks cache decorators (`cache_data_portfolio`, `cache_resource_portfolio`, `st.cache_*`, `lru_cache`)

## Recommendation Rules

- Memoization candidate:
  - expensive + called by page flow + no cache decorator
- Pre-aggregation candidate:
  - high `groupby` density or repeated groupby dimensions indicated by repeated chains

