# Reapply Thread Fix Pack v2 (NEXT2)

Applied from `/Users/brunobeyer/Documents/GitHub/NEXT` into `/Users/brunobeyer/Downloads/NEXT2`:

- core/ado_recon.py
- core/canonical_costs.py
- core/data.py
- db/__init__.py
- db/bootstrap_portfolio_db.py
- db/control_db.py
- db/mssql_backend.py
- main.py
- pages/0_Welcome.py
- pages/10_Settings.py
- pages/1_Dashboard.py
- pages/1_Insights.py
- pages/2_Contracts.py
- pages/2_Invoices.py
- pages/4_Teams.py
- pages/6_Applications.py
- pages/Budget.py
- scripts/ado_sync.py
- utils/app_refresh.py
- utils/app_shell.py
- utils/auth.py
- utils/sidebar.py
- utils/theme.py

Already aligned from previous pass:
- core/init.py
- pages/2_Roadmap.py
- pages/11_Data_Quality.py
- scripts/* snapshot/diagnostics helpers
- docs runbooks

Validation:
- `py_compile` passed for core/db/pages/utils key files.
- Streamlit AppTest smoke passed for Welcome, Dashboard, Insights, Roadmap, Data Quality.

Operational note:
- If you still see `Authentication is not available`, run NEXT2 with Python that has dependencies (streamlit/pyodbc/msal), e.g.:
  `/Users/brunobeyer/Documents/GitHub/NEXT/.venv/bin/python -m streamlit run main.py`
