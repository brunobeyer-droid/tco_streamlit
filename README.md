# TCO Streamlit App (Azure SQL)

Streamlit app for Total Cost of Ownership analytics backed by Azure SQL (MSSQL). Includes dashboard, invoice tracking, ADO feature import, and admin utilities.

## Quick Start
- Create a virtual environment and install dependencies:
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  ```
- Configure MSSQL credentials in `.streamlit/secrets.toml` (see example below).
- Run the app:
  ```bash
  streamlit run main.py
  ```

Open the “MSSQL Health Check” page first to verify drivers and DB connectivity, create tables, and (re)build views as needed.

## Configure MSSQL (`.streamlit/secrets.toml`)
```toml
[db]
engine = "mssql"

[mssql]
server = "tcp:YOUR_SERVER.database.windows.net,1433"
database = "YOUR_DATABASE"
user = "YOUR_USER"
password = "YOUR_PASSWORD"
driver = "ODBC Driver 18 for SQL Server"  # or omit to auto-pick
encrypt = "yes"
trust_server_certificate = "no"

[auth]
admin_email = "admin@admin.com"
admin_password = "admin"
auto_login = true  # optional: auto-sign in as local admin in dev

# Optional: Azure AD SSO (device flow)
#[azuread]
#client_id = "YOUR-AAD-APP-CLIENT-ID"
#tenant_id = "YOUR-TENANT-ID"

# Optional: prefills for ADO OData
#[ado]
#org = "your-org"
#project = "your-project"
#pat = "YOUR_ADO_PAT"
```

Security note: do not commit real credentials to source control. Keep `.streamlit/secrets.toml` local or use environment variables (`MSSQL_*`).

## First-run initialization (NEXT_CONTROL)
On a brand-new control DB, the app will bootstrap the schema and seed the first global admin on the first login page render. This is safe to rerun.

Required env vars for first-run + auth (when `st.secrets` is not available):
- `AUTH_ADMIN_EMAIL`
- `AUTH_ADMIN_PASSWORD`
- `AUTH_AUTO_LOGIN` (true/false)
- `AUTH_SHOW_ADMIN_LOGIN_PAGE` (true/false)
- `AUTH_ENABLE_LOCAL_ADMIN_UI` (true/false)
- `AZUREAD_ENABLED` (true/false)
- `AZUREAD_TENANT_ID`
- `AZUREAD_CLIENT_ID`
- `AZUREAD_CLIENT_SECRET`
- `AZUREAD_REDIRECT_URI`

## Install the SQL Server ODBC Driver
The app uses `pyodbc`, which requires a system ODBC driver for SQL Server (msodbcsql).

- macOS (Homebrew):
  ```bash
  brew install unixodbc
  brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
  brew update && brew install --no-sandbox msodbcsql18 mssql-tools18
  ```
- Ubuntu/Debian:
  ```bash
  curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
  curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
  sudo apt-get update
  sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
  ```
- Windows:
  Install “ODBC Driver 18 for SQL Server”: https://learn.microsoft.com/sql/connect/odbc/windows/release-notes-odbc-sql-server

Verify from Python:
```bash
python -c "import pyodbc; print(pyodbc.drivers())"
```
You should see something like `['ODBC Driver 18 for SQL Server', ...]`.

## Authentication Options
- Local Admin: set `[auth]` `admin_email`/`admin_password`. Enable `auto_login = true` in dev to bypass the login UI.
- Impersonation (simulate SSO users): set `[auth] impersonate = "user@company.com"` and optionally `impersonate_role = "VIEWER|CONTRIBUTOR|ADMIN"`. The app will treat you as that user without SSO.
- Azure AD SSO (device flow): set `[azuread] client_id` and `tenant_id`. In the sidebar, click “Sign in with Microsoft” and follow the device code prompt. Users must exist in the `APP_USERS` table and be active unless they are local admin.

## ADO Features Sync (Live and Mock)
There are two ways to import ADO features:

1) In‑App: “Sync ADO Features” page
   - Upload Excel/CSV with the expected columns or fetch via OData using your PAT.
   - The page normalizes to the canonical schema and upserts into `ADO_FEATURES`.
   - Area hierarchy (`AreaLevel2/3/4`) is preserved so you can map ADO Programs and Teams even when different products nest teams at different levels.
   - Use “MSSQL Health Check” to (re)build analytics views as needed.

2) CLI: `scripts/ado_sync.py`
   - Live OData:
     ```bash
     export MSSQL_SERVER=... MSSQL_DATABASE=... MSSQL_USER=... MSSQL_PASSWORD=...
     export ADO_PAT=...  # Analytics read
     python scripts/ado_sync.py --org ORG --project PROJECT \
     --entity WorkItems \
       --select "WorkItemId,Title,State,CreatedDate,ChangedDate,Effort,Custom_ApplicationName,Custom_InvestmentDimension" \
       --expand "Team($select=TeamName),Iteration($select=IterationPath),Area($select=AreaPath,AreaLevel2,AreaLevel3,AreaLevel4)" \
       --filter "WorkItemType eq 'Feature'"
     ```
   - Mock file (no network):
     ```bash
     python scripts/ado_sync.py --url file:///absolute/path/to/ado_mock.json
     ```
   The CLI follows `@odata.nextLink` and supports either real OData URLs or local JSON (`list` or `{ "value": [...] }`).
   Include the `Area` expand so the loader can derive `PROGRAM_RAW`, `AREA_LEVEL3_RAW`, and `AREA_LEVEL4_RAW` for flexible program/team mapping.

## Simulating SSO and ADO Sync in a Work Environment
When copying this repo to your work machine, gather a few items so you can run the app and simulate integrations at home/offline:

- SSO Simulation (no Microsoft login required):
  - Add an app user in the DB if you want realistic roles, or simply use one of:
    - Local admin: set `[auth] admin_email`, `admin_password`, and `auto_login = true`.
    - Impersonation: set `[auth] impersonate = "user@company.com"` and optionally `impersonate_role`.

- ADO Sync Simulation (no ADO access required at home):
  - On your work network, open the in‑app page “ADO Export & Mock”.
    - Paste a full OData URL (WorkItems), PAT, and click “Fetch & Build JSON”.
    - Download `ado_mock.json` — it contains `{ "value": [...] }` rows.
  - At home, serve that file locally:
    ```bash
    cd /path/to/folder/with/ado_mock.json
    python -m http.server 8000
    # URL will be: http://localhost:8000/ado_mock.json
    ```
  - In “Sync ADO Features”, paste the local URL (or use `file:///.../ado_mock.json`) and continue with preview/upsert.
  - Alternatively use the CLI with `--url file:///.../ado_mock.json`.

## Troubleshooting
- Drivers: Use “MSSQL Health Check” → “ODBC Drivers on Host” to confirm the configured driver is installed.
- Views: Use “Rebuild Analytics Views” in Health Check to (re)create `VW_*` views.
- Secrets: Keep real secrets out of git; prefer local `.streamlit/secrets.toml` or environment variables.
## Release Notes

### 0.6a — 2025-11-30
- CORE: Added a new data source integration for APPTIO to retrieve program actuals.
- Dashboard: New chart to represent the raw data coming from APPTIO called Actuals vs Forecasts.
- Application: New type of application group to properly calculate Feature costs where application name is set to Others (BASE Work).
- Settings: New Reconciliation tab with a KPI highlighting MOITCO accuracy mapping across ADO and other integration tools.

### 0.6 — 2025-11-28
- Dashboard & Welcome: Updated charts to include Program Additional Costs (monthly → PI split) so treemaps and breakdowns reflect new NWF cost types.
- Rates: Rates now retrieved by cost location (not per-team/program); added tabs for clearer layout and per-location editing.
- Teams & Programs: Pages now use the new cost location components; added Headcount session and expanded change logs for Team/Delivery/Contractor (/CS, /C).

### 0.5 — 2025-11-20
- Dashboard: Added Detailed Cost section (Program, Team, App Group, Cost Type, Category, Feature Dimension, Total Cost sorted by Total Cost); updated page title; fixed KPI layout; R&M Reports section planned.
- Edit → Access Control: Reordered “Add Users Manually” to the bottom; formatted “Last Login”; improved user search to avoid auto-selecting all results.
- Edit → Schema: Moved Schema section to the end; updated diagrams; removed Rollover and Rollback tabs.
- Edit → Bulk Edit: Fixed issue where page refresh prevented bulk changes from being saved.
- Edit → Entities: Added Access Control → Users entity.
- Rates: Moved Preview Calculations tab to last position; rate history per PI/Year under consideration.
- Rates → MSP: Fixed duplicate primary key when assigning multiple app groups; MSP assignments now show Team Name only.
- Rates → Teams: Removed seed button from current teams list; merged team composition into Teams (non-MSP); removed duplicated section.
- Rates → Edit Rates: Renamed “current rates” table to Team Rates and grouped it with per-team rates; grouped Program Rates tables together; removed seed default rates button.
- Applications → Explore & Link: Fixed SQL MERGE error in MSP assignment; corrected SQL syntax for transferring ownership of app groups; removed the All App Groups table.
- Invoice Tracking → Contracts: Removed duplicate delete-by-contract option; removed ID fields from UI; added initial Program filter in Edit Contract; prevented duplicate contract creation; improved form layout.
- Invoice Tracking → Invoices: Renamed tab to Invoices; “Responsible” now shows user’s name; invoices filter to the connected user’s Program and Team.
- Welcome: Added version and release notes section; removed Light/Dark toggle; fixed “Invoices This Month”; ensured MSP values show correctly; replaced “Unknown” Feature Investment Dimension with Not Applicable; included MSP in cost breakdown charts.
- General: Created a unified Change Log table for Invoice Tracking, Programs, Teams, Vendor, Application, and Rates pages with Updated At/User/Changes columns.
