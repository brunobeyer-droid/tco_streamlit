# TCO Streamlit App

## Run locally
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure Azure SQL (MSSQL) credentials via `.streamlit/secrets.toml`
cp .env.example .env  # fill values

streamlit run main.py
```
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
