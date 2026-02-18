from __future__ import annotations

# Schema manifest used by Admin → Schema & Dependencies.
# Keep this updated when new tables/views are added in db/mssql_backend.py.

EXPECTED_TABLES = [
    {"name": "PROGRAMS", "category": "core", "description": "Programs master data", "keys": ["PROGRAMID", "PROGRAMNAME"]},
    {"name": "TEAMS", "category": "core", "description": "Teams master data", "keys": ["TEAMID", "TEAMNAME", "PROGRAMID"]},
    {"name": "VENDORS", "category": "core", "description": "Vendors master data", "keys": ["VENDORID", "VENDORNAME"]},
    {"name": "APPLICATION_GROUPS", "category": "core", "description": "Applications (groups)", "keys": ["GROUPID", "GROUPNAME", "TEAMID"]},
    {"name": "APPLICATIONS", "category": "core", "description": "Application instances", "keys": ["APPLICATIONID", "GROUPID"]},
    {"name": "INVOICES", "category": "invoices", "description": "Invoices", "keys": ["INVOICEID", "APPLICATIONID", "TEAMID"]},
    {"name": "INVOICE_NOTES", "category": "invoices", "description": "Invoice notes", "keys": ["NOTEID"]},
    {"name": "INVOICE_ATTACHMENTS", "category": "invoices", "description": "Invoice attachments", "keys": ["ATTACHMENTID"]},
    {"name": "ROLLOVER_LOG", "category": "invoices", "description": "Invoice rollover log", "keys": ["BATCH_ID"]},
    {"name": "PROGRAM_ADDITIONAL_COSTS", "category": "apptio", "description": "Program additional costs", "keys": ["PROGRAMID", "YEAR", "MONTH"]},
    {"name": "PROGRAM_APPTIO_WORKIDS", "category": "apptio", "description": "Apptio program mappings", "keys": ["WORK_ID", "PROGRAMID"]},
    {"name": "APPTIO_ACTUALS", "category": "apptio", "description": "Apptio actuals (raw)", "keys": ["WORK_ID", "FISCAL_YEAR", "MONTH"]},
    {"name": "APPTIO_ACTUALS_LINES", "category": "apptio", "description": "Apptio actuals lines", "keys": ["WORK_ID"]},
    {"name": "MAP_APPTIO_TO_COST_TYPE", "category": "apptio", "description": "Apptio cost type mapping", "keys": ["APPTIO_TYPE"]},
    {"name": "TCO_COST_EVENTS", "category": "legacy", "description": "Cost events log", "keys": ["EVENT_ID"]},
    {"name": "APP_GROUP_TEAM_LINKS", "category": "core", "description": "Supporting team links", "keys": ["GROUPID", "TEAMID"]},
    {"name": "TEAM_RATE_HISTORY", "category": "rates", "description": "Team rates per PI", "keys": ["TEAMID", "YEAR", "PI", "LOCATION"]},
    {"name": "PROGRAM_RATE_HISTORY", "category": "rates", "description": "Program rates per PI", "keys": ["PROGRAMID", "YEAR", "PI", "LOCATION"]},
    {"name": "TEAM_HEADCOUNT_HISTORY", "category": "workforce", "description": "Team headcount history", "keys": ["TEAMID", "YEAR", "PI"]},
    {"name": "CONTRACTOR_COMPANY", "category": "workforce", "description": "Contractor companies", "keys": ["COMPANY_ID", "COMPANY_NAME"]},
    {"name": "CONTRACTOR_RATE_HISTORY", "category": "workforce", "description": "Contractor rates", "keys": ["COMPANY_ID", "YEAR", "PI"]},
    {"name": "TEAM_CONTRACTOR_HEADCOUNT", "category": "workforce", "description": "Contractor headcount", "keys": ["TEAMID", "YEAR", "PI"]},
    {"name": "TEAM_MSP_RATE", "category": "msp", "description": "MSP rate card", "keys": ["TEAMID"]},
    {"name": "TEAM_MSP_ASSIGNMENTS", "category": "msp", "description": "MSP assignments", "keys": ["TEAMID", "GROUPID", "YEAR"]},
    {"name": "CONTRACTS", "category": "invoices", "description": "Contracts", "keys": ["CONTRACT_ID", "APPLICATIONID", "TEAMID"]},
    {"name": "ADO_FEATURES", "category": "ado", "description": "ADO features", "keys": ["FEATURE_ID"]},
    {"name": "MAP_ADO_TEAM_TO_TCO_TEAM", "category": "ado", "description": "ADO team mapping", "keys": ["ADO_TEAM_KEY", "TEAMID"]},
    {"name": "MAP_ADO_PROGRAM_TO_TCO_PROGRAM", "category": "ado", "description": "ADO program mapping", "keys": ["ADO_PROGRAM", "PROGRAMID"]},
    {"name": "MAP_ADO_APP_TO_TCO_GROUP", "category": "ado", "description": "ADO app mapping", "keys": ["ADO_APP", "APP_GROUP"]},
    {"name": "ADO_APP_CANDIDATES", "category": "ado", "description": "ADO app candidates", "keys": ["APP_NAME_RAW"]},
    {"name": "ADO_ITERATION_CALENDAR", "category": "ado", "description": "ADO iteration calendar", "keys": ["ITERATION_SK"]},
    {"name": "ADO_PORTFOLIO_SETTINGS", "category": "ado", "description": "ADO portfolio settings", "keys": ["SETTING_KEY"]},
    {"name": "EMAIL_ALERT_CONFIG", "category": "security", "description": "Email alert configuration", "keys": ["ALERT_KEY"]},
    {"name": "APP_USERS", "category": "security", "description": "App users", "keys": ["EMAIL"]},
    {"name": "APP_USER_MEMBERSHIP", "category": "security", "description": "User memberships", "keys": ["USER_EMAIL"]},
    {"name": "ADMIN_AUDIT_LOG", "category": "audit", "description": "Admin audit log", "keys": ["AUDIT_ID"]},
    {"name": "TEAM_COMPOSITION_HISTORY", "category": "workforce", "description": "Team composition history", "keys": ["TEAMID", "YEAR", "PI"]},
    {"name": "PROGRAM_COMPOSITION_HISTORY", "category": "workforce", "description": "Program composition history", "keys": ["PROGRAMID", "YEAR", "PI"]},
    {"name": "TCO_TEAM_COMPOSITION_CHANGELOG", "category": "audit", "description": "Team composition changelog", "keys": ["CHANGE_ID"]},
    {"name": "TCO_PROGRAM_COMPOSITION_CHANGELOG", "category": "audit", "description": "Program composition changelog", "keys": ["CHANGE_ID"]},
]

EXPECTED_VIEWS = [
    {"name": "VW_TEAM_RATE_EFFECTIVE", "description": "Effective team rates", "depends_on": ["TEAM_RATE_HISTORY"]},
    {"name": "VW_PROGRAM_RATE_EFFECTIVE", "description": "Effective program rates", "depends_on": ["PROGRAM_RATE_HISTORY"]},
    {"name": "VW_GLOBAL_RATE_EFFECTIVE", "description": "Global rates", "depends_on": ["TEAM_RATE_HISTORY", "PROGRAM_RATE_HISTORY"]},
    {"name": "VW_TEAM_HEADCOUNT_EFFECTIVE", "description": "Effective team headcount", "depends_on": ["TEAM_HEADCOUNT_HISTORY"]},
    {"name": "VW_CONTRACTOR_RATE_EFFECTIVE", "description": "Effective contractor rates", "depends_on": ["CONTRACTOR_RATE_HISTORY"]},
    {"name": "VW_TEAM_CONTRACTOR_HEADCOUNT_EFFECTIVE", "description": "Effective contractor headcount", "depends_on": ["TEAM_CONTRACTOR_HEADCOUNT"]},
    {"name": "VW_TEAM_WEIGHTED_RATES", "description": "Team weighted rates", "depends_on": ["VW_TEAM_RATE_EFFECTIVE", "TEAM_COMPOSITION_HISTORY"]},
    {"name": "VW_TEAM_CONTRACTOR_RATE_JOIN_GAPS", "description": "Contractor rate join gaps", "depends_on": ["CONTRACTOR_RATE_HISTORY", "TEAM_CONTRACTOR_HEADCOUNT"]},
    {"name": "VW_TEAM_COMPOSITION_EFFECTIVE", "description": "Effective team composition", "depends_on": ["TEAM_COMPOSITION_HISTORY"]},
    {"name": "VW_PROGRAM_COMPOSITION_EFFECTIVE", "description": "Effective program composition", "depends_on": ["PROGRAM_COMPOSITION_HISTORY"]},
    {"name": "VW_MSP_COSTS", "description": "MSP costs", "depends_on": ["TEAM_MSP_ASSIGNMENTS", "TEAM_MSP_RATE", "TEAM_RATE_HISTORY", "PROGRAM_RATE_HISTORY"]},
    {"name": "VW_TCO_FEATURE_DEMAND", "description": "Feature demand (SWAG)", "depends_on": ["ADO_FEATURES", "ADO_ITERATION_CALENDAR", "TEAM_MSP_RATE"]},
    {"name": "VW_TCO_TEAM_LABOR_COMPOSITION", "description": "Team labor composition", "depends_on": ["VW_TCO_FEATURE_DEMAND", "TEAM_COMPOSITION_HISTORY"]},
    {"name": "VW_TCO_WF_LABOR_SPLIT", "description": "Workforce split", "depends_on": ["VW_TCO_FEATURE_DEMAND", "VW_TCO_TEAM_LABOR_COMPOSITION"]},
    {"name": "VW_INVOICE_SPEND", "description": "Invoice spend by program/team", "depends_on": ["INVOICES", "TEAMS", "PROGRAMS", "APPLICATIONS", "APPLICATION_GROUPS"]},
    {"name": "VW_INVOICE_SPEND_PI", "description": "Invoice spend by PI", "depends_on": ["VW_INVOICE_SPEND"]},
    {"name": "VW_PROGRAM_ADDITIONAL_COSTS_PI", "description": "Program additional costs by PI", "depends_on": ["PROGRAM_ADDITIONAL_COSTS", "PROGRAMS"]},
    {"name": "VW_COSTS_AND_INVOICES", "description": "Combined workforce/invoice costs", "depends_on": ["VW_TCO_WF_LABOR_SPLIT", "VW_INVOICE_SPEND_PI", "VW_MSP_COSTS", "VW_PROGRAM_ADDITIONAL_COSTS_PI"]},
    {"name": "VW_TCO_WORKFORCE_SPLIT", "description": "Workforce split to app groups", "depends_on": ["VW_COSTS_AND_INVOICES", "APPLICATION_GROUPS", "TEAMS", "PROGRAMS"]},
    {"name": "VW_TCO_COMPLETENESS", "description": "Completeness score", "depends_on": ["VW_TCO_WORKFORCE_SPLIT", "TEAM_RATE_HISTORY", "APPLICATION_GROUPS", "TEAMS"]},
]

EXPECTED_EDGES = []
for view in EXPECTED_VIEWS:
    for dep in view.get("depends_on", []):
        EXPECTED_EDGES.append((view["name"], dep))
