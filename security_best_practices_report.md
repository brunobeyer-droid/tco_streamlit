# Security Best Practices Report

## Executive Summary
This review covered the Python/Streamlit backend, admin tooling, and the React/TypeScript frontend component. The highest-risk issues are secret exposure patterns and insecure local-auth/session persistence behavior that can lead to privilege abuse in non-SSO mode. I also found credential storage weaknesses and one frontend URL handling gap.

Scope highlights:
- Backend: Streamlit app, auth/session logic, DB helpers, admin console.
- Frontend: React roadmap component.
- Dependency spot check: `npm audit` for frontend runtime deps reported 0 known vulnerabilities.

## Critical Findings

### SBP-001: Plaintext secrets present in local secrets file
- Severity: Critical
- Location:
  - `.streamlit/secrets.toml:11`
  - `.streamlit/secrets.toml:18`
  - `.streamlit/secrets.toml:23`
  - `.streamlit/secrets.toml:34`
  - `.streamlit/secrets.toml:42`
- Evidence: DB passwords, admin password, Azure client secret, and ADO PAT are stored as plaintext values.
- Impact: Any local compromise, screen-share leak, backup leak, or accidental commit exposes privileged credentials and long-lived tokens.
- Fix:
  - Rotate all exposed credentials/tokens immediately.
  - Replace real values with placeholders in local examples.
  - Move sensitive values to environment variables or a secrets manager (Key Vault, etc.).
- Mitigation:
  - Add pre-commit secret scanning (`gitleaks`/`detect-secrets`).
  - Keep `.streamlit/secrets.toml` untracked (already in `.gitignore`) and use `.streamlit/secrets.example.toml` for documentation.
- False positive notes: File is ignored by git, but risk still exists for local exfiltration and accidental disclosure.

## High Findings

### SBP-002: Debug helper can disclose all runtime secrets and raw secrets files
- Severity: High
- Location:
  - `check_secrets.py:28`
  - `check_secrets.py:35`
  - `check_secrets.py:42`
  - `check_secrets.py:54`
  - `check_secrets.py:56`
- Evidence: Script renders `st.secrets` sections and raw `.streamlit/secrets.toml` content directly.
- Impact: If run in any shared environment, it becomes a direct credential exfiltration page.
- Fix:
  - Remove this script from production repositories or gate it behind explicit dev-only environment checks.
  - Redact sensitive keys if debugging is needed.
- Mitigation:
  - Add a runtime hard stop unless `ENV=dev` and local host is detected.
- False positive notes: If never executed outside local isolated dev, risk is reduced but still operationally dangerous.

### SBP-003: Local admin auth flow accepts static password with no brute-force controls
- Severity: High
- Location:
  - `utils/auth.py:1171`
  - `utils/auth.py:1172`
  - `utils/auth.py:1173`
  - `utils/auth.py:540`
  - `utils/auth.py:621`
- Evidence: Plain equality check for email/password, local admin auto-login path, and no lockout/rate limiting.
- Impact: In any deployment where local admin mode is left enabled, password guessing and misconfiguration can lead to admin compromise.
- Fix:
  - Disable local admin login by default in non-dev environments.
  - Enforce SSO in production.
  - If local fallback is required, add login throttling/lockouts and audit alerts.
- Mitigation:
  - Require strong randomly generated admin password and explicit `ENV=dev` guard to enable local mode.
- False positive notes: Risk depends on deployment mode; this is mostly severe when local auth is enabled outside isolated development.

## Medium Findings

### SBP-004: Session restoration token is placed in URL query parameters
- Severity: Medium
- Location:
  - `utils/auth.py:562`
  - `utils/auth.py:672`
  - `utils/auth.py:673`
  - `utils/auth.py:687`
  - `utils/auth.py:691`
- Evidence: `local_token` is written to query params and used to restore authenticated local sessions.
- Impact: Token leakage through browser history, logs, screenshots, shared links, or referrers can enable session takeover for token lifetime.
- Fix:
  - Avoid URL token persistence for auth.
  - Store local-session state in secure server-side session keyed by httpOnly cookie.
- Mitigation:
  - Shorten TTL and rotate token on every restore if URL persistence cannot be removed immediately.
- False positive notes: Only active when Azure AD is disabled and local persistence is enabled.

### SBP-005: `PAT_ENC` is stored and returned as plaintext
- Severity: Medium
- Location:
  - `db/mssql_backend.py:6931`
  - `db/mssql_backend.py:6938`
  - `db/mssql_backend.py:6947`
  - `db/mssql_backend.py:6952`
- Evidence: `set_profile_pat` stores plaintext PAT into `PAT_ENC`; `get_profile_pat` returns raw value.
- Impact: DB read access directly yields ADO PATs; naming may create false confidence that secrets are encrypted.
- Fix:
  - Encrypt PATs at rest using envelope encryption (KMS/Key Vault-backed key).
  - Or remove DB PAT storage and require env/key-vault retrieval only.
- Mitigation:
  - Keep `ALLOW_DB_PAT_FALLBACK` disabled (already default false) and enforce this in production.
- False positive notes: Exposure is highest where DB fallback is enabled.

### SBP-006: External links use unvalidated `href` from data model
- Severity: Medium
- Location:
  - `components/roadmap_ui/frontend/src/components/RoadmapDrawer.tsx:146`
  - `components/roadmap_ui/frontend/src/components/RoadmapDrawer.tsx:147`
  - `components/roadmap_ui/frontend/src/components/RoadmapDrawer.tsx:158`
- Evidence: `feature_url` / `epic_url` / `epicUrl` are used directly in anchor `href`.
- Impact: If untrusted data reaches these fields, `javascript:` or malicious URL schemes can be used for phishing or client-side code execution on click.
- Fix:
  - Add URL sanitizer/allowlist (only `https:` and expected domains such as Azure DevOps hosts).
  - Render link only when validation passes.
- Mitigation:
  - Log and drop non-conforming URLs during data ingestion.
- False positive notes: If upstream strictly enforces trusted HTTPS URLs, practical risk is lower.

## Low Findings

### SBP-007: Raw SQL console allows dangerous writes/DDL in admin UI
- Severity: Low
- Location:
  - `utils/admin_raw_sql.py:229`
  - `utils/admin_raw_sql.py:267`
  - `utils/admin_raw_sql.py:281`
  - `pages/99_Admin.py:4431`
- Evidence: Admin tab executes arbitrary SQL including writes/DDL once toggles and confirmation are used.
- Impact: Increases blast radius of compromised admin account and operational mistakes.
- Fix:
  - Default this feature off in production via explicit environment flag.
  - Restrict to read-only unless separate break-glass mode is enabled.
- Mitigation:
  - Keep typed confirmation and audit logging; add stronger RBAC and IP restrictions.
- False positive notes: Intended as an admin maintenance capability, so this is hardening advice rather than a design bug.

## Dependency Notes
- Frontend runtime dependency audit:
  - Command: `npm audit --prefix components/roadmap_ui/frontend --omit=dev --json`
  - Result: 0 known vulnerabilities in production dependencies at scan time.
- Python package audit tool (`pip-audit`) was not available in this environment, so Python dependency CVEs were not fully enumerated.

## Secure-by-Default Improvement Plan (Prioritized)
1. Immediate credential hygiene:
   - Rotate exposed DB/admin/Azure/PAT secrets.
   - Replace local secret values with placeholders.
2. Hard-disable insecure auth paths in production:
   - Enforce SSO, disable local admin mode and auto-login unless explicit dev-only flag.
3. Remove URL-based local session auth tokening:
   - Replace `local_token` query persistence with server-side session + httpOnly cookie.
4. Encrypt PATs at rest or eliminate DB PAT storage:
   - Keep DB fallback disabled by policy.
5. Add URL validation for external links in React drawer.
6. Gate debug/diagnostic secret pages behind strict dev-only controls or remove them.
7. Add CI security controls:
   - Secret scanning (pre-commit + CI), dependency audit jobs, and a minimal security regression checklist.
