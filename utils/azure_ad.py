from __future__ import annotations
import time
from typing import Any, Dict, List
import streamlit as st

try:
    import msal  # type: ignore
except Exception as e:  # pragma: no cover
    msal = None  # type: ignore

try:
    import requests  # type: ignore
except Exception as e:  # pragma: no cover
    requests = None  # type: ignore


def _aad_cfg() -> Dict[str, Any]:
    cfg = getattr(st, "secrets", {}).get("azuread", {})
    if not cfg or not cfg.get("client_id") or not cfg.get("tenant_id"):
        raise RuntimeError("Azure AD not configured. Set [azuread] client_id and tenant_id in secrets.")
    return cfg


from typing import Optional


def acquire_graph_token_device(scopes: Optional[List[str]] = None) -> Dict[str, Any]:
    """Device code flow to acquire a token for Microsoft Graph.

    Stores token in session_state under _aad_token.
    """
    if msal is None:
        raise RuntimeError("msal is not installed.")
    cfg = _aad_cfg()
    tenant = cfg.get("tenant_id")
    client_id = cfg.get("client_id")
    authority = f"https://login.microsoftonline.com/{tenant}"
    app = msal.PublicClientApplication(client_id, authority=authority)  # type: ignore
    scopes = scopes or ["User.ReadBasic.All"]
    flow = app.initiate_device_flow(scopes=scopes)
    st.info(f"Open {flow['verification_uri']} and enter code: {flow['user_code']}")
    token = app.acquire_token_by_device_flow(flow)  # blocks until done or timeout
    if "access_token" not in token:
        raise RuntimeError(token.get("error_description") or "Could not obtain token")
    st.session_state["_aad_token"] = token
    st.session_state["_aad_token_time"] = time.time()
    return token


def _get_token() -> str:
    tok = st.session_state.get("_aad_token")
    if not tok or "access_token" not in tok:
        raise RuntimeError("Not signed in to Azure AD for directory search.")
    return tok["access_token"]


def search_users_basic(query: str, top: int = 25) -> List[Dict[str, Any]]:
    """Basic Graph /users search using startswith filters on displayName/mail/UPN.

    Requires token via acquire_graph_token_device. Returns list of {displayName, mail, userPrincipalName}.
    """
    if requests is None:
        raise RuntimeError("requests not installed")
    if not query:
        return []
    token = _get_token()
    q = query.replace("'", "")
    # Build $filter with OR conditions
    filt = (
        f"startswith(displayName,'{q}') or startswith(mail,'{q}') or startswith(userPrincipalName,'{q}')"
    )
    url = f"https://graph.microsoft.com/v1.0/users?$top={int(top)}&$select=displayName,mail,userPrincipalName&$filter={filt}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Graph query failed: {resp.status_code} {resp.text[:200]}")
    data = resp.json()
    vals = data.get("value") or []
    out: List[Dict[str, Any]] = []
    for r in vals:
        out.append(
            {
                "displayName": r.get("displayName"),
                "mail": r.get("mail"),
                "userPrincipalName": r.get("userPrincipalName"),
            }
        )
    return out
