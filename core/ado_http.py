from __future__ import annotations

from typing import Any, Dict, Optional, List

import pandas as pd

from utils.ado import fetch_ado_odata
from utils.ado_pat import resolve_ado_pat_source
from core.portfolio_context import get_active_pat_env_key

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

import xml.etree.ElementTree as _ET


def get_ado_auth_context(portfolio_settings: Dict[str, Any], profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return PAT auth context for ADO OData calls."""
    profile_key = str(
        portfolio_settings.get("_profile_name")
        or (profile or {}).get("profile_name")
        or (profile or {}).get("PROFILE_NAME")
        or portfolio_settings.get("_profile_id")
        or ""
    ).strip()
    pat = None
    source = "none"
    if profile_key:
        pat_env_key = str(portfolio_settings.get("pat_env_key") or get_active_pat_env_key() or "").strip()
        pat, source = resolve_ado_pat_source(profile_key, pat_env_key=pat_env_key)
    has_pat = bool(pat)
    if not has_pat:
        source = "none"
    return {
        "pat": pat or "",
        "has_pat": has_pat,
        "source": source,
        "auth_header": {},
    }


def ado_odata_get(
    url: str,
    auth_ctx: Dict[str, Any],
    diag: Optional[Dict[str, Any]] = None,
    *,
    max_pages: int = 1,
    timeout: int = 60,
) -> Dict[str, Any]:
    """Fetch OData rows using the same PAT auth as sync; return JSON-like payload."""
    diag = diag if diag is not None else {}
    df = fetch_ado_odata(url, auth_ctx.get("pat") or "", diag, max_pages=max_pages, timeout=timeout)
    if df is None or df.empty:
        return {"value": [], "diag": diag}
    rows = df.to_dict(orient="records")
    return {"value": rows, "diag": diag}


def fetch_ado_odata_metadata(base_url: str, auth_ctx: Dict[str, Any], timeout: int = 30) -> str:
    if not base_url:
        raise ValueError("Missing base_url for metadata fetch.")
    if requests is None:
        raise RuntimeError("'requests' not installed; cannot fetch metadata.")
    base = base_url.rstrip("/")
    metadata_url = base + "/$metadata"
    headers = {"Accept": "application/xml"}
    pat = auth_ctx.get("pat") or ""
    auth = ("", pat) if pat else None
    resp = requests.get(metadata_url, headers=headers, auth=auth, timeout=timeout, verify=False)
    if not resp.ok:
        raise RuntimeError(f"OData metadata request failed ({resp.status_code}): {resp.text[:400]}")
    return resp.text


def parse_ado_metadata_fields(xml_text: str) -> List[str]:
    if not xml_text:
        return []
    try:
        root = _ET.fromstring(xml_text)
    except Exception:
        return []
    fields: List[str] = []
    for ent in root.iter():
        tag = ent.tag.split("}")[-1]
        if tag != "EntityType":
            continue
        name = ent.attrib.get("Name", "") or ""
        if "workitem" not in name.lower():
            continue
        for prop in ent:
            ptag = prop.tag.split("}")[-1]
            if ptag != "Property":
                continue
            pname = prop.attrib.get("Name")
            if pname:
                fields.append(pname)
        if fields:
            break
    return sorted({f for f in fields if f})
