"""Utility helpers to send email notifications via Microsoft Graph."""
from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Optional, Sequence

import streamlit as st

try:  # pragma: no cover - optional dependency guard
    import msal  # type: ignore
except Exception:  # pragma: no cover
    msal = None  # type: ignore

try:  # pragma: no cover - optional dependency guard
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


class EmailConfigError(RuntimeError):
    """Raised when email configuration is missing or invalid."""


class EmailSendError(RuntimeError):
    """Raised when an email cannot be sent."""


_GRAPH_APP: Optional["msal.ConfidentialClientApplication"] = None
_TOKEN_CACHE: Dict[str, Any] = {"access_token": None, "expires_at": 0.0}


def _azure_cfg() -> Dict[str, Any]:
    cfg = getattr(st, "secrets", {}).get("azuread", {})
    if not cfg:
        raise EmailConfigError("Azure AD configuration missing in secrets.")
    if not cfg.get("client_id") or not cfg.get("tenant_id") or not cfg.get("client_secret"):
        raise EmailConfigError("Azure AD client credentials incomplete. Set client_id, tenant_id and client_secret.")
    return cfg


def _mail_sender() -> str:
    secrets_obj = getattr(st, "secrets", {})
    email_cfg = secrets_obj.get("email", {})
    sender = email_cfg.get("sender") or email_cfg.get("from_address")
    if not sender:
        sender = secrets_obj.get("azuread", {}).get("mail_sender")
    if not sender:
        raise EmailConfigError("Email sender address missing. Configure email.sender or azuread.mail_sender in secrets.")
    return str(sender)


def _graph_app() -> "msal.ConfidentialClientApplication":
    global _GRAPH_APP
    if _GRAPH_APP is not None:
        return _GRAPH_APP
    if msal is None:
        raise EmailConfigError("msal package not installed; cannot send mail via Graph.")
    cfg = _azure_cfg()
    authority = f"https://login.microsoftonline.com/{cfg['tenant_id']}"
    _GRAPH_APP = msal.ConfidentialClientApplication(
        client_id=cfg["client_id"],
        authority=authority,
        client_credential=cfg["client_secret"],
    )
    return _GRAPH_APP


def _graph_token(scopes: Optional[Sequence[str]] = None) -> str:
    scopes = list(scopes or ["https://graph.microsoft.com/.default"])
    now = time.time()
    cached = _TOKEN_CACHE.get("access_token")
    exp = float(_TOKEN_CACHE.get("expires_at") or 0.0)
    if cached and now < exp - 90:  # reuse token with small safety margin
        return cached
    app = _graph_app()
    result = app.acquire_token_silent(scopes, account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=scopes)
    if not result or "access_token" not in result:
        raise EmailSendError(result.get("error_description") if isinstance(result, dict) else "Failed to acquire Graph token")
    token = result["access_token"]
    expires_in = int(result.get("expires_in") or 3600)
    _TOKEN_CACHE["access_token"] = token
    _TOKEN_CACHE["expires_at"] = now + expires_in
    return token


def _norm_addresses(values: Optional[Iterable[str]]) -> List[str]:
    if not values:
        return []
    seen = set()
    out: List[str] = []
    for raw in values:
        if not raw:
            continue
        addr = str(raw).strip()
        if not addr:
            continue
        key = addr.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(addr)
    return out


def send_graph_mail(
    subject: str,
    body_html: str,
    to_recipients: Sequence[str],
    cc_recipients: Optional[Sequence[str]] = None,
    save_to_sent: bool = False,
    scopes: Optional[Sequence[str]] = None,
    sender_override: Optional[str] = None,
) -> None:
    """Send an email using Microsoft Graph on behalf of the configured sender."""
    if requests is None:
        raise EmailConfigError("requests package not installed; cannot send mail via Graph.")

    to_list = _norm_addresses(to_recipients)
    if not to_list:
        raise EmailSendError("At least one recipient is required.")
    cc_list = _norm_addresses(cc_recipients)

    sender = None
    if sender_override:
        override = str(sender_override).strip()
        if override:
            sender = override
    if not sender:
        sender = _mail_sender()
    token = _graph_token(scopes)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    message: Dict[str, Any] = {
        "subject": subject,
        "body": {"contentType": "HTML", "content": body_html},
        "toRecipients": [{"emailAddress": {"address": addr}} for addr in to_list],
    }
    if cc_list:
        message["ccRecipients"] = [{"emailAddress": {"address": addr}} for addr in cc_list]

    payload = {
        "message": message,
        "saveToSentItems": bool(save_to_sent),
    }

    url = f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code not in (200, 202):
        raise EmailSendError(f"Graph sendMail failed: {resp.status_code} {resp.text[:200]}")


__all__ = [
    "EmailConfigError",
    "EmailSendError",
    "send_graph_mail",
    "get_mail_status",
]


def get_mail_status(test_token: bool = False, sender_override: Optional[str] = None) -> Dict[str, Any]:
    """Run basic configuration checks and optionally attempt to acquire a Graph token."""
    checks: List[Dict[str, Any]] = []
    ok = True

    if requests is None:
        checks.append({"name": "requests", "ok": False, "detail": "requests package not installed."})
        ok = False
    else:
        checks.append({"name": "requests", "ok": True, "detail": "requests package available."})

    if msal is None:
        checks.append({"name": "msal", "ok": False, "detail": "msal package not installed."})
        ok = False
    else:
        checks.append({"name": "msal", "ok": True, "detail": "msal package available."})

    sender_source = sender_override.strip() if isinstance(sender_override, str) else None

    try:
        sender = sender_source or _mail_sender()
        checks.append({"name": "sender", "ok": True, "detail": f"Sender configured: {sender}"})
    except EmailConfigError as exc:
        checks.append({"name": "sender", "ok": False, "detail": str(exc)})
        ok = False

    try:
        cfg = _azure_cfg()
        has_secret = bool(cfg.get("client_secret"))
        checks.append({"name": "azuread", "ok": True, "detail": "Azure AD client credentials found." if has_secret else "Azure AD config loaded."})
    except EmailConfigError as exc:
        checks.append({"name": "azuread", "ok": False, "detail": str(exc)})
        ok = False

    if test_token and ok:
        try:
            _graph_token()
            checks.append({"name": "graph_token", "ok": True, "detail": "Successfully acquired Microsoft Graph token."})
        except Exception as exc:  # pragma: no cover - network/credential dependent
            checks.append({"name": "graph_token", "ok": False, "detail": str(exc)})
            ok = False

    return {"ok": ok, "checks": checks}
