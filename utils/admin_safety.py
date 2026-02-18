from __future__ import annotations

import hashlib
import json
from typing import Any, Callable, Dict, Optional

import pandas as pd
import streamlit as st

from utils.admin_audit import log_admin_action

# Preview/apply contract:
# - preview_fn returns dict: ok, message, counts (dict), sample (df), details (dict)
# - apply_fn returns dict: ok, message, counts (dict), details (dict)
# Integrate by calling render_preview_apply_audit_block(...) inside Admin tabs.


def compute_action_fingerprint(context: Dict[str, Any]) -> str:
    payload = json.dumps(context or {}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def render_preview_apply_audit_block(
    *,
    title: str,
    area: str,
    action_type: str,
    entity: Optional[str],
    context: Dict[str, Any],
    preview_fn: Callable[[], Dict[str, Any]],
    apply_fn: Callable[[], Dict[str, Any]],
    preview_table: Optional[pd.DataFrame] = None,
    apply_table: Optional[pd.DataFrame] = None,
    require_typed_confirm: bool = False,
    confirm_phrase: str = "APPLY",
    danger_level: str = "normal",
) -> Dict[str, Any]:
    st.markdown(f"#### {title}")
    if danger_level == "danger":
        st.warning("This action can modify or delete data. Proceed carefully.")

    fingerprint = compute_action_fingerprint(
        {"title": title, "area": area, "action_type": action_type, "entity": entity, "context": context}
    )
    preview_key = f"admin_preview_{fingerprint}"
    apply_key = f"admin_apply_{fingerprint}"
    confirm_key = f"admin_confirm_{fingerprint}"

    preview_btn = st.button("Preview impact", key=f"preview_{fingerprint}")
    if preview_btn:
        preview = preview_fn()
        st.session_state[preview_key] = preview
        st.session_state[f"{preview_key}_ctx"] = context
        log_admin_action(
            action_type=f"{action_type}_preview",
            area=area,
            entity=entity or "",
            summary=preview.get("message") or "Preview completed.",
            extra_json={
                "ok": preview.get("ok"),
                "counts": preview.get("counts"),
                "context": context,
                "fingerprint": fingerprint,
            },
        )

    if st.session_state.get(f"{preview_key}_ctx") != context:
        st.session_state.pop(preview_key, None)
        st.session_state.pop(f"{preview_key}_ctx", None)
    preview = st.session_state.get(preview_key)
    if preview:
        if preview.get("ok"):
            st.success(preview.get("message") or "Preview ready.")
        else:
            st.warning(preview.get("message") or "Preview could not estimate impact.")
        counts = preview.get("counts") or {}
        if counts:
            st.write("Counts:")
            st.json(counts)
        sample_df = preview.get("sample")
        if sample_df is None:
            sample_df = preview_table
        if isinstance(sample_df, pd.DataFrame) and not sample_df.empty:
            with st.expander("Sample rows"):
                st.dataframe(sample_df, use_container_width=True)
        if preview.get("details"):
            with st.expander("Preview details"):
                st.json(preview.get("details"))
        if st.button("Clear preview", key=f"clear_preview_{fingerprint}"):
            st.session_state.pop(preview_key, None)
            st.session_state.pop(f"{preview_key}_ctx", None)
            st.rerun()

    confirm_ok = True
    if require_typed_confirm:
        typed = st.text_input(
            f"Type {confirm_phrase} to confirm",
            key=confirm_key,
        )
        confirm_ok = typed.strip().upper() == confirm_phrase.upper()

    apply_disabled = not (preview and preview.get("ok")) or not confirm_ok
    apply_btn = st.button(
        "Apply changes",
        key=f"apply_{fingerprint}",
        disabled=apply_disabled,
    )
    result: Dict[str, Any] = {}
    if apply_btn:
        try:
            result = apply_fn()
            st.session_state[apply_key] = result
            if result.get("ok"):
                st.success(result.get("message") or "Changes applied.")
            else:
                st.error(result.get("message") or "Apply failed.")
        except Exception as exc:
            result = {"ok": False, "message": str(exc)}
            st.session_state[apply_key] = result
            st.error(f"Apply failed: {exc}")
        log_admin_action(
            action_type=action_type,
            area=area,
            entity=entity or "",
            summary=result.get("message") or "Apply completed.",
            extra_json={
                "ok": result.get("ok"),
                "counts": result.get("counts"),
                "context": context,
                "preview_counts": (preview or {}).get("counts"),
                "fingerprint": fingerprint,
            },
        )

    if apply_table is not None and isinstance(apply_table, pd.DataFrame) and not apply_table.empty:
        with st.expander("Apply details"):
            st.dataframe(apply_table, use_container_width=True)

    if result:
        st.caption(f"Audit reference: {fingerprint}")
    return {"preview": preview, "result": result, "fingerprint": fingerprint}
