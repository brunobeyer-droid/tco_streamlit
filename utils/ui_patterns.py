from __future__ import annotations

from typing import Any, Iterable, Optional

import streamlit as st

from utils.ux_copy import (
    empty_state_message,
    permission_message,
    error_state_message,
    success_state_message,
)


def render_page_intro(
    *,
    title: str,
    purpose: str,
    status_line: Optional[str] = None,
    next_step: Optional[str] = None,
    show_title: bool = True,
) -> None:
    if show_title:
        st.title(str(title or "").strip())
    purpose_txt = str(purpose or "").strip()
    # Avoid repeating page subtitles when a page already renders a rich header.
    if purpose_txt and (show_title or (not status_line and not next_step)):
        st.caption(purpose_txt)
    if status_line:
        st.caption(str(status_line).strip())
    if next_step:
        st.caption(f"Next: {str(next_step).strip()}")


def render_page_frame(
    *,
    title: str,
    purpose: str,
    status_line: Optional[str] = None,
    scope_line: Optional[str] = None,
    next_step: Optional[str] = None,
) -> None:
    with st.container(border=True):
        render_page_intro(
            title=title,
            purpose=purpose,
            status_line=status_line,
            next_step=next_step,
            show_title=True,
        )
        if scope_line:
            st.caption(str(scope_line).strip())


def render_scope_banner(
    *,
    principal: Any,
    scope: Any,
    entity_label: str,
) -> None:
    is_contributor = bool(getattr(principal, "is_contributor", False))
    is_admin = bool(getattr(principal, "is_admin", False))
    has_scope = bool(getattr(scope, "has_scope", False))

    if is_contributor and not has_scope:
        st.info("🔒 " + permission_message("READ_ONLY_NO_SCOPE", entity=entity_label))
    elif is_contributor and has_scope and not is_admin:
        st.info("✅ " + permission_message("SCOPED_EDIT_MODE", entity=entity_label))
        st.caption(f"Showing only {entity_label} in your assigned scope.")


def render_empty_state(
    *,
    kind: str,
    action_target: Optional[str] = None,
    action_label: Optional[str] = None,
    resource: Optional[str] = None,
) -> None:
    ctx = {}
    if resource:
        ctx["resource"] = resource
    msg = empty_state_message(kind, **ctx) or "No data available."
    if action_target:
        label = action_label or "Next step"
        msg = f"{msg} {label}: {action_target}."
    st.info(msg)


def render_status_state(
    *,
    level: str,
    key: str,
    detail: Optional[str] = None,
    next_step: Optional[str] = None,
    technical_detail: Optional[str] = None,
) -> None:
    lvl = str(level or "info").strip().lower()
    msg = ""
    if lvl == "error":
        msg = error_state_message(key)
    elif lvl == "success":
        msg = success_state_message(key)
    else:
        msg = empty_state_message(key)
    if not msg:
        msg = str(detail or "").strip()
    if detail and detail not in msg:
        msg = f"{msg} {detail}"
    if next_step:
        msg = f"{msg} Next step: {next_step}."

    if lvl == "error":
        st.error(msg)
    elif lvl == "warning":
        st.warning(msg)
    elif lvl == "success":
        st.success(msg)
    else:
        st.info(msg)

    if technical_detail:
        with st.expander("Debug - diagnostics", expanded=False):
            st.code(str(technical_detail))


def render_section_picker(
    *,
    key: str,
    options: list[str],
    label: str = "Section",
    default: Optional[str] = None,
    horizontal: bool = True,
    label_visibility: str = "visible",
    prefer_segmented: bool = True,
) -> str:
    if not options:
        return ""
    fallback = default if (default in options) else options[0]
    # Normalize the keyed value before binding the widget.
    # Writing here is safe because the widget does not exist yet in this run.
    if key not in st.session_state or st.session_state.get(key) not in options:
        st.session_state[key] = fallback
    initial = st.session_state.get(key)
    use_segmented = bool(prefer_segmented and hasattr(st, "segmented_control"))
    if use_segmented:
        val = st.segmented_control(
            label,
            options,
            key=key,
            label_visibility=label_visibility,
        )
    else:
        idx = options.index(initial) if initial in options else 0
        val = st.radio(
            label,
            options,
            index=idx,
            horizontal=horizontal,
            key=key,
            label_visibility=label_visibility,
        )
    # segmented_control can briefly return None; prefer the keyed session value to avoid
    # rendering a different section than the one currently selected in the UI.
    # Do not write st.session_state[key] here because Streamlit manages widget state
    # and raises if we mutate the key after widget instantiation.
    if val in options:
        return str(val)
    current = st.session_state.get(key)
    if current in options:
        return str(current)
    return str(fallback)


def render_active_filters_summary(title: str, filters: Iterable[tuple[str, Any]]) -> None:
    chips: list[str] = []
    for name, value in filters:
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            vals = [str(v).strip() for v in value if str(v).strip()]
            if vals:
                chips.append(f"{name}: {len(vals)} selected")
            continue
        txt = str(value).strip()
        if txt and txt.lower() not in {"all", "select…", "(select a program)"}:
            chips.append(f"{name}: {txt}")
    if chips:
        st.caption(f"{title}: " + " | ".join(chips))


def viewing_scope_text(
    *,
    year: int,
    programs: Optional[list[str]] = None,
    teams: Optional[list[str]] = None,
    groups: Optional[list[str]] = None,
    include_scope_mode: bool = True,
) -> str:
    prog = [str(v).strip() for v in (programs or []) if str(v).strip()]
    team = [str(v).strip() for v in (teams or []) if str(v).strip()]
    grp = [str(v).strip() for v in (groups or []) if str(v).strip()]
    parts: list[str] = []
    if include_scope_mode:
        parts.append("Custom filters" if (prog or team or grp) else "Default scope")
    parts.append(f"Year: {int(year)}")
    if prog:
        parts.append(f"Programs: {len(prog)}")
    if team:
        parts.append(f"Teams: {len(team)}")
    if grp:
        parts.append(f"Applications: {len(grp)}")
    return "Viewing: " + " • ".join(parts)
