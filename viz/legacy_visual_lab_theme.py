# Summary: update visual lab preview labels to NEXT branding.
from __future__ import annotations

"""Legacy Visual Lab (theme playground).

Moved out of `/pages` to avoid Streamlit multipage URL conflicts with the current
Visual Lab page.
"""

from datetime import date
from typing import Dict, List

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from utils.theme import THEME, apply_echarts_theme, apply_ui_theme_css, use_theme


def _mark_touched(theme_key: str) -> None:
    touched: List[str] = st.session_state.setdefault("theme_lab_touched", [])
    if theme_key not in touched:
        touched.append(theme_key)


def _theme_color_picker(theme_key: str, label: str) -> None:
    value = st.session_state["theme_lab"].get(theme_key, THEME.get(theme_key, "#ffffff"))
    if isinstance(value, str) and value.startswith("#") and len(value) == 9:
        # Streamlit color_picker rejects 8-digit hex (with alpha). Strip alpha for UI controls.
        value = value[:7]
    st.color_picker(
        label,
        value=value,
        key=f"theme_lab_color_{theme_key}",
        on_change=_mark_touched,
        args=(theme_key,),
    )


def _apply_primary_links(lab: Dict[str, str], touched: List[str]) -> Dict[str, str]:
    """If primary is changed, keep related colors linked unless user edited them explicitly."""
    if "primary" not in touched:
        return lab
    for dep in ("accent", "link", "toggle_track_on", "tab_active_border"):
        if dep not in touched:
            lab[dep] = lab["primary"]
    return lab


def _badge_html(text: str) -> str:
    colors = {
        "Critical": "#E74C3C",
        "Pending": "#F1C40F",
        "Contract": "#E67E22",
        "OK": "#2ECC71",
    }
    color = colors.get(text, "#95A5A6")
    fg = "#000" if text in ("Pending", "Contract") else "#fff"
    return (
        '<span style="display:inline-block;white-space:nowrap;'
        'padding:2px 8px;border-radius:999px;vertical-align:middle;'
        f'background:{color};color:{fg};font-weight:700;font-size:0.85em;line-height:1;">'
        f"{text}</span>"
    )


def _sync_lab_defaults(lab: Dict[str, str], touched: List[str]) -> List[str]:
    touched_list = list(touched or [])
    touched_set = set(touched_list)

    for k, v in THEME.items():
        if k not in lab:
            lab[k] = v

    # Migration: if older sessions used "border" as input border, shift it.
    if "input_border" in THEME and "input_border" not in lab:
        lab["input_border"] = lab.get("border", THEME.get("input_border", THEME.get("border")))
    if "input_border" in THEME and "border" in touched_set and "input_border" not in touched_set:
        lab["input_border"] = lab.get("border", THEME.get("input_border", THEME.get("border")))
        lab["border"] = THEME.get("border")
        touched_list = [("input_border" if t == "border" else t) for t in touched_list]
        touched_set = set(touched_list)

    # Keep untouched values in sync with THEME defaults (so edits to theme.py are reflected).
    for k, v in THEME.items():
        if k not in touched_set:
            lab[k] = v

    st.session_state["theme_lab_touched"] = touched_list
    st.session_state["theme_lab"] = lab
    return touched_list


def _apply_toast_preview_css(lab: Dict[str, str]) -> None:
    bg = str(lab.get("toast_bg") or THEME.get("toast_bg", ""))
    border = str(lab.get("toast_border") or THEME.get("toast_border", ""))
    text = str(lab.get("toast_text") or THEME.get("toast_text", ""))
    css = f"""<style>
div[data-baseweb="toast"][data-testid="stToast"],
div[data-baseweb="toast"],
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"],
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[role="alert"][data-baseweb="toast"] {{
  background: {bg} !important;
  background-color: {bg} !important;
  border: 1px solid {border} !important;
  color: {text} !important;
  opacity: 1 !important;
  filter: none !important;
}}
div[data-baseweb="toast"][data-testid="stToast"]::before,
div[data-baseweb="toast"][data-testid="stToast"]::after,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"]::before,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"]::after {{
  background: {bg} !important;
  background-color: {bg} !important;
}}
div[data-baseweb="toast"][data-testid="stToast"] > div,
div[data-baseweb="toast"][data-testid="stToast"] > div > div,
div[data-baseweb="toast"][data-testid="stToast"] > div > div > div,
div[data-baseweb="toast"][data-testid="stToast"] div[data-testid="stMarkdownContainer"],
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] > div,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] > div > div,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] > div > div > div,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] div[data-testid="stMarkdownContainer"] {{
  background: {bg} !important;
  background-color: {bg} !important;
  color: {text} !important;
}}
div[data-baseweb="toast"][data-testid="stToast"] *,
div[data-baseweb="toast"][data-testid="stToast"] svg,
div[data-baseweb="toast"][data-testid="stToast"] svg path,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] *,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] svg,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] svg path {{
  color: {text} !important;
  fill: {text} !important;
}}
</style>"""
    st.markdown(css, unsafe_allow_html=True)


def _preview_toast(message: str, lab: Dict[str, str]) -> None:
    prefix = str(lab.get("toast_icon_info", "i") or "").strip()
    prefix = prefix[:1] if prefix else ""
    body = f"{prefix} {message}" if prefix else message
    st.toast(body)
    _apply_toast_preview_css(lab)


def _render_theme_section(lab: Dict[str, str]) -> None:
    st.subheader("Core")
    core_left, core_right = st.columns([1, 2], vertical_alignment="top")
    with core_left:
        _theme_color_picker("bg", "Main background")
        _theme_color_picker("surface", "Surface")
        _theme_color_picker("sidebar", "Sidebar")
        _theme_color_picker("sidebar2", "Sidebar secondary")
        _theme_color_picker("text", "Text")
        _theme_color_picker("muted", "Muted")
        _theme_color_picker("link", "Link")
    with core_right:
        with st.container(border=True):
            st.markdown("**Preview**")
            st.write("This is body text. **Bold**, `code`, and a link:")
            st.write("[Open Streamlit](https://streamlit.io)")
            st.info("Info message")
            st.warning("Warning message")

    st.subheader("Layout")
    layout_left, layout_right = st.columns([1, 2], vertical_alignment="top")
    with layout_left:
        st.slider(
            "Corner radius (px)",
            0,
            32,
            int(float(lab.get("radius", "14px").replace("px", "") or 14)),
            key="theme_lab_radius_px",
            on_change=_mark_touched,
            args=("radius",),
        )
        st.slider(
            "Button radius (px)",
            0,
            999,
            int(float(lab.get("button_radius", "999px").replace("px", "") or 999)),
            key="theme_lab_button_radius_px",
            on_change=_mark_touched,
            args=("button_radius",),
        )
        st.slider(
            "Button padding Y (rem)",
            0.0,
            1.5,
            float(lab.get("button_padding_y", "0.55rem").replace("rem", "") or 0.55),
            0.05,
            key="theme_lab_button_py",
            on_change=_mark_touched,
            args=("button_padding_y",),
        )
        st.slider(
            "Button padding X (rem)",
            0.0,
            2.0,
            float(lab.get("button_padding_x", "0.90rem").replace("rem", "") or 0.9),
            0.05,
            key="theme_lab_button_px",
            on_change=_mark_touched,
            args=("button_padding_x",),
        )
        st.slider(
            "Input radius (px)",
            0,
            24,
            int(float(lab.get("input_radius", "10px").replace("px", "") or 10)),
            key="theme_lab_input_radius",
            on_change=_mark_touched,
            args=("input_radius",),
        )
    with layout_right:
        with st.container(border=True):
            st.markdown("**Preview**")
            st.button("Primary button", type="primary", key="theme_lab_btn_primary_layout")
            st.button("Secondary button", type="secondary", key="theme_lab_btn_secondary_layout")
            st.text_input("Input", value="Input radius preview", key="theme_lab_input_layout")

    st.subheader("Tabs")
    tabs_left, tabs_right = st.columns([1, 2], vertical_alignment="top")
    with tabs_left:
        _theme_color_picker("tab_text", "Tab text")
        _theme_color_picker("tab_active_text", "Tab active text")
        _theme_color_picker("tab_active_border", "Tab active border")
    with tabs_right:
        with st.container(border=True):
            st.markdown("**Preview**")
            t1, t2 = st.tabs(["Tab A", "Tab B"])
            with t1:
                st.write("Tab content A")
            with t2:
                st.write("Tab content B")


def _render_widgets_section(lab: Dict[str, str]) -> None:
    st.subheader("Controls")
    ctl_left, ctl_right = st.columns([1, 2], vertical_alignment="top")
    with ctl_left:
        _theme_color_picker("toggle_track_on", "Toggle track on")
        _theme_color_picker("toggle_track_off", "Toggle track off")
        _theme_color_picker("toggle_thumb", "Toggle thumb")
    with ctl_right:
        with st.container(border=True):
            st.markdown("**Preview**")
            st.button("Primary button", type="primary", key="theme_lab_btn_primary_controls")
            st.button("Secondary button", type="secondary", key="theme_lab_btn_secondary_controls")
            st.toggle("Toggle", value=True, key="theme_lab_toggle_preview")
            st.radio("Radio", ["Portfolio", "R&M", "Process Ops"], index=0, horizontal=True, key="theme_lab_radio_preview")
            st.checkbox("Checkbox", value=True, key="theme_lab_checkbox_preview")
            st.progress(45)

    st.subheader("Inputs + Forms")
    inp_left, inp_right = st.columns([1, 2], vertical_alignment="top")
    with inp_left:
        st.markdown("**Inputs**")
        _theme_color_picker("input_bg", "Input background")
        _theme_color_picker("input_text", "Input text")
        _theme_color_picker("input_border", "Input border")

        st.markdown("**Form Buttons**")
        _theme_color_picker("primary", "Primary (submit)")
        _theme_color_picker("button_bg", "Button background")
        _theme_color_picker("button_fg", "Button text")
        _theme_color_picker("button_hover", "Button hover")
    with inp_right:
        with st.container(border=True):
            st.markdown("**Preview**")
            with st.form("theme_lab_form_preview", clear_on_submit=False):
                st.columns(1)[0].text_input("Text input", value="Hello", key="theme_lab_form_text_input")
                st.text_area("Text area", value="Multiline\ntext", key="theme_lab_form_text_area")
                c1, c2 = st.columns(2)
                c1.number_input("Number input", value=42, step=1, key="theme_lab_form_number")
                c2.date_input("Date input", value=date.today(), key="theme_lab_form_date")
                st.selectbox("Selectbox", ["Option A", "Option B", "Option C"], index=0, key="theme_lab_form_selectbox")
                st.multiselect("Multiselect", ["Alpha", "Beta", "Gamma"], default=["Alpha"], key="theme_lab_form_multiselect")
                st.slider("Slider", 0, 100, 55, key="theme_lab_form_slider")
                st.file_uploader("File uploader", accept_multiple_files=True, key="theme_lab_form_uploader")
                sc1, sc2, sc3 = st.columns(3)
                _ = sc1.form_submit_button("Submit", type="primary", use_container_width=True)
                _ = sc2.form_submit_button("Secondary", type="secondary", use_container_width=True)
                _ = sc3.form_submit_button("Disabled", disabled=True, use_container_width=True)
            st.download_button(
                "Download sample CSV",
                data="a,b\n1,2\n3,4\n",
                file_name="sample.csv",
                mime="text/csv",
                use_container_width=False,
                key="theme_lab_download_btn",
            )

    st.subheader("Feedback")
    fb_left, fb_right = st.columns([1, 2], vertical_alignment="top")
    with fb_left:
        st.markdown("**Toast**")
        _theme_color_picker("toast_bg", "Toast background")
        _theme_color_picker("toast_border", "Toast border")
        _theme_color_picker("toast_text", "Toast text")

        st.markdown("**Toast icons (1 character)**")
        icon_options = ["✓", "×", "!", "i", "•", "→", "▲", "▼", "+", "-", "*", "#", "?"]
        st.selectbox(
            "Success icon",
            options=icon_options,
            index=max(0, icon_options.index(str(lab.get("toast_icon_success", "✓"))[:1]) if str(lab.get("toast_icon_success", "✓"))[:1] in icon_options else 0),
            key="theme_lab_toast_icon_success",
            on_change=_mark_touched,
            args=("toast_icon_success",),
        )
        st.selectbox(
            "Error icon",
            options=icon_options,
            index=max(0, icon_options.index(str(lab.get("toast_icon_error", "×"))[:1]) if str(lab.get("toast_icon_error", "×"))[:1] in icon_options else 0),
            key="theme_lab_toast_icon_error",
            on_change=_mark_touched,
            args=("toast_icon_error",),
        )
        st.selectbox(
            "Warning icon",
            options=icon_options,
            index=max(0, icon_options.index(str(lab.get("toast_icon_warning", "!"))[:1]) if str(lab.get("toast_icon_warning", "!"))[:1] in icon_options else 0),
            key="theme_lab_toast_icon_warning",
            on_change=_mark_touched,
            args=("toast_icon_warning",),
        )
        st.selectbox(
            "Info icon",
            options=icon_options,
            index=max(0, icon_options.index(str(lab.get("toast_icon_info", "i"))[:1]) if str(lab.get("toast_icon_info", "i"))[:1] in icon_options else 0),
            key="theme_lab_toast_icon_info",
            on_change=_mark_touched,
            args=("toast_icon_info",),
        )
    with fb_right:
        with st.container(border=True):
            st.success("Success message")
            st.info("Info message")
            st.warning("Warning message")
            st.error("Error message")
            if st.button("Show toast", type="secondary", key="theme_lab_toast_btn"):
                _preview_toast("Toast message", lab)
            with st.expander("Expander", expanded=False):
                st.write("Expander body text")
            with st.spinner("Spinner"):
                pass
            st.json({"preview": True, "theme": "dark"}, expanded=False)


def _render_data_section() -> None:
    st.subheader("Dataframes + Editors")
    df_preview = pd.DataFrame(
        {
            "Application": ["SmartCloud", "HUWR", "SPPID"],
            "Cost": [123456, 98765, 54321],
            "Status": ["Active", "Active", "Terminated"],
        }
    )

    data_left, data_right = st.columns([1, 2], vertical_alignment="top")
    with data_left:
        st.markdown("**Dataframe (st.dataframe / st.data_editor)**")
        _theme_color_picker("dataframe_bg", "Background")
        _theme_color_picker("dataframe_header_bg", "Header background")
        _theme_color_picker("dataframe_border", "Border")
        _theme_color_picker("dataframe_gridline", "Gridlines")
        _theme_color_picker("dataframe_text", "Text")
        st.markdown("**Static table (st.table)**")
        _theme_color_picker("table_bg", "Background")
        _theme_color_picker("table_header_bg", "Header background")
        _theme_color_picker("table_border", "Border")
        _theme_color_picker("table_text", "Text")
        _theme_color_picker("table_row_alt_bg", "Alt row background")
        _theme_color_picker("table_row_hover_bg", "Hover background")
    with data_right:
        with st.container(border=True):
            st.markdown("**Preview (st.dataframe)**")
            st.dataframe(
                df_preview,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Application": st.column_config.TextColumn("Application"),
                    "Cost": st.column_config.NumberColumn("Cost", format="$%,.0f"),
                    "Status": st.column_config.TextColumn("Status"),
                },
            )
        with st.container(border=True):
            st.markdown("**Preview (st.data_editor)**")
            edited = st.data_editor(
                df_preview,
                use_container_width=True,
                hide_index=True,
                key="theme_lab_data_editor",
                num_rows="dynamic",
                column_config={
                    "Application": st.column_config.TextColumn("Application", required=True),
                    "Cost": st.column_config.NumberColumn("Cost", format="$%,.0f", min_value=0),
                    "Status": st.column_config.SelectboxColumn("Status", options=["Active", "Terminated", "Planned"]),
                },
            )
            st.caption(f"Rows: {len(edited)}")
        with st.container(border=True):
            st.markdown("**Preview (st.table)**")
            st.table(df_preview.head(2))


def _render_charts_section(page_overrides: Dict[str, str], palette: Dict[str, str]) -> None:
    style_options = [
        "Ocean",
        "Calm",
        "Choppy",
        "Flat",
        "SimpleBadge",
        "GlassBadge",
        "SingleWave",
        "StaticLevel",
        "CounterWave",
        "VariableWave",
        "Diamond",
        "Triangle",
        "Pin",
        "Rect",
        "RoundRect",
    ]
    st.subheader("Charts")
    chart_left, chart_right = st.columns([1, 2], vertical_alignment="top")
    with chart_left:
        _theme_color_picker("echarts_bg", "ECharts background")
        _theme_color_picker("echarts_text", "ECharts text")
        _theme_color_picker("echarts_axis_text", "Axis labels")
        _theme_color_picker("echarts_axis_line", "Axis lines")
        _theme_color_picker("echarts_gridline", "Gridlines")
        _theme_color_picker("echarts_tooltip_bg", "Tooltip background")
        _theme_color_picker("echarts_tooltip_border", "Tooltip border")
        _theme_color_picker("echarts_tooltip_text", "Tooltip text")
        st.slider(
            "Symbol border width",
            0,
            6,
            int(float(st.session_state["theme_lab"].get("echarts_symbol_border_width", "0") or 0)),
            key="theme_lab_echarts_symbol_border_width",
            on_change=_mark_touched,
            args=("echarts_symbol_border_width",),
        )
        st.divider()
        st.markdown("**Forecast Status liquidFill**")
        style_val = str(st.session_state["theme_lab"].get("forecast_liquid_style", THEME.get("forecast_liquid_style", "Ocean")))
        st.selectbox(
            "Widget style",
            style_options,
            index=max(0, style_options.index(style_val) if style_val in style_options else 0),
            key="theme_lab_forecast_liquid_style",
            on_change=_mark_touched,
            args=("forecast_liquid_style",),
        )
        st.slider(
            "Label size (px)",
            18,
            72,
            int(float(st.session_state["theme_lab"].get("forecast_liquid_label_size_px", THEME.get("forecast_liquid_label_size_px", "46")) or 46)),
            key="theme_lab_forecast_liquid_label_size_px",
            on_change=_mark_touched,
            args=("forecast_liquid_label_size_px",),
        )
        st.slider(
            "Wave opacity",
            0.1,
            1.0,
            float(st.session_state["theme_lab"].get("forecast_liquid_wave_opacity", THEME.get("forecast_liquid_wave_opacity", "0.85")) or 0.85),
            0.05,
            key="theme_lab_forecast_liquid_wave_opacity",
            on_change=_mark_touched,
            args=("forecast_liquid_wave_opacity",),
        )
        st.slider(
            "Background top opacity",
            0.0,
            0.6,
            float(st.session_state["theme_lab"].get("forecast_liquid_bg_top_opacity", THEME.get("forecast_liquid_bg_top_opacity", "0.22")) or 0.22),
            0.02,
            key="theme_lab_forecast_liquid_bg_top_opacity",
            on_change=_mark_touched,
            args=("forecast_liquid_bg_top_opacity",),
        )
        st.slider(
            "Background bottom opacity",
            0.0,
            0.6,
            float(st.session_state["theme_lab"].get("forecast_liquid_bg_bottom_opacity", THEME.get("forecast_liquid_bg_bottom_opacity", "0.08")) or 0.08),
            0.02,
            key="theme_lab_forecast_liquid_bg_bottom_opacity",
            on_change=_mark_touched,
            args=("forecast_liquid_bg_bottom_opacity",),
        )
        st.slider(
            "Wave highlight opacity",
            0.0,
            0.6,
            float(st.session_state["theme_lab"].get("forecast_liquid_wave_highlight_opacity", THEME.get("forecast_liquid_wave_highlight_opacity", "0.14")) or 0.14),
            0.02,
            key="theme_lab_forecast_liquid_wave_highlight_opacity",
            on_change=_mark_touched,
            args=("forecast_liquid_wave_highlight_opacity",),
        )
        _theme_color_picker("forecast_liquid_wave_highlight_over", "Wave highlight (>=99%)")
        st.slider(
            "Wave highlight opacity (>=99%)",
            0.0,
            0.6,
            float(
                st.session_state["theme_lab"].get(
                    "forecast_liquid_wave_highlight_over_opacity",
                    THEME.get("forecast_liquid_wave_highlight_over_opacity", "0.00"),
                )
                or 0.0
            ),
            0.02,
            key="theme_lab_forecast_liquid_wave_highlight_over_opacity",
            on_change=_mark_touched,
            args=("forecast_liquid_wave_highlight_over_opacity",),
        )
        st.slider(
            "Preview % (controls over-forecast)",
            0,
            160,
            62,
            key="theme_lab_forecast_liquid_preview_pct",
        )
        _theme_color_picker("forecast_liquid_primary", "Widget primary (waves)")
        _theme_color_picker("forecast_liquid_border", "Widget border")
        _theme_color_picker("forecast_liquid_label", "Widget label")
        _theme_color_picker("forecast_liquid_wave_1", "Wave 1")
        _theme_color_picker("forecast_liquid_wave_2", "Wave 2")
        _theme_color_picker("forecast_liquid_wave_3", "Wave 3")
        _theme_color_picker("forecast_liquid_wave_4", "Wave highlight")
        _theme_color_picker("forecast_liquid_bg_top", "Background top")
        _theme_color_picker("forecast_liquid_bg_bottom", "Background bottom")
        _theme_color_picker("forecast_liquid_label_outside", "Label (outside)")
        _theme_color_picker("forecast_liquid_label_inside", "Label (inside)")
        _theme_color_picker("forecast_liquid_over_primary", "Over-forecast (waves)")
        _theme_color_picker("forecast_liquid_over_border", "Over-forecast (border)")
    with chart_right:
        with st.container(border=True):
            st.markdown("**Preview (ECharts)**")
            try:
                from streamlit_echarts import st_echarts  # type: ignore
            except Exception:
                st.info("Install `streamlit-echarts` to preview ECharts here.")
            else:
                option = {
                    "grid": {"left": "8%", "right": "6%", "top": "12%", "bottom": "12%"},
                    "xAxis": {"type": "category", "data": ["Jan", "Feb", "Mar", "Apr", "May"]},
                    "yAxis": {"type": "value", "splitLine": {"show": True}},
                    "series": [{"type": "line", "data": [120, 200, 150, 80, 70], "smooth": True}],
                    "tooltip": {"trigger": "axis"},
                    "title": {"text": "Sample chart"},
                    "legend": {"data": ["Series 1"]},
                }
                themed = apply_echarts_theme(option, ui_theme="dark", overrides=page_overrides)
                st_echarts(themed, height="260px", theme=None)

        with st.container(border=True):
            st.markdown("**Preview (Forecast Status liquidFill)**")
            lab_now = st.session_state["theme_lab"]
            style_now_raw = str(st.session_state.get("theme_lab_forecast_liquid_style") or lab_now.get("forecast_liquid_style") or THEME.get("forecast_liquid_style", "Ocean"))
            style_now = style_now_raw if style_now_raw in style_options else "Ocean"
            wave1_hex = str(lab_now.get("forecast_liquid_wave_1", THEME.get("forecast_liquid_wave_1", lab_now.get("forecast_liquid_primary", "#3177cb"))))
            wave2_hex = str(lab_now.get("forecast_liquid_wave_2", THEME.get("forecast_liquid_wave_2", wave1_hex)))
            wave3_hex = str(lab_now.get("forecast_liquid_wave_3", THEME.get("forecast_liquid_wave_3", wave1_hex)))
            wave4_hex = str(lab_now.get("forecast_liquid_wave_4", THEME.get("forecast_liquid_wave_4", "#ffffff")))
            border_hex = str(lab_now.get("forecast_liquid_border", THEME.get("forecast_liquid_border", wave1_hex)))
            bg_top_hex = str(lab_now.get("forecast_liquid_bg_top", THEME.get("forecast_liquid_bg_top", "#eaf5ff")))
            bg_bottom_hex = str(lab_now.get("forecast_liquid_bg_bottom", THEME.get("forecast_liquid_bg_bottom", "#cfe6ff")))
            label_out_hex = str(lab_now.get("forecast_liquid_label_outside", THEME.get("forecast_liquid_label_outside", "#f5f5f5")))
            label_in_hex = str(lab_now.get("forecast_liquid_label_inside", THEME.get("forecast_liquid_label_inside", "#ffffff")))
            label_size_px = int(float(lab_now.get("forecast_liquid_label_size_px", THEME.get("forecast_liquid_label_size_px", "46")) or 46))
            wave_opacity = float(lab_now.get("forecast_liquid_wave_opacity", THEME.get("forecast_liquid_wave_opacity", "0.85")) or 0.85)
            bg_top_opacity = float(lab_now.get("forecast_liquid_bg_top_opacity", THEME.get("forecast_liquid_bg_top_opacity", "0.22")) or 0.22)
            bg_bottom_opacity = float(lab_now.get("forecast_liquid_bg_bottom_opacity", THEME.get("forecast_liquid_bg_bottom_opacity", "0.08")) or 0.08)
            highlight_opacity = float(lab_now.get("forecast_liquid_wave_highlight_opacity", THEME.get("forecast_liquid_wave_highlight_opacity", "0.14")) or 0.14)
            highlight_over_hex = str(
                lab_now.get(
                    "forecast_liquid_wave_highlight_over",
                    THEME.get("forecast_liquid_wave_highlight_over", "#ffffff"),
                )
            )
            highlight_over_opacity = float(
                lab_now.get(
                    "forecast_liquid_wave_highlight_over_opacity",
                    THEME.get("forecast_liquid_wave_highlight_over_opacity", "0.00"),
                )
                or 0.0
            )
            over_wave_hex = str(lab_now.get("forecast_liquid_over_primary", THEME.get("forecast_liquid_over_primary", "#e74c3c")))
            over_border_hex = str(lab_now.get("forecast_liquid_over_border", THEME.get("forecast_liquid_over_border", over_wave_hex)))

            def _rgb(h: str, default: tuple[int, int, int]) -> tuple[int, int, int]:
                try:
                    hh = str(h).strip().lstrip("#")
                    return (int(hh[0:2], 16), int(hh[2:4], 16), int(hh[4:6], 16))
                except Exception:
                    return default

            w1r, w1g, w1b = _rgb(wave1_hex, (49, 119, 203))
            w2r, w2g, w2b = _rgb(wave2_hex, (38, 95, 166))
            w3r, w3g, w3b = _rgb(wave3_hex, (91, 180, 255))
            w4r, w4g, w4b = _rgb(wave4_hex, (255, 255, 255))
            hor, hog, hob = _rgb(highlight_over_hex, (255, 255, 255))
            br, bg, bb = _rgb(border_hex, (w1r, w1g, w1b))
            tr, tg, tb = _rgb(bg_top_hex, (234, 245, 255))
            rr, rg_, rb_ = _rgb(bg_bottom_hex, (207, 230, 255))
            lor, log, lob = _rgb(label_out_hex, (245, 245, 245))
            lir, lig, lib = _rgb(label_in_hex, (255, 255, 255))
            orr, org, orb = _rgb(over_wave_hex, (231, 76, 60))
            obr, obg, obb = _rgb(over_border_hex, (231, 76, 60))

            border_rgba = f"rgba({br},{bg},{bb},0.9)"
            bg_top_rgba = f"rgba({tr},{tg},{tb},{max(0.0, min(1.0, bg_top_opacity))})"
            bg_bottom_rgba = f"rgba({rr},{rg_},{rb_},{max(0.0, min(1.0, bg_bottom_opacity))})"
            simple_badge_bg_rgba = f"rgba({tr},{tg},{tb},{max(0.03, min(0.16, bg_top_opacity))})"
            glass_top_rgba = f"rgba({tr},{tg},{tb},{max(0.04, min(0.18, bg_top_opacity * 0.75))})"
            glass_bottom_rgba = f"rgba({rr},{rg_},{rb_},{max(0.02, min(0.12, bg_bottom_opacity * 0.75))})"
            label_out_rgb = f"rgb({lor},{log},{lob})"
            label_in_rgb = f"rgb({lir},{lig},{lib})"
            over_border_rgba = f"rgba({obr},{obg},{obb},0.95)"
            highlight_over_rgba = f"rgba({hor},{hog},{hob},{max(0.0, min(1.0, highlight_over_opacity))})"
            preview_pct = int(st.session_state.get("theme_lab_forecast_liquid_preview_pct", 62) or 62)
            demo_ratio = max(0.0, float(preview_pct) / 100.0)
            ratio = min(demo_ratio, 1.0)
            preview_id = f"tco-liquid-preview-{date.today().isoformat()}-{style_now}-{preview_pct}-{label_size_px}-{w1r}{w1g}{w1b}-{br}{bg}{bb}"
            if " " in preview_id:
                preview_id = preview_id.replace(" ", "-")

            style_params = {
                "Ocean": {"amplitude": 10, "period": 2000, "waveLength": "80%"},
                "Calm": {"amplitude": 7, "period": 2600, "waveLength": "95%"},
                "Choppy": {"amplitude": 13, "period": 1500, "waveLength": "70%"},
                "Flat": {"amplitude": 0, "period": 2200, "waveLength": "90%"},
                "SimpleBadge": {"amplitude": 4, "period": 2600, "waveLength": "110%"},
                "GlassBadge": {"amplitude": 6, "period": 2400, "waveLength": "95%"},
                "SingleWave": {"amplitude": 8, "period": 2200, "waveLength": "90%"},
                "StaticLevel": {"amplitude": 0, "period": 2200, "waveLength": "90%"},
                "CounterWave": {"amplitude": 8, "period": 2400, "waveLength": "88%"},
                "VariableWave": {"amplitude": 10, "period": 2100, "waveLength": "84%"},
                "Diamond": {"amplitude": 8, "period": 2200, "waveLength": "90%"},
                "Triangle": {"amplitude": 0, "period": 2200, "waveLength": "90%"},
                "Pin": {"amplitude": 8, "period": 2200, "waveLength": "90%"},
                "Rect": {"amplitude": 8, "period": 2200, "waveLength": "90%"},
                "RoundRect": {"amplitude": 8, "period": 2200, "waveLength": "90%"},
            }.get(style_now, {"amplitude": 10, "period": 2000, "waveLength": "80%"})

            html_block = f"""
            <div id="{preview_id}" style="width: 100%; height: 240px;"></div>
            <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/echarts-liquidfill@3/dist/echarts-liquidfill.min.js"></script>
            <script>
              (function() {{
                const el = document.getElementById("{preview_id}");
                if (!el || !window.echarts) return;
                const chart = echarts.init(el);
                const ratio = {ratio};
                const demoRatio = {demo_ratio};
                const styleNow = '{style_now}';
                const isOver = demoRatio > 1.0;
                const displayPct = Math.round(demoRatio * 100);
                const isNearFull = displayPct > 99;
                const waveOpacity = {max(0.0, min(1.0, wave_opacity))};
                let waveData = [
                  ratio,
                  {{ value: Math.max(ratio - 0.03, 0), direction: 'left' }},
                  Math.max(ratio - 0.06, 0),
                  {{ value: Math.max(ratio - 0.09, 0), direction: 'left' }}
                ];
                let waveColors = [
                  isOver ? 'rgba({orr},{org},{orb},0.65)' : 'rgba({w1r},{w1g},{w1b},0.65)',
                  isOver ? 'rgba({orr},{org},{orb},0.45)' : 'rgba({w2r},{w2g},{w2b},0.45)',
                  isOver ? 'rgba({orr},{org},{orb},0.30)' : 'rgba({w3r},{w3g},{w3b},0.30)',
                  isNearFull ? '{highlight_over_rgba}' : 'rgba({w4r},{w4g},{w4b},{max(0.0, min(1.0, highlight_opacity))})'
                ];
                let outlineConfig = {{
                  show: true,
                  borderDistance: 6,
                  itemStyle: {{
                    borderWidth: 3,
                    borderColor: isOver ? '{over_border_rgba}' : '{border_rgba}'
                  }}
                }};
                let backgroundStyleConfig = {{
                  color: {{
                    type: 'linear',
                    x: 0, y: 0, x2: 0, y2: 1,
                    colorStops: [
                      {{ offset: 0, color: '{bg_top_rgba}' }},
                      {{ offset: 1, color: '{bg_bottom_rgba}' }}
                    ]
                  }}
                }};
                let itemStyleConfig = {{
                  opacity: waveOpacity
                }};
                let shapeNow = 'circle';
                let radiusNow = '70%';
                let centerNow = ['50%', '50%'];
                let waveAnimationNow = true;
                let silentNow = false;
                let labelConfig = {{
                  show: true,
                  formatter: function () {{ return displayPct + '%'; }},
                  fontSize: {label_size_px},
                  fontWeight: '700',
                  fontStyle: 'normal',
                  color: '{label_out_rgb}',
                  insideColor: '{label_in_rgb}',
                  align: 'center',
                  verticalAlign: 'middle'
                }};

                if (styleNow === 'SimpleBadge') {{
                  waveData = [ratio];
                  waveColors = [isOver ? 'rgba({orr},{org},{orb},0.72)' : 'rgba({w1r},{w1g},{w1b},0.72)'];
                  outlineConfig = {{ show: false }};
                  backgroundStyleConfig = {{ color: '{simple_badge_bg_rgba}' }};
                }} else if (styleNow === 'GlassBadge') {{
                  waveData = [ratio, {{ value: Math.max(ratio - 0.05, 0), direction: 'left' }}];
                  waveColors = [
                    isOver ? 'rgba({orr},{org},{orb},0.62)' : 'rgba({w1r},{w1g},{w1b},0.62)',
                    isOver ? 'rgba({orr},{org},{orb},0.42)' : 'rgba({w2r},{w2g},{w2b},0.42)'
                  ];
                  outlineConfig = {{ show: false }};
                  backgroundStyleConfig = {{
                    color: {{
                      type: 'linear',
                      x: 0, y: 0, x2: 0, y2: 1,
                      colorStops: [
                        {{ offset: 0, color: '{glass_top_rgba}' }},
                        {{ offset: 1, color: '{glass_bottom_rgba}' }}
                      ]
                    }}
                  }};
                  itemStyleConfig.shadowBlur = 10;
                  itemStyleConfig.shadowColor = 'rgba(0,0,0,0.25)';
                }} else if (styleNow === 'SingleWave') {{
                  waveData = [ratio];
                }} else if (styleNow === 'StaticLevel') {{
                  waveData = [ratio];
                  waveAnimationNow = false;
                  silentNow = true;
                }} else if (styleNow === 'CounterWave') {{
                  waveData = [
                    ratio,
                    {{ value: Math.max(ratio - 0.03, 0), direction: 'left' }},
                    Math.max(ratio - 0.06, 0),
                    {{ value: Math.max(ratio - 0.09, 0), direction: 'left' }}
                  ];
                }} else if (styleNow === 'VariableWave') {{
                  waveData = [
                    ratio,
                    Math.max(ratio - 0.03, 0),
                    {{ value: Math.max(ratio - 0.06, 0), amplitude: 15 }},
                    {{ value: Math.max(ratio - 0.09, 0), amplitude: 20, waveLength: 100 }}
                  ];
                }} else if (styleNow === 'Diamond') {{
                  shapeNow = 'diamond';
                  outlineConfig = {{ show: false }};
                  backgroundStyleConfig.borderWidth = 2;
                  backgroundStyleConfig.borderColor = isOver ? '{over_border_rgba}' : '{border_rgba}';
                }} else if (styleNow === 'Triangle') {{
                  shapeNow = 'triangle';
                  outlineConfig = {{ show: false }};
                  backgroundStyleConfig.borderWidth = 2;
                  backgroundStyleConfig.borderColor = isOver ? '{over_border_rgba}' : '{border_rgba}';
                  labelConfig.position = ['50%', '65%'];
                }} else if (styleNow === 'Pin') {{
                  shapeNow = 'pin';
                  radiusNow = '90%';
                  centerNow = ['50%', '40%'];
                  outlineConfig = {{ show: false }};
                  backgroundStyleConfig.borderWidth = 2;
                  backgroundStyleConfig.borderColor = isOver ? '{over_border_rgba}' : '{border_rgba}';
                  itemStyleConfig.shadowBlur = 0;
                  labelConfig.position = ['50%', '45%'];
                }} else if (styleNow === 'Rect') {{
                  shapeNow = 'rect';
                  outlineConfig = {{ show: false }};
                  backgroundStyleConfig.borderWidth = 2;
                  backgroundStyleConfig.borderColor = isOver ? '{over_border_rgba}' : '{border_rgba}';
                }} else if (styleNow === 'RoundRect') {{
                  shapeNow = 'roundRect';
                  outlineConfig = {{ show: false }};
                  backgroundStyleConfig.borderWidth = 2;
                  backgroundStyleConfig.borderColor = isOver ? '{over_border_rgba}' : '{border_rgba}';
                }}

                const option = {{
                  backgroundColor: 'transparent',
                  series: [{{
                    type: 'liquidFill',
                    radius: radiusNow,
                    center: centerNow,
                    data: waveData,
                    waveAnimation: waveAnimationNow,
                    silent: silentNow,
                    animationDuration: 0,
                    animationDurationUpdate: {style_params["period"]},
                    period: {style_params["period"]},
                    amplitude: {style_params["amplitude"]},
                    waveLength: '{style_params["waveLength"]}',
                    direction: 'right',
                    shape: shapeNow,
                    color: waveColors,
                    itemStyle: itemStyleConfig,
                    outline: outlineConfig,
                    backgroundStyle: backgroundStyleConfig,
                    label: labelConfig
                  }}]
                }};
                chart.setOption(option);
                window.addEventListener('resize', function() {{ chart.resize(); }});
              }})();
            </script>
            """
            components.html(html_block, height=260)
            st.caption(f"Preset: {style_now}")

        with st.container(border=True):
            st.markdown("**Preview (Graphviz)**")
            dot = """
 digraph G {{
   rankdir=LR;
   node [shape=box, style=\"rounded,filled\", fillcolor=\"{fill}\", color=\"{border}\", fontcolor=\"{text}\"];
   A [label=\"Programs\"];
   B [label=\"Teams\"];
   C [label=\"Invoices\"];
   A -> B -> C;
 }}
 """.format(
                fill=palette.get("surface", "#161616"),
                border=palette.get("border", "#2D2D2D"),
                text=palette.get("text", "#F2F2F2"),
            )
            st.graphviz_chart(dot, use_container_width=True)


def _render_html_section() -> None:
    st.subheader("Inline HTML patterns used in the app")
    html_left, html_right = st.columns([1, 2], vertical_alignment="top")
    with html_left:
        st.caption("These preview common `unsafe_allow_html=True` patterns (badges/KPIs/tables).")
    with html_right:
        with st.container(border=True):
            st.markdown("**Badges (Invoices)**")
            st.markdown(
                "<div style='display:flex;gap:10px;align-items:center;flex-wrap:wrap;'>"
                f"{_badge_html('Critical')}"
                f"{_badge_html('Pending')}"
                f"{_badge_html('Contract')}"
                f"{_badge_html('OK')}"
                "</div>",
                unsafe_allow_html=True,
            )

        with st.container(border=True):
            st.markdown("**KPI mini-cards (Welcome/Dashboard style)**")
            k1, k2, k3 = st.columns(3)
            for col, label, value in (
                (k1, "Total NEXT", "$1,234,567.89"),
                (k2, "Apps", "123"),
                (k3, "Invoices this month", "7"),
            ):
                col.markdown(
                    "<div style='line-height:1.1; margin-bottom:6px;'>"
                    f"<div style='font-size:22px; font-weight:600'>{value}</div>"
                    f"<div style='font-size:12px; color:var(--tco-muted)'>{label}</div>"
                    "</div>",
                    unsafe_allow_html=True,
                )

        with st.container(border=True):
            st.markdown("**HTML table (Settings/Reconciliation pattern)**")
            df_small = pd.DataFrame(
                {
                    "PROGRAM": ["Program A", "Program B"],
                    "TEAM": ["Team 1", "Team 2"],
                    "FEATURES": [12, 5],
                }
            )
            df_small["FEATURES"] = df_small["FEATURES"].apply(lambda x: f'<a href="https://example.com" target="_blank" rel="noopener noreferrer">{x}</a>')
            st.markdown(
                """
 <style>
 .tco-html-table table { width: 100%; border-collapse: collapse; }
 .tco-html-table th, .tco-html-table td { padding: 8px 10px; border-bottom: 1px solid var(--tco-border); }
 .tco-html-table thead tr { background: var(--tco-table-header-bg); }
 .tco-html-table tbody tr:nth-child(even) { background: var(--tco-table-row-alt-bg); }
 .tco-html-table a { color: var(--tco-link); }
 </style>
 """,
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div class='tco-html-table'>{df_small.to_html(index=False, escape=False)}</div>",
                unsafe_allow_html=True,
            )


def _render_export_section() -> None:
    st.subheader("Copy/paste snippet")
    st.caption("This shows the full `THEME` dict (including any layout unit values).")
    snippet = "THEME: Dict[str, str] = {\n"
    for k in THEME.keys():
        snippet += '    "{}": "{}",\n'.format(k, st.session_state["theme_lab"].get(k, THEME[k]))
    snippet += "}\n"
    st.code(snippet, language="python")

    st.subheader("Streamlit theme snippet (affects `st.dataframe` cell colors)")
    st.caption("Streamlit’s built-in theme engine controls `st.dataframe` rendering; update `.streamlit/config.toml` and restart to apply.")
    lab_now = st.session_state["theme_lab"]
    toml_snippet = (
        "[theme]\n"
        'base = "dark"\n'
        'font = "sans serif"\n'
        f'primaryColor = "{lab_now.get("primary", THEME.get("primary", "#3177cb"))}"\n'
        f'backgroundColor = "{lab_now.get("bg", THEME.get("bg", "#363535"))}"\n'
        f'secondaryBackgroundColor = "{lab_now.get("surface", THEME.get("surface", "#161616"))}"\n'
        f'textColor = "{lab_now.get("text", THEME.get("text", "#F2F2F2"))}"\n'
    )
    st.code(toml_snippet, language="toml")


# Do not override the global app page title; this module is embedded in Admin.

# Persistent state
st.session_state.setdefault("theme_lab_touched", [])
st.session_state.setdefault("theme_lab", {})

base_sig = repr(sorted(THEME.items()))
if st.session_state.get("theme_lab_base_sig") != base_sig:
    st.session_state["theme_lab_base_sig"] = base_sig
    st.session_state["theme_lab"] = THEME.copy()
    st.session_state["theme_lab_touched"] = []
    for k in list(THEME.keys()):
        st.session_state.pop(f"theme_lab_color_{k}", None)
    for widget_key in ("theme_lab_toast_icon_success", "theme_lab_toast_icon_error", "theme_lab_toast_icon_warning", "theme_lab_toast_icon_info"):
        st.session_state.pop(widget_key, None)
    for widget_key in ("theme_lab_radius_px", "theme_lab_button_radius_px", "theme_lab_button_py", "theme_lab_button_px", "theme_lab_input_radius", "theme_lab_echarts_symbol_border_width"):
        st.session_state.pop(widget_key, None)
    st.session_state.pop("theme_lab_forecast_liquid_style", None)
    st.session_state.pop("theme_lab_forecast_liquid_label_size_px", None)
    st.session_state.pop("theme_lab_forecast_liquid_preview_pct", None)
    st.session_state.pop("theme_lab_forecast_liquid_wave_opacity", None)
    st.session_state.pop("theme_lab_forecast_liquid_bg_top_opacity", None)
    st.session_state.pop("theme_lab_forecast_liquid_bg_bottom_opacity", None)
    st.session_state.pop("theme_lab_forecast_liquid_wave_highlight_opacity", None)
    st.session_state.pop("theme_lab_forecast_liquid_wave_highlight_over_opacity", None)

lab = st.session_state["theme_lab"]
touched = _sync_lab_defaults(lab, st.session_state.get("theme_lab_touched", []))

# Pull widget state into theme_lab (Streamlit restores widget keys before rerun)
for k in THEME.keys():
    widget_key = f"theme_lab_color_{k}"
    if widget_key in st.session_state:
        lab[k] = st.session_state[widget_key]

toast_icon_widget_map = {
    "theme_lab_toast_icon_success": "toast_icon_success",
    "theme_lab_toast_icon_error": "toast_icon_error",
    "theme_lab_toast_icon_warning": "toast_icon_warning",
    "theme_lab_toast_icon_info": "toast_icon_info",
}
for widget_key, theme_key in toast_icon_widget_map.items():
    if widget_key in st.session_state:
        lab[theme_key] = str(st.session_state[widget_key] or "").strip()[:1]

layout_widget_map = {
    "theme_lab_radius_px": ("radius", "px"),
    "theme_lab_button_radius_px": ("button_radius", "px"),
    "theme_lab_button_py": ("button_padding_y", "rem"),
    "theme_lab_button_px": ("button_padding_x", "rem"),
    "theme_lab_input_radius": ("input_radius", "px"),
    "theme_lab_echarts_symbol_border_width": ("echarts_symbol_border_width", "int"),
    "theme_lab_forecast_liquid_label_size_px": ("forecast_liquid_label_size_px", "int"),
    "theme_lab_forecast_liquid_wave_opacity": ("forecast_liquid_wave_opacity", "float"),
    "theme_lab_forecast_liquid_bg_top_opacity": ("forecast_liquid_bg_top_opacity", "float"),
    "theme_lab_forecast_liquid_bg_bottom_opacity": ("forecast_liquid_bg_bottom_opacity", "float"),
    "theme_lab_forecast_liquid_wave_highlight_opacity": ("forecast_liquid_wave_highlight_opacity", "float"),
    "theme_lab_forecast_liquid_wave_highlight_over_opacity": ("forecast_liquid_wave_highlight_over_opacity", "float"),
}
for widget_key, (theme_key, unit) in layout_widget_map.items():
    if widget_key in st.session_state:
        val = st.session_state[widget_key]
        if unit == "px":
            lab[theme_key] = f"{int(val)}px"
        elif unit == "rem":
            lab[theme_key] = f"{float(val):.2f}{unit}"
        elif unit == "float":
            lab[theme_key] = f"{float(val):.2f}"
        else:
            lab[theme_key] = str(int(val))

if "theme_lab_forecast_liquid_style" in st.session_state:
    lab["forecast_liquid_style"] = str(st.session_state["theme_lab_forecast_liquid_style"] or "").strip() or THEME.get("forecast_liquid_style", "Ocean")

lab = _apply_primary_links(lab, touched)
st.session_state["theme_lab"] = lab

# Always apply lab overrides to THIS page so previews always reflect the controls.
page_overrides = {k: lab.get(k, v) for k, v in THEME.items()}

# Apply theme + per-page overrides for preview.
use_theme(render_toggle=False)
apply_ui_theme_css(overrides=page_overrides)

palette = page_overrides

st.title("Visual Lab")
st.caption(
    "Visual Lab is an experimental sandbox for charts and ideas. "
    "Numbers here may not fully match the canonical cost engine used in the main pages."
)
st.caption("Tune theme colors/layout, preview widgets next to their controls, then copy/paste the snippet into `utils/theme.py`.")

top = st.columns([2, 1])
with top[0]:
    st.caption("Tip: change `primary` to recolor toggles/radios/progress. Override related keys only when you need them different.")
    with top[1]:
        if st.button("Reset lab", type="secondary", key="theme_lab_reset"):
            st.session_state["theme_lab"] = THEME.copy()
            st.session_state["theme_lab_touched"] = []
            for k in list(THEME.keys()):
                st.session_state.pop(f"theme_lab_color_{k}", None)
            for widget_key in layout_widget_map.keys():
                st.session_state.pop(widget_key, None)
            st.session_state.pop("theme_lab_forecast_liquid_style", None)
            st.session_state.pop("theme_lab_forecast_liquid_preview_pct", None)
            st.rerun()

st.divider()

sections = ["Theme", "Widgets", "Data", "Charts", "HTML", "Export"]
section = st.radio("Section", sections, horizontal=True, label_visibility="collapsed", key="theme_lab_section")

if section == "Theme":
    _render_theme_section(lab)
elif section == "Widgets":
    _render_widgets_section(lab)
elif section == "Data":
    _render_data_section()
elif section == "Charts":
    _render_charts_section(page_overrides, palette)
elif section == "HTML":
    _render_html_section()
elif section == "Export":
    _render_export_section()
