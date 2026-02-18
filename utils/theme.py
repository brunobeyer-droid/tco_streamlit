from __future__ import annotations

import copy
from typing import Any, Dict, Optional

import streamlit as st


THEME_CSS_VERSION = "2026-02-05-sidebar-nav-clean"

THEME: Dict[str, str] = {
    "bg": "#363535",
    "bg2": "#363535",
    "surface": "#181818",
    "sidebar": "#181818",
    "sidebar2": "#181818",
    "text": "#f5f5f5",
    "muted": "#b3b3b3",
    "border": "#181818",
    "primary": "#3177cb",
    "accent": "#3177cb",
    "link": "#3177cb",
    "table_bg": "#181818",
    "table_header_bg": "#1f1f1f",
    "table_border": "#2a2a2a",
    "table_text": "#f5f5f5",
    "table_row_alt_bg": "#202020",
    "table_row_hover_bg": "#262626",
    "dataframe_bg": "#363535",
    "dataframe_header_bg": "#1f1f1f",
    "dataframe_border": "#363535",
    "dataframe_text": "#f5f5f5",
    "dataframe_gridline": "#2c2c2c",
    "echarts_bg": "#363535",
    "echarts_text": "#f5f5f5",
    "echarts_axis_text": "#b3b3b3",
    "echarts_axis_line": "#6899bd",
    "echarts_gridline": "#696868",
    "echarts_tooltip_bg": "#3B3B3B",
    "echarts_tooltip_border": "#2a2a2a",
    "echarts_tooltip_text": "#f5f5f5",
    "echarts_symbol_border_width": "0",
    "echarts_panel_bg_top": "#0f172a",
    "echarts_panel_bg_bottom": "#111c33",
    "echarts_gridline_soft": "rgba(148,163,184,0.10)",
    "echarts_axis_text_dim": "#AFC0D3",
    "echarts_tooltip_glass_bg": "rgba(15,23,42,0.88)",
    "echarts_tooltip_glass_border": "rgba(148,163,184,0.28)",
    "echarts_tooltip_shadow": "0 10px 30px rgba(0,0,0,0.35)",
    "echarts_series_baseline": "#3B82F6",
    "echarts_series_forecast": "#84CC16",
    "echarts_series_actual": "#F59E0B",
    "echarts_series_risk": "#F97316",
    "echarts_series_neutral": "#94A3B8",
    "echarts_glow_strength_sm": "8",
    "echarts_glow_strength_md": "12",
    "echarts_anim_duration_enter_ms": "450",
    "echarts_anim_duration_update_ms": "300",
    "toast_bg": "#3076c9",
    "toast_border": "#3177cb",
    "toast_text": "#f5f5f5",
    "toast_icon_success": "✓",
    "toast_icon_error": "×",
    "toast_icon_warning": "!",
    "toast_icon_info": "i",
    "button_bg": "#181818",
    "button_fg": "#f5f5f5",
    "button_hover": "#3177cb",
    "button_secondary_bg": "#2a2a2a",
    "button_secondary_fg": "#f5f5f5",
    "tab_text": "#dcdcdc",
    "tab_active_text": "#ffffff",
    "tab_active_border": "#3177cb",
    "input_bg": "#181818",
    "input_text": "#f5f5f5",
    "input_border": "#181818",
    "toggle_track_on": "#3177cb",
    "toggle_track_off": "#3a3a3a",
    "toggle_thumb": "#f5f5f5",
    "radius": "6px",
    "gap": "0.75rem",
    "button_radius": "7px",
    "button_padding_y": "0.10rem",
    "button_padding_x": "1.25rem",
    "input_radius": "7px",
    "forecast_liquid_primary": "#3177cb",
    "forecast_liquid_border": "#256abd",
    "forecast_liquid_label": "#ffffff",
    "forecast_liquid_style": "Ocean",
    "forecast_liquid_wave_1": "#81b5f1",
    "forecast_liquid_wave_2": "#e9ecef",
    "forecast_liquid_wave_3": "#0582ec",
    "forecast_liquid_wave_4": "#1970e6",
    "forecast_liquid_bg_top": "#eaf5ff",
    "forecast_liquid_bg_bottom": "#cfe6ff",
    "forecast_liquid_label_outside": "#f5f5f5",
    "forecast_liquid_label_inside": "#ffffff",
    "forecast_liquid_label_size_px": "40",
    "forecast_liquid_wave_opacity": "1.00",
    "forecast_liquid_bg_top_opacity": "0.02",
    "forecast_liquid_bg_bottom_opacity": "0.12",
    "forecast_liquid_wave_highlight_opacity": "0.54",
    "forecast_liquid_wave_highlight_over": "#ffffff",
    "forecast_liquid_wave_highlight_over_opacity": "0.00",
    "forecast_liquid_over_primary": "#d43120",
    "forecast_liquid_over_border": "#d43120",
}
# Backwards-compatible mapping for legacy imports (provide both keys to avoid KeyError)
THEME_PALETTES: Dict[str, Dict[str, str]] = {
    "dark": THEME,
    "light": THEME,  # alias to satisfy legacy code paths expecting a light key
}


def use_theme(render_toggle: bool = False, default: str = "dark") -> str:
    """Apply the single custom theme once per run and return its name.

    `render_toggle` is ignored intentionally to avoid theme drift and navigation glitches.
    """
    # Streamlit rebuilds the DOM on every rerun/navigation, so CSS must be injected
    # on every run to avoid "reverting" when switching pages.
    apply_ui_theme_css()
    st.session_state["ui_theme"] = "dark"
    if st.session_state.get("debug_theme"):
        st.toast("Theme test")
        st.caption(f"Toast bg: {THEME.get('toast_bg')}")
    return "dark"


def _merged_theme(overrides: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    merged = THEME.copy()
    if overrides:
        for k, v in overrides.items():
            if v is not None and str(v).strip() != "":
                merged[k] = str(v)
    # Back-compat / sensible defaults
    merged.setdefault("primary", merged.get("accent", "#4971FF"))

    # If caller overrides primary but not these, keep them tied to primary.
    if overrides and "primary" in overrides:
        if "toggle_track_on" not in overrides:
            merged["toggle_track_on"] = merged["primary"]
        if "tab_active_border" not in overrides:
            merged["tab_active_border"] = merged["primary"]
        if "accent" not in overrides:
            merged["accent"] = merged["primary"]
        if "link" not in overrides:
            merged["link"] = merged["primary"]

    # Dataframe defaults mirror table defaults unless explicitly set.
    merged.setdefault("dataframe_bg", merged.get("table_bg", merged["bg"]))
    merged.setdefault("dataframe_header_bg", merged.get("table_header_bg", merged["surface"]))
    merged.setdefault("dataframe_border", merged.get("table_border", merged["border"]))
    merged.setdefault("dataframe_text", merged.get("table_text", merged["text"]))
    merged.setdefault("dataframe_gridline", merged.get("dataframe_border", merged["border"]))

    merged.setdefault("echarts_bg", merged.get("bg2", merged["bg"]))
    merged.setdefault("echarts_text", merged.get("text", "#F2F2F2"))
    merged.setdefault("echarts_axis_text", merged.get("muted", "#B3B3B3"))
    merged.setdefault("echarts_axis_line", merged.get("border", "#2D2D2D"))
    merged.setdefault("echarts_gridline", "#1f1f1f")
    merged.setdefault("echarts_tooltip_bg", merged.get("echarts_bg", merged.get("bg2", merged["bg"])))
    merged.setdefault("echarts_tooltip_border", merged.get("border", "#2D2D2D"))
    merged.setdefault("echarts_tooltip_text", merged.get("text", "#F2F2F2"))
    merged.setdefault("echarts_symbol_border_width", "0")
    merged.setdefault("echarts_panel_bg_top", "#0f172a")
    merged.setdefault("echarts_panel_bg_bottom", "#111c33")
    merged.setdefault("echarts_gridline_soft", "rgba(148,163,184,0.10)")
    merged.setdefault("echarts_axis_text_dim", merged.get("echarts_axis_text", merged.get("muted", "#B3B3B3")))
    merged.setdefault("echarts_tooltip_glass_bg", "rgba(15,23,42,0.88)")
    merged.setdefault("echarts_tooltip_glass_border", "rgba(148,163,184,0.28)")
    merged.setdefault("echarts_tooltip_shadow", "0 10px 30px rgba(0,0,0,0.35)")
    merged.setdefault("echarts_series_baseline", "#3B82F6")
    merged.setdefault("echarts_series_forecast", "#84CC16")
    merged.setdefault("echarts_series_actual", "#F59E0B")
    merged.setdefault("echarts_series_risk", "#F97316")
    merged.setdefault("echarts_series_neutral", "#94A3B8")
    merged.setdefault("echarts_glow_strength_sm", "8")
    merged.setdefault("echarts_glow_strength_md", "12")
    merged.setdefault("echarts_anim_duration_enter_ms", "450")
    merged.setdefault("echarts_anim_duration_update_ms", "300")
    merged.setdefault("toast_bg", merged.get("surface", merged["bg"]))
    merged.setdefault("toast_border", merged.get("border", "#2D2D2D"))
    merged.setdefault("toast_text", merged.get("text", "#F2F2F2"))
    merged.setdefault("toast_icon_success", "✓")
    merged.setdefault("toast_icon_error", "×")
    merged.setdefault("toast_icon_warning", "!")
    merged.setdefault("toast_icon_info", "i")
    merged.setdefault("input_border", merged.get("border", "#2a2a2a"))
    merged.setdefault("button_secondary_bg", merged.get("button_bg", merged["surface"]))
    merged.setdefault("button_secondary_fg", merged.get("button_fg", merged["text"]))
    return merged


def apply_ui_theme_css(*, overrides: Optional[Dict[str, str]] = None) -> None:
    """Theme the Streamlit DOM via CSS (injected into the main document)."""
    theme = _merged_theme(overrides)
    theme_sig = f"{repr(sorted(theme.items()))}|css:{THEME_CSS_VERSION}"
    if st.session_state.get("_tco_theme_css_sig") == theme_sig:
        return
    css = f"""<style>
:root {{
  --tco-bg: {theme["bg"]};
  --tco-bg2: {theme.get("bg2", theme["bg"])};
  --tco-surface: {theme["surface"]};
  --tco-sidebar: {theme["sidebar"]};
  --tco-sidebar2: {theme["sidebar2"]};
  --tco-text: {theme["text"]};
  --tco-muted: {theme["muted"]};
  --tco-border: {theme["border"]};
  --tco-primary: {theme["primary"]};
  --tco-link: {theme["link"]};
  --tco-accent: {theme["accent"]};
  --tco-table-bg: {theme["table_bg"]};
  --tco-table-header-bg: {theme["table_header_bg"]};
  --tco-table-border: {theme["table_border"]};
  --tco-table-text: {theme["table_text"]};
	  --tco-table-row-alt-bg: {theme["table_row_alt_bg"]};
	  --tco-table-row-hover-bg: {theme["table_row_hover_bg"]};
	  --tco-df-bg: {theme["dataframe_bg"]};
	  --tco-df-header-bg: {theme["dataframe_header_bg"]};
	  --tco-df-border: {theme["dataframe_border"]};
	  --tco-df-text: {theme["dataframe_text"]};
	  --tco-df-gridline: {theme["dataframe_gridline"]};
	  --tco-button-bg: {theme["button_bg"]};
  --tco-button-fg: {theme["button_fg"]};
  --tco-button-hover: {theme["button_hover"]};
  --tco-button-secondary-bg: {theme.get("button_secondary_bg", theme["button_bg"])};
  --tco-button-secondary-fg: {theme.get("button_secondary_fg", theme["button_fg"])};
  --tco-tab-text: {theme["tab_text"]};
  --tco-tab-active-text: {theme["tab_active_text"]};
  --tco-tab-active-border: {theme["tab_active_border"]};
  --tco-input-bg: {theme["input_bg"]};
  --tco-input-text: {theme["input_text"]};
  --tco-input-border: {theme["input_border"]};
	  --tco-toggle-track-on: {theme["toggle_track_on"]};
	  --tco-toggle-track-off: {theme["toggle_track_off"]};
	  --tco-toggle-thumb: {theme["toggle_thumb"]};
	  --tco-toast-bg: {theme["toast_bg"]};
	  --tco-toast-border: {theme["toast_border"]};
	  --tco-toast-text: {theme["toast_text"]};
	  --tco-radius: {theme["radius"]};
	  --tco-gap: {theme["gap"]};
	  --tco-button-radius: {theme["button_radius"]};
	  --tco-button-padding-y: {theme["button_padding_y"]};
	  --tco-button-padding-x: {theme["button_padding_x"]};
	  --tco-input-radius: {theme["input_radius"]};

	  /* Streamlit variables (helps built-in widgets match the theme) */
	  --primary-color: {theme["primary"]};
	  --primaryColor: {theme["primary"]};
	  --background-color: {theme["bg"]};
	  --secondary-background-color: {theme["surface"]};
	  --text-color: {theme["text"]};
	  --border-color: {theme["border"]};
}}

body, [data-testid="stAppViewContainer"], [data-testid="stMain"] {{
  background-color: var(--tco-bg) !important;
  color: var(--tco-text) !important;
}}

/* Layout spacing (leave header toolbar visible) */
div.block-container {{
  padding-top: 1rem !important;
  padding-bottom: 1.25rem !important;
}}

/* Sidebar + navigation */
section[data-testid="stSidebar"] > div:first-child {{
  background-color: var(--tco-sidebar) !important;
  border-right: 1px solid var(--tco-border) !important;
  color: var(--tco-text) !important;
  /* Keep sidebar widgets (e.g., login form) visible; nav compaction is scoped separately. */
  overflow: visible !important;
}}
section[data-testid="stSidebar"] > div:first-child > div {{
  background-color: var(--tco-sidebar) !important;
}}
[data-testid="stNavigation"] {{
  background-color: var(--tco-sidebar2) !important;
  border-bottom: 1px solid var(--tco-border) !important;
}}
[data-testid="stSidebarNav"] a {{
  color: var(--tco-text) !important;
}}
[data-testid="stSidebarNav"] a svg {{
  fill: var(--tco-text) !important;
}}

/* Header */
header[data-testid="stHeader"] {{
  background-color: transparent !important;
  border-bottom: none !important;
}}

/* Typography */
h1, h2, h3, h4, h5, h6 {{
  color: var(--tco-text) !important;
  letter-spacing: -0.01em;
}}
p, li, span, label {{
  color: var(--tco-text);
}}

/* Links */
[data-testid="stAppViewContainer"] a {{
  color: var(--tco-link) !important;
}}

/* "Cards" / bordered wrappers - use subtle separation without hard borders */
div[data-testid="stVerticalBlockBorderWrapper"] {{
  background: var(--tco-surface) !important;
  border: none !important;
  box-shadow: none !important;
  outline: none !important;
  border-radius: var(--tco-radius) !important;
  padding: var(--tco-gap) !important;
}}
div[data-testid="stVerticalBlockBorderWrapper"] > div {{
  border: none !important;
  box-shadow: none !important;
  outline: none !important;
  background: transparent !important;
}}
div[data-testid="stVerticalBlockBorderWrapper"] > div > div {{
  border: none !important;
  box-shadow: none !important;
  outline: none !important;
  background: transparent !important;
}}
/* Metrics */
div[data-testid="stMetric"] {{
  background: var(--tco-surface) !important;
  border: 1px solid var(--tco-border) !important;
  border-radius: var(--tco-radius) !important;
  padding: 0.75rem 0.9rem !important;
}}
div[data-testid="stMetric"] [data-testid="stMetricLabel"] {{
  color: var(--tco-muted) !important;
}}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
  color: var(--tco-text) !important;
}}

/* Tabs (BaseWeb + Streamlit) */
div[data-testid="stTabs"] button,
div[data-testid="stTabs"] button span,
div[data-testid="stTabs"] button p,
div[data-testid="stTabs"] button div,
div[data-testid="stTabs"] [role="tab"],
div[data-testid="stTabs"] [role="tab"] *,
div[data-testid="stTabs"] [role="tabpanel"],
div[data-testid="stTabs"] [role="tabpanel"] *,
div[data-baseweb="tab"],
div[data-baseweb="tab"] *,
span[data-baseweb="typo"],
div[data-baseweb="typo"] {{
  color: var(--tco-tab-text) !important;
}}
div[data-testid="stTabs"] > div:first-child {{
  background: var(--tco-bg2) !important;
  border: none !important;
  border-radius: var(--tco-radius) !important;
  padding: 0.15rem 0.35rem 0 0.35rem !important;
}}
div[data-testid="stTabs"] button[aria-selected="true"],
div[data-baseweb="tab"][aria-selected="true"] {{
  color: var(--tco-tab-active-text) !important;
  border-color: var(--tco-tab-active-border) !important;
  border-bottom: 2px solid var(--tco-tab-active-border) !important;
}}
div[data-baseweb="tab-highlight"] {{
  background-color: var(--tco-tab-active-border) !important;
}}

/* Inputs */
input, textarea, select,
.stTextInput input,
.stSelectbox select,
.stDateInput input,
.stNumberInput input {{
  background-color: var(--tco-input-bg) !important;
  color: var(--tco-input-text) !important;
  border: 1px solid var(--tco-input-border) !important;
  border-radius: var(--tco-input-radius) !important;
}}

/* Buttons */
.stButton > button,
[data-testid="baseButton-secondary"] {{
  background-color: var(--tco-button-secondary-bg) !important;
  color: var(--tco-button-secondary-fg) !important;
  border: 1px solid var(--tco-border) !important;
  box-shadow: none !important;
  border-radius: var(--tco-button-radius) !important;
  padding: var(--tco-button-padding-y) var(--tco-button-padding-x) !important;
}}
.stDownloadButton > button {{
  background-color: var(--tco-button-secondary-bg) !important;
  color: var(--tco-button-secondary-fg) !important;
  border: 1px solid var(--tco-border) !important;
  border-radius: var(--tco-button-radius) !important;
  padding: var(--tco-button-padding-y) var(--tco-button-padding-x) !important;
}}
.stButton > button:hover,
[data-testid="baseButton-secondary"]:hover {{
  background-color: var(--tco-button-hover) !important;
  color: var(--tco-button-fg) !important;
}}

/* Primary button uses primary color */
[data-testid="baseButton-primary"] {{
  background-color: var(--tco-primary) !important;
  color: var(--tco-button-fg) !important;
  border: 1px solid var(--tco-primary) !important;
  box-shadow: none !important;
  border-radius: var(--tco-button-radius) !important;
  padding: var(--tco-button-padding-y) var(--tco-button-padding-x) !important;
}}
[data-testid="baseButton-primary"]:hover {{
  filter: brightness(1.05);
}}

/* Dataframes (st.dataframe + st.data_editor) */
div[data-testid="stDataFrame"],
div[data-testid="stDataEditor"] {{
  background: var(--tco-df-bg) !important;
  border: none !important;
  border-radius: var(--tco-radius) !important;
  padding: 0.25rem !important;
  color: var(--tco-text) !important;
  /* Let Streamlit's dataframe component consume these variables. */
  --text-color: var(--tco-df-text);
  --header-text-color: var(--tco-df-text);
  --background-color: var(--tco-df-bg);
  --header-background-color: var(--tco-df-header-bg);
  --grid-background-color: var(--tco-df-bg);
  --grid-border-color: var(--tco-df-gridline);
  --text-color-hover: var(--tco-df-text);
  --row-header-text-color: var(--tco-df-text);
  --row-header-background-color: var(--tco-df-header-bg);

  /* Best-effort (newer Streamlit dataframe renderer) */
  --gdg-bg-cell: var(--tco-df-bg);
  --gdg-bg-header: var(--tco-df-header-bg);
  --gdg-border-color: var(--tco-df-gridline);
  --gdg-text-color: var(--tco-df-text);
  --gdg-accent-color: var(--tco-primary);
  --gdg-bg-cell-medium: var(--tco-table-row-alt-bg);
  --gdg-bg-cell-hover: var(--tco-table-row-hover-bg);
  --gdg-bg-header-hover: var(--tco-df-header-bg);
  --gdg-text-dark: var(--tco-df-text);
  --gdg-text-medium: var(--tco-df-text);
  --gdg-text-light: var(--tco-df-text);
}}
/* Best-effort background for dataframe canvas/grid without breaking text/gridlines */
div[data-testid="stDataFrame"] [role="grid"],
div[data-testid="stDataFrame"] canvas,
div[data-testid="stDataEditor"] [role="grid"],
div[data-testid="stDataEditor"] canvas {{
  background-color: var(--tco-df-bg) !important;
}}
div[data-testid="stDataFrame"] [data-testid="stDataFrameGlideDataEditor"],
div[data-testid="stDataFrame"] [data-testid="stDataFrameGlideDataEditor"] > div,
div[data-testid="stDataEditor"] [data-testid="stDataFrameGlideDataEditor"],
div[data-testid="stDataEditor"] [data-testid="stDataFrameGlideDataEditor"] > div {{
  background-color: var(--tco-df-bg) !important;
}}

/* Static tables (st.table) */
div[data-testid="stTable"] table {{
  width: 100% !important;
  border-collapse: collapse !important;
  background: var(--tco-table-bg) !important;
  color: var(--tco-table-text) !important;
  border: none !important;
  border-radius: var(--tco-radius) !important;
  overflow: hidden !important;
}}
div[data-testid="stTable"] th {{
  background: var(--tco-table-header-bg) !important;
  color: var(--tco-table-text) !important;
  border-bottom: 1px solid transparent !important;
}}
div[data-testid="stTable"] td {{
  background: var(--tco-table-bg) !important;
  color: var(--tco-table-text) !important;
  border-bottom: 1px solid transparent !important;
}}
div[data-testid="stTable"] tbody tr:nth-child(even) td {{
  background: var(--tco-table-row-alt-bg) !important;
}}
div[data-testid="stTable"] tbody tr:hover td {{
  background: var(--tco-table-row-hover-bg) !important;
}}

/* Toggle */
[data-testid="stToggle"] [data-baseweb="toggle"] > div,
[data-testid="stToggle"] [role="switch"] > div {{
  background-color: var(--tco-toggle-track-off) !important;
  border: 1px solid var(--tco-border) !important;
}}
[data-testid="stToggle"] [data-baseweb="toggle"][aria-checked="true"] > div,
[data-testid="stToggle"] [role="switch"][aria-checked="true"] > div {{
  background-color: var(--tco-toggle-track-on) !important;
  border-color: var(--tco-toggle-track-on) !important;
}}
[data-testid="stToggle"] [data-baseweb="toggle"] div > div {{
  background-color: var(--tco-toggle-thumb) !important;
}}

/* BaseWeb switch variants (Streamlit uses these in some versions) */
[data-baseweb="switch"] > div {{
  background-color: var(--tco-toggle-track-off) !important;
  border: 1px solid var(--tco-border) !important;
}}
[data-baseweb="switch"][aria-checked="true"] > div {{
  background-color: var(--tco-toggle-track-on) !important;
  border-color: var(--tco-toggle-track-on) !important;
}}
[data-baseweb="switch"] div > div {{
  background-color: var(--tco-toggle-thumb) !important;
}}

/* Native accent (where supported) */
input[type="checkbox"],
input[type="radio"] {{
  accent-color: var(--tco-primary) !important;
}}

/* Radio + checkbox (force primary for checked state) */
div[data-baseweb="radio"][aria-checked="true"] > div {{
  border-color: var(--tco-primary) !important;
}}
div[data-baseweb="radio"][aria-checked="true"] > div > div {{
  background-color: var(--tco-primary) !important;
}}
div[data-baseweb="checkbox"][aria-checked="true"] > div {{
  background-color: var(--tco-primary) !important;
  border-color: var(--tco-primary) !important;
}}
label[data-baseweb="radio"][aria-checked="true"] > div {{
  border-color: var(--tco-primary) !important;
}}
label[data-baseweb="radio"][aria-checked="true"] > div > div {{
  background-color: var(--tco-primary) !important;
}}
label[data-baseweb="checkbox"][aria-checked="true"] > div {{
  background-color: var(--tco-primary) !important;
  border-color: var(--tco-primary) !important;
}}

/* Progress bar: keep visible track + primary fill */
div[data-testid="stProgress"] div[role="progressbar"] {{
  background-color: color-mix(in srgb, var(--tco-primary) 20%, transparent) !important;
}}
div[data-testid="stProgress"] div[role="progressbar"] > div {{
  background-color: var(--tco-primary) !important;
}}

/* Toast notifications (Streamlit BaseWeb toast) */
div[data-baseweb="toast"][data-testid="stToast"],
div[data-baseweb="toast"],
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"],
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[role="alert"][data-baseweb="toast"] {{
  background: var(--tco-toast-bg) !important;
  background-color: var(--tco-toast-bg) !important;
  border: 1px solid var(--tco-toast-border) !important;
  color: var(--tco-toast-text) !important;
  opacity: 1 !important;
  filter: none !important;
}}
div[data-baseweb="toast"][data-testid="stToast"]::before,
div[data-baseweb="toast"][data-testid="stToast"]::after,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"]::before,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"]::after {{
  background: var(--tco-toast-bg) !important;
  background-color: var(--tco-toast-bg) !important;
}}
div[data-baseweb="toast"][data-testid="stToast"] > div,
div[data-baseweb="toast"][data-testid="stToast"] > div > div,
div[data-baseweb="toast"][data-testid="stToast"] > div > div > div,
div[data-baseweb="toast"][data-testid="stToast"] div[data-testid="stMarkdownContainer"],
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] > div,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] > div > div,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] > div > div > div,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] div[data-testid="stMarkdownContainer"] {{
  background: var(--tco-toast-bg) !important;
  background-color: var(--tco-toast-bg) !important;
  color: var(--tco-toast-text) !important;
}}
div[data-baseweb="toast"][data-testid="stToast"] *,
div[data-baseweb="toast"][data-testid="stToast"] svg,
div[data-baseweb="toast"][data-testid="stToast"] svg path,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] *,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] svg,
div[data-baseweb="toaster"][data-testid="stToastContainer"] div[data-baseweb="toast"][data-testid="stToast"] svg path {{
  color: var(--tco-toast-text) !important;
  fill: var(--tco-toast-text) !important;
}}
/* Tooltip text (info icon hover) */
div[data-baseweb="tooltip"],
div[role="tooltip"] {{
  font-style: normal !important;
  font-family: inherit !important;
  white-space: pre-line !important;
}}
div[data-baseweb="tooltip"] *,
div[role="tooltip"] * {{
  font-style: normal !important;
  font-family: inherit !important;
}}

.tco-sidebar-divider {{
  height: 1px;
  width: 100%;
  margin: 0.6rem 0 0.4rem 0;
  background: rgba(255,255,255,0.08);
}}
</style>"""
    st.markdown(css, unsafe_allow_html=True)
    st.session_state["_tco_theme_css_sig"] = theme_sig


def echarts_semantic_colors(theme: Dict[str, str]) -> Dict[str, str]:
    """Canonical semantic colors for KPI-consistent chart storytelling."""
    return {
        "baseline": str(theme.get("echarts_series_baseline", "#3B82F6")),
        "forecast": str(theme.get("echarts_series_forecast", "#84CC16")),
        "actual": str(theme.get("echarts_series_actual", "#F59E0B")),
        "risk": str(theme.get("echarts_series_risk", "#F97316")),
        "neutral": str(theme.get("echarts_series_neutral", "#94A3B8")),
    }


def apply_sidebar_nav_compact_style() -> None:
    """Compact Spotify-like sidebar nav styling for native navigation."""
    sig = "sidebar-nav-compact-v1"
    if st.session_state.get("_tco_sidebar_nav_css_sig") == sig:
        return
    css = """<style>
section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] {
  margin-top: 0 !important;
  padding-top: 0 !important;
}
section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] ul {
  list-style: none !important;
  padding-left: 0 !important;
  margin: 0 !important;
}
section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] li {
  margin: 0 !important;
  padding: 0 !important;
}

section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] {
  --tco-nav-hover-bg: rgba(255,255,255,0.06);
  --tco-nav-active-bg: rgba(255,255,255,0.10);
}
@media (prefers-color-scheme: light) {
  section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] {
    --tco-nav-hover-bg: rgba(0,0,0,0.06);
    --tco-nav-active-bg: rgba(0,0,0,0.10);
  }
}
@supports (background-color: color-mix(in srgb, currentColor 10%, transparent)) {
  section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] {
    --tco-nav-hover-bg: color-mix(in srgb, currentColor 10%, transparent);
    --tco-nav-active-bg: color-mix(in srgb, currentColor 16%, transparent);
  }
}

section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] header[data-testid="stNavSectionHeader"] {
  display: flex !important;
  align-items: center !important;
  justify-content: space-between !important;
  cursor: pointer !important;
  font-size: 0.875rem !important;
  font-weight: 600 !important;
  line-height: 1.25rem !important;
  padding: 0.25rem 0.75rem !important;
  margin: 0 !important;
  background: transparent !important;
  border: 0 !important;
  color: inherit !important;
}
section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] header[data-testid="stNavSectionHeader"] span[data-testid="stIconMaterial"] {
  color: inherit !important;
  width: 18px !important;
  height: 18px !important;
  min-width: 18px !important;
  display: inline-flex !important;
  align-items: center !important;
  justify-content: center !important;
}
section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] header[data-testid="stNavSectionHeader"] span[data-testid="stIconMaterial"] svg {
  width: 18px !important;
  height: 18px !important;
}

section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] ul li a,
section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] ul li button {
  font-size: 0.875rem !important;
  font-weight: 400 !important;
  line-height: 1.25rem !important;
  padding: 0.25rem 0.75rem 0.25rem 1.5rem !important;
  border-radius: 0.5rem !important;
  text-decoration: none !important;
  background: transparent !important;
  margin: 0 !important;
}
section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] ul li a:hover,
section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] ul li button:hover {
  background-color: var(--tco-nav-hover-bg) !important;
}
section[data-testid="stSidebar"] nav[data-testid="stSidebarNav"] ul li a[aria-current="page"] {
  background-color: var(--tco-nav-active-bg) !important;
  font-weight: 400 !important;
  color: inherit !important;
}
</style>"""
    st.markdown(css, unsafe_allow_html=True)
    st.session_state["_tco_sidebar_nav_css_sig"] = sig


def apply_echarts_theme(
    option: Dict[str, Any],
    ui_theme: str | None = None,
    overrides: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Force Spotify-like chart styling (no Streamlit ECharts theme)."""
    opt: Dict[str, Any] = copy.deepcopy(option or {})
    theme = _merged_theme(overrides)
    sem = echarts_semantic_colors(theme)

    # Keep charts aligned with the app theme.
    opt.setdefault("backgroundColor", "transparent")
    opt.setdefault("textStyle", {})
    if isinstance(opt["textStyle"], dict):
        opt["textStyle"].setdefault("color", theme.get("echarts_text", theme["text"]))
    opt.setdefault(
        "color",
        [
            sem["baseline"],
            sem["forecast"],
            sem["actual"],
            sem["risk"],
            sem["neutral"],
            "#22D3EE",
            "#A78BFA",
            "#FB7185",
        ],
    )
    try:
        enter_ms = int(float(str(theme.get("echarts_anim_duration_enter_ms", "450")).strip() or "450"))
    except Exception:
        enter_ms = 450
    try:
        update_ms = int(float(str(theme.get("echarts_anim_duration_update_ms", "300")).strip() or "300"))
    except Exception:
        update_ms = 300
    opt.setdefault("animationDuration", enter_ms)
    opt.setdefault("animationDurationUpdate", update_ms)
    opt.setdefault("animationEasing", "cubicOut")
    opt.setdefault("animationEasingUpdate", "cubicOut")
    opt.setdefault("grid", {})
    if isinstance(opt["grid"], dict):
        opt["grid"].setdefault("containLabel", True)

    def _patch_axis(axis_obj: Dict[str, Any]) -> Dict[str, Any]:
        axis_obj.setdefault("axisLabel", {})
        if isinstance(axis_obj["axisLabel"], dict):
            axis_obj["axisLabel"].setdefault("color", theme.get("echarts_axis_text_dim", theme.get("echarts_axis_text", theme["muted"])))
        axis_obj.setdefault("axisLine", {})
        if isinstance(axis_obj["axisLine"], dict):
            axis_obj["axisLine"].setdefault("lineStyle", {})
            if isinstance(axis_obj["axisLine"].get("lineStyle"), dict):
                axis_obj["axisLine"]["lineStyle"].setdefault("color", theme.get("echarts_axis_line", theme["border"]))
        axis_obj.setdefault("splitLine", {})
        if isinstance(axis_obj["splitLine"], dict):
            axis_obj["splitLine"].setdefault("lineStyle", {})
            if isinstance(axis_obj["splitLine"].get("lineStyle"), dict):
                axis_obj["splitLine"]["lineStyle"].setdefault("color", theme.get("echarts_gridline_soft", theme["echarts_gridline"]))
        return axis_obj

    for key in ("xAxis", "yAxis"):
        ax = opt.get(key)
        if isinstance(ax, list):
            opt[key] = [_patch_axis(a if isinstance(a, dict) else {}) for a in ax]
        elif isinstance(ax, dict):
            opt[key] = _patch_axis(ax)

    legend = opt.setdefault("legend", {})
    if isinstance(legend, dict):
        legend.setdefault("textStyle", {})
        if isinstance(legend["textStyle"], dict):
            legend["textStyle"].setdefault("color", theme.get("echarts_text", theme["text"]))

    title = opt.get("title")
    if isinstance(title, dict):
        title.setdefault("textStyle", {})
        if isinstance(title["textStyle"], dict):
            title["textStyle"].setdefault("color", theme.get("echarts_text", theme["text"]))
        title.setdefault("subtextStyle", {})
        if isinstance(title.get("subtextStyle"), dict):
            title["subtextStyle"].setdefault("color", theme.get("echarts_axis_text_dim", theme.get("echarts_axis_text", theme["muted"])))

    tooltip = opt.setdefault("tooltip", {})
    if isinstance(tooltip, dict):
        tooltip.setdefault("backgroundColor", theme.get("echarts_tooltip_glass_bg", theme.get("echarts_tooltip_bg", theme.get("echarts_bg", theme["bg"]))))
        tooltip.setdefault("borderColor", theme.get("echarts_tooltip_glass_border", theme.get("echarts_tooltip_border", theme["border"])))
        tooltip.setdefault("borderWidth", 1)
        tooltip.setdefault(
            "extraCssText",
            (
                f"backdrop-filter: blur(6px); border-radius:10px; "
                f"box-shadow:{theme.get('echarts_tooltip_shadow', '0 10px 30px rgba(0,0,0,0.35)')};"
            ),
        )
        tooltip.setdefault("textStyle", {})
        if isinstance(tooltip["textStyle"], dict):
            tooltip["textStyle"].setdefault("color", theme.get("echarts_tooltip_text", theme.get("echarts_text", theme["text"])))

    try:
        symbol_border_width = int(float(str(theme.get("echarts_symbol_border_width", "0")).strip() or "0"))
    except Exception:
        symbol_border_width = 0
    try:
        glow_sm = int(float(str(theme.get("echarts_glow_strength_sm", "8")).strip() or "8"))
    except Exception:
        glow_sm = 8
    try:
        glow_md = int(float(str(theme.get("echarts_glow_strength_md", "12")).strip() or "12"))
    except Exception:
        glow_md = 12

    def _hex_to_rgba(color: str, alpha: float) -> str:
        c = str(color or "").strip()
        if c.startswith("#") and len(c) in (4, 7):
            if len(c) == 4:
                c = "#" + "".join([ch * 2 for ch in c[1:]])
            try:
                r = int(c[1:3], 16)
                g = int(c[3:5], 16)
                b = int(c[5:7], 16)
                return f"rgba({r},{g},{b},{alpha})"
            except Exception:
                return str(color)
        return str(color)

    def _vertical_gradient(base_color: Any) -> Any:
        if isinstance(base_color, dict):
            return base_color
        c = str(base_color or "").strip()
        if not c:
            return base_color
        if c.startswith("#") and len(c) in (4, 7):
            return {
                "type": "linear",
                "x": 0,
                "y": 0,
                "x2": 0,
                "y2": 1,
                "colorStops": [
                    {"offset": 0, "color": _hex_to_rgba(c, 0.96)},
                    {"offset": 1, "color": _hex_to_rgba(c, 0.62)},
                ],
                "global": False,
            }
        return base_color

    # Series labels / item styles (common cases)
    series = opt.get("series")
    if isinstance(series, list):
        for idx, s in enumerate(series):
            if not isinstance(s, dict):
                continue
            palette = opt.get("color") if isinstance(opt.get("color"), list) else []
            default_color = palette[idx % len(palette)] if palette else sem["baseline"]
            s.setdefault("label", {})
            if isinstance(s["label"], dict):
                s["label"].setdefault("color", theme.get("echarts_text", theme["text"]))
            item_style = s.setdefault("itemStyle", {})
            if isinstance(item_style, dict):
                if symbol_border_width <= 0:
                    item_style.setdefault("borderWidth", 0)
                else:
                    item_style.setdefault("borderWidth", symbol_border_width)
                    item_style.setdefault("borderColor", theme.get("echarts_axis_line", theme["border"]))
            series_type = str(s.get("type", "")).strip().lower()
            # Stronger, KPI-like chart polish with additive defaults only.
            if series_type == "line":
                line_style = s.setdefault("lineStyle", {})
                if isinstance(line_style, dict):
                    line_style.setdefault("width", 3)
                    line_style.setdefault("color", item_style.get("color", default_color))
                s.setdefault("showSymbol", False)
                s.setdefault("symbol", "circle")
                s.setdefault("symbolSize", 6)
                area_style = s.get("areaStyle")
                if isinstance(area_style, dict):
                    area_style.setdefault("opacity", 0.16)
                    area_style.setdefault(
                        "color",
                        {
                            "type": "linear",
                            "x": 0,
                            "y": 0,
                            "x2": 0,
                            "y2": 1,
                            "colorStops": [
                                {"offset": 0, "color": _hex_to_rgba(str(item_style.get("color", default_color)), 0.35)},
                                {"offset": 1, "color": _hex_to_rgba(str(item_style.get("color", default_color)), 0.02)},
                            ],
                            "global": False,
                        },
                    )
            elif series_type == "bar":
                if isinstance(item_style, dict):
                    base = item_style.get("color", default_color)
                    item_style["color"] = _vertical_gradient(base)
                    item_style.setdefault("borderColor", "rgba(255,255,255,0.18)")
                    item_style.setdefault("borderWidth", 1 if symbol_border_width <= 0 else symbol_border_width)
                    item_style.setdefault("borderRadius", 6)
                data_vals = s.get("data")
                if isinstance(data_vals, list):
                    for point in data_vals:
                        if not isinstance(point, dict):
                            continue
                        p_style = point.setdefault("itemStyle", {})
                        if not isinstance(p_style, dict):
                            continue
                        p_base = p_style.get("color", item_style.get("color", default_color))
                        p_style["color"] = _vertical_gradient(p_base)
                        p_style.setdefault("borderColor", "rgba(255,255,255,0.16)")
                        p_style.setdefault("borderWidth", 1 if symbol_border_width <= 0 else symbol_border_width)
                        p_style.setdefault("borderRadius", 6)
            elif series_type == "scatter":
                if isinstance(item_style, dict):
                    item_style.setdefault("color", default_color)
                    item_style.setdefault("borderColor", "rgba(255,255,255,0.25)")
                    item_style.setdefault("borderWidth", 1 if symbol_border_width <= 0 else symbol_border_width)
            elif series_type == "pie":
                if isinstance(item_style, dict):
                    item_style.setdefault("borderColor", "rgba(15,23,42,0.55)")
                    item_style.setdefault("borderWidth", 1)
                s.setdefault("avoidLabelOverlap", True)
            # Some chart types use nested label objects
            if isinstance(s.get("emphasis"), dict):
                emph_label = s["emphasis"].setdefault("label", {})
                if isinstance(emph_label, dict):
                    emph_label.setdefault("color", theme.get("echarts_text", theme["text"]))

    return opt


def apply_echarts_spotify_theme(
    option: Dict[str, Any],
    ui_theme: str | None = None,
    overrides: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    return apply_echarts_theme(option, ui_theme, overrides=overrides)
