from __future__ import annotations

import colorsys
from typing import Any, Dict, List, Optional

import pandas as pd


def _to_amount(df: pd.DataFrame) -> pd.Series:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or "AMOUNT" not in df.columns:
        return pd.Series([], dtype=float)
    return pd.to_numeric(df.get("AMOUNT"), errors="coerce").fillna(0.0).astype(float)


def _norm_text(val: Any) -> str:
    return str(val).strip().upper()


def _pick_nwf_bucket_col(df: pd.DataFrame) -> Optional[str]:
    candidates = ["NWF_SUBCOMPONENT_ROLLUP", "NWF_SUBCOMPONENT", "SUBCOMPONENT", "COST_SUBTYPE", "COST_TYPE"]
    for col in candidates:
        if col in df.columns:
            s = df[col].fillna("").astype(str).str.strip()
            if (s != "").any():
                return col
    return None


def _shade(color: str, factor: float) -> str:
    hex_color = color.lstrip("#")
    if len(hex_color) != 6:
        return color
    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    l = max(0.0, min(1.0, l * factor))
    nr, ng, nb = colorsys.hls_to_rgb(h, l, s)
    return f"#{int(nr*255):02x}{int(ng*255):02x}{int(nb*255):02x}"


def build_cost_anatomy_from_cost_lines(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return {
            "total": 0.0,
            "wf_total": 0.0,
            "nwf_total": 0.0,
            "wf_sod_total": 0.0,
            "wf_overhead_total": 0.0,
            "nwf_buckets": [],
            "wf_buckets": [{"name": "SoD", "value": 0.0}, {"name": "Overhead", "value": 0.0}],
        }

    amt = _to_amount(df)
    df = df.loc[amt > 0].copy()
    amt = amt.loc[df.index]
    if df.empty:
        return {
            "total": 0.0,
            "wf_total": 0.0,
            "nwf_total": 0.0,
            "wf_sod_total": 0.0,
            "wf_overhead_total": 0.0,
            "nwf_buckets": [],
            "wf_buckets": [{"name": "SoD", "value": 0.0}, {"name": "Overhead", "value": 0.0}],
        }

    # WF vs NWF detection
    if "COST_CATEGORY" in df.columns:
        cat = df["COST_CATEGORY"].fillna("").astype(str).str.upper()
        wf_mask = cat.str.contains("WORK") & ~cat.str.contains("NON")
        nwf_mask = cat.str.contains("NWF") | cat.str.contains("NON")
    elif "WF_NWF" in df.columns:
        val = df["WF_NWF"].fillna("").astype(str).str.upper().str.strip()
        wf_mask = val.eq("WF")
        nwf_mask = val.eq("NWF")
    elif "IS_WORKFORCE" in df.columns:
        val = pd.to_numeric(df["IS_WORKFORCE"], errors="coerce").fillna(0)
        wf_mask = val.eq(1)
        nwf_mask = val.eq(0)
    else:
        wf_mask = pd.Series(True, index=df.index)
        nwf_mask = pd.Series(False, index=df.index)

    df_wf = df.loc[wf_mask].copy()
    df_nwf = df.loc[nwf_mask].copy()

    # WF SoD vs Overhead
    if "WF_LAYER2" in df_wf.columns:
        layer = df_wf["WF_LAYER2"].fillna("").astype(str).str.upper().str.strip()
        sod_mask = layer.eq("SOD")
        overhead_mask = layer.eq("OVERHEAD")
        if "WORKFORCE_TYPE" in df_wf.columns:
            wf_type = df_wf["WORKFORCE_TYPE"].fillna("").astype(str).str.upper().str.strip()
            is_team = wf_type.eq("TEAM")
            sod_mask = sod_mask | is_team
            overhead_mask = overhead_mask & ~is_team
    elif "WORKFORCE_TYPE" in df_wf.columns:
        wf_type = df_wf["WORKFORCE_TYPE"].fillna("").astype(str).str.upper().str.strip()
        overhead_mask = wf_type.eq("PROGRAM")
        sod_mask = ~overhead_mask
    else:
        sod_mask = pd.Series(True, index=df_wf.index)
        overhead_mask = pd.Series(False, index=df_wf.index)

    wf_amt = amt.loc[df_wf.index] if not df_wf.empty else pd.Series([], dtype=float)
    wf_sod_total = float(wf_amt.loc[sod_mask].sum() or 0.0)
    wf_overhead_total = float(wf_amt.loc[overhead_mask].sum() or 0.0)

    # NWF buckets
    nwf_buckets: List[Dict[str, Any]] = []
    if not df_nwf.empty:
        bucket_col = _pick_nwf_bucket_col(df_nwf)
        src = df_nwf[bucket_col] if bucket_col else pd.Series("", index=df_nwf.index)
        src_u = src.fillna("").astype(str).str.upper()

        travel_kw = src_u.str.contains("TRAVEL|T&E|HOTEL|AIRFARE|FLIGHT|AIRLINE", regex=True)
        invoice_kw = src_u.str.contains("INVOICE", regex=True)
        azure_kw = src_u.str.contains("AZURE", regex=True)
        aws_kw = src_u.str.contains("AWS", regex=True)
        msp_kw = src_u.str.contains("MSP", regex=True)
        contractor_kw = src_u.str.contains("CONTRACTOR CS|CS CONTRACTOR|MSP VENDOR", regex=True)

        bucket = pd.Series("Other", index=df_nwf.index, dtype=str)
        bucket.loc[travel_kw] = "Travel"
        bucket.loc[bucket.eq("Other") & invoice_kw] = "Invoices"
        bucket.loc[bucket.eq("Other") & azure_kw] = "Azure"
        bucket.loc[bucket.eq("Other") & aws_kw] = "AWS"
        bucket.loc[bucket.eq("Other") & msp_kw] = "MSP"
        bucket.loc[bucket.eq("Other") & contractor_kw] = "Contractor CS"

        order = ["Invoices", "Azure", "AWS", "MSP", "Contractor CS", "Travel", "Other"]
        for name in order:
            val = float(amt.loc[df_nwf.index[bucket.eq(name)]].sum() or 0.0)
            if val > 0:
                nwf_buckets.append({"name": name, "value": val})

    total = float(amt.sum() or 0.0)
    wf_total = float(wf_amt.sum() or 0.0)
    nwf_total = float(amt.loc[df_nwf.index].sum() or 0.0) if not df_nwf.empty else 0.0

    return {
        "total": total,
        "wf_total": wf_total,
        "nwf_total": nwf_total,
        "wf_sod_total": wf_sod_total,
        "wf_overhead_total": wf_overhead_total,
        "nwf_buckets": nwf_buckets,
        "wf_buckets": [{"name": "SoD", "value": wf_sod_total}, {"name": "Overhead", "value": wf_overhead_total}],
    }


def build_cost_anatomy_donut_option(anatomy: Dict[str, Any], *, title: str, theme: Dict[str, Any]) -> Dict[str, Any]:
    text_col = theme.get("echarts_text", theme.get("text", "#f5f5f5"))
    axis_col = theme.get("echarts_axis_text", theme.get("muted", "#b3b3b3"))
    tooltip_bg = theme.get("echarts_tooltip_bg", "#3B3B3B")
    tooltip_border = theme.get("echarts_tooltip_border", "#2a2a2a")
    bg = theme.get("echarts_bg", theme.get("background", "#333333"))

    wf_color = theme.get("wf_color", "#5B7BE3")
    nwf_color = theme.get("nwf_color", "#F4C358")

    total = float(anatomy.get("total") or 0.0)
    wf_total = float(anatomy.get("wf_total") or 0.0)
    nwf_total = float(anatomy.get("nwf_total") or 0.0)

    wf_buckets = anatomy.get("wf_buckets") or []
    nwf_buckets = anatomy.get("nwf_buckets") or []

    outer_data: List[Dict[str, Any]] = []
    for idx, b in enumerate(wf_buckets):
        name = str(b.get("name"))
        val = float(b.get("value") or 0.0)
        outer_data.append(
            {
                "name": name,
                "value": val,
                "fullName": f"WF / {name}",
                "itemStyle": {"color": _shade(wf_color, 0.9 - idx * 0.12)},
            }
        )
    for idx, b in enumerate(nwf_buckets):
        name = str(b.get("name"))
        val = float(b.get("value") or 0.0)
        outer_data.append(
            {
                "name": name,
                "value": val,
                "fullName": f"NWF / {name}",
                "itemStyle": {"color": _shade(nwf_color, 0.92 - idx * 0.08)},
            }
        )

    tooltip_total = total if total > 0 else 1.0
    tooltip_fn = (
        "function(p){"
        f" var total={tooltip_total};"
        " var v=(typeof p.value==='number') ? p.value : (p.value||0);"
        " var name=(p.data && p.data.fullName) ? p.data.fullName : String(p.name||'');"
        " var pct= total>0 ? (v/total*100.0) : 0;"
        " return name"
        "  +'<br/>$'+Number(v||0).toLocaleString(undefined,{maximumFractionDigits:0})"
        "  +' ('+pct.toFixed(1)+'%)';"
        "}"
    )

    label_abbrev = (
        "function(p){"
        " var name=String(p.name||'');"
        " var map={"
        "  'Contractor CS':'Contr CS',"
        "  'Contractor C':'Contr C',"
        "  'Team Overhead':'Team OH',"
        "  'Program Overhead':'Prog OH'"
        " };"
        " return map[name] || name;"
        "}"
    )

    return {
        "backgroundColor": bg,
        "title": {"text": title, "left": "left", "textStyle": {"color": text_col}},
        "tooltip": {
            "trigger": "item",
            "backgroundColor": tooltip_bg,
            "borderColor": tooltip_border,
            "textStyle": {"color": text_col},
            "formatter": label_abbrev if not tooltip_fn else tooltip_fn,
        },
        "series": [
            {
                "type": "pie",
                "radius": ["28%", "46%"],
                "center": ["50%", "52%"],
                "padAngle": 2,
                "label": {
                    "show": True,
                    "position": "inside",
                    "rotate": 0,
                    "fontSize": 12,
                    "fontWeight": 600,
                    "color": text_col,
                },
                "labelLine": {"show": False},
                "data": [
                    {"name": "WF", "value": wf_total, "fullName": "WF", "itemStyle": {"color": wf_color}},
                    {"name": "NWF", "value": nwf_total, "fullName": "NWF", "itemStyle": {"color": nwf_color}},
                ],
            },
            {
                "type": "pie",
                "radius": ["52%", "82%"],
                "center": ["50%", "52%"],
                "padAngle": 6,
                "minAngle": 6,
                "labelLayout": {"hideOverlap": True},
                "itemStyle": {"borderWidth": 3, "borderColor": bg},
                "label": {
                    "show": True,
                    "position": "inside",
                    "rotate": 0,
                    "overflow": "truncate",
                    "color": text_col,
                    "formatter": label_abbrev,
                },
                "labelLine": {"show": False},
                "data": outer_data,
            },
        ],
    }
