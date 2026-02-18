from __future__ import annotations

from typing import Sequence

import pandas as pd


SHOW_SCOPE_OPTIONS = ("Programs", "Teams", "Applications", "Initiatives")


def _text_col(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    return df.get(col, default).fillna(default).astype(str).str.strip()


def _num_col(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df.get(col), errors="coerce").fillna(0.0)


def _single_or_multiple(values: pd.Series, fallback: str = "Unassigned") -> str:
    uniq = [str(v).strip() for v in values.dropna().astype(str).tolist() if str(v).strip()]
    uniq = sorted(set(uniq))
    if not uniq:
        return fallback
    if len(uniq) == 1:
        return uniq[0]
    return "(Multiple)"


def resolve_show_default(
    eff_programs: Sequence[str] | None,
    eff_teams: Sequence[str] | None,
    eff_groups: Sequence[str] | None,
) -> str:
    programs = [str(v).strip() for v in (eff_programs or []) if str(v).strip()]
    teams = [str(v).strip() for v in (eff_teams or []) if str(v).strip()]
    groups = [str(v).strip() for v in (eff_groups or []) if str(v).strip()]
    if groups:
        return "Applications"
    if teams:
        return "Applications"
    if programs:
        return "Teams"
    return "Programs"


def _rollup_entity(df: pd.DataFrame, key_col: str, *, show_scope: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "ENTITY_KEY",
                "ENTITY_LABEL",
                "PROGRAMNAME",
                "TEAMNAME",
                "DEMAND_AXIS",
                "ACTUAL_COST",
                "BENEFITS_SELECTED",
                "Y_VALUE_PLOT",
                "POINT_COUNT",
                "IS_BASE",
                "SHOW_SCOPE",
            ]
        )
    w = df.copy()
    w["ENTITY_LABEL"] = _text_col(w, key_col)
    w = w[w["ENTITY_LABEL"].ne("")].copy()
    if w.empty:
        return pd.DataFrame(
            columns=[
                "ENTITY_KEY",
                "ENTITY_LABEL",
                "PROGRAMNAME",
                "TEAMNAME",
                "DEMAND_AXIS",
                "ACTUAL_COST",
                "BENEFITS_SELECTED",
                "Y_VALUE_PLOT",
                "POINT_COUNT",
                "IS_BASE",
                "SHOW_SCOPE",
            ]
        )
    w["PROGRAMNAME"] = _text_col(w, "PROGRAMNAME")
    w["TEAMNAME"] = _text_col(w, "TEAMNAME")
    w["GROUPNAME"] = _text_col(w, "GROUPNAME")
    w["DEMAND_AXIS"] = _num_col(w, "DEMAND_AXIS")
    w["ACTUAL_COST"] = _num_col(w, "ACTUAL_COST")
    w["BENEFITS_SELECTED"] = _num_col(w, "BENEFITS_SELECTED")
    w["IS_BASE"] = pd.to_numeric(w.get("IS_BASE"), errors="coerce").fillna(0).astype(int)

    rows: list[dict] = []
    for label, g in w.groupby("ENTITY_LABEL", dropna=False):
        total_cost = float(g["ACTUAL_COST"].sum())
        total_benefits = float(g["BENEFITS_SELECTED"].sum())
        y_val = (total_benefits / total_cost) if total_cost > 0 else 0.0
        rows.append(
            {
                "ENTITY_KEY": str(label),
                "ENTITY_LABEL": str(label),
                "PROGRAMNAME": _single_or_multiple(g["PROGRAMNAME"]),
                "TEAMNAME": _single_or_multiple(g["TEAMNAME"]),
                "DEMAND_AXIS": float(g["DEMAND_AXIS"].sum()),
                "ACTUAL_COST": total_cost,
                "BENEFITS_SELECTED": total_benefits,
                "Y_VALUE_PLOT": y_val,
                "POINT_COUNT": int(g["GROUPNAME"].nunique()) if "GROUPNAME" in g.columns else int(len(g.index)),
                "IS_BASE": int(g["IS_BASE"].max()) if "IS_BASE" in g.columns else 0,
                "SHOW_SCOPE": show_scope,
            }
        )
    out = pd.DataFrame(rows)
    out = out[(out["DEMAND_AXIS"] > 0) & (out["Y_VALUE_PLOT"] > 0) & (out["ACTUAL_COST"] > 0)].copy()
    return out.reset_index(drop=True)


def rollup_scope_plot_df(app_plot_df: pd.DataFrame, show_scope: str) -> pd.DataFrame:
    scope = str(show_scope or "").strip()
    if scope not in set(SHOW_SCOPE_OPTIONS):
        scope = "Applications"
    if app_plot_df is None or app_plot_df.empty:
        return _rollup_entity(pd.DataFrame(), "GROUPNAME", show_scope=scope)

    if scope == "Applications":
        w = app_plot_df.copy()
        w["ENTITY_KEY"] = _text_col(w, "GROUPNAME")
        w["ENTITY_LABEL"] = w["ENTITY_KEY"]
        w["PROGRAMNAME"] = _text_col(w, "PROGRAMNAME")
        w["TEAMNAME"] = _text_col(w, "TEAMNAME")
        w["DEMAND_AXIS"] = _num_col(w, "DEMAND_AXIS")
        w["ACTUAL_COST"] = _num_col(w, "ACTUAL_COST")
        w["BENEFITS_SELECTED"] = _num_col(w, "BENEFITS_SELECTED")
        w["Y_VALUE_PLOT"] = pd.to_numeric(w.get("Y_VALUE_PLOT"), errors="coerce")
        if w["Y_VALUE_PLOT"].isna().any():
            w["Y_VALUE_PLOT"] = w["BENEFITS_SELECTED"] / w["ACTUAL_COST"].replace(0, pd.NA)
        w["Y_VALUE_PLOT"] = pd.to_numeric(w["Y_VALUE_PLOT"], errors="coerce").fillna(0.0)
        w["IS_BASE"] = pd.to_numeric(w.get("IS_BASE"), errors="coerce").fillna(0).astype(int)
        w["POINT_COUNT"] = pd.to_numeric(w.get("FEATURE_COUNT"), errors="coerce").fillna(1).astype(int)
        w["SHOW_SCOPE"] = scope
        keep = [
            "ENTITY_KEY",
            "ENTITY_LABEL",
            "PROGRAMNAME",
            "TEAMNAME",
            "DEMAND_AXIS",
            "ACTUAL_COST",
            "BENEFITS_SELECTED",
            "Y_VALUE_PLOT",
            "POINT_COUNT",
            "IS_BASE",
            "SHOW_SCOPE",
        ]
        out = w[keep].copy()
        out = out[(out["DEMAND_AXIS"] > 0) & (out["Y_VALUE_PLOT"] > 0) & (out["ACTUAL_COST"] > 0)].copy()
        return out.reset_index(drop=True)
    if scope == "Programs":
        return _rollup_entity(app_plot_df, "PROGRAMNAME", show_scope=scope)
    if scope == "Teams":
        return _rollup_entity(app_plot_df, "TEAMNAME", show_scope=scope)
    return pd.DataFrame(
        columns=[
            "ENTITY_KEY",
            "ENTITY_LABEL",
            "PROGRAMNAME",
            "TEAMNAME",
            "DEMAND_AXIS",
            "ACTUAL_COST",
            "BENEFITS_SELECTED",
            "Y_VALUE_PLOT",
            "POINT_COUNT",
            "IS_BASE",
            "SHOW_SCOPE",
        ]
    )


def build_initiative_feature_detail_df(
    ado_curr: pd.DataFrame,
    cost_curr: pd.DataFrame,
    df_explorer: pd.DataFrame,
) -> pd.DataFrame:
    cols = [
        "ENTITY_KEY",
        "ENTITY_LABEL",
        "FEATURE_ID",
        "FEATURE_TITLE",
        "EPIC_ID",
        "EPIC_TITLE",
        "PROGRAMNAME",
        "TEAMNAME",
        "GROUPNAME",
        "DEMAND_FTE",
        "BENEFIT_SELECTED",
        "ALLOCATED_COST",
        "ROI_PROXY",
        "IS_BASE",
    ]
    if ado_curr is None or ado_curr.empty:
        return pd.DataFrame(columns=cols)
    if cost_curr is None or cost_curr.empty:
        return pd.DataFrame(columns=cols)
    if df_explorer is None or df_explorer.empty:
        return pd.DataFrame(columns=cols)

    ado = ado_curr.copy()
    exp = df_explorer.copy()
    cost = cost_curr.copy()

    ado["FEATURE_ID"] = pd.to_numeric(ado.get("FEATURE_ID"), errors="coerce").astype("Int64")
    exp["FEATURE_ID"] = pd.to_numeric(exp.get("FEATURE_ID"), errors="coerce").astype("Int64")
    ado = ado[ado["FEATURE_ID"].notna()].copy()
    exp = exp[exp["FEATURE_ID"].notna()].copy()
    if ado.empty and exp.empty:
        return pd.DataFrame(columns=cols)

    for col in ["GROUPNAME", "PROGRAMNAME", "TEAMNAME", "TITLE", "EPIC_TITLE", "EPIC_ID"]:
        ado[col] = _text_col(ado, col)
    for col in ["GROUPNAME", "PROGRAMNAME", "TEAMNAME", "FEATURE_TITLE", "EPIC_TITLE", "EPIC_ID"]:
        exp[col] = _text_col(exp, col)
    ado["BENEFIT_SELECTED"] = _num_col(ado, "BENEFIT_SELECTED")
    ado["IS_BASE"] = pd.to_numeric(ado.get("IS_BASE"), errors="coerce").fillna(0).astype(int)
    exp["DERIVED_FTE"] = _num_col(exp, "DERIVED_FTE")

    ado_feat = (
        ado.groupby("FEATURE_ID", dropna=False)
        .agg(
            GROUPNAME=("GROUPNAME", lambda s: _single_or_multiple(s, "")),
            PROGRAMNAME=("PROGRAMNAME", lambda s: _single_or_multiple(s, "")),
            TEAMNAME=("TEAMNAME", lambda s: _single_or_multiple(s, "")),
            FEATURE_TITLE=("TITLE", lambda s: _single_or_multiple(s, "")),
            EPIC_ID=("EPIC_ID", lambda s: _single_or_multiple(s, "")),
            EPIC_TITLE=("EPIC_TITLE", lambda s: _single_or_multiple(s, "")),
            BENEFIT_SELECTED=("BENEFIT_SELECTED", "sum"),
            IS_BASE=("IS_BASE", "max"),
        )
        .reset_index()
    )
    exp_feat = (
        exp.groupby("FEATURE_ID", dropna=False)
        .agg(
            GROUPNAME=("GROUPNAME", lambda s: _single_or_multiple(s, "")),
            PROGRAMNAME=("PROGRAMNAME", lambda s: _single_or_multiple(s, "")),
            TEAMNAME=("TEAMNAME", lambda s: _single_or_multiple(s, "")),
            FEATURE_TITLE=("FEATURE_TITLE", lambda s: _single_or_multiple(s, "")),
            EPIC_ID=("EPIC_ID", lambda s: _single_or_multiple(s, "")),
            EPIC_TITLE=("EPIC_TITLE", lambda s: _single_or_multiple(s, "")),
            DEMAND_FTE=("DERIVED_FTE", "sum"),
        )
        .reset_index()
    )
    feat = exp_feat.merge(ado_feat, on="FEATURE_ID", how="outer", suffixes=("_EXP", "_ADO"))
    feat["GROUPNAME"] = _text_col(feat, "GROUPNAME_ADO").where(_text_col(feat, "GROUPNAME_ADO").ne(""), _text_col(feat, "GROUPNAME_EXP"))
    feat["PROGRAMNAME"] = _text_col(feat, "PROGRAMNAME_ADO").where(_text_col(feat, "PROGRAMNAME_ADO").ne(""), _text_col(feat, "PROGRAMNAME_EXP"))
    feat["TEAMNAME"] = _text_col(feat, "TEAMNAME_ADO").where(_text_col(feat, "TEAMNAME_ADO").ne(""), _text_col(feat, "TEAMNAME_EXP"))
    feat["FEATURE_TITLE"] = _text_col(feat, "FEATURE_TITLE_ADO").where(
        _text_col(feat, "FEATURE_TITLE_ADO").ne(""), _text_col(feat, "FEATURE_TITLE_EXP")
    )
    feat["EPIC_ID"] = _text_col(feat, "EPIC_ID_ADO").where(_text_col(feat, "EPIC_ID_ADO").ne(""), _text_col(feat, "EPIC_ID_EXP"))
    feat["EPIC_TITLE"] = _text_col(feat, "EPIC_TITLE_ADO").where(
        _text_col(feat, "EPIC_TITLE_ADO").ne(""), _text_col(feat, "EPIC_TITLE_EXP")
    )
    feat["DEMAND_FTE"] = _num_col(feat, "DEMAND_FTE")
    feat["BENEFIT_SELECTED"] = _num_col(feat, "BENEFIT_SELECTED")
    feat["IS_BASE"] = pd.to_numeric(feat.get("IS_BASE"), errors="coerce").fillna(0).astype(int)
    feat = feat[feat["GROUPNAME"].ne("")].copy()
    if feat.empty:
        return pd.DataFrame(columns=cols)

    cost["GROUPNAME"] = _text_col(cost, "GROUPNAME")
    cost_amount_col = "AMOUNT" if "AMOUNT" in cost.columns else "ACTUAL_COST"
    cost[cost_amount_col] = _num_col(cost, cost_amount_col)
    cost_by_group = cost.groupby("GROUPNAME", dropna=False)[cost_amount_col].sum().reset_index().rename(columns={cost_amount_col: "GROUP_COST"})
    feat = feat.merge(cost_by_group, on="GROUPNAME", how="left")
    feat["GROUP_COST"] = _num_col(feat, "GROUP_COST")
    feat["GROUP_TOTAL_DEMAND"] = feat.groupby("GROUPNAME", dropna=False)["DEMAND_FTE"].transform("sum")
    feat["GROUP_FEATURE_COUNT"] = feat.groupby("GROUPNAME", dropna=False)["FEATURE_ID"].transform("count")

    feat["COST_WEIGHT"] = 0.0
    demand_mask = feat["GROUP_TOTAL_DEMAND"] > 0
    feat.loc[demand_mask, "COST_WEIGHT"] = (
        feat.loc[demand_mask, "DEMAND_FTE"] / feat.loc[demand_mask, "GROUP_TOTAL_DEMAND"]
    )
    fallback_mask = (~demand_mask) & (feat["GROUP_FEATURE_COUNT"] > 0)
    feat.loc[fallback_mask, "COST_WEIGHT"] = 1.0 / feat.loc[fallback_mask, "GROUP_FEATURE_COUNT"]
    feat["ALLOCATED_COST"] = feat["GROUP_COST"] * feat["COST_WEIGHT"]

    feat_id_text = feat["FEATURE_ID"].astype("Int64").astype(str).replace({"<NA>": ""})
    epic_id = _text_col(feat, "EPIC_ID")
    epic_title = _text_col(feat, "EPIC_TITLE")
    feature_title = _text_col(feat, "FEATURE_TITLE")
    feat["ENTITY_KEY"] = epic_id.apply(lambda s: f"EPIC::{s}" if str(s).strip() else "")
    no_epic = feat["ENTITY_KEY"].eq("")
    feat.loc[no_epic, "ENTITY_KEY"] = feat_id_text[no_epic].apply(lambda s: f"FEATURE::{s}" if str(s).strip() else "")
    feat["ENTITY_LABEL"] = epic_title.where(epic_title.ne(""), epic_id.apply(lambda s: f"Epic {s}" if str(s).strip() else ""))
    no_label = feat["ENTITY_LABEL"].eq("")
    feat.loc[no_label, "ENTITY_LABEL"] = feature_title[no_label].where(
        feature_title[no_label].ne(""), feat_id_text[no_label].apply(lambda s: f"Feature {s}" if str(s).strip() else "(Unassigned)")
    )
    feat["ROI_PROXY"] = feat["BENEFIT_SELECTED"] / feat["ALLOCATED_COST"].replace(0, pd.NA)
    feat["ROI_PROXY"] = pd.to_numeric(feat["ROI_PROXY"], errors="coerce").fillna(0.0)
    feat["FEATURE_ID"] = feat["FEATURE_ID"].astype("Int64")

    out = feat[
        [
            "ENTITY_KEY",
            "ENTITY_LABEL",
            "FEATURE_ID",
            "FEATURE_TITLE",
            "EPIC_ID",
            "EPIC_TITLE",
            "PROGRAMNAME",
            "TEAMNAME",
            "GROUPNAME",
            "DEMAND_FTE",
            "BENEFIT_SELECTED",
            "ALLOCATED_COST",
            "ROI_PROXY",
            "IS_BASE",
        ]
    ].copy()
    return out.reset_index(drop=True)


def build_initiative_plot_df(
    ado_curr: pd.DataFrame,
    cost_curr: pd.DataFrame,
    df_explorer: pd.DataFrame,
) -> pd.DataFrame:
    cols = [
        "ENTITY_KEY",
        "ENTITY_LABEL",
        "PROGRAMNAME",
        "TEAMNAME",
        "DEMAND_AXIS",
        "ACTUAL_COST",
        "BENEFITS_SELECTED",
        "Y_VALUE_PLOT",
        "POINT_COUNT",
        "IS_BASE",
        "SHOW_SCOPE",
    ]
    detail = build_initiative_feature_detail_df(ado_curr, cost_curr, df_explorer)
    if detail.empty:
        return pd.DataFrame(columns=cols)

    rows: list[dict] = []
    for (entity_key, entity_label), g in detail.groupby(["ENTITY_KEY", "ENTITY_LABEL"], dropna=False):
        total_cost = float(pd.to_numeric(g.get("ALLOCATED_COST"), errors="coerce").fillna(0.0).sum())
        total_benefits = float(pd.to_numeric(g.get("BENEFIT_SELECTED"), errors="coerce").fillna(0.0).sum())
        y_val = (total_benefits / total_cost) if total_cost > 0 else 0.0
        rows.append(
            {
                "ENTITY_KEY": str(entity_key),
                "ENTITY_LABEL": str(entity_label),
                "PROGRAMNAME": _single_or_multiple(_text_col(g, "PROGRAMNAME")),
                "TEAMNAME": _single_or_multiple(_text_col(g, "TEAMNAME")),
                "DEMAND_AXIS": float(pd.to_numeric(g.get("DEMAND_FTE"), errors="coerce").fillna(0.0).sum()),
                "ACTUAL_COST": total_cost,
                "BENEFITS_SELECTED": total_benefits,
                "Y_VALUE_PLOT": y_val,
                "POINT_COUNT": int(pd.to_numeric(g.get("FEATURE_ID"), errors="coerce").dropna().nunique()),
                "IS_BASE": int(pd.to_numeric(g.get("IS_BASE"), errors="coerce").fillna(0).max()),
                "SHOW_SCOPE": "Initiatives",
            }
        )
    out = pd.DataFrame(rows, columns=cols)
    out = out[(out["DEMAND_AXIS"] > 0) & (out["Y_VALUE_PLOT"] > 0) & (out["ACTUAL_COST"] > 0)].copy()
    return out.reset_index(drop=True)
