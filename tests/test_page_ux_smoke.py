from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_master_pages_use_scope_banner_helper():
    for rel in ["pages/3_Programs.py", "pages/4_Teams.py", "pages/6_Applications.py"]:
        txt = _read(rel)
        assert "render_scope_banner" in txt


def test_major_pages_have_intro_contract_primitives():
    pages = [
        "pages/0_Welcome.py",
        "pages/1_Dashboard.py",
        "pages/1_Insights.py",
        "pages/2_Roadmap.py",
        "pages/2_Invoices.py",
        "pages/2_Contracts.py",
        "pages/Budget.py",
        "pages/10_Settings.py",
        "pages/11_Data_Quality.py",
        "pages/99_Admin.py",
        "pages/9_How_To.py",
    ]
    for rel in pages:
        txt = _read(rel)
        assert ("st.title(" in txt) or ("render_page_header(" in txt)
        assert "st.caption(" in txt


def test_insights_contains_program_maturity_tabs():
    txt = _read("pages/1_Insights.py")
    assert "Program Maturity (Strategic Positioning)" in txt
    assert "NEXT Governance Readiness (Operational Control)" in txt
    assert "Risk Signals" in txt
    assert "Cost vs Value" in txt
    assert "NWF Actuals" in txt


def test_insights_program_maturity_has_explainability_copy():
    txt = _read("pages/1_Insights.py")
    assert "Program Maturity reflects how strategically healthy the program's investment mix" in txt
    assert "This score measures how well the program is structured and controlled inside the NEXT cost governance model" in txt
    assert "How the score is calculated" in txt
    assert "What is Program Maturity?" in txt
    assert "How it's calculated" in txt
    assert "Grow/Scale/Watch/Stop" in txt
    assert "Click a bubble to drill down" in txt
    assert "Invest = high ROI, low demand." in txt
    assert "Optimize = high ROI, high demand." in txt
    assert "Monitor = low ROI, low demand." in txt
    assert "Reassess = low ROI, high demand." in txt
    assert "Top Mature" in txt
    assert "Needs Attention" in txt
