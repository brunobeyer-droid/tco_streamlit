import os, json
from pathlib import Path
import streamlit as st

st.title("Secrets Debug")

# Show where Streamlit is running from
st.write("**cwd**:", os.getcwd())
st.write("**Here files**:", os.listdir("."))

# Where Streamlit looks for secrets
proj = Path(os.getcwd())
proj_secret = proj/".streamlit"/"secrets.toml"
home_secret = Path.home()/".streamlit"/"secrets.toml"

st.write("**Expected secrets paths:**")
st.write(str(proj_secret))
st.write(str(home_secret))

st.write("**Exists?**",
         {"project_secrets": proj_secret.exists(),
          "home_secrets": home_secret.exists()})

st.subheader("st.secrets snapshots")
try:
    cfg_mssql = st.secrets.get("mssql", {})
    st.success("✅ MSSQL secrets loaded!")
    st.json(dict(cfg_mssql))
except Exception as e:
    st.error(f"❌ mssql not available: {e}")

try:
    cfg_auth = st.secrets.get("auth", {})
    st.success("✅ AUTH secrets loaded!")
    st.json(dict(cfg_auth))
except Exception as e:
    st.error(f"❌ auth not available: {e}")

try:
    cfg_ado = st.secrets.get("ado", {})
    st.info("ℹ️ ADO defaults (optional)")
    st.json(dict(cfg_ado))
except Exception:
    pass

# Also try reading the file directly so we know if it's a parsing/path issue
def read_text(p: Path):
    try:
        return p.read_text()
    except Exception as e:
        return f"<error reading: {e}>"

st.subheader("Raw secrets.toml (project)")
st.code(read_text(proj_secret))
st.subheader("Raw secrets.toml (home)")
st.code(read_text(home_secret))
