# TCO Streamlit App

## Run locally
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Provide Snowflake creds via .env
cp .env.example .env  # fill values

streamlit run app.py
