import pandas as pd
from db import fetch_df

def main():
    q = (
        "SELECT TOP 20 CONTRACT_ID, APPLICATIONID, TEAMID, START_FY, END_FY, RENEWAL_MONTH, "
        "ANNUAL_AMOUNT, ESCALATION_PCT, STATUS, AGREEMENT_NUMBER, COMPANY_CODE, COST_CENTER, SERVICE_TYPE, "
        "CONTRACT_RENEWAL_DATE, INVOICE_RENEWAL_DATE, TOTAL_CONTRACT_COST, CREATED_AT "
        "FROM CONTRACTS ORDER BY CREATED_AT DESC"
    )
    try:
        df = fetch_df(q)
        if df is None or df.empty:
            print("No contracts found.")
        else:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"ERR {e}")

if __name__ == "__main__":
    main()

