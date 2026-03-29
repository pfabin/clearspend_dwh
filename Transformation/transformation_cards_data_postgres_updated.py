import psycopg
import pandas as pd
import re
from io import StringIO

def main():
    table_name = "cards_data"

    conn = psycopg.connect(
        host="localhost",
        port=5432,
        dbname="db_clearspend",
        user="postgres",
        password="password"
    )

    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
    cur.execute(f"DROP TABLE IF EXISTS transformation.{table_name};")

    cur.execute(f"""
        CREATE TABLE transformation.{table_name} (
            id INT,
            card_brand VARCHAR(20),
            card_type VARCHAR(20),
            expiry_date DATE,
            has_chip VARCHAR(10),
            credit_limit DECIMAL(10,2),
            acct_open_date DATE,
            year_pin_last_changed INT,
            card_on_dark_web VARCHAR(10),
            issuer_bank_name VARCHAR(20),
            issuer_bank_state VARCHAR(10),
            issuer_bank_type VARCHAR(20),
            issuer_risk_rating VARCHAR(20)
        );
    """)

    df = pd.read_sql_query(f"SELECT * FROM ingestion.{table_name}", conn)

    # ------------------------------
    # Card brand cleaning
    # ------------------------------
    df["card_brand"] = df["card_brand"].apply(
        lambda value: (
            "NA" if pd.isna(value) else
            "Visa" if "visa" in str(value).lower() else
            "Mastercard" if "master" in str(value).lower() else
            "Amex" if "amex" in str(value).lower() else
            "Discover" if "discover" in str(value).lower() else
            "NA"
        )
    )

    # ------------------------------
    # Card number truncate logic
    # ------------------------------
    df["card_number"] = df["card_number"].astype("string").str.strip()
    df["card_number"] = df["card_number"].apply(
        lambda x: str(x).split(".", 1)[0] if pd.notna(x) else pd.NA
    )

    # ------------------------------
    # Card number validation
    # ------------------------------
    df = df[
        df.apply(
            lambda row: (
                False if pd.isna(row["card_number"]) or not str(row["card_number"]).isdigit()
                else len(str(row["card_number"])) in {13, 16, 19} if row["card_brand"] == "Visa"
                else len(str(row["card_number"])) == 16 if row["card_brand"] in {"Mastercard", "Discover"}
                else len(str(row["card_number"])) == 15 if row["card_brand"] == "Amex"
                else len(str(row["card_number"])) in {15, 16}
            ),
            axis=1
        )
    ].copy()

    # ------------------------------
    # Credit limit cleaning
    # ------------------------------
    def clean_credit_limit(value):
        if pd.isna(value):
            return pd.NA
        raw = str(value).strip().lower()
        raw = raw.replace("$", "").replace(",", "")
        try:
            return float(raw)
        except:
            return pd.NA

    df["credit_limit"] = df["credit_limit"].apply(clean_credit_limit)
    df["credit_limit"] = df["credit_limit"].abs()

    # ------------------------------
    # Final columns
    # ------------------------------
    final_columns = [
        "id",
        "card_brand",
        "card_type",
        "expiry_date",
        "has_chip",
        "credit_limit",
        "acct_open_date",
        "year_pin_last_changed",
        "card_on_dark_web",
        "issuer_bank_name",
        "issuer_bank_state",
        "issuer_bank_type",
        "issuer_risk_rating"
    ]

    df = df[final_columns].copy()

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    with cur.copy(f"""
        COPY transformation.{table_name} (
            id,
            card_brand,
            card_type,
            expiry_date,
            has_chip,
            credit_limit,
            acct_open_date,
            year_pin_last_changed,
            card_on_dark_web,
            issuer_bank_name,
            issuer_bank_state,
            issuer_bank_type,
            issuer_risk_rating)
        FROM STDIN WITH (FORMAT CSV)
    """) as copy:
        copy.write(buffer.getvalue())

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
