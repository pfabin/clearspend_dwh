import psycopg
import pandas as pd
import re
from io import StringIO

def main():
    table_name = "cards_data"

    # ==============================
    # Connect
    # ==============================
    conn = psycopg.connect(
        host="localhost",
        port=5432,
        dbname="db_clearspend",
        user="postgres",
        password="password"
    )

    cur = conn.cursor()

    # ==============================
    # Setup schema + table
    # ==============================
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

    print(f"✅ Table transformation.{table_name} created")

    # ==============================
    # Read from ingestion (only code and description)
    # ==============================
    df = pd.read_sql_query(f"SELECT * FROM ingestion.{table_name}", conn)
    print(f"🔄 Read {len(df)} rows from ingestion.{table_name}")

    # ==============================
    # CLEANING
    # ==============================

    # 1. card_brand
    df["card_brand"] = df["card_brand"].apply(
        lambda value: (
            "NA" if pd.isna(value) or re.sub(r"[^a-z]", "", str(value).lower().strip()) in {"", "nan", "none", "null", "unknown"}
            else "Visa" if ("visa" in re.sub(r"[^a-z]", "", str(value).lower().strip()) or re.sub(r"[^a-z]", "", str(value).lower().strip()) in {"v", "vis", "vsa"})
            else "Mastercard" if "master" in re.sub(r"[^a-z]", "", str(value).lower().strip())
            else "Amex" if ("amex" in re.sub(r"[^a-z]", "", str(value).lower().strip()) or re.sub(r"[^a-z]", "", str(value).lower().strip()) in {"ame", "amx"})
            else "Discover" if ("discover" in re.sub(r"[^a-z]", "", str(value).lower().strip()) or "disc" in re.sub(r"[^a-z]", "", str(value).lower().strip()))
            else "NA"
        )
    )
    ### standardising all anomalous combinations of card brands using regex

    # 2. card_type
    df["card_type"] = df["card_type"].apply(
        lambda value: (
            "NA" if pd.isna(value) or re.sub(r"[^a-z]", "", str(value).lower().strip()) in {"", "nan", "none", "null", "unknown"}
            else "Prepaid Debit" if ("prepaid" in re.sub(r"[^a-z]", "", str(value).lower().strip()) or re.sub(r"[^a-z]", "", str(value).lower().strip()) in {"dp", "dpp", "ppd", "dbpp"})
            else "Credit" if ("credit" in re.sub(r"[^a-z]", "", str(value).lower().strip()) or re.sub(r"[^a-z]", "", str(value).lower().strip()) in {"cr", "cc", "cred", "crdeit"})
            else "Debit" if ("debit" in re.sub(r"[^a-z]", "", str(value).lower().strip()) or re.sub(r"[^a-z]", "", str(value).lower().strip()) in {"deb", "db", "d"})
            else "NA"
        )
    )
    ### standardising all anomalous combinations of card types

    # 3. issuer_bank_name
    bank_map = {
        # Bank of America
        "bank of america": "Bank of America",
        "bk of america": "Bank of America",

        # Discover
        "discover bank": "Discover Bank",
        "discover bk": "Discover Bank",

        # Ally
        "ally bank": "Ally Bank",
        "ally bk": "Ally Bank",

        # Wells Fargo
        "wells fargo": "Wells Fargo",

        # Chase
        "chase bank": "Chase Bank",
        "chase bk": "Chase Bank",

        # JPMorgan
        "jpmorgan chase": "JPMorgan Chase",
        "jp morgan chase": "JPMorgan Chase",

        # US Bank
        "u.s. bank": "U.S. Bank",
        "u.s. bk": "U.S. Bank",

        # Truist
        "truist": "Truist",

        # PNC
        "pnc bank": "PNC Bank",
        "pnc bk": "PNC Bank",

        # Capital One
        "capital one": "Capital One",

        # Citi
        "citi": "Citi"
    }

    df["issuer_bank_name"] = df["issuer_bank_name"].apply(
        lambda value: (
            "NA" if pd.isna(value)
            else bank_map.get(
                re.sub(r"\s+", " ", str(value).strip().lower()),
                "NA"
            )
        )
    )
    ### standardising all anomalous combinations of bank names

    # 4. issuer_bank_state
    state_map = {
        "ca": "CA",
        "california": "CA",
        "il": "IL",
        "illinois": "IL",
        "mi": "MI",
        "michigan": "MI",
        "mn": "MN",
        "minnesota": "MN",
        "nc": "NC",
        "north carolina": "NC",
        "ny": "NY",
        "new york": "NY",
        "pa": "PA",
        "pennsylvania": "PA",
        "va": "VA",
        "virginia": "VA",
    }
    df["issuer_bank_state"] = df["issuer_bank_state"].apply(
        lambda value: "NA" if pd.isna(value) else state_map.get(str(value).strip().lower(), "NA")
    )
    ### setting standard US state codes, and "unknown" for missing

    # 5. issuer_bank_type
    df["issuer_bank_type"] = df["issuer_bank_type"].apply(
        lambda value: (
            "National" if "national" in re.sub(r"[^a-z]", "", str(value).lower().strip())
            else "Regional" if "regional" in re.sub(r"[^a-z]", "", str(value).lower().strip())
            else "Online" if "online" in re.sub(r"[^a-z]", "", str(value).lower().strip())
            else "NA"
        )
    )
    ### Standardisation between 4 levels, this time using regex as it is simpler than previous tasks which required
    ### hard coding

    # 6. issuer_risk_rating
    df["issuer_risk_rating"] = df["issuer_risk_rating"].apply(
        lambda value: (
            "Low" if "low" in re.sub(r"[^a-z]", "", str(value).lower().strip())
            else "Medium" if "med" in re.sub(r"[^a-z]", "", str(value).lower().strip())
            else "NA"
        )
    )
    ### Same rules applied as issuer bank type

    # 7. card_number length validation check
    df["card_number"] = df["card_number"].astype("string").str.strip()
    mask_bad_decimal = df["card_number"].str.contains(r"\.[1-9]\d*$", na=False)
    df = df.loc[~mask_bad_decimal].copy()
    df["card_number"] = df["card_number"].str.replace(r"\.0$", "", regex=True)
    ### Removing rows with decimals, and converting to numeric

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
    ### Keeping only valid length of card numbers based on brand, the rest are considered invalid and will be ignored

    # 8. removing duplicates
    df = df.drop_duplicates(subset=["id", "card_number"]).copy()

    # 9. CVV 
    df["cvv"] = df["cvv"].astype("string").str.strip()
    df["cvv"] = df["cvv"].str.extract(r"^(\d+)$", expand=False).str.zfill(3)
    df = df[df["cvv"].str.match(r"^\d{3}$", na=False)].copy()
    ### filling cvv's with less than 3 digits ingested with 0's, e.g. 1 = "001"
    ### dropping non-three digit cvv's

    # 10. Account open date
    month_map = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
        "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
        "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
    }
    df["acct_open_date"] = df["acct_open_date"].astype("string").str.strip()
    acct_parts = df["acct_open_date"].str.extract(
        r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2})$"
    )
    acct_month_num = acct_parts[0].map(month_map)
    acct_year_full = acct_parts[1].apply(
        lambda value: pd.NA if pd.isna(value) else (2000 + int(value) if int(value) <= 26 else 1900 + int(value))
    )
    df["acct_open_date"] = pd.to_datetime(
        {"year": acct_year_full, "month": acct_month_num, "day": 1},
        errors="coerce"
    )
    current_year = pd.Timestamp.today().year
    df = df[df["acct_open_date"].isna() | (df["acct_open_date"].dt.year <= current_year)].copy()
    ### splittling month and year, converting to numeric, fusing, and then making a date for easier querying
    ### we assume the day of action is always the first of the month

    # 11. Expiry date
    df["expires"] = df["expires"].astype("string").str.strip()
    exp_parts = df["expires"].str.extract(
        r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2})$"
    )
    exp_month_num = exp_parts[0].map(month_map)
    exp_year_full = exp_parts[1].apply(
        lambda value: pd.NA if pd.isna(value) else (2000 + int(value) if int(value) <= 26 else 1900 + int(value))
    )
    df["expiry_date"] = pd.to_datetime(
        {"year": exp_year_full, "month": exp_month_num, "day": 1},
        errors="coerce"
    )
    df = df.drop(columns=["expires"])
    ### once again converting to numeric, fusing, and then making it a date for easier querying.
    ### we assume the day of action is always the first of the month

    # 12. Credit limit cleanup
    df["credit_limit"] = df["credit_limit"].apply(
        lambda value: (
            pd.NA if pd.isna(value)
            else pd.NA if str(value).strip().lower() in {"", "nan", "none", "null", "unknown", "limit_unknown", "error_value"}
            else 10000.0 if str(value).strip().lower() == "ten thousand"
            else (
                float(str(value).strip().lower()[:-1]) * 1000
                if str(value).strip().lower().endswith("k")
                and str(value).strip().lower()[:-1].replace(".", "", 1).replace("-", "", 1).isdigit()
                else pd.to_numeric(
                    str(value).strip().lower().replace("$", "").replace(",", ""),
                    errors="coerce"
                )
            )
        )
    )
    df = df[df["credit_limit"].isna() | (df["credit_limit"] >= 0)].copy()
    ### Standardising the credit limit and removing anomoulous text and currency, assuming all is USD

    # Final column order
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
    ### Dropped CVV and number, was only a validation check in length, more details in report

    df = df[final_columns].copy()

    # ==============================
    # Copy to Postgres
    # ==============================
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

    print(f"✅ Inserted {len(df)} rows into transformation.{table_name}")

    # ==============================
    # Close
    # ==============================
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
        print("✅ Pipeline completed successfully")
    except Exception as e:
        print("Error occurred:")
        print(e)
