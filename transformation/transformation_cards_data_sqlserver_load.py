import pyodbc
import pandas as pd
import re

SERVER = r"DESKTOP-VFOTSRD\SQLEXPRESS"
DATABASE = "db_clearspend"
TARGET_SCHEMA = "transformation"
TARGET_TABLE = "cards_data_clean"

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)


# ---------------------------
# Helper functions
# ---------------------------

def normalize_text(value):
    """Lowercase, trim, and keep letters only for fuzzy categorical matching."""
    if pd.isna(value):
        return ""
    return re.sub(r"[^a-z]", "", str(value).lower().strip())


def map_card_brand(value):
    """Standardize card brand values."""
    val = normalize_text(value)

    if val in {"", "nan", "none", "null", "unknown"}:
        return "Unknown"
    if "visa" in val or val in {"v", "vis", "vsa"}:
        return "Visa"
    if "master" in val:
        return "Mastercard"
    if "amex" in val or val in {"ame", "amx"}:
        return "Amex"
    if "discover" in val or "disc" in val:
        return "Discover"
    return "Unknown"


def map_card_type(value):
    """Standardize card type values."""
    val = normalize_text(value)

    if val in {"", "nan", "none", "null", "unknown"}:
        return "Unknown"
    if "prepaid" in val or val in {"dp", "dpp", "ppd", "dbpp"}:
        return "Prepaid Debit"
    if "credit" in val or val in {"cr", "cc", "cred", "crdeit"}:
        return "Credit"
    if "debit" in val or val in {"deb", "db", "d"}:
        return "Debit"
    return "Unknown"


def map_bank_name(value):
    """Standardize issuer bank names."""
    val = normalize_text(value)

    if val in {"", "nan", "none", "null", "unknown"}:
        return "Unknown"
    if "ally" in val:
        return "Ally Bank"
    if "bankofamerica" in val or "bkofamerica" in val:
        return "Bank of America"
    if "capitalone" in val:
        return "Capital One"
    if "chase" in val and "jpm" not in val:
        return "Chase Bank"
    if val == "citi":
        return "Citi"
    if "discover" in val:
        return "Discover Bank"
    if "jpmorgan" in val or "jpm" in val:
        return "JPMorgan Chase"
    if "pnc" in val:
        return "PNC Bank"
    if "truist" in val:
        return "Truist"
    if "usbank" in val:
        return "U.S. Bank"
    if "wellsfargo" in val:
        return "Wells Fargo"
    return "Unknown"


def map_bank_state(value):
    """Standardize issuer bank states to 2-letter abbreviations."""
    if pd.isna(value):
        return "Unknown"

    raw = str(value).strip().lower()

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

    return state_map.get(raw, "Unknown")


def map_bank_type(value):
    """Standardize issuer bank type values."""
    val = normalize_text(value)

    if "national" in val:
        return "National"
    if "regional" in val:
        return "Regional"
    if "online" in val:
        return "Online"
    return "Unknown"


def map_risk(value):
    """Standardize issuer risk rating values."""
    val = normalize_text(value)

    if "low" in val:
        return "Low"
    if "med" in val:
        return "Medium"
    return "Unknown"


def expand_two_digit_year(value):
    """Convert 2-digit year to 4-digit year using a simple current-era cutoff."""
    if pd.isna(value):
        return pd.NA
    year = int(value)
    return 2000 + year if year <= 26 else 1900 + year


def is_valid_card_number(number, brand):
    """Validate cleaned card number against brand-specific length rules."""
    if pd.isna(number) or not str(number).isdigit():
        return False

    length = len(number)

    if brand == "Visa":
        return length in {13, 16, 19}
    if brand in {"Mastercard", "Discover"}:
        return length == 16
    if brand == "Amex":
        return length == 15
    return length in {15, 16}


def clean_credit_limit(value):
    """
    Convert messy credit limit values to numeric.
    Returns pd.NA when the source value cannot be interpreted safely.
    """
    if pd.isna(value):
        return pd.NA

    raw = str(value).strip().lower()

    if raw in {"", "nan", "none", "null", "unknown", "limit_unknown", "error_value"}:
        return pd.NA

    if raw == "ten thousand":
        return 10000.0

    if raw.endswith("k"):
        try:
            return float(raw[:-1]) * 1000
        except ValueError:
            return pd.NA

    raw = raw.replace("$", "").replace(",", "")

    try:
        return float(raw)
    except ValueError:
        return pd.NA


def qident(name: str) -> str:
    """Quote a SQL Server identifier with brackets."""
    return f"[{str(name).replace(']', ']]')}]"


def sql_type_for_column(column_name: str) -> str:
    """Return the target SQL Server type for each output column."""
    type_map = {
        "id": "INT",
        "client_id": "INT",
        "card_brand": "NVARCHAR(50)",
        "card_type": "NVARCHAR(50)",
        "card_number": "NVARCHAR(25)",
        "cvv": "CHAR(3)",
        "has_chip": "NVARCHAR(20)",
        "num_cards_issued": "INT",
        "credit_limit": "FLOAT",
        "year_pin_last_changed": "INT",
        "card_on_dark_web": "NVARCHAR(20)",
        "issuer_bank_name": "NVARCHAR(100)",
        "issuer_bank_state": "NVARCHAR(10)",
        "issuer_bank_type": "NVARCHAR(50)",
        "issuer_risk_rating": "NVARCHAR(50)",
        "acct_open_month": "NVARCHAR(10)",
        "acct_open_year": "INT",
        "expires_month": "NVARCHAR(10)",
        "expires_year": "INT",
    }
    return type_map.get(column_name, "NVARCHAR(MAX)")


def create_target_table(cur, df: pd.DataFrame):
    """Create the output schema/table fresh in SQL Server."""
    cur.execute(
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ?)
            EXEC('CREATE SCHEMA {qident(TARGET_SCHEMA)}')
        """,
        TARGET_SCHEMA,
    )

    full_table_name = f"{qident(TARGET_SCHEMA)}.{qident(TARGET_TABLE)}"

    cur.execute(
        f"""
        IF OBJECT_ID(N'{TARGET_SCHEMA}.{TARGET_TABLE}', N'U') IS NOT NULL
            DROP TABLE {full_table_name};
        """
    )

    column_defs = ", ".join(
        f"{qident(col)} {sql_type_for_column(col)} NULL" for col in df.columns
    )
    cur.execute(f"CREATE TABLE {full_table_name} ({column_defs})")


def load_dataframe_to_sql_server(cur, conn, df: pd.DataFrame, batch_size: int = 1000):
    """Insert the cleaned dataframe into SQL Server."""
    full_table_name = f"{qident(TARGET_SCHEMA)}.{qident(TARGET_TABLE)}"
    column_list = ", ".join(qident(col) for col in df.columns)
    placeholders = ", ".join("?" for _ in df.columns)
    insert_sql = f"INSERT INTO {full_table_name} ({column_list}) VALUES ({placeholders})"

    rows = []
    for record in df.itertuples(index=False, name=None):
        cleaned = tuple(None if pd.isna(value) else value for value in record)
        rows.append(cleaned)

    cur.fast_executemany = True

    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        cur.executemany(insert_sql, batch)
        conn.commit()


# ---------------------------
# Main transformation
# ---------------------------

def main():
    conn = pyodbc.connect(CONN_STR, autocommit=False)

    try:
        df = pd.read_sql_query("SELECT * FROM ingestion.cards_data", conn)

        initial_count = len(df)
        print(f"Initial rows: {initial_count}")

        # ---- Standardize categorical fields ----
        df["card_brand"] = df["card_brand"].apply(map_card_brand)
        df["card_type"] = df["card_type"].apply(map_card_type)
        df["issuer_bank_name"] = df["issuer_bank_name"].apply(map_bank_name)
        df["issuer_bank_state"] = df["issuer_bank_state"].apply(map_bank_state)
        df["issuer_bank_type"] = df["issuer_bank_type"].apply(map_bank_type)
        df["issuer_risk_rating"] = df["issuer_risk_rating"].apply(map_risk)

        # ---- Clean card numbers ----
        df["card_number"] = df["card_number"].astype("string").str.strip()

        # Drop rows with bad decimals like .5
        mask_bad_decimal = df["card_number"].str.contains(r"\.[1-9]\d*$", na=False)
        before = len(df)
        df = df.loc[~mask_bad_decimal].copy()
        print(f"Removed (decimal card numbers): {before - len(df)}")

        # Remove harmless .0
        df["card_number"] = df["card_number"].str.replace(r"\.0$", "", regex=True)

        # Keep only valid card numbers
        before = len(df)
        df = df[df.apply(lambda row: is_valid_card_number(row["card_number"], row["card_brand"]), axis=1)].copy()
        print(f"Removed (invalid card numbers): {before - len(df)}")

        # ---- Deduplicate ----
        before = len(df)
        df = df.drop_duplicates(subset=["id", "card_number"]).copy()
        print(f"Removed (duplicates): {before - len(df)}")

        # ---- CVV cleanup ----
        df["cvv"] = df["cvv"].astype("string").str.strip()
        df["cvv"] = df["cvv"].str.extract(r"^(\d+)$", expand=False).str.zfill(3)

        # Keep only valid CVVs
        before = len(df)
        df = df[df["cvv"].str.match(r"^\d{3}$", na=False)].copy()
        print(f"Removed (invalid CVV): {before - len(df)}")

        # ---- Account open date ----
        df["acct_open_date"] = df["acct_open_date"].astype("string").str.strip()

        acct_parts = df["acct_open_date"].str.extract(
            r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2})$"
        )

        df["acct_open_month"] = acct_parts[0]
        df["acct_open_year"] = acct_parts[1].apply(expand_two_digit_year)

        current_year = pd.Timestamp.today().year

        # Remove future dates
        before = len(df)
        df = df[df["acct_open_year"].isna() | (df["acct_open_year"] <= current_year)].copy()
        print(f"Removed (future account dates): {before - len(df)}")

        df = df.drop(columns=["acct_open_date"])

        # ---- Expiry date ----
        df["expires"] = df["expires"].astype("string").str.strip()

        exp_parts = df["expires"].str.extract(
            r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2})$"
        )

        df["expires_month"] = exp_parts[0]
        df["expires_year"] = exp_parts[1].apply(expand_two_digit_year)

        df = df.drop(columns=["expires"])

        # ---- Credit limit ----
        df["credit_limit"] = df["credit_limit"].apply(clean_credit_limit)

        null_credit_limit_count = df["credit_limit"].isna().sum()
        print(f"Null credit_limit count: {null_credit_limit_count}")

        # Keep NULLs, drop negatives only
        before = len(df)
        df = df[df["credit_limit"].isna() | (df["credit_limit"] >= 0)].copy()
        print(f"Removed (negative credit limits): {before - len(df)}")

        final_count = len(df)

        print("\n--- Summary ---")
        print(f"Initial rows: {initial_count}")
        print(f"Final rows: {final_count}")
        print(f"Total removed: {initial_count - final_count}")
        print(f"Retention rate: {final_count / initial_count:.2%}")

        # Reorder columns so split date parts sit near the original fields.
        final_columns = [
            "id",
            "client_id",
            "card_brand",
            "card_type",
            "card_number",
            "expires_month",
            "expires_year",
            "cvv",
            "has_chip",
            "num_cards_issued",
            "credit_limit",
            "acct_open_month",
            "acct_open_year",
            "year_pin_last_changed",
            "card_on_dark_web",
            "issuer_bank_name",
            "issuer_bank_state",
            "issuer_bank_type",
            "issuer_risk_rating",
        ]
        df = df[final_columns].copy()

        # Create the target table and load the cleaned data.
        cur = conn.cursor()
        create_target_table(cur, df)
        conn.commit()

        load_dataframe_to_sql_server(cur, conn, df)

        print(f"\nLoaded {len(df)} rows into {TARGET_SCHEMA}.{TARGET_TABLE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
