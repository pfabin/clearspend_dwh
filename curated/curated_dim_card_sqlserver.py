import pyodbc

SERVER = r"DESKTOP-VFOTSRD\SQLEXPRESS"
DATABASE = "db_clearspend"
SOURCE_SCHEMA = "transformation"
SOURCE_TABLE = "cards_data"
TARGET_SCHEMA = "curated"
TARGET_TABLE = "dim_card"

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)

def qident(name: str) -> str:
    return f"[{str(name).replace(']', ']]')}]"

def main():
    conn = pyodbc.connect(CONN_STR, autocommit=False)
    try:
        cur = conn.cursor()

        # Create curated schema if it does not exist
        cur.execute(
            f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ?)
                EXEC('CREATE SCHEMA {qident(TARGET_SCHEMA)}')
            """,
            TARGET_SCHEMA,
        )

        full_target = f"{qident(TARGET_SCHEMA)}.{qident(TARGET_TABLE)}"
        full_source = f"{qident(SOURCE_SCHEMA)}.{qident(SOURCE_TABLE)}"

        # Recreate dimension table
        cur.execute(
            f"""
            IF OBJECT_ID(N'{TARGET_SCHEMA}.{TARGET_TABLE}', N'U') IS NOT NULL
                DROP TABLE {full_target};

            CREATE TABLE {full_target} (
                card_key INT IDENTITY(1,1) PRIMARY KEY,
                card_id_nk INT NOT NULL,
                brand VARCHAR(20) NULL,
                type VARCHAR(20) NULL,
                expiry_date DATE NULL,
                has_chip VARCHAR(10) NULL,
                credit_limit DECIMAL(18,2) NULL,
                acct_open_date DATE NULL,
                card_on_dark_web VARCHAR(10) NULL,
                issuer_bank_name VARCHAR(30) NULL,
                issuer_bank_state VARCHAR(10) NULL,
                issuer_bank_type VARCHAR(20) NULL,
                issuer_risk_rating VARCHAR(20) NULL
            );
            """
        )

        # Load one row per card natural key from the transformed layer
        cur.execute(
            f"""
            INSERT INTO {full_target} (
                card_id_nk,
                brand,
                type,
                expiry_date,
                has_chip,
                credit_limit,
                acct_open_date,
                card_on_dark_web,
                issuer_bank_name,
                issuer_bank_state,
                issuer_bank_type,
                issuer_risk_rating
            )
            SELECT DISTINCT
                id AS card_id_nk,
                card_brand AS brand,
                card_type AS type,
                expiry_date,
                has_chip,
                credit_limit,
                acct_open_date,
                card_on_dark_web,
                issuer_bank_name,
                issuer_bank_state,
                issuer_bank_type,
                issuer_risk_rating
            FROM {full_source}
            ORDER BY id;
            """
        )

        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM {full_target}")
        row_count = cur.fetchone()[0]
        print(f"✅ Loaded {row_count:,} rows into {TARGET_SCHEMA}.{TARGET_TABLE}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
