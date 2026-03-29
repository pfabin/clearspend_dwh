import psycopg
from psycopg import sql

HOST = "localhost"
PORT = 5432
DATABASE = "db_clearspend"
USER = "postgres"
PASSWORD = "password"

SOURCE_SCHEMA = "transformation"
SOURCE_TABLE = "cards_data"
TARGET_SCHEMA = "curated"
TARGET_TABLE = "dim_card"


def main():
    conn = psycopg.connect(
        host=HOST,
        port=PORT,
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
    )
    conn.autocommit = False

    try:
        cur = conn.cursor()

        # Create curated schema if it does not exist
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(TARGET_SCHEMA)
            )
        )

        full_target = sql.SQL("{}.{}").format(
            sql.Identifier(TARGET_SCHEMA),
            sql.Identifier(TARGET_TABLE),
        )

        full_source = sql.SQL("{}.{}").format(
            sql.Identifier(SOURCE_SCHEMA),
            sql.Identifier(SOURCE_TABLE),
        )

        # Recreate dimension table
        cur.execute(
            sql.SQL(
                """
                DROP TABLE IF EXISTS {target};

                CREATE TABLE {target} (
                    card_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    card_id_nk INTEGER NOT NULL,
                    brand VARCHAR(20),
                    card_type VARCHAR(20),
                    expiry_date DATE,
                    has_chip VARCHAR(10),
                    credit_limit NUMERIC(18,2),
                    acct_open_date DATE,
                    card_on_dark_web VARCHAR(10),
                    issuer_bank_name VARCHAR(30),
                    issuer_bank_state VARCHAR(10),
                    issuer_bank_type VARCHAR(20),
                    issuer_risk_rating VARCHAR(20)
                );
                """
            ).format(target=full_target)
        )

        # Load one row per card natural key from the transformed layer
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {target} (
                    card_id_nk,
                    brand,
                    card_type,
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
                    card_type,
                    expiry_date,
                    has_chip,
                    credit_limit,
                    acct_open_date,
                    card_on_dark_web,
                    issuer_bank_name,
                    issuer_bank_state,
                    issuer_bank_type,
                    issuer_risk_rating
                FROM {source}
                ORDER BY id;
                """
            ).format(target=full_target, source=full_source)
        )

        conn.commit()

        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(full_target))
        row_count = cur.fetchone()[0]
        print(f"Loaded {row_count:,} rows into {TARGET_SCHEMA}.{TARGET_TABLE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
