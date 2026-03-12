import pandas as pd
import psycopg
from psycopg import sql

DB = dict(
    dbname="db_clearspend",
    user="postgres",
    password="password",
    host="localhost",
    port=5432
)

SCHEMA = "ingestion"

FILES = [
    ("datasets/cards_data.csv", "cards_data"),
    ("datasets/mcc_data.csv", "mcc_data"),
    ("datasets/transactions_data.csv", "transactions_data"),
    ("datasets/users_data.csv", "users_data")
]

def create_table_from_header(cur, csv_path: str, table_name: str):
    df_header = pd.read_csv(csv_path, nrows=0)
    cols = [str(c) for c in df_header.columns]

    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))

    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
        sql.Identifier(SCHEMA),
        sql.Identifier(table_name)
    ))

    col_defs = sql.SQL(", ").join(
        sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in cols
    )

    cur.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(SCHEMA),
            sql.Identifier(table_name),
            col_defs
        )
    )

    return cols

def copy_csv(cur, csv_path: str, table_name: str, cols):
    with open(csv_path, "r", encoding="utf-8") as f:
        copy_sql = sql.SQL(
            "COPY {}.{} ({}) FROM STDIN WITH CSV HEADER"
        ).format(
            sql.Identifier(SCHEMA),
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(c) for c in cols)
        )

        with cur.copy(copy_sql) as copy:
            while chunk := f.read(1024 * 1024):
                copy.write(chunk)

    print(f"✅ {csv_path} -> {SCHEMA}.{table_name}")

def main():
    with psycopg.connect(**DB) as conn:
        with conn.cursor() as cur:
            for path, table in FILES:
                cols = create_table_from_header(cur, path, table)
                copy_csv(cur, path, table, cols)

        conn.commit()

if __name__ == "__main__":
    main()