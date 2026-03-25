import psycopg
import pandas as pd
from io import StringIO

def main():
    table_name = "mcc_data"

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
            code INTEGER,
            description VARCHAR(200)
        );
    """)

    print(f"✅ Table transformation.{table_name} created")

    # ==============================
    # Read from ingestion (only code and description)
    # ==============================
    df = pd.read_sql_query(
        "SELECT code, description FROM ingestion.mcc_data", conn
    )

    print(f"🔄 Read {len(df)} rows from ingestion.{table_name}")

    # ==============================
    # Clean code column
    # ==============================
    df["code"] = df["code"].str.replace('"""', '', regex=False)
    df["code"] = df["code"].str.replace('MCC', '', regex=False)
    df["code"] = df["code"].str.strip()

    # Filter out non-numeric rows (e.g. NOTE, COMMENT)
    df = df[df["code"].str.isnumeric()]

    # Cast to integer
    df["code"] = df["code"].astype(int)

    # ==============================
    # Clean description column
    # ==============================
    df["description"] = df["description"].str.strip()
    df["description"] = df["description"].str.title()

    # ==============================
    # Deduplicate and sort
    # ==============================
    df = df.drop_duplicates(subset=["code", "description"])
    df = df.sort_values("code").reset_index(drop=True)

    print(f"✅ Cleaned to {len(df)} rows (removed junk + duplicates)")

    # ==============================
    # Copy to Postgres
    # ==============================
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    with cur.copy(f"""
        COPY transformation.{table_name} (code, description)
        FROM STDIN WITH (FORMAT CSV)
    """) as copy:
        copy.write(buffer.getvalue())

    print(f"✅ Inserted {len(df)} rows into transformation.{table_name}")

    # ==============================
    # Commit + close
    # ==============================
    conn.commit()
    cur.close()
    conn.close()

    print("✅ Pipeline completed successfully")

if __name__ == "__main__":
    main()
