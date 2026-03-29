import psycopg
import pandas as pd
from io import StringIO

def main():
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
    df = pd.read_sql("""SELECT code, description
                      FROM transformation.mcc_data""", conn)

    # ==============================
    # CLEANING
    # ==============================
    dim_mcc = pd.DataFrame({   
        "code": df["code"],
        "description": df["description"]
    })
    dim_mcc.insert(0, "mcc_key", dim_mcc.index + 1)
    ### surrogate added, duplicates already dropped in trasnformation

    # ==============================
    # Curated
    # ==============================
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS curated;
    """)
    conn.commit()

    cur.execute("""
        DROP TABLE IF EXISTS curated.dim_mcc CASCADE;
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS curated.dim_mcc (
            mcc_key INT,
            code INT,
            description VARCHAR(200)
        );
    """)
    conn.commit()

    # ==============================
    # Copying chunk directly to Postgres
    # ==============================

    buffer = StringIO()
    dim_mcc.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    with cur.copy("""
        COPY curated.dim_mcc
        FROM STDIN WITH (FORMAT CSV)
    """) as copy:
        copy.write(buffer.getvalue())
    conn.commit()

    print("✅ dim_mcc loaded into curated.dim_mcc")

    # ==============================
    # Close
    # ==============================
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
        print("✅ Pipeline completed successfully")
    except Exception as e:
        print("Error occurred:")
        print(e)
