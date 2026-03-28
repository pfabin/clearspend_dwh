import psycopg
import pandas as pd

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
        DROP TABLE IF EXISTS curated.dim_mcc;
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

    csv_file = "star_schema/dim_mcc.csv"
    dim_mcc.to_csv(csv_file, index=False)

    with open(csv_file, "r", encoding="utf-8") as f:
        with cur.copy("""
            COPY curated.dim_mcc
            FROM STDIN WITH CSV HEADER
        """) as copy:
            copy.write(f.read())
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
