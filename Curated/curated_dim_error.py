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
    df = pd.read_sql("""SELECT is_error_tech, is_error_client, error_client_message
                      FROM transformation.transactions_data""", conn)

    # ==============================
    # CLEANING
    # ==============================
    dim_error = pd.DataFrame({
        "is_error_tech": df["is_error_tech"],
        "is_error_client": df["is_error_client"],
        "error_client_message": df["error_client_message"],
    })
    dim_error = dim_error.drop_duplicates().reset_index(drop=True)
    dim_error.insert(0, "error_key", dim_error.index + 1)

    # ==============================
    # Curated
    # ==============================
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS curated;
    """)
    conn.commit()

    cur.execute("""
        DROP TABLE IF EXISTS curated.dim_error;
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS curated.dim_error (
            error_key INT,
            is_error_tech BOOLEAN,
            is_error_client BOOLEAN,
            error_client_message VARCHAR(150)
        );
    """)
    conn.commit()

    # ==============================
    # Copying chunk directly to Postgres
    # ==============================

    csv_file = "star_schema/dim_error.csv"
    dim_error.to_csv(csv_file, index=False)

    with open(csv_file, "r", encoding="utf-8") as f:
        with cur.copy("""
            COPY curated.dim_error
            FROM STDIN WITH CSV HEADER
        """) as copy:
            copy.write(f.read())
    conn.commit()

    print("✅ dim_error loaded into curated.dim_error")

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
