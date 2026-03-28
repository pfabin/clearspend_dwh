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
    df = pd.read_sql("""SELECT date FROM transformation.transactions_data""", conn)

    # ==============================
    # CLEANING
    # ==============================
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()
    ### Ensuring DATE type
    # Surrogate
    df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype("Int64")
    ### Converting date column to the industry-standard surrogate

    # Others
    dim_date = pd.DataFrame({
        "date_key": df["date_key"],
        "year": df["date"].dt.year,
        "month": df["date"].dt.month,
        "day": df["date"].dt.day,
        "quarter": df["date"].dt.quarter,
        "day_of_week": df["date"].dt.day_name(),
        "is_weekend": df["date"].dt.weekday >= 5
    })
    ### Extracting date components for the date dimension

    # Removing duplicates
    dim_date = dim_date.drop_duplicates().copy()
    ### Keeping only unique dates in the date dimension

    dim_date = dim_date.sort_values("date_key").reset_index(drop=True)
    ### Sorting the date values for aesthetic

    # ==============================
    # Curated
    # ==============================
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS curated;
    """)
    conn.commit()

    cur.execute("""
        DROP TABLE IF EXISTS curated.dim_date;
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS curated.dim_date (
            date_key INT,
            year INT,
            month INT,
            day INT,
            quarter INT,
            day_of_week VARCHAR(10),
            is_weekend BOOLEAN
        );
    """)
    conn.commit()

    # ==============================
    # Copying chunk directly to Postgres
    # ==============================

    csv_file = "star_schema/dim_date.csv"
    dim_date.to_csv(csv_file, index=False)

    with open(csv_file, "r", encoding="utf-8") as f:
        with cur.copy("""
            COPY curated.dim_date
            FROM STDIN WITH CSV HEADER
        """) as copy:
            copy.write(f.read())
    conn.commit()

    print("✅ dim_date loaded into curated.dim_date")

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
