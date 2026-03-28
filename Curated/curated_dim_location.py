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
    df = pd.read_sql("""SELECT is_online, merchant_city, zip, merchant_state, merchant_country
                      FROM transformation.transactions_data""", conn)

    # ==============================
    # CLEANING
    # ==============================
    dim_location = pd.DataFrame({   
        "is_online": df["is_online"],
        "zip": df["zip"],
        "city": df["merchant_city"],
        "state": df["merchant_state"],
        "country": df["merchant_country"]
    })
    dim_location = dim_location.drop_duplicates().reset_index(drop=True)
    dim_location.insert(0, "location_key", dim_location.index + 1)
    ### Location is popualted with rows from transactions data, a surrogate is also made, and duplicates dropped
    ### to keep the locations unique

    # ==============================
    # Curated
    # ==============================
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS curated;
    """)
    conn.commit()

    cur.execute("""
        DROP TABLE IF EXISTS curated.dim_location;
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS curated.dim_location (
            location_key INT,
            is_online BOOLEAN,
            zip VARCHAR(10),
            city VARCHAR(50),
            state VARCHAR(5),
            country VARCHAR(50)
        );
    """)
    conn.commit()

    # ==============================
    # Copying chunk directly to Postgres
    # ==============================

    csv_file = "star_schema/dim_location.csv"
    dim_location.to_csv(csv_file, index=False)

    with open(csv_file, "r", encoding="utf-8") as f:
        with cur.copy("""
            COPY curated.dim_location
            FROM STDIN WITH CSV HEADER
        """) as copy:
            copy.write(f.read())
    conn.commit()

    print("✅ dim_location loaded into curated.dim_location")

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
