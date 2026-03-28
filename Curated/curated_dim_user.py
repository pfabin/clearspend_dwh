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
    df = pd.read_sql("""SELECT * FROM transformation.users_data""", conn)

    # ==============================
    # CLEANING
    # ==============================
    dim_user = pd.DataFrame({   
        "user_id": df["id"],
        "birth_year": df["birth_year"],
        "birth_month": df["birth_month"],
        "gender": df["gender"],
        "address": df["address"],
        "latitude": df["latitude"],
        "longitude": df["longitude"],
        "annual_income": df["yearly_income"],
        "total_debt": df["total_debt"],
        "credit_score": df["credit_score"],
        "num_credit_cards": df["num_credit_cards"],
        "employment_status": df["employment_status"],
        "education_level": df["education_level"]
    })
    dim_user = dim_user.drop_duplicates().copy()
    dim_user.insert(0, "user_key", dim_user.index + 1)
    ### surrogate added

    # ==============================
    # Curated
    # ==============================
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS curated;
    """)
    conn.commit()

    cur.execute("""
        DROP TABLE IF EXISTS curated.dim_user;
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS curated.dim_user (
            user_key INT,
            user_id INT,
            birth_year INT,
            birth_month INT,
            gender VARCHAR(10),
            address VARCHAR(150),
            latitude DECIMAL(10,6),
            longitude DECIMAL(10,6),
            annual_income DECIMAL(10,2),
            total_debt DECIMAL(10,2),
            credit_score INT,
            num_credit_cards INT,
            employment_status VARCHAR(20),
            education_level VARCHAR(20)
        );
    """)
    conn.commit()

    # ==============================
    # Copying chunk directly to Postgres
    # ==============================
    csv_file = "star_schema/dim_user.csv"
    dim_user.to_csv(csv_file, index=False)

    with open(csv_file, "r", encoding="utf-8") as f:
        with cur.copy("""
            COPY curated.dim_user
            FROM STDIN WITH CSV HEADER
        """) as copy:
            copy.write(f.read())
    conn.commit()

    print("✅ dim_user loaded into curated.dim_user")

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
