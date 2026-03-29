import psycopg
import pandas as pd
import re
from io import StringIO

def main():
    table_name = "users_data"

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
            id INTEGER,
            birth_year INTEGER,
            birth_month INTEGER,
            gender VARCHAR(10),
            address VARCHAR(50),
            latitude DECIMAL(10,6),
            longitude DECIMAL(10,6),
            annual_income DECIMAL(10,2),
            total_debt DECIMAL(10,2),
            credit_score INTEGER,
            num_credit_cards INTEGER,
            employment_status VARCHAR(20),
            education_level VARCHAR(20)
        );
    """)

    print(f"✅ Table transformation.{table_name} created")

    # ==============================
    # Read from ingestion
    # ==============================
    df = pd.read_sql_query(f"SELECT * FROM ingestion.{table_name}", conn)
    print(f"🔄 Read {len(df)} rows from ingestion.{table_name}")

    # ==============================
    # Cleaning
    # ==============================
    # 1. id
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df[df["id"].notna()].copy()
    df["id"] = df["id"].astype("Int64")
    df = df.drop_duplicates(subset=["id"]).copy()
    ### Converting id to numeric, dropping missing, and keeping only unique ids for no duplicated

    # 2. gender
    df["gender"] = (
        df["gender"]
        .astype("string")
        .str.strip()
        .str.title()
    )
    ### Converting gender to title format

    # 3. address
    df["address"] = (
        df["address"]
        .astype("string")
        .str.strip()
    )
    ### Removing empty spaces at the beginning and end of address

    # 4. yearly_income
    df["yearly_income"] = df["yearly_income"].apply(
        lambda value: (
            pd.NA if pd.isna(value) or str(value).strip().lower() in {"", "nan", "none", "null", "unknown"}
            else (
                float(
                    str(value).strip().lower().replace("$", "").replace(",", "").replace(".", "")[:-1]
                ) * 1000
                if str(value).strip().lower().replace("$", "").replace(",", "").replace(".", "").endswith("k")
                and str(value).strip().lower().replace("$", "").replace(",", "").replace(".", "")[:-1].isdigit()
                else pd.to_numeric(
                    str(value).strip().lower().replace("$", "").replace(",", "").replace(".", ""),
                    errors="coerce"
                )
            )
        )
    )
    ### Keeping only numeric yearly income values, removing dollar signs, commas, and full stops, and converting k to *1000
    df = df.rename(columns={"yearly_income": "annual_income"})

    # 5. total_debt
    df["total_debt"] = df["total_debt"].apply(
        lambda value: (
            pd.NA if pd.isna(value) or str(value).strip().lower() in {"", "nan", "none", "null", "unknown"}
            else (
                float(
                    str(value).strip().lower().replace("$", "").replace(",", "").replace(".", "")[:-1]
                ) * 1000
                if str(value).strip().lower().replace("$", "").replace(",", "").replace(".", "").endswith("k")
                and str(value).strip().lower().replace("$", "").replace(",", "").replace(".", "")[:-1].isdigit()
                else pd.to_numeric(
                    str(value).strip().lower().replace("$", "").replace(",", "").replace(".", ""),
                    errors="coerce"
                )
            )
        )
    )
    ### Keeping only numeric total_debt values, removing dollar signs, commas, and full stops, and converting k to *1000, like in previous

    # 6. credit_score
    df["credit_score"] = pd.to_numeric(df["credit_score"], errors="coerce")
    df = df[df["credit_score"].isna() | df["credit_score"].between(100, 999)].copy()
    df["credit_score"] = df["credit_score"].astype("Int64")
    ### Keeping only three-digit credit scores and setting invalid values aside

    # 7. num_credit_cards
    df["num_credit_cards"] = pd.to_numeric(df["num_credit_cards"], errors="coerce").astype("Int64")
    ### Keeping num_credit_cards numeric and leaving missing values as NA

    # 8. dropping unnecessary columns
    df = df.drop(columns=["retirement_age", "current_age", "per_capita_income"], errors="ignore")
    ### Dropping retirement_age, current_age, and per_capita_income, justified in the report

    # 9. employment_status
    employment_map = {
        "retird": "Retired",
        "ret.": "Retired",
        "retired": "Retired",

        "student": "Student",
        "studnt": "Student",

        "un-employed": "Unemployed",
        "unemployed": "Unemployed",
        "unemployd": "Unemployed",

        "self-employed": "Self-Employed",
        "self employed": "Self-Employed",
        "self-employd": "Self-Employed",

        "employed": "Employed",
        "empl0yed": "Employed"
    }

    df["employment_status"] = df["employment_status"].apply(
        lambda value: (
            "Unknown" if pd.isna(value)
            else employment_map.get(
                str(value).strip().lower(),
                "Unknown"
            )
        )
    )
    ### we must hard code the categories, even regex fails to read. Better to make sure source always gives
    ### standard input.

    # 10. education_level
    education_map = {
        # High School
        "high school": "High School",
        "highschool": "High School",
        "hs": "High School",

        # Associate
        "associate": "Associate Degree",
        "associate degree": "Associate Degree",
        "assoc degree": "Associate Degree",
        "associate deg.": "Associate Degree",
        "associatedeg": "Associate Degree",

        # Bachelor
        "bachelor degree": "Bachelor Degree",
        "bachelors": "Bachelor Degree",
        "bachelor's degree": "Bachelor Degree",
        "ba/bs": "Bachelor Degree",

        # Master
        "master degree": "Master Degree",
        "masters": "Master Degree",
        "master's degree": "Master Degree",
        "ms/ma": "Master Degree",

        # Doctorate
        "doctorate": "Doctorate"
    }

    df["education_level"] = df["education_level"].apply(
        lambda value: (
            "Unknown" if pd.isna(value)
            else education_map.get(
                re.sub(r"\s+", " ", str(value).strip().lower()),
                "Unknown"
            )
        )
    )
    ### hard coded categories again in case code fails to read abbevations like HS for High School 

    # Final column order
    final_columns = [
        "id",
        "birth_year",
        "birth_month",
        "gender",
        "address",
        "latitude",
        "longitude",
        "annual_income",
        "total_debt",
        "credit_score",
        "num_credit_cards",
        "employment_status",
        "education_level"
    ]

    df = df[final_columns].copy()

    # ==============================
    # Copy to Postgres
    # ==============================
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    with cur.copy(f"""
        COPY transformation.{table_name}
        (id, birth_year, birth_month,
        gender, address, latitude, longitude,
        annual_income, total_debt, credit_score, num_credit_cards,
        employment_status, education_level)
        FROM STDIN WITH (FORMAT CSV)
        """) as copy:
        copy.write(buffer.getvalue())

    print(f"✅ Inserted {len(df)} rows into transformation.{table_name}")
    # ==============================
    # Close
    # ==============================
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
        print("✅ Pipeline completed successfully")
    except Exception as e:
        print("Error occurred:")
        print(e)
