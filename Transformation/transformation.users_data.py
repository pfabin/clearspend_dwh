import psycopg
import pandas as pd
from io import StringIO

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
# Helper functions
# ==============================
def normalize_spaces(series):
    return (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

def parse_money(series):
    s = normalize_spaces(series)
    s = s.str.replace("$", "", regex=False)
    s = s.str.replace(",", "", regex=False)

    k_mask = s.str.lower().str.endswith("k", na=False)
    s = s.str.replace(r"[kK]$", "", regex=True)

    out = pd.to_numeric(s, errors="coerce")
    out.loc[k_mask] = out.loc[k_mask] * 1000

    return out.round(2)

def clean_employment_status(series):
    s = normalize_spaces(series).str.lower()

    replacements = {
        "employed": "Employed",
        "empl0yed": "Employed",

        "student": "Student",
        "studnt": "Student",

        "retired": "Retired",
        "ret.": "Retired",
        "retird": "Retired",

        "unemployed": "Unemployed",
        "un-employed": "Unemployed",
        "unemployd": "Unemployed",

        "self-employed": "Self-Employed",
        "self employed": "Self-Employed",
        "self-employd": "Self-Employed",
        "self employd": "Self-Employed",
    }

    return s.replace(replacements)

def clean_education_level(series):
    s = normalize_spaces(series).str.lower()

    replacements = {
        "high school": "High School",
        "highschool": "High School",
        "hs": "High School",

        "associate degree": "Associate Degree",
        "associate": "Associate Degree",
        "assoc degree": "Associate Degree",
        "associate deg.": "Associate Degree",

        "bachelor degree": "Bachelor Degree",
        "bachelors": "Bachelor Degree",
        "bachelor's degree": "Bachelor Degree",
        "ba/bs": "Bachelor Degree",

        "master degree": "Master Degree",
        "masters": "Master Degree",
        "master's degree": "Master Degree",
        "ms/ma": "Master Degree",

        "doctorate": "Doctorate",
        "doct.": "Doctorate"
    }

    return s.replace(replacements)

# ==============================
# Setup schema + table
# ==============================
cur.execute("CREATE SCHEMA IF NOT EXISTS transformation;")
cur.execute(f"DROP TABLE IF EXISTS transformation.{table_name};")

cur.execute(f"""
    CREATE TABLE transformation.{table_name} (
        id INTEGER,
        current_age INTEGER,
        retirement_age INTEGER,
        birth_year INTEGER,
        birth_month INTEGER,
        gender VARCHAR(20),
        address TEXT,
        latitude DECIMAL(10,6),
        longitude DECIMAL(10,6),
        per_capita_income DECIMAL(12,2),
        yearly_income DECIMAL(12,2),
        total_debt DECIMAL(12,2),
        credit_score INTEGER,
        num_credit_cards INTEGER,
        employment_status VARCHAR(50),
        education_level VARCHAR(50),
        years_to_retirement INTEGER,
        duplicate_id_flag BOOLEAN,
        duplicate_id_conflict_flag BOOLEAN,
        age_birthyear_mismatch_flag BOOLEAN,
        retirement_age_conflict_flag BOOLEAN,
        income_consistency_flag BOOLEAN
    );
""")

print(f"✅ Table transformation.{table_name} created")

# ==============================
# Read from ingestion
# ==============================
cur.execute(f"SELECT * FROM ingestion.{table_name}")
rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]

df = pd.DataFrame(rows, columns=cols)
print(f"🔄 Loaded {len(df)} rows from ingestion.{table_name}")

# ==============================
# Convert numeric columns
# ==============================
numeric_cols = [
    "id",
    "current_age",
    "retirement_age",
    "birth_year",
    "birth_month",
    "latitude",
    "longitude",
    "credit_score",
    "num_credit_cards"
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ==============================
# Basic text cleanup
# ==============================
text_cols = ["gender", "address", "employment_status", "education_level"]
for col in text_cols:
    df[col] = normalize_spaces(df[col])

df["gender"] = df["gender"].str.title()

# ==============================
# Remove exact duplicate rows
# ==============================
before = len(df)
df = df.drop_duplicates()
print(f"✅ Removed {before - len(df)} exact duplicate rows")

# ==============================
# Standardize categories
# ==============================
df["employment_status"] = clean_employment_status(df["employment_status"])
df["education_level"] = clean_education_level(df["education_level"])

# ==============================
# Convert money fields
# ==============================
df["per_capita_income"] = parse_money(df["per_capita_income"])
df["yearly_income"] = parse_money(df["yearly_income"])
df["total_debt"] = parse_money(df["total_debt"])

# ==============================
# Create flags before dedup by id
# ==============================
df = df.sort_values("id").reset_index(drop=True)

df["duplicate_id_flag"] = df["id"].duplicated(keep=False)

emp_conflict = df.groupby("id")["employment_status"].transform("nunique") > 1
edu_conflict = df.groupby("id")["education_level"].transform("nunique") > 1
df["duplicate_id_conflict_flag"] = emp_conflict | edu_conflict

# age plausibility check based on inferred 2019/2020 snapshot
age_2019_after_bday = 2019 - df["birth_year"]
age_2019_before_bday = age_2019_after_bday - 1
age_2020_after_bday = 2020 - df["birth_year"]
age_2020_before_bday = age_2020_after_bday - 1

valid_age = (
    (df["current_age"] == age_2019_after_bday) |
    (df["current_age"] == age_2019_before_bday) |
    (df["current_age"] == age_2020_after_bday) |
    (df["current_age"] == age_2020_before_bday)
)

df["age_birthyear_mismatch_flag"] = ~valid_age

df["retirement_age_conflict_flag"] = df["retirement_age"] < df["current_age"]
df["income_consistency_flag"] = df["per_capita_income"] > df["yearly_income"]
df["years_to_retirement"] = df["retirement_age"] - df["current_age"]

# ==============================
# Keep one row per user id
# ==============================
before = len(df)
df = df.drop_duplicates(subset=["id"], keep="first")
print(f"✅ Removed {before - len(df)} duplicate id rows")
print(f"✅ Final rows after cleaning: {len(df)}")

# ==============================
# Keep only target columns
# ==============================
df = df[
    [
        "id",
        "current_age",
        "retirement_age",
        "birth_year",
        "birth_month",
        "gender",
        "address",
        "latitude",
        "longitude",
        "per_capita_income",
        "yearly_income",
        "total_debt",
        "credit_score",
        "num_credit_cards",
        "employment_status",
        "education_level",
        "years_to_retirement",
        "duplicate_id_flag",
        "duplicate_id_conflict_flag",
        "age_birthyear_mismatch_flag",
        "retirement_age_conflict_flag",
        "income_consistency_flag"
    ]
]

# ==============================
# Copy to Postgres
# ==============================
buffer = StringIO()
df.to_csv(buffer, index=False, header=False, na_rep="")
buffer.seek(0)

with cur.copy(f"""
    COPY transformation.{table_name}
    (id, current_age, retirement_age, birth_year, birth_month,
     gender, address, latitude, longitude, per_capita_income,
     yearly_income, total_debt, credit_score, num_credit_cards,
     employment_status, education_level, years_to_retirement,
     duplicate_id_flag, duplicate_id_conflict_flag,
     age_birthyear_mismatch_flag, retirement_age_conflict_flag,
     income_consistency_flag)
    FROM STDIN WITH (FORMAT CSV)
""") as copy:
    copy.write(buffer.getvalue())

# ==============================
# Save transformed dataset as CSV
# ==============================
output_path = "/Users/mirzakilic/Downloads/clearspend_dwh/Transformation/users_data_transformed.csv"
df.to_csv(output_path, index=False)
print(f"✅ CSV saved to: {output_path}")

# ==============================
# Commit + close
# ==============================
conn.commit()
cur.close()
conn.close()

print("✅ Pipeline completed successfully")
