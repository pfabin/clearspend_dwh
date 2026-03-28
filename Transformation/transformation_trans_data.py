import psycopg
import pandas as pd
from io import StringIO

table_name = "transactions_data"
chunk_size = 100_000

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
        date DATE,
        client_id INTEGER,
        card_id INTEGER,
        amount DECIMAL(10,2),
        is_refund BOOLEAN,
        use_chip VARCHAR(50),
        merchant_id INTEGER,
        merchant_city VARCHAR(200),
        merchant_country VARCHAR(50),
        is_online BOOLEAN,
        merchant_state VARCHAR(200),
        zip VARCHAR(10),
        mcc INTEGER,
        is_error_tech BOOLEAN,
        is_error_other BOOLEAN,
        error_text VARCHAR(100)
    );
""")

print(f"✅ Table transformation.{table_name} created")

# ==============================
# Read + transform in chunks
# ==============================
query = f"SELECT * FROM ingestion.{table_name}"
total_rows = 0
chunk_num = 0
### Chunks help process the data faster

for df in pd.read_sql_query(query, conn, chunksize=chunk_size):
    chunk_num += 1
    print(f" Processing chunk {chunk_num} ({len(df)} rows)")

    # ==============================
    # Cleaning
    # ==============================
    # 1. Amount
    amount_str = df["amount"].astype("string").str.strip()
    amount_str = amount_str.str.replace("$", "", regex=False)
    df["amount"] = pd.to_numeric(amount_str, errors="coerce")
    ### This ensures amount has no "$" present as prefix, and also makes it numeric data type 

    # 2. is_online + merchant_city
    city_str = df["merchant_city"].astype("string").str.strip()
    df["is_online"] = city_str.str.lower().eq("online").fillna(False)
    df["merchant_city"] = city_str
    df.loc[df["is_online"], "merchant_city"] = "NA"
    ### Cleans the text of merchant_city column, and then creates new boolean column is_online
    ### for all online transactions. Later fills the online values in merchant_city as NA.

    # 3. merchant_state + merchant_country
    state_str = df["merchant_state"].astype("string").str.strip()
    US_STATE_CODES = {
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
        "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
        "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
        "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
        "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
        "DC","AA"
    }
    is_us_state = state_str.isin(US_STATE_CODES)
    ### Checks if all codes are true US states, we have 52 (incl. DC and Armed Forces)

    df["merchant_country"] = "NA"
    df.loc[is_us_state, "merchant_country"] = "United States"
    df.loc[~is_us_state & state_str.notna() & (state_str != ""), "merchant_country"] = state_str
    ### Creates merchany_country column, and if it is a state, labels it as "United States".
    ### Since country and state were mixed, if the value is not a state + not NA + not empty, we move the 
    ### value to the new country column, this differentiates between countries and state codes.
    ### Assumption: only state codes are in 2 letter format. e.g. DE is Delaware and not Germany

    df["merchant_state"] = state_str
    df.loc[~is_us_state, "merchant_state"] = "NA"
    ### If state is missing we apply NA

    df.loc[df["merchant_state"].isna() | (df["merchant_state"] == ""), "merchant_state"] = "NA"
    df.loc[df["merchant_country"].isna() | (df["merchant_country"] == ""), "merchant_country"] = "NA"
    ### all missing value combinations are given NA

    # 4. zip
    df["zip"] = (df["zip"].astype("string").str.replace(r"\.0$", "", regex=True).str.strip())
    df.loc[df["zip"].isna() | (df["zip"] == ""), "zip"] = "NA"
    ### Here the decimal is removed, and missing zip's are NA

    # 5. is_error_tech + is_error_other + error_text
    df["errors"] = df["errors"].fillna("").astype("string").str.strip()
    df["is_error_tech"] = df["errors"].str.contains("Technical Glitch", regex=False)
    ### We create a technical error flag in the case of an error being on our end, assuming that this is only
    ### technical error, and that all possible errors are listed thus far.

    df["is_error_other"] = ((df["errors"] != "") & (~df["errors"].str.fullmatch("Technical Glitch")))
    ### In the case of another error, client-end, it is flagged, assuming we do not need to analyse client-end errors

    def extract_other_errors(err):
        if not err or err == "Technical Glitch":
            return "NA"
        ### returns NA if it is only technical glitch or if no error at all

        parts = [e.strip() for e in err.split(",")]
        ### Splits the several errors to individual fragments
        filtered = [e for e in parts if e != "Technical Glitch"]
        ### Removes technical glitch, keeps the rest
        
        return ",".join(filtered) if filtered else "NA"
        ### If filtered is empty it returns NA

    df["error_text"] = df["errors"].apply(extract_other_errors)
    df.drop(columns=["errors"], inplace=True)
    ### Adds the errors to error_text and drops original column

    # 6. adding the is_refund
    df["is_refund"] = df["amount"] < 0
    df["is_refund"] = df["is_refund"].fillna(False)
    ### if amount is negative we know it is a refund, therefore we flag it for analysis

    # ==============================
    # Keeping only target columns
    # ==============================
    df = df[
        [
            "id", "date", "client_id", "card_id", "amount", "is_refund", "use_chip",
            "merchant_id", "merchant_city", "is_online", "merchant_country", "merchant_state", "zip", "mcc", 
            "is_error_tech", "is_error_other", "error_text"
        ]
    ]

    # ==============================
    # Copying chunk directly to Postgres
    # ==============================
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    with cur.copy(f"""
        COPY transformation.{table_name}
        (id, date, client_id, card_id, amount, is_refund,
         use_chip, merchant_id, merchant_city, is_online, merchant_country, merchant_state,
         zip, mcc, is_error_tech, is_error_other, error_text)
        FROM STDIN WITH (FORMAT CSV)
    """) as copy:
        copy.write(buffer.getvalue())

    total_rows += len(df)
    print(f"✅ Inserted chunk {chunk_num} | total rows: {total_rows}")

# ==============================
# Close
# ==============================
conn.commit()
cur.close()
conn.close()

print("✅ Pipeline completed successfully")
