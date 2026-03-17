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
        use_chip VARCHAR(50),
        merchant_id INTEGER,
        merchant_city VARCHAR(200),
        merchant_state VARCHAR(200),
        zip VARCHAR(10),
        mcc INTEGER,
        errors VARCHAR(200)
    );
""")

print(f"✅ Table transformation.{table_name} created")

# ==============================
# Read + transform in chunks
# ==============================
query = f"SELECT * FROM ingestion.{table_name}"
total_rows = 0
chunk_num = 0

for df in pd.read_sql_query(query, conn, chunksize=chunk_size):
    chunk_num += 1
    print(f"🔄 Processing chunk {chunk_num} ({len(df)} rows)")

    # ==============================
    # Cleaning (UNCHANGED)
    # ==============================
    online_mask = df["merchant_city"].eq("ONLINE")

    df.loc[df["merchant_state"].isna() & ~online_mask, "merchant_state"] = "NA"
    df.loc[df["zip"].isna() & ~online_mask, "zip"] = "NA"

    df["errors"] = df["errors"].fillna("")

    amount_str = df["amount"].astype("string")
    amount_str = amount_str.str.replace("$", "", regex=False)
    amount_str = amount_str.str.replace(",", "", regex=False)
    amount_str = amount_str.str.replace("USD", "", regex=False)
    amount_str = amount_str.str.strip()

    df["amount"] = pd.to_numeric(amount_str, errors="coerce")
    df.loc[df["amount"] < 0, "amount"] = 0

    # ==============================
    # Keep only target columns
    # ==============================
    df = df[
        [
            "id", "date", "client_id", "card_id", "amount", "use_chip",
            "merchant_id", "merchant_city", "merchant_state", "zip", "mcc", "errors"
        ]
    ]

    # ==============================
    # Copy chunk directly to Postgres
    # ==============================
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    with cur.copy(f"""
        COPY transformation.{table_name}
        (id, date, client_id, card_id, amount,
         use_chip, merchant_id, merchant_city, merchant_state,
         zip, mcc, errors)
        FROM STDIN WITH (FORMAT CSV)
    """) as copy:
        copy.write(buffer.getvalue())

    total_rows += len(df)
    print(f"✅ Inserted chunk {chunk_num} | total rows: {total_rows}")

# ==============================
# Commit + close
# ==============================
conn.commit()
cur.close()
conn.close()

print("✅ Pipeline completed successfully")