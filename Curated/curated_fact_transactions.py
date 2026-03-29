import psycopg

def main():
    conn = psycopg.connect(
        host="localhost",
        port=5432,
        dbname="db_clearspend",
        user="postgres",
        password="password"
    )
    cur = conn.cursor()

    print("Step 1: create schema")
    cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")
    conn.commit()

    print("Step 2: drop old fact")
    cur.execute("DROP TABLE IF EXISTS curated.fact_transactions;")
    conn.commit()

    print("Step 3: create fact table")
    cur.execute("""
        CREATE TABLE curated.fact_transactions (
            transaction_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            id_nk INT NOT NULL,
            date_key INT,
            user_key INT,
            card_key INT,
            location_key INT,
            mcc_key INT,
            error_key INT,
            merchant_id INT,
            use_chip VARCHAR(50),
            is_refund BOOLEAN,
            amount DECIMAL(10,2)
        );
    """)
    conn.commit()

    print("Step 4: load 1 percent sample")
    cur.execute("""
        INSERT INTO curated.fact_transactions (
            id_nk,
            date_key,
            user_key,
            card_key,
            location_key,
            mcc_key,
            error_key,
            merchant_id,
            use_chip,
            is_refund,
            amount
        )
        SELECT
            t.id AS id_nk,
            d.date_key,
            u.user_key,
            c.card_key,
            l.location_key,
            m.mcc_key,
            e.error_key,
            t.merchant_id,
            t.use_chip,
            t.is_refund,
            t.amount
        FROM transformation.transactions_data t
        TABLESAMPLE SYSTEM (1)
        LEFT JOIN curated.dim_date d
            ON t.date_id = d.date_key
        LEFT JOIN curated.dim_user u
            ON t.client_id = u.user_id
        LEFT JOIN curated.dim_card c
            ON t.card_id = c.card_id_nk
        LEFT JOIN curated.dim_location l
            ON t.is_online IS NOT DISTINCT FROM l.is_online
            AND t.zip IS NOT DISTINCT FROM l.zip
            AND t.merchant_city IS NOT DISTINCT FROM l.city
            AND t.merchant_state IS NOT DISTINCT FROM l.state
            AND t.merchant_country IS NOT DISTINCT FROM l.country
        LEFT JOIN curated.dim_mcc m
            ON t.mcc = m.code
        LEFT JOIN curated.dim_error e
            ON t.is_error_tech IS NOT DISTINCT FROM e.is_error_tech
            AND t.is_error_client IS NOT DISTINCT FROM e.is_error_client
            AND t.error_client_message IS NOT DISTINCT FROM e.error_client_message;
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM curated.fact_transactions;")
    row_count = cur.fetchone()[0]
    print(f"✅ Loaded {row_count:,} sampled rows into curated.fact_transactions")

    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
        print("✅ Pipeline completed successfully")
    except Exception as e:
        print("Error occurred:")
        print(e)
