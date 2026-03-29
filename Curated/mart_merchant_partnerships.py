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

    cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")
    conn.commit()


    # View 1: Merchant Transaction Volume
    print("Creating mart_merchant_volume...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_merchant_volume AS
        SELECT
            f.merchant_id,
            COUNT(*) AS transaction_count,
            SUM(f.amount) AS total_amount
        FROM curated.fact_transactions f
        GROUP BY f.merchant_id
        ORDER BY transaction_count DESC;
    """)
    conn.commit()


    # View 2: Industry Growth (monthly by MCC)
    print("Creating mart_merchant_industry_growth...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_merchant_industry_growth AS
        SELECT
            m.code AS mcc_code,
            m.description AS mcc_description,
            d.year,
            d.month,
            COUNT(*) AS transaction_count,
            SUM(f.amount) AS total_amount
        FROM curated.fact_transactions f
        JOIN curated.dim_mcc m  ON f.mcc_key = m.mcc_key
        JOIN curated.dim_date d ON f.date_key = d.date_key
        GROUP BY m.code, m.description, d.year, d.month
        ORDER BY m.code, d.year, d.month;
    """)
    conn.commit()

 
    # View 3: Merchant Error Rates
    print("Creating mart_merchant_error_rates...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_merchant_error_rates AS
        SELECT
            f.merchant_id,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN e.is_error_client = TRUE
                       OR e.is_error_tech = TRUE THEN 1 ELSE 0 END) AS error_count,
            ROUND(
                SUM(CASE WHEN e.is_error_client = TRUE
                           OR e.is_error_tech = TRUE THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS error_rate_pct
        FROM curated.fact_transactions f
        JOIN curated.dim_error e ON f.error_key = e.error_key
        GROUP BY f.merchant_id
        ORDER BY error_rate_pct DESC;
    """)
    conn.commit()


    # View 4: Geographic Revenue
    print("Creating mart_merchant_geographic_revenue...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_merchant_geographic_revenue AS
        SELECT
            l.country,
            l.state,
            SUM(f.amount) AS total_revenue,
            COUNT(*) AS transaction_count
        FROM curated.fact_transactions f
        JOIN curated.dim_location l ON f.location_key = l.location_key
        GROUP BY l.country, l.state
        ORDER BY total_revenue DESC;
    """)
    conn.commit()

    print("✅ All merchant partnerships mart views created")
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
        print("✅ Pipeline completed successfully")
    except Exception as e:
        print("Error occurred:")
        print(e)
