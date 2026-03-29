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


    # View 1: Customer Lifetime Value
    print("Creating mart_customer_lifetime_value...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_customer_lifetime_value AS
        SELECT
            f.user_key,
            u.user_id,
            SUM(f.amount) AS total_spending,
            COUNT(*) AS transaction_count,
            MIN(f.date_key) AS first_transaction_date,
            MAX(f.date_key) AS last_transaction_date,
            AVG(f.amount) AS avg_transaction_amount
        FROM curated.fact_transactions f
        JOIN curated.dim_user u ON f.user_key = u.user_key
        GROUP BY f.user_key, u.user_id
        ORDER BY total_spending DESC;
    """)
    conn.commit()


    # View 2: Online vs In-Store
    print("Creating mart_customer_online_vs_instore...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_customer_online_vs_instore AS
        SELECT
            l.is_online,
            CASE WHEN l.is_online = TRUE THEN 'Online' ELSE 'In-Store' END AS channel,
            SUM(f.amount) AS total_amount,
            COUNT(*) AS transaction_count,
            AVG(f.amount) AS avg_amount
        FROM curated.fact_transactions f
        JOIN curated.dim_location l ON f.location_key = l.location_key
        GROUP BY l.is_online
        ORDER BY l.is_online;
    """)
    conn.commit()


    # View 3: Active Cards per Customer
    print("Creating mart_customer_active_cards...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_customer_active_cards AS
        SELECT
            u.user_key,
            u.user_id,
            COUNT(DISTINCT c.card_key) AS active_card_count
        FROM curated.dim_user u
        JOIN curated.fact_transactions f ON f.user_key = u.user_key
        JOIN curated.dim_card c          ON f.card_key = c.card_key
        WHERE c.expiry_date > CURRENT_DATE
        GROUP BY u.user_key, u.user_id
        ORDER BY active_card_count DESC;
    """)
    conn.commit()


    # View 4: Suspicious Patterns
    print("Creating mart_customer_suspicious_patterns...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_customer_suspicious_patterns AS
        SELECT
            f.user_key,
            u.user_id,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN e.is_error_client = TRUE THEN 1 ELSE 0 END) AS client_error_count,
            ROUND(
                SUM(CASE WHEN e.is_error_client = TRUE THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS client_error_rate_pct,
            SUM(CASE WHEN e.is_error_tech = TRUE THEN 1 ELSE 0 END) AS tech_error_count,
            SUM(CASE WHEN f.is_refund = TRUE THEN 1 ELSE 0 END) AS refund_count,
            ROUND(
                SUM(CASE WHEN f.is_refund = TRUE THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS refund_rate_pct
        FROM curated.fact_transactions f
        JOIN curated.dim_user u  ON f.user_key = u.user_key
        JOIN curated.dim_error e ON f.error_key = e.error_key
        GROUP BY f.user_key, u.user_id
        ORDER BY client_error_rate_pct DESC;
    """)
    conn.commit()

    print("✅ All customer analytics mart views created")
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
        print("✅ Pipeline completed successfully")
    except Exception as e:
        print("Error occurred:")
        print(e)
