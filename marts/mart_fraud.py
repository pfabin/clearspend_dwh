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


    # View 1: High Error Users
    print("Creating mart_fraud_high_error_users...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_fraud_high_error_users AS
        SELECT
            f.user_key,
            u.user_id,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN e.is_error_client = TRUE THEN 1 ELSE 0 END) AS error_count,
            ROUND(
                SUM(CASE WHEN e.is_error_client = TRUE THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS error_rate_pct
        FROM curated.fact_transactions f
        JOIN curated.dim_user u  ON f.user_key = u.user_key
        JOIN curated.dim_error e ON f.error_key = e.error_key
        GROUP BY f.user_key, u.user_id
        ORDER BY error_rate_pct DESC;
    """)
    conn.commit()


    # View 2: Refund Anomalies
    print("Creating mart_fraud_refund_anomalies...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_fraud_refund_anomalies AS
        SELECT
            f.user_key,
            u.user_id,
            f.card_key,
            c.card_id_nk,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN f.is_refund = TRUE THEN 1 ELSE 0 END) AS refund_count,
            ROUND(
                SUM(CASE WHEN f.is_refund = TRUE THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS refund_rate_pct
        FROM curated.fact_transactions f
        JOIN curated.dim_user u ON f.user_key = u.user_key
        JOIN curated.dim_card c ON f.card_key = c.card_key
        GROUP BY f.user_key, u.user_id, f.card_key, c.card_id_nk
        ORDER BY refund_rate_pct DESC;
    """)
    conn.commit()

    print("✅ All fraud mart views created")
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
        print("✅ Pipeline completed successfully")
    except Exception as e:
        print("Error occurred:")
        print(e)
