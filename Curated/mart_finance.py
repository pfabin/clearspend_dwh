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


    # View 1: Monthly Revenue
    print("Creating mart_finance_monthly_revenue...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_finance_monthly_revenue AS
        SELECT
            d.year,
            d.month,
            SUM(f.amount) AS total_revenue,
            COUNT(*)      AS transaction_count
        FROM curated.fact_transactions f
        JOIN curated.dim_date d ON f.date_key = d.date_key
        WHERE f.is_refund = FALSE
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month;
    """)
    conn.commit()


    # View 2: Refund Rate
    print("Creating mart_finance_refund_rate...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_finance_refund_rate AS
        SELECT
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN f.is_refund = TRUE THEN 1 ELSE 0 END) AS refund_count,
            ROUND(
                SUM(CASE WHEN f.is_refund = TRUE THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS refund_rate_pct,
            SUM(CASE WHEN f.is_refund = TRUE THEN ABS(f.amount) ELSE 0 END) AS refund_amount,
            SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END) AS revenue_amount,
            ROUND(
                SUM(CASE WHEN f.is_refund = TRUE THEN ABS(f.amount) ELSE 0 END)::NUMERIC
                / NULLIF(SUM(CASE WHEN f.is_refund = FALSE THEN f.amount ELSE 0 END), 0) * 100, 2
            ) AS refund_amount_pct
        FROM curated.fact_transactions f;
    """)
    conn.commit()


    # View 3: Revenue by State
    print("Creating mart_finance_revenue_by_state...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_finance_revenue_by_state AS
        SELECT
            l.state,
            SUM(f.amount) AS total_revenue,
            COUNT(*) AS transaction_count
        FROM curated.fact_transactions f
        JOIN curated.dim_location l ON f.location_key = l.location_key
        WHERE l.state IS NOT NULL
        GROUP BY l.state
        ORDER BY total_revenue DESC;
    """)
    conn.commit()


    # View 4: Spending by MCC Category
    print("Creating mart_finance_spending_by_category...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_finance_spending_by_category AS
        SELECT
            m.code AS mcc_code,
            m.description AS mcc_description,
            SUM(f.amount) AS total_spending,
            COUNT(*) AS transaction_count
        FROM curated.fact_transactions f
        JOIN curated.dim_mcc m ON f.mcc_key = m.mcc_key
        GROUP BY m.code, m.description
        ORDER BY total_spending DESC;
    """)
    conn.commit()

    print("✅ All finance mart views created")
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
        print("✅ Pipeline completed successfully")
    except Exception as e:
        print("Error occurred:")
        print(e)
