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


    # View 1: KPI Overview
    print("Creating mart_leadership_kpi_overview...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_leadership_kpi_overview AS
        SELECT
            SUM(f.amount) AS total_revenue,
            COUNT(*) AS total_transactions,
            COUNT(DISTINCT f.user_key) AS unique_customers,
            ROUND(AVG(f.amount), 2) AS avg_transaction_amount
        FROM curated.fact_transactions f;
    """)
    conn.commit()


    # View 2: Monthly Trends (MoM % change)
    print("Creating mart_leadership_monthly_trends...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_leadership_monthly_trends AS
        WITH monthly AS (
            SELECT
                d.year,
                d.month,
                SUM(f.amount) AS total_revenue,
                COUNT(*) AS transaction_count
            FROM curated.fact_transactions f
            JOIN curated.dim_date d ON f.date_key = d.date_key
            GROUP BY d.year, d.month
        )
        SELECT
            m.year,
            m.month,
            m.total_revenue,
            m.transaction_count,
            LAG(m.total_revenue) OVER (ORDER BY m.year, m.month) AS prev_month_revenue,
            ROUND(
                (m.total_revenue - LAG(m.total_revenue) OVER (ORDER BY m.year, m.month))::NUMERIC
                / NULLIF(ABS(LAG(m.total_revenue) OVER (ORDER BY m.year, m.month)), 0) * 100, 2
            ) AS revenue_mom_change_pct,
            LAG(m.transaction_count) OVER (ORDER BY m.year, m.month) AS prev_month_transactions,
            ROUND(
                (m.transaction_count - LAG(m.transaction_count) OVER (ORDER BY m.year, m.month))::NUMERIC
                / NULLIF(LAG(m.transaction_count) OVER (ORDER BY m.year, m.month), 0) * 100, 2
            ) AS transactions_mom_change_pct
        FROM monthly m
        ORDER BY m.year, m.month;
    """)
    conn.commit()


    # View 3: Top States (ranked)
    print("Creating mart_leadership_top_states...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_leadership_top_states AS
        SELECT
            l.state,
            SUM(f.amount) AS total_revenue,
            COUNT(*) AS transaction_count,
            RANK() OVER (ORDER BY SUM(f.amount) DESC) AS revenue_rank
        FROM curated.fact_transactions f
        JOIN curated.dim_location l ON f.location_key = l.location_key
        WHERE l.state IS NOT NULL
        GROUP BY l.state
        ORDER BY revenue_rank;
    """)
    conn.commit()


    # View 4: Online vs In-Store
    print("Creating mart_leadership_online_vs_instore...")
    cur.execute("""
        CREATE OR REPLACE VIEW curated.mart_leadership_online_vs_instore AS
        SELECT
            l.is_online,
            CASE WHEN l.is_online = TRUE THEN 'Online' ELSE 'In-Store' END AS channel,
            SUM(f.amount) AS total_revenue,
            COUNT(*) AS transaction_count,
            ROUND(AVG(f.amount), 2) AS avg_transaction_amount
        FROM curated.fact_transactions f
        JOIN curated.dim_location l ON f.location_key = l.location_key
        GROUP BY l.is_online
        ORDER BY l.is_online;
    """)
    conn.commit()

    print("✅ All leadership mart views created")
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        main()
        print("✅ Pipeline completed successfully")
    except Exception as e:
        print("Error occurred:")
        print(e)
