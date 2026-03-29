import psycopg

def main():
    # Create Database
    conn = psycopg.connect(
        dbname="postgres",
        user="postgres",
        password="password",
        host="localhost",
        port=5432
    )

    conn.autocommit = True
    cursor = conn.cursor()

    # Check if db exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'db_clearspend'")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("CREATE DATABASE db_clearspend")
        print("✅ Database created")
    else:
        print("⚠ Database already exists")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
