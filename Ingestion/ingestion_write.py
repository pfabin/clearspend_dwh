import psycopg

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

# schema ingestion
conn = psycopg.connect(
    dbname= "db_clearspend",
    user="postgres",
    password="password",
    host="localhost",
    port=5432
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
cursor.execute("SET search_path TO ingestion")
print("✅ Schema 'ingestion' ready for tables")

cursor.close()
conn.close()