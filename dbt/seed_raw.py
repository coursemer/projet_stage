"""Load raw CSV files into DuckDB so dbt models can run."""
import duckdb

DB_PATH = "C:/Users/SETUP GAME/projet_stage/dbt/dev.duckdb"
SALES_CSV = "C:/Users/SETUP GAME/projet_stage/spark/data/raw/sales.csv"

con = duckdb.connect(DB_PATH)
con.execute("CREATE SCHEMA IF NOT EXISTS raw")

con.execute(f"""
    CREATE OR REPLACE TABLE raw.sales AS
    SELECT * FROM read_csv_auto('{SALES_CSV}')
""")
print(f"raw.sales: {con.execute('SELECT COUNT(*) FROM raw.sales').fetchone()[0]} rows")

# events: no CSV available, create empty table with expected columns
con.execute("""
    CREATE OR REPLACE TABLE raw.events (
        id VARCHAR,
        timestamp TIMESTAMP,
        user_id VARCHAR,
        event_type VARCHAR,
        _etl_loaded_at TIMESTAMP DEFAULT now()
    )
""")
print("raw.events: empty table created")

con.close()
print("Done.")
