import duckdb
con = duckdb.connect("dbt/dev.duckdb")
print("=== sales_summary ===")
print(con.execute("""
    SELECT sale_date, region, channel, nb_orders, total_revenue
    FROM main.sales_summary
    ORDER BY sale_date DESC, total_revenue DESC
    LIMIT 10
""").df().to_string(index=False))
total = con.execute("SELECT COUNT(*) FROM main.sales_summary").fetchone()[0]
print(f"\nTotal lignes : {total:,}")
con.close()
