import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs")
con.execute("""
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path'
""")

tables = {
    "customers":          "s3://datalake/bronze/customers/**/*.parquet",
    "orders":             "s3://datalake/bronze/orders/**/*.parquet",
    "products":           "s3://datalake/bronze/products/**/*.parquet",
    "product_categories": "s3://datalake/bronze/product_categories/**/*.parquet",
    "products_x_category":"s3://datalake/bronze/products_x_category/**/*.parquet",
}

for table, path in tables.items():
    print(f"\n{'='*55}")
    print(f"  {table}")
    print(f"{'='*55}")
    try:
        rows = con.execute(f"""
            DESCRIBE SELECT * 
            FROM read_parquet('{path}', union_by_name=true)
            LIMIT 1
        """).fetchall()
        for r in rows:
            print(f"  {r[0]:<25} {r[1]}")
    except Exception as e:
        print(f"  ERROR: {e}")
