import duckdb

con = duckdb.connect("datalake.duckdb")

def run(label, sql):
    result = con.execute(sql).fetchall()
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"{'='*55}")
    for row in result:
        print(row)

# --------------------------------------------------
# 1. ROW COUNTS
# --------------------------------------------------
run("Row counts", """
    SELECT 'stg_customers'          AS tbl, COUNT(*) AS cnt FROM silver.stg_customers
    UNION ALL
    SELECT 'stg_orders',                    COUNT(*)        FROM silver.stg_orders
    UNION ALL
    SELECT 'stg_products',                  COUNT(*)        FROM silver.stg_products
    UNION ALL
    SELECT 'stg_product_categories',        COUNT(*)        FROM silver.stg_product_categories
    UNION ALL
    SELECT 'stg_products_x_category',       COUNT(*)        FROM silver.stg_products_x_category
    ORDER BY tbl
""")

# --------------------------------------------------
# 2. CUSTOMERS — nincs duplikát customer_id szerint
# --------------------------------------------------
run("Duplicate customer_ids (expect 0 rows)", """
    SELECT customer_id, COUNT(*) AS cnt
    FROM silver.stg_customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
""")

# --------------------------------------------------
# 3. CUSTOMERS — _op oszlop NEM létezik silver-ben (helyes!)
# --------------------------------------------------
run("Columns in stg_customers (verify no _op column)", """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'silver'
      AND table_name   = 'stg_customers'
    ORDER BY ordinal_position
""")

# --------------------------------------------------
# 4. ORDERS — státusz eloszlás
# --------------------------------------------------
run("Order status distribution", """
    SELECT order_status, COUNT(*) AS cnt
    FROM silver.stg_orders
    GROUP BY order_status
    ORDER BY cnt DESC
""")

# --------------------------------------------------
# 5. ORDERS — egy order összes státuszváltása (historik)
# --------------------------------------------------
run("Order history for order_id=1", """
    SELECT order_id, order_status, status_updated_at, order_created_at
    FROM silver.stg_orders
    WHERE CAST(order_id AS BIGINT) = 1
    ORDER BY status_updated_at
""")



# --------------------------------------------------
# 6. ORDERS — NULL customer_id vagy product_item_id
# --------------------------------------------------
run("Orders with NULL FK (expect 0 rows)", """
    SELECT COUNT(*) AS bad_rows
    FROM silver.stg_orders
    WHERE customer_id IS NULL OR product_item_id IS NULL
""")

# --------------------------------------------------
# 7. PRODUCTS — negatív vagy nulla készlet
# --------------------------------------------------
run("Products with zero or negative stock", """
    SELECT item_id, product_name, current_stock
    FROM silver.stg_products
    WHERE current_stock <= 0
""")

# --------------------------------------------------
# 8. PRODUCTS — ár eloszlás
# --------------------------------------------------
run("Product price stats", """
    SELECT
        MIN(unit_price)           AS min_price,
        MAX(unit_price)           AS max_price,
        ROUND(AVG(unit_price), 2) AS avg_price,
        COUNT(*)                  AS total_products
    FROM silver.stg_products
""")

# --------------------------------------------------
# 9. JUNCTION — orphaned product (expect 0 rows)
# --------------------------------------------------
run("Orphaned product_x_category (no matching product, expect 0 rows)", """
    SELECT pxc.item_id
    FROM silver.stg_products_x_category pxc
    LEFT JOIN silver.stg_products p ON pxc.item_id = p.item_id
    WHERE p.item_id IS NULL
""")

# --------------------------------------------------
# 10. JUNCTION — orphaned category (expect 0 rows)
# --------------------------------------------------
run("Orphaned product_x_category (no matching category, expect 0 rows)", """
    SELECT pxc.category_id
    FROM silver.stg_products_x_category pxc
    LEFT JOIN silver.stg_product_categories c ON pxc.category_id = c.category_id
    WHERE c.category_id IS NULL
""")

# --------------------------------------------------
# 11. CATEGORIES — duplikált nevek
# --------------------------------------------------
run("Duplicate category names (expect 0 rows)", """
    SELECT category_name, COUNT(*) AS cnt
    FROM silver.stg_product_categories
    GROUP BY category_name
    HAVING COUNT(*) > 1
""")

# --------------------------------------------------
# 12. Top 5 legtöbb rendelésű ügyfél
# --------------------------------------------------
run("Top 5 customers by order count", """
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name AS full_name,
        COUNT(DISTINCT o.order_id)          AS order_count
    FROM silver.stg_customers c
    JOIN silver.stg_orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, full_name
    ORDER BY order_count DESC
    LIMIT 5
""")

con.close()
print("\nDone.")
