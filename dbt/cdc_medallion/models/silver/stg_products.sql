WITH raw AS (
    SELECT *
    FROM read_parquet('s3://datalake/bronze/products/**/*.parquet')
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM raw
)
SELECT
    item_id,
    product_name,
    quantity        AS current_stock,
    unit_price,
    created_at,
    created_by,
    modified_at,
    modified_by,
    _ingested_at    AS last_seen_at
FROM deduped
WHERE rn = 1
  AND _op != 'delete'
