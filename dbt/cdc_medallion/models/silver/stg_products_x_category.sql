WITH raw AS (
    SELECT *
    FROM read_parquet('s3://datalake/bronze/products_x_category/**/*.parquet')
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, category_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM raw
)
SELECT
    item_id,
    category_id,
    _ingested_at AS last_seen_at
FROM deduped
WHERE rn = 1
  AND _op != 'delete'
