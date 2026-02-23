WITH raw AS (
    SELECT *
    FROM read_parquet('s3://datalake/bronze/product_categories/**/*.parquet')
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY category_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM raw
)
SELECT
    category_id,
    category_name,
    created_at,
    created_by,
    modified_at,
    modified_by,
    _ingested_at AS last_seen_at
FROM deduped
WHERE rn = 1
  AND _op != 'delete'
