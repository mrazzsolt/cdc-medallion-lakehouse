WITH raw AS (
    SELECT *
    FROM read_parquet('s3://datalake/bronze/customers/**/*.parquet')
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM raw
)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    mobile,
    city,
    country,
    created_at,
    created_by,
    modified_at,
    modified_by,
    _ingested_at AS last_seen_at
FROM deduped
WHERE rn = 1
  AND _op != 'delete'
