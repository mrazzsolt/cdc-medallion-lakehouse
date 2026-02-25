WITH raw AS (
    SELECT *
    FROM read_parquet(
            's3://datalake/bronze/products_x_category/**/*.parquet',
            union_by_name = true
        )
),
filtered AS (
    SELECT *
    FROM raw
    WHERE item_id IS NOT NULL
        AND category_id IS NOT NULL
),
casted AS (
    SELECT CAST(item_id AS BIGINT) AS item_id,
        CAST(category_id AS INTEGER) AS category_id,
        CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
        _op
    FROM filtered
),
scd AS (
    SELECT *,
        _ingested_at AS valid_from,
        LEAD(_ingested_at) OVER (
            PARTITION BY item_id,
            category_id
            ORDER BY _ingested_at ASC
        ) AS valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY item_id,
            category_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM casted
)
SELECT item_id,
    category_id,
    valid_from,
    COALESCE(valid_to, CAST('9999-12-31' AS TIMESTAMP)) AS valid_to,
    CASE
        WHEN rn = 1
        AND _op != 'delete' THEN TRUE
        ELSE FALSE
    END AS is_current
FROM scd