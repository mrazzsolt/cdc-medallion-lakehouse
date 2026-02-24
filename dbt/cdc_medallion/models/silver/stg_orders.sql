WITH raw AS (
    SELECT *
    FROM read_parquet('s3://datalake/bronze/orders/**/*.parquet')
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_status
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM raw
    WHERE _op != 'delete'
)
SELECT
    order_id,
    customer_id,
    product_item_id,
    ordered_quantity,
    order_status,
    city,
    country,
    street,
    additional_info,
    created_at,
    created_by,
    modified_at,
    modified_by,
    _ingested_at AS status_updated_at,
    FIRST_VALUE(created_at) OVER (
        PARTITION BY order_id
        ORDER BY created_at ASC
    ) AS order_created_at
FROM deduped
WHERE rn = 1
