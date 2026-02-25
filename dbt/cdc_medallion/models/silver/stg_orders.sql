{{ config(
    materialized='incremental',
    unique_key=['order_id', 'valid_from'],
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT *
    FROM read_parquet(
            's3://datalake/bronze/orders/**/*.parquet',
            union_by_name = true
        ) 
    {% if is_incremental() %}
    WHERE CAST(_ingested_at AS TIMESTAMP) > (SELECT MAX(valid_from) FROM {{ this }})
    {% endif %}
),
filtered AS (
    SELECT *
    FROM raw
    WHERE order_id IS NOT NULL
        AND customer_id IS NOT NULL
        AND product_item_id IS NOT NULL
        AND ordered_quantity IS NOT NULL
        AND CAST(ordered_quantity AS INTEGER) > 0
        AND order_status IN (
            'PENDING',
            'PROCESSING',
            'SHIPPED',
            'DELIVERED',
            'CANCELLED'
        )
        AND _op != 'delete'
),
casted AS (
    SELECT CAST(('0x' || hex(order_id.value)) AS BIGINT) AS order_id,
        CAST(('0x' || hex(customer_id.value)) AS BIGINT) AS customer_id,
        CAST(('0x' || hex(product_item_id.value)) AS BIGINT) AS product_item_id,
        CAST(ordered_quantity AS INTEGER) AS ordered_quantity,
        order_status,
        city,
        country,
        street,
        CAST(additional_info AS VARCHAR) AS additional_info,
        make_timestamp(created_at) AS created_at,
        created_by,
        make_timestamp(modified_at) AS modified_at,
        modified_by,
        CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
    FROM filtered
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id,
            order_status
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM casted
),
scd AS (
    SELECT *,
        _ingested_at AS valid_from,
        LEAD(_ingested_at) OVER (
            PARTITION BY order_id
            ORDER BY _ingested_at ASC
        ) AS valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _ingested_at DESC
        ) AS rn_current
    FROM deduped
    WHERE rn = 1
)
SELECT order_id,
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
    valid_from,
    COALESCE(valid_to, CAST('9999-12-31' AS TIMESTAMP)) AS valid_to,
    CASE
        WHEN rn_current = 1 THEN TRUE
        ELSE FALSE
    END AS is_current,
    FIRST_VALUE(created_at) OVER (
        PARTITION BY order_id
        ORDER BY created_at ASC
    ) AS order_created_at
FROM scd