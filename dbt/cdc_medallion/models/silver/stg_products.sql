{{ config(
    materialized='incremental',
    unique_key=['item_id', 'valid_from'],
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT *
    FROM read_parquet(
            's3://datalake/bronze/products/**/*.parquet',
            union_by_name = true
        ) 
    {% if is_incremental() %}
    WHERE CAST(_ingested_at AS TIMESTAMP) > (SELECT MAX(valid_from) FROM {{ this }})
    {% endif %}
),
filtered AS (
    SELECT *
    FROM raw
    WHERE item_id IS NOT NULL
        AND item_id.value IS NOT NULL
        AND product_name IS NOT NULL
        AND TRIM(product_name) != ''
        AND quantity IS NOT NULL
        AND CAST(quantity AS INTEGER) >= 0
        AND unit_price IS NOT NULL
        AND CAST(unit_price AS DOUBLE) > 0
        AND _op != 'delete'
),
casted AS (
    SELECT CAST(('0x' || hex(item_id.value)) AS BIGINT) AS item_id,
        TRIM(product_name) AS product_name,
        CAST(quantity AS INTEGER) AS current_stock,
        CAST(unit_price AS DOUBLE) AS unit_price,
        make_timestamp(created_at) AS created_at,
        created_by,
        make_timestamp(modified_at) AS modified_at,
        modified_by,
        CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
    FROM filtered
),
scd AS (
    SELECT *,
        _ingested_at AS valid_from,
        LEAD(_ingested_at) OVER (
            PARTITION BY item_id
            ORDER BY _ingested_at ASC
        ) AS valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY item_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM casted
)
SELECT item_id,
    product_name,
    current_stock,
    unit_price,
    created_at,
    created_by,
    modified_at,
    modified_by,
    valid_from,
    COALESCE(valid_to, CAST('9999-12-31' AS TIMESTAMP)) AS valid_to,
    CASE
        WHEN rn = 1 THEN TRUE
        ELSE FALSE
    END AS is_current
FROM scd