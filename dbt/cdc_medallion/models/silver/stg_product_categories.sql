{{ config(
    materialized='incremental',
    unique_key=['category_id', 'valid_from'],
    incremental_strategy='merge'
) }}

WITH raw AS (
    SELECT *
    FROM read_parquet(
            's3://datalake/bronze/product_categories/**/*.parquet',
            union_by_name = true
        ) 
    {% if is_incremental() %}
    WHERE CAST(_ingested_at AS TIMESTAMP) > (SELECT MAX(valid_from) FROM {{ this }})
    {% endif %}
),
filtered AS (
    SELECT *
    FROM raw
    WHERE category_id IS NOT NULL
        AND category_name IS NOT NULL
        AND TRIM(category_name) != ''
        AND _op != 'delete'
),
casted AS (
    SELECT CAST(category_id AS INTEGER) AS category_id,
        TRIM(category_name) AS category_name,
        make_timestamp(CAST(created_at AS BIGINT)) AS created_at,
        created_by,
        make_timestamp(CAST(modified_at AS BIGINT)) AS modified_at,
        modified_by,
        CAST(_ingested_at AS TIMESTAMP) AS _ingested_at
    FROM filtered
),
scd AS (
    SELECT *,
        _ingested_at AS valid_from,
        LEAD(_ingested_at) OVER (
            PARTITION BY category_id
            ORDER BY _ingested_at ASC
        ) AS valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY category_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM casted
)
SELECT category_id,
    category_name,
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