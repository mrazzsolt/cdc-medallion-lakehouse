{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'valid_from'],
    incremental_strategy='merge'
) }}


WITH raw AS (
    SELECT *
    FROM read_parquet(
            's3://datalake/bronze/customers/**/*.parquet',
            union_by_name = true
        ) {% if is_incremental() %}
WHERE CAST(_ingested_at AS TIMESTAMP) > (
    SELECT MAX(valid_from) FROM {{ this }}
)
{% endif %}

),
filtered AS (
    SELECT *
    FROM raw
    WHERE customer_id IS NOT NULL
        AND email IS NOT NULL
        AND TRIM(email) != ''
        AND email LIKE '%@%'
        AND _op != 'delete'
),
casted AS (
    SELECT CAST(('0x' || hex(customer_id.value)) AS BIGINT) AS customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        mobile,
        city,
        country,
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
            PARTITION BY customer_id
            ORDER BY _ingested_at ASC
        ) AS valid_to,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM casted
)
SELECT customer_id,
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
    valid_from,
    COALESCE(valid_to, CAST('9999-12-31' AS TIMESTAMP)) AS valid_to,
    CASE
        WHEN rn = 1 THEN TRUE
        ELSE FALSE
    END AS is_current
FROM scd