{{
    config(
        materialized = 'view'
    )
}}


WITH dim_hosts AS (
    SELECT * FROM {{ ref('src_hosts') }}
)
SELECT
    host_id,
    NVL(
        host_name,
        'Anonymous'
    ) AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    dim_hosts



