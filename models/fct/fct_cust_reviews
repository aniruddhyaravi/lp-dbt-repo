{{
    config(
        materialized='incremental',
        unique_keys='customer_id',
        on_schema_change='fail'
    )
}}
SELECT
    customer_id, 
    customer_name, 
    updated_at
FROM AIRBNB.RAW.CUSTOMER_DETAILS

{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}