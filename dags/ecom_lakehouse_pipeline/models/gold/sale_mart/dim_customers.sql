{{ config(
    materialized='table',
    schema='sale_mart',
    file_format='delta'
) }}

WITH dim_customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        gender,
        address,
        tier
    FROM {{ ref('customers') }}
)

SELECT * FROM dim_customers
