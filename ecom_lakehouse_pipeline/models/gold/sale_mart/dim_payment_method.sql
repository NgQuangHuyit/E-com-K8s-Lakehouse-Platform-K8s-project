{{ config(
    materialized='table',
    schema='sale_mart',
    file_format='delta',
    unique_key='payment_method_id'
) }}


WITH dim_payment_method AS (
    SELECT
        payment_method_id,
        display_name,
        provider,   
        type
    FROM {{ ref('payment_method') }}
)
SELECT * FROM dim_payment_method
