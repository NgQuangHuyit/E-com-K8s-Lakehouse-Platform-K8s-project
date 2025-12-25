{{ config(
    materialized='table',
    schema='silver',
    file_format='delta'
) }}

WITH payment_method_cleaned AS (
    SELECT
        CAST(payment_method_id AS INT) AS payment_method_id,
        TRIM(display_name) AS display_name,
        TRIM(provider) AS provider,
        TRIM(type) AS type
    FROM {{ source('bronze', 'payment_method') }})

SELECT * FROM payment_method_cleaned