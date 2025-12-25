{{ config(
    materialized='table',
    schema='silver',
    file_format='delta'
) }}

WITH customers_clean AS (
    SELECT
        CAST(customer_id AS INT) AS customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        TRIM(email) AS email,
        TRIM(phone_number) AS phone_number,
        TRIM(gender) AS gender,
        TRIM(tier) AS tier,
        address,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM {{ source('bronze', 'customers') }}
    )

SELECT * FROM customers_clean