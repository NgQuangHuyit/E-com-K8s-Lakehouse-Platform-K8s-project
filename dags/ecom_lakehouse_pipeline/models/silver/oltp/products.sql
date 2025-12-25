{{ config(
    materialized='table',
    schema='silver',
    file_format='delta'
) }}

WITH product_cleaned AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(product_description) AS product_description,
        CAST(price AS DOUBLE) AS price,
        CAST(category_id AS INT) AS category_id,
        TRIM(brand_id) AS brand_id,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM {{ source('bronze', 'products') }} 
)

SELECT * FROM product_cleaned



