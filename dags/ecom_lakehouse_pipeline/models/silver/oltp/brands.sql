{{ config(
    materialized='table',
    schema='silver',
    file_format='delta'
) }}

WITH brands_cleaned AS (
    SELECT
        TRIM(brand_id) AS brand_id,
        TRIM(brand_name) AS brand_name,
        TRIM(brand_origin) AS brand_origin
    FROM {{ source('bronze', 'brands') }} )

SELECT * FROM brands_cleaned