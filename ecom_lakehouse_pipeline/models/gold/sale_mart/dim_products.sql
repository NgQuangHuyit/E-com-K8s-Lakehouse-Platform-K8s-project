{{ config(
    materialized='table',
    schema='sale_mart',
    file_format='delta'
) }}

WITH brands_cleaned AS (
    SELECT
        brand_id,
        brand_name,
        brand_origin
    FROM {{ ref('brands') }} ),

categories_cleaned AS (
    SELECT
        category_id,
        category_name,
        category_description
    FROM {{ ref('categories') }} ),

dim_products AS (
    SELECT
        p.product_id,
        p.product_name,
        p.product_description,
        p.price,
        c.category_name AS category,
        b.brand_name,
        b.brand_origin
    FROM {{ ref('products') }} AS p
    LEFT JOIN categories_cleaned AS c
        ON p.category_id = c.category_id
    LEFT JOIN brands_cleaned AS b
        ON p.brand_id = b.brand_id
)

SELECT * FROM dim_products