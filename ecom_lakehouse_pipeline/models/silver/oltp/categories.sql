{{ config(
    materialized='table',
    schema='silver',
    file_format='delta'
) }}


with categories_cleaned AS (
    SELECT
        CAST(category_id AS INT) AS category_id,
        TRIM(category_display_name) AS category_name,
        TRIM(category_description) AS category_description
    FROM {{ source('bronze', 'category') }}
)

SELECT * FROM categories_cleaned