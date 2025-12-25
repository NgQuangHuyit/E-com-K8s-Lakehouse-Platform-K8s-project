{{ config(
    materialized='incremental',
    schema='silver',
    file_format='delta',
    unique_key='order_item_id',
    strategy='merge'
) }}

WITH order_items_clean AS (
    SELECT
        CAST(order_item_id AS INT) AS order_item_id,
        CAST(order_id AS INT) AS order_id,
        CAST(product_id AS INT) AS product_id,
        CAST(quantity AS INT) AS quantity,
        CAST(unit_price AS DOUBLE) AS unit_price,
        CAST(discount AS DOUBLE) AS discount

    FROM {{ source('bronze', 'order_items') }}
)

SELECT * FROM order_items_clean