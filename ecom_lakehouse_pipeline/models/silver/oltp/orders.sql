{{ config(
    materialized='incremental',
    schema='silver',
    file_format='delta',
    unique_key='order_id',
    strategy='merge'
) }}

WITH orders_clean AS (
    SELECT
        CAST(order_id AS INT) AS order_id,
        CAST(customer_id AS INT) AS customer_id,
        to_date(order_date, 'yyyy-MM-dd') AS order_date,
        CAST(total_amount AS DOUBLE) AS total_amount,
        CAST(payment_method_id AS INT) AS payment_method_id,
        to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss') AS created_at,
        to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss') AS updated_at
    FROM {{ source('bronze', 'orders') }}
)

-- order_items_clean AS (
--     SELECT
--         CAST(order_item_id AS INT) AS order_item_id,
--         CAST(order_id AS INT) AS order_id,
--         CAST(product_id AS INT) AS product_id,
--         CAST(quantity AS INT) AS quantity,
--         CAST(unit_price AS DOUBLE) AS unit_price
--     FROM {{ source('bronze', 'order_items') }}
-- ),

-- enriched AS (
--     SELECT
--         o.order_id,
--         o.customer_id,
--         o.order_date,
--         o.total_amount,
--         o.discount,
--         o.total_final,
--         o.payment_method_id,
--         oi.order_item_id,
--         oi.product_id,
--         oi.quantity,
--         oi.unit_price AS item_unit_price,
--         (oi.quantity * oi.unit_price) AS item_total_price,
--         o.created_at,
--         o.updated_at
--     FROM orders_clean o
--     JOIN order_items_clean oi
--       ON o.order_id = oi.order_id
-- )

SELECT * FROM orders_clean
