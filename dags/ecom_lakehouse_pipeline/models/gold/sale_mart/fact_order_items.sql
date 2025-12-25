{{ config(
    materialized='incremental',
    schema='sale_mart',
    file_format='delta',
    unique_key='order_item_id',
    strategy='merge',
    partition_by='date_key'
) }}


WITH order_cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        payment_method_id
    FROM {{ ref('orders') }}
)
, order_items_cleaned AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount
    FROM {{ ref('orders_items') }}
),
fact_sales AS (
    SELECT
        oi.order_item_id AS order_item_id,
        o.customer_id AS customer_key,
        CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS date_key,
        o.payment_method_id as payment_method_key,
        oi.product_id AS product_key,
        oi.quantity AS quantity,
        oi.unit_price AS unit_price,
        oi.discount AS discount_percent,
        (oi.unit_price * oi.quantity) AS sub_total,
        (oi.unit_price * oi.quantity * oi.discount) AS discount_amt,
        (oi.unit_price * oi.quantity) - (oi.unit_price * oi.quantity * oi.discount) AS total_amount
    FROM order_cleaned o
    JOIN order_items_cleaned oi
        ON o.order_id = oi.order_id
)

SELECT * FROM fact_sales
