{{ config(
    materialized='incremental',
    schema='sale_mart',
    file_format='delta',
    unique_key='order_item_id',
    incremental_strategy='merge',
    partition_by=['year', 'month', 'day']
) }}


WITH order_cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        payment_method_id
    FROM {{ ref('orders') }}
    {% if is_incremental() %}
    WHERE year = {{ var('etl_year') }}
      AND month = {{ var('etl_month') }}
    {% endif %}
)
, order_items_cleaned AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount,
        year,
        month,
        day
    FROM {{ ref('orders_items') }}
    {% if is_incremental() %}
    WHERE year = {{ var('etl_year') }}
      AND month = {{ var('etl_month') }}
    {% endif %}
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
        (oi.unit_price * oi.quantity) - (oi.unit_price * oi.quantity * oi.discount) AS total_amount,
        oi.year AS year,
        oi.month AS month,
        oi.day AS day
    FROM order_items_cleaned oi
    JOIN order_cleaned o
        ON oi.order_id = o.order_id
)

SELECT * FROM fact_sales
