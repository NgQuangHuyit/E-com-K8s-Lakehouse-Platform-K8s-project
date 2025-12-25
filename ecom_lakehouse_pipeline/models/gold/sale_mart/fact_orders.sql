{{ config(
    materialized='incremental',
    schema='sale_mart',
    file_format='delta',
    unique_key='order_id',
    strategy='merge',
    partition_by=['date_key']
) }}

WITH fact_orders AS (
    SELECT
        order_id,
        customer_id as customer_key,
        CAST(date_format(order_date, 'yyyyMMdd') AS INT) AS date_key,
        payment_method_id as payment_method_key,
        total_amount
    FROM {{ ref('orders') }}
)
SELECT * from fact_orders

