{{ config(
    materialized='incremental',
    schema='sale_mart',
    file_format='delta',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by=['year', 'month', 'day']
) }}

WITH fact_orders AS (
    SELECT
        order_id,
        customer_id as customer_key,
        CAST(date_format(order_date, 'yyyyMMdd') AS INT) AS date_key,
        payment_method_id as payment_method_key,
        total_amount,
        year,
        month,
        day
    FROM {{ ref('orders') }}

    {% if is_incremental() %}
    WHERE year = {{ var('etl_year') }}
      AND month = {{ var('etl_month') }}
    {% endif %}
)
SELECT * from fact_orders

