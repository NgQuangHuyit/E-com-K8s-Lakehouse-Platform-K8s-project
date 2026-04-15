{{ config(
    materialized='incremental',
    schema='silver',
    file_format='delta',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by=['year', 'month', 'day']
) }}

WITH orders_clean AS (
    SELECT
        CAST(order_id AS INT) AS order_id,
        CAST(customer_id AS INT) AS customer_id,
        to_date(order_date, 'yyyy-MM-dd') AS order_date,
        CAST(total_amount AS DOUBLE) AS total_amount,
        CAST(payment_method_id AS INT) AS payment_method_id,
        to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss') AS created_at,
        to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss') AS updated_at,
        YEAR(to_date(order_date, 'yyyy-MM-dd')) AS year,
        MONTH(to_date(order_date, 'yyyy-MM-dd')) AS month,
        DAY(to_date(order_date, 'yyyy-MM-dd')) AS day
    FROM {{ source('bronze', 'orders') }}     
    {% if is_incremental() %}
    WHERE year = {{ var('etl_year') }}
      AND month = {{ var('etl_month') }}
    {% endif %}
)



SELECT * FROM orders_clean
