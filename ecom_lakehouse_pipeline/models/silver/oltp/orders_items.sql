{{ config(
    materialized='incremental',
    schema='silver',
    file_format='delta',
    unique_key='order_item_id',
    incremental_strategy='merge',
    partition_by=['year', 'month', 'day']
) }}

WITH order_date as (
    SELECT
        CAST(order_id AS INT) AS order_id,
        year, 
        month,
        day
    FROM {{ ref('orders') }} o
    {% if is_incremental() %}
    WHERE year = {{ var('etl_year') }}
      AND month = {{ var('etl_month') }}
    {% endif %}
),
order_items_clean AS (
    SELECT
        CAST(order_item_id AS INT) AS order_item_id,
        CAST(order_id AS INT) AS order_id,
        CAST(product_id AS INT) AS product_id,
        CAST(quantity AS INT) AS quantity,
        CAST(unit_price AS DOUBLE) AS unit_price,
        CAST(discount AS DOUBLE) AS discount
    FROM {{ source('bronze', 'order_items') }}
    {% if is_incremental() %}
    WHERE year = {{ var('etl_year') }}
      AND month = {{ var('etl_month') }}
    {% endif %}
)

SELECT 
    oic.order_item_id,
    oic.order_id,
    oic.product_id,
    oic.quantity,
    oic.unit_price,
    oic.discount,
    od.year AS year,
    od.month AS month,
    od.day AS day    
FROM order_items_clean oic
JOIN order_date od
ON oic.order_id = od.order_id
