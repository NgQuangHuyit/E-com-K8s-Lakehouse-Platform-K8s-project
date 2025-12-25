{{ config(
    materialized='incremental',
    schema='sale_mart',
    file_format='delta',
    unique_key='product_date_key',
    strategy='merge'
) }}

WITH daily_agg AS (
    SELECT
        date_key,
        product_key,
        SUM(sub_total) AS total_sales_amt,
        SUM(quantity) AS total_quantity_sold,
        SUM(sub_total) / NULLIF(SUM(quantity), 0) AS avg_unit_price,  -- Weighted average
        SUM(discount_amt) AS total_discount_amount
    FROM {{ ref('fact_sales') }}
    GROUP BY date_key, product_key
), 
fact_sale_product_daily as (SELECT
    CONCAT(date_key,'_',product_key) AS product_date_key, -- unique key cho incremental
    date_key,
    product_key,
    total_sales_amt,
    total_quantity_sold,
    avg_unit_price,
    total_discount_amount
FROM daily_agg)

SELECT * FROM fact_sale_product_daily