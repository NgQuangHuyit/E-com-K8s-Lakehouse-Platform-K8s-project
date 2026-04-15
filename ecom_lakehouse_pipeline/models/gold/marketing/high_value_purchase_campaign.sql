/*
    Purchase Prediction Campaign Dataset
    
    Purpose: Target customers với xác suất mua cao nhất cho chiến dịch marketing
    
    Business Logic:
    - Lấy top customers có purchase_probability cao nhất từ temp table
    - Phân khúc đơn giản: Hot (>80%), Warm (60-80%), Cold (<60%)
    - Join với customer data từ silver layer
    
*/

{{ config(
    materialized='incremental',
    file_format='delta',
    schema='marketing',
    unique_key=['customer_id', 'campaign_date'],
    incremental_strategy='merge',
    partition_by=['year', 'month', 'day']
) }}

WITH temp_top_customers AS (
    SELECT 
        user_id as customer_id,
        prediction_date as campaign_date,
        purchase_probability,
        prediction_timestamp as predicted_at
    FROM {{ source('ml', 'next_day_purchase_prediction') }}
    WHERE will_purchase_tomorrow = 1
        AND prediction_date = '{{ var("etl_date") }}'
        AND purchase_probability >= 0.50
), 
customer_info AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone_number
    FROM {{ ref('customers') }}
)

SELECT 
    c.customer_id,
    c.campaign_date,
    c.purchase_probability,
    

    CASE 
        WHEN c.purchase_probability >= 0.80 THEN 'Hot Lead'
        WHEN c.purchase_probability >= 0.60 THEN 'Warm Lead'
        ELSE 'Cold Lead'
    END as customer_segment,
    

    CASE 
        WHEN c.purchase_probability >= 0.80 THEN 'Send Premium Offer'
        WHEN c.purchase_probability >= 0.60 THEN 'Send Discount Code'
        ELSE 'Send General Newsletter'
    END as campaign_action,
    
    -- Customer contact info
    i.first_name,
    i.last_name,
    i.email,
    i.phone_number as phone,
    YEAR(c.campaign_date) as year,
    MONTH(c.campaign_date) as month,
    DAY(c.campaign_date) as day,
    
    -- Metadata
    c.predicted_at,
    CURRENT_TIMESTAMP as created_at

FROM temp_top_customers c
LEFT JOIN customer_info i ON c.customer_id = i.customer_id
WHERE i.email IS NOT NULL
ORDER BY c.purchase_probability DESC
