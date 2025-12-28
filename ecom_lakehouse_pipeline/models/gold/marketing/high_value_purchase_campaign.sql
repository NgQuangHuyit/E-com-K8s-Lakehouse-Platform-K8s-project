/*
    Purchase Prediction Campaign Dataset
    
    Purpose: Target customers với xác suất mua cao nhất cho chiến dịch marketing
    
    Business Logic:
    - Lấy top customers có purchase_probability cao nhất từ temp table
    - Phân khúc đơn giản: Hot (>80%), Warm (60-80%), Cold (<60%)
    - Join với customer data từ silver layer
    
    Use Case: Email marketing, push notifications
*/

{{ config(
    materialized='incremental',
    file_format='delta',
    schema='marketing',
    unique_key=['customer_id', 'campaign_date'],
    incremental_strategy='merge',
    partition_by=['year', 'month', 'day']
) }}


SELECT 
    c.customer_id,
    c.campaign_date,
    c.purchase_probability,
    
    -- Phân khúc đơn giản
    CASE 
        WHEN c.purchase_probability >= 0.80 THEN 'Hot Lead'
        WHEN c.purchase_probability >= 0.60 THEN 'Warm Lead'
        ELSE 'Cold Lead'
    END as customer_segment,
    
    -- Recommendation đơn giản
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

FROM {{ ref('temp_top_customers') }} c
LEFT JOIN {{ ref('customers') }} i ON c.customer_id = i.customer_id
WHERE i.email IS NOT NULL
ORDER BY c.purchase_probability DESC
