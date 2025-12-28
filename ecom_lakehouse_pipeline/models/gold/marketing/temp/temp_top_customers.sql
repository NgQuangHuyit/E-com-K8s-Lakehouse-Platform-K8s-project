/*
    Temp: Top Customers for Campaign
    
    Purpose: Materialize filtered customers với probability >= 50%
*/

{{ config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://lakehouse/tmp/top_customers',
    tags=['temp', 'marketing'],
    schema='default',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"
) }}

SELECT 
    customer_id,
    prediction_date as campaign_date,
    purchase_probability,
    predicted_at
FROM {{ ref('temp_latest_predictions') }}
WHERE rn = 1
    AND purchase_probability >= 0.50
