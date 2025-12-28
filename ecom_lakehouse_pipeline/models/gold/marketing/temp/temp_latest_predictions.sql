
{{ config(
    materialized='table',
    file_format='parquet',
    location_root='s3a://lakehouse/tmp/latest_predictions',
    schema='default',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"
) }}

SELECT 
    user_id as customer_id,
    prediction_date,
    purchase_probability,
    prediction_timestamp as predicted_at,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY prediction_timestamp DESC) as rn
FROM {{ source('ml', 'next_day_purchase_prediction') }}
WHERE will_purchase_tomorrow = 1
    AND prediction_date = '2025-12-11'
    -- AND prediction_date >= CURRENT_DATE - INTERVAL '7' DAY
