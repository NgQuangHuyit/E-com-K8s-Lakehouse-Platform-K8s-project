{{
    config(
        materialized='table',
        file_format='parquet',
        location_root='s3a://lakehouse/tmp/sessions_daily_tmp',
        schema='default'
    )
}}
WITH date_filter AS (
    SELECT 
        {% if is_incremental() %}
            COALESCE(MAX(prediction_date), DATE '2020-01-01') AS min_date
        {% else %}
            DATE '2020-01-01' AS min_date
        {% endif %}
    FROM {{ this }}
)
SELECT
        user_id,
        CAST(timestamp AS DATE) AS session_date,
        COUNT(DISTINCT session_id) AS sessions_count,
        SUM(duration_seconds) AS total_duration,
        AVG(duration_seconds) AS avg_duration,
        SUM(page_views) AS total_page_views,
        SUM(actions_count) AS total_actions,
        SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS purchase_sessions_count,
        SUM(CAST(revenue AS DOUBLE)) AS total_revenue,
        MAX(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS has_purchase_today
    FROM {{ ref('user_sessions') }}
    {% if is_incremental() %}
    WHERE CAST(timestamp AS DATE) >= (SELECT date_sub(min_date, 3) FROM date_filter)
    {% endif %}
    GROUP BY user_id, CAST(timestamp AS DATE)