{{
    config(
        materialized='incremental',
        file_format='delta',
        schema='ml',
        unique_key=['user_id', 'prediction_date'],
        incremental_strategy='merge'
    )
}}



WITH daily_metrics AS (
    SELECT
        *
    FROM {{ref('user_daily_metric_tmp')}} s
    
),

-- Single window pass for all metrics - memory efficient
rolling_features AS (
    SELECT
        user_id,
        activity_date,
        has_purchase_today,
        LEAD(has_purchase_today, 1) OVER w AS label_purchase_tomorrow,
        
        -- All window functions in one pass (T-3 to T-1)
        SUM(sessions_count) OVER w3 AS sessions_3d,
        SUM(total_duration) OVER w3 AS total_duration_3d,
        AVG(avg_duration) OVER w3 AS avg_session_duration_3d,
        SUM(total_page_views) OVER w3 AS total_page_views_3d,
        SUM(total_actions) OVER w3 AS total_actions_3d,
        SUM(purchase_sessions_count) OVER w3 AS purchase_sessions_3d,
        SUM(total_revenue) OVER w3 AS total_revenue_3d,
        SUM(view_count) OVER w3 AS view_count_3d,
        SUM(add_to_cart_count) OVER w3 AS add_to_cart_count_3d,
        SUM(purchase_count) OVER w3 AS purchase_count_3d,
        SUM(search_count) OVER w3 AS search_count_3d,
        SUM(wishlist_count) OVER w3 AS wishlist_count_3d,
        SUM(checkout_view_count) OVER w3 AS checkout_view_count_3d,
        SUM(distinct_products_viewed) OVER w3 AS distinct_products_3d,
        AVG(avg_product_price) OVER w3 AS avg_product_price_3d
        
    FROM daily_metrics
    WINDOW 
        w AS (PARTITION BY user_id ORDER BY activity_date),
        w3 AS (PARTITION BY user_id ORDER BY activity_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)

SELECT
    user_id,
    activity_date AS prediction_date,
    year(activity_date) AS prediction_year,
    month(activity_date) AS prediction_month,
    
    -- LABEL
    label_purchase_tomorrow,
    
    -- FEATURES: 3-day lookback metrics
    COALESCE(sessions_3d, 0) AS sessions_3d,
    COALESCE(total_duration_3d, 0) AS total_duration_3d,
    COALESCE(avg_session_duration_3d, 0.0) AS avg_session_duration_3d,
    COALESCE(total_page_views_3d, 0) AS total_page_views_3d,
    COALESCE(total_actions_3d, 0) AS total_actions_3d,
    COALESCE(purchase_sessions_3d, 0) AS purchase_sessions_3d,
    COALESCE(total_revenue_3d, 0.0) AS total_revenue_3d,
    COALESCE(view_count_3d, 0) AS view_count_3d,
    COALESCE(add_to_cart_count_3d, 0) AS add_to_cart_count_3d,
    COALESCE(purchase_count_3d, 0) AS purchase_count_3d,
    COALESCE(search_count_3d, 0) AS search_count_3d,
    COALESCE(wishlist_count_3d, 0) AS wishlist_count_3d,
    COALESCE(checkout_view_count_3d, 0) AS checkout_view_count_3d,
    COALESCE(distinct_products_3d, 0) AS distinct_products_3d,
    COALESCE(avg_product_price_3d, 0.0) AS avg_product_price_3d,
    
    -- Derived features (conversion rates)
    CASE 
        WHEN COALESCE(view_count_3d, 0) > 0 
        THEN CAST(COALESCE(add_to_cart_count_3d, 0) AS DOUBLE) / view_count_3d 
        ELSE 0.0 
    END AS cart_conversion_rate_3d,
    
    CASE 
        WHEN COALESCE(add_to_cart_count_3d, 0) > 0 
        THEN CAST(COALESCE(purchase_count_3d, 0) AS DOUBLE) / add_to_cart_count_3d 
        ELSE 0.0 
    END AS purchase_conversion_rate_3d,
    
    CASE 
        WHEN COALESCE(sessions_3d, 0) > 0 
        THEN CAST(COALESCE(total_actions_3d, 0) AS DOUBLE) / sessions_3d 
        ELSE 0.0 
    END AS actions_per_session_3d,
    
    CASE 
        WHEN COALESCE(sessions_3d, 0) > 0 
        THEN CAST(COALESCE(purchase_sessions_3d, 0) AS DOUBLE) / sessions_3d 
        ELSE 0.0 
    END AS purchase_session_rate_3d
    
FROM rolling_features
WHERE 
     sessions_3d > 0
    {% if is_incremental() %}
    AND activity_date > (SELECT min_date FROM date_filter)
    {% endif %}
