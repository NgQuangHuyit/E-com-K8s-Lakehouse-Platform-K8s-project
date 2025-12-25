{{
    config(
        materialized='table',
        file_format='parquet',
        location_root='s3a://lakehouse/tmp/user_daily_metric_tmp',
        schema='default',
    )
}}


    SELECT
        COALESCE(s.user_id, a.user_id) AS user_id,
        COALESCE(s.session_date, a.action_date) AS activity_date,
        COALESCE(s.sessions_count, 0) AS sessions_count,
        COALESCE(s.total_duration, 0) AS total_duration,
        COALESCE(s.avg_duration, 0.0) AS avg_duration,
        COALESCE(s.total_page_views, 0) AS total_page_views,
        COALESCE(s.total_actions, 0) AS total_actions,
        COALESCE(s.purchase_sessions_count, 0) AS purchase_sessions_count,
        COALESCE(s.total_revenue, 0.0) AS total_revenue,
        COALESCE(s.has_purchase_today, 0) AS has_purchase_today,
        COALESCE(a.view_count, 0) AS view_count,
        COALESCE(a.add_to_cart_count, 0) AS add_to_cart_count,
        COALESCE(a.purchase_count, 0) AS purchase_count,
        COALESCE(a.search_count, 0) AS search_count,
        COALESCE(a.wishlist_count, 0) AS wishlist_count,
        COALESCE(a.checkout_view_count, 0) AS checkout_view_count,
        COALESCE(a.distinct_products_viewed, 0) AS distinct_products_viewed,
        COALESCE(a.avg_product_price, 0.0) AS avg_product_price
    FROM {{ ref('sessions_daily_tmp') }} s
    FULL OUTER JOIN {{ ref('actions_daily_tmp') }} a 
        ON s.user_id = a.user_id AND s.session_date = a.action_date
