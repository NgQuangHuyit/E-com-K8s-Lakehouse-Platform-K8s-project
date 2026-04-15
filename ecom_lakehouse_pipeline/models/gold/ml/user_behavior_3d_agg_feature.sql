{{
    config(
        materialized='table',
        file_format='delta',
        schema='ml',
        unique_key=['user_id', 'prediction_date']
    )
}}



WITH base AS (
    SELECT
        user_id,
        activity_date,
        has_purchase_today,

        sessions_count,
        total_duration,
        avg_duration,
        total_page_views,
        total_actions,
        purchase_sessions_count,
        total_revenue,

        view_count,
        add_to_cart_count,
        purchase_count,
        search_count,
        wishlist_count,
        checkout_view_count,
        distinct_products_viewed,
        avg_product_price
    FROM {{ ref('user_daily_metric_tmp') }}
),

features AS (
    SELECT
        user_id,
        activity_date AS prediction_date,
        year(activity_date)  AS prediction_year,
        month(activity_date) AS prediction_month,

        /* ================= LABEL ================= */
        CASE
            WHEN LEAD(activity_date) OVER (PARTITION BY user_id ORDER BY activity_date)
                 = date_add(activity_date, 1)
            THEN LEAD(has_purchase_today) OVER (PARTITION BY user_id ORDER BY activity_date)
            ELSE 0
        END AS label_purchase_tomorrow,

        /* ================= 3-DAY FEATURES ================= */
        SUM(sessions_count) OVER w AS sessions_3d,
        SUM(total_duration) OVER w AS total_duration_3d,

        CASE
            WHEN SUM(sessions_count) OVER w > 0
            THEN SUM(total_duration) OVER w / SUM(sessions_count) OVER w
            ELSE 0
        END AS avg_session_duration_3d,

        SUM(total_page_views) OVER w AS total_page_views_3d,
        SUM(total_actions) OVER w AS total_actions_3d,
        SUM(purchase_sessions_count) OVER w AS purchase_sessions_3d,
        SUM(total_revenue) OVER w AS total_revenue_3d,

        SUM(view_count) OVER w AS view_count_3d,
        SUM(add_to_cart_count) OVER w AS add_to_cart_count_3d,
        SUM(purchase_count) OVER w AS purchase_count_3d,
        SUM(search_count) OVER w AS search_count_3d,
        SUM(wishlist_count) OVER w AS wishlist_count_3d,
        SUM(checkout_view_count) OVER w AS checkout_view_count_3d,
        SUM(distinct_products_viewed) OVER w AS distinct_products_3d,

        AVG(avg_product_price) OVER w AS avg_product_price_3d,

        /* ================= DERIVED METRICS ================= */
        CASE
            WHEN SUM(view_count) OVER w > 0
            THEN SUM(add_to_cart_count) OVER w / SUM(view_count) OVER w
            ELSE 0
        END AS cart_conversion_rate_3d,

        CASE
            WHEN SUM(add_to_cart_count) OVER w > 0
            THEN SUM(purchase_count) OVER w / SUM(add_to_cart_count) OVER w
            ELSE 0
        END AS purchase_conversion_rate_3d,

        CASE
            WHEN SUM(sessions_count) OVER w > 0
            THEN SUM(total_actions) OVER w / SUM(sessions_count) OVER w
            ELSE 0
        END AS actions_per_session_3d,

        CASE
            WHEN SUM(sessions_count) OVER w > 0
            THEN SUM(purchase_sessions_count) OVER w / SUM(sessions_count) OVER w
            ELSE 0
        END AS purchase_session_rate_3d

    FROM base
    WINDOW w AS (
        PARTITION BY user_id
        ORDER BY activity_date
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    )
)

SELECT *
FROM features
where 
    sessions_3d IS NOT NULL 
    and sessions_3d > 0

