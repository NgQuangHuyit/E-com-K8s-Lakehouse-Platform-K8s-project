{{
    config(
        materialized='table',
        file_format='parquet',
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
        s.user_id,
        CAST(a.action_timestamp AS DATE) AS action_date,
        SUM(a.is_view) AS view_count,
        SUM(a.is_add_to_cart) AS add_to_cart_count,
        SUM(a.is_purchase) AS purchase_count,
        SUM(a.is_search) AS search_count,
        SUM(a.is_wishlist) AS wishlist_count,
        SUM(a.is_checkout_view) AS checkout_view_count,
        COUNT(DISTINCT a.product_id) AS distinct_products_viewed,
        AVG(CAST(a.product_price AS DOUBLE)) AS avg_product_price,
        SUM(CAST(a.revenue AS DOUBLE)) AS action_revenue
    FROM {{ ref('session_actions') }} a
    INNER JOIN {{ ref('user_sessions') }} s ON a.session_id = s.session_id
    {% if is_incremental() %}
    WHERE CAST(a.action_timestamp AS DATE) >= (SELECT date_sub(min_date, 3) FROM date_filter)
    {% endif %}
    GROUP BY s.user_id, CAST(a.action_timestamp AS DATE)