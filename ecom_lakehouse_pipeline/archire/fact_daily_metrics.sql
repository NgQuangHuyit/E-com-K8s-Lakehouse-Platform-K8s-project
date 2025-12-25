-- {{
--     config(
--         materialized='incremental',
--         unique_key=['metric_date', 'traffic_source', 'device_type'],
--         file_format='delta',
--         incremental_strategy='merge',
--         partition_by=['metric_date']
--     )
-- }}

-- -- Daily aggregated metrics for dashboard
-- with daily_behavior as (
--     select
--         action_date as metric_date,
--         traffic_source,
--         device_type,
        
--         -- User & session counts
--         count(distinct user_id) as active_users,
--         count(distinct session_id) as total_sessions,
        
--         -- Action counts
--         sum(is_view) as total_views,
--         sum(is_search) as total_searches,
--         sum(is_add_to_cart) as total_add_to_carts,
--         sum(is_purchase) as total_purchases,
        
--         -- Revenue
--         sum(case when is_purchase = 1 then revenue else 0 end) as total_revenue,
        
--         -- Session metrics
--         avg(session_duration) as avg_session_duration,
--         avg(session_page_views) as avg_page_views,
        
--         -- Conversion
--         count(distinct case when is_purchase = 1 then session_id end) as converted_sessions,
        
--         current_timestamp() as created_at
        
--     from {{ ref('fact_user_behavior') }}
    
--     {% if is_incremental() %}
--     where action_date >= (select max(metric_date) from {{ this }})
--     {% endif %}
    
--     group by 1, 2, 3
-- )

-- select
--     *,
--     round(100.0 * converted_sessions / nullif(total_sessions, 0), 2) as conversion_rate_pct,
--     round(total_revenue / nullif(total_purchases, 0), 2) as avg_order_value
-- from daily_behavior
