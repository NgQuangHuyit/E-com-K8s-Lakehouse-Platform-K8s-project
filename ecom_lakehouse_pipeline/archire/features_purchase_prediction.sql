{{
    config(
        materialized='table',
        file_format='delta',
        partition_by=['year', 'month', 'day']
    )
}}

-- Feature engineering for purchase prediction model
-- Optimized for Spark Thrift performance - single scan approach

with session_base_info as (
    select
        us.session_id,
        us.user_id,
        us.duration_seconds,
        us.page_views,
        us.device_type,
        us.actions_count,
        us.has_purchase,
        us.user_segment,
        us.referrer_type,
        us.year,
        us.month,
        us.day,
        hour(us.timestamp) as hour_of_day,
        case 
            when dayofweek(us.timestamp) in (1, 7) then 1
            else 0
        end as is_weekend
    from {{ ref('user_sessions') }} us
),

-- Single scan of user_actions to compute all action-based features
action_features as (
    select 
        ua.session_id,
        
        -- Action type counts
        sum(ua.is_view) as num_views,
        sum(ua.is_add_to_cart) as num_add_to_cart,
        sum(ua.is_wishlist) as num_wishlist,
        sum(ua.is_search) as num_searches,
        sum(ua.is_review) as num_reviews,
        sum(ua.is_remove_from_cart) as num_remove_from_cart,
        max(ua.is_checkout_view) as has_checkout_view,
        
        -- Product diversity (computed in same scan)
        count(distinct case when ua.product_id is not null then ua.product_id end) as unique_products_viewed,
        count(distinct case when ua.product_category is not null then ua.product_category end) as unique_categories_viewed,
        
        -- Price features (computed in same scan)
        avg(case when ua.product_price is not null then ua.product_price end) as avg_product_price,
        max(case when ua.product_price is not null then ua.product_price end) as max_price_viewed,
        min(case when ua.product_price is not null then ua.product_price end) as min_price_viewed
        
    from {{ ref('user_actions') }} ua
    group by ua.session_id
)

-- Final feature set with all calculations
select
    sbi.session_id,
    sbi.user_id,
    
    -- Label
    sbi.has_purchase as label,
    
    -- Core behavior metrics
    sbi.duration_seconds,
    sbi.actions_count,
    sbi.page_views,
    
    -- Calculated behavior metrics
    case 
        when sbi.duration_seconds > 0 
        then cast(sbi.actions_count as double) / sbi.duration_seconds 
        else 0.0
    end as action_velocity,
    
    case 
        when sbi.duration_seconds > 0 and sbi.page_views > 0
        then cast(sbi.duration_seconds as double) / sbi.page_views
        else 0.0
    end as avg_time_per_page,
    
    -- Action counts with null handling
    coalesce(af.num_views, 0) as num_views,
    coalesce(af.num_add_to_cart, 0) as num_add_to_cart,
    coalesce(af.num_wishlist, 0) as num_wishlist,
    coalesce(af.num_searches, 0) as num_searches,
    coalesce(af.num_reviews, 0) as num_reviews,
    coalesce(af.num_remove_from_cart, 0) as num_remove_from_cart,
    coalesce(af.has_checkout_view, 0) as has_checkout_view,
    
    -- Boolean features
    case when coalesce(af.num_searches, 0) > 0 then 1 else 0 end as has_search,
    case when coalesce(af.num_remove_from_cart, 0) > 0 then 1 else 0 end as has_cart_removal,
    
    -- Conversion rate
    case 
        when coalesce(af.num_views, 0) > 0 
        then cast(coalesce(af.num_add_to_cart, 0) as double) / af.num_views
        else 0.0
    end as cart_conversion_rate,
    
    -- Diversity metrics
    coalesce(af.unique_products_viewed, 0) as unique_products_viewed,
    coalesce(af.unique_categories_viewed, 0) as unique_categories_viewed,
    
    -- Price metrics
    coalesce(af.avg_product_price, 0.0) as avg_product_price,
    coalesce(af.max_price_viewed, 0.0) - coalesce(af.min_price_viewed, 0.0) as price_range_interest,
    coalesce(af.max_price_viewed, 0.0) as max_price_viewed,
    coalesce(af.min_price_viewed, 0.0) as min_price_viewed,
    
    -- Time-based features
    sbi.hour_of_day,
    sbi.is_weekend,
    
    -- Device feature  
    case when sbi.device_type = 'mobile' then 1 else 0 end as is_mobile,
    
    -- User and marketing features
    sbi.user_segment,
    sbi.referrer_type,
    
    -- Partition columns
    sbi.year,
    sbi.month,
    sbi.day
    
from session_base_info sbi
left join action_features af
    on sbi.session_id = af.session_id
