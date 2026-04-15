{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='action_id',
        file_format='delta',
        schema='silver',
        partition_by=['year', 'month', 'day']
    )
}}


with exploded_actions as (
    select
        session_id,
        -- Action details
        timestamp,
        posexplode(actions) as (action_index, action),
        ingest_date
        
    from {{ source('bronze', 'user_activity_logs') }}
    {% if is_incremental() %}
    where ingest_date = '{{ var('etl_date') }}'
    {% endif %}
)



select
    md5(concat(ea.session_id, cast(ea.action_index as string))) as action_id,
    ea.session_id as session_id,
    to_timestamp(ea.timestamp) + CAST(ea.action.time_offset as INT) * INTERVAL 1 SECOND AS action_timestamp,
    
    -- Action details (access struct fields from the exploded action)
    ea.action.type as action_type,
    ea.action.product_id as product_id,
    ea.action.search_term as search_term,
    ea.action.order_id as order_id,
    ea.action.total_amount as revenue,
    ea.action.price as product_price,
    ea.action.category as product_category,
    -- Flags for easy filtering
    case when ea.action.type = 'view' then 1 else 0 end as is_view,
    case when ea.action.type = 'add_to_cart' then 1 else 0 end as is_add_to_cart,
    case when ea.action.type = 'purchase' then 1 else 0 end as is_purchase,
    case when ea.action.type = 'search' then 1 else 0 end as is_search,
    case when ea.action.type = 'wishlist' then 1 else 0 end as is_wishlist,
    case when ea.action.type = 'review' then 1 else 0 end as is_review,
    case when ea.action.type = 'remove_from_cart' then 1 else 0 end as is_remove_from_cart,
    case when ea.action.type = 'view' and ea.action.page = '/checkout' then 1 else 0 end as is_checkout_view,
    year(ea.ingest_date) as year,
    month(ea.ingest_date) as month,
    day(ea.ingest_date) as day
from exploded_actions ea



