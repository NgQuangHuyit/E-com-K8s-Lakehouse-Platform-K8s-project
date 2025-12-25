{{
    config(
        materialized='incremental',
        unique_key='behavior_id',
        file_format='delta',
        incremental_strategy='merge',
        partition_by=['action_date']
    )
}}

-- Main fact table for user behavior analytics
with user_events as (
    select * from {{ ref('user_sessions') }}
    
    {% if is_incremental() %}
    where event_date >= (select max(action_date) from {{ this }})
    {% endif %}
),

-- Explode actions array
exploded_actions as (
    select
        event_id,
        user_id,
        session_id,
        event_date,
        timestamp,
        
        -- Device & location
        device_type,
        device_os,
        browser,
        city,
        country,
        latitude,
        longitude,
        -- Traffic source
        referrer,
        utm_source,
        utm_campaign,
        
        -- Session metrics
        duration_seconds,
        page_views,
        
        -- Action details
        posexplode(actions) as (action_index, action),
        ingest_date
        
    from user_events 
),

final as (
    select
        md5(concat(event_id, cast(action_index as string))) as behavior_id,
        event_id,
        user_id,
        session_id,
        date(timestamp) as action_date,
        timestamp as action_timestamp,
        
        -- Action details (access struct fields from the exploded action)
        action.type as action_type,
        action.product_id,
        action.search_term,
        action.order_id,
        action.total_amount as revenue,
        
        -- Session context
        duration_seconds as session_duration,
        page_views as session_page_views,
        
        -- Device & location
        device_type,
        device_os,
        city,
        country,
        latitude,
        longitude,
        
        -- Traffic
        coalesce(utm_source, 'direct') as traffic_source,
        utm_campaign,
        
        -- Flags for easy filtering
        case when action.type = 'view' then 1 else 0 end as is_view,
        case when action.type = 'add_to_cart' then 1 else 0 end as is_add_to_cart,
        case when action.type = 'purchase' then 1 else 0 end as is_purchase,
        case when action.type = 'search' then 1 else 0 end as is_search,
        
        -- Join to existing dimensions
        p.product_sk,
        d.datekey as date_sk,
        
        current_timestamp() as created_at,
        ingest_date
        
    from exploded_actions
    left join {{ ref('dim_products') }} p 
        on action.product_id = p.product_nk 
        and p.is_current = true
    left join {{ ref('dim_date') }} d 
        on date(timestamp) = d.CalendarDate
)

select * from final
