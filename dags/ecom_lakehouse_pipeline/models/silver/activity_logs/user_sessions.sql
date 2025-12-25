{{
    config(
        materialized='table',
        unique_key='session_id',
        file_format='delta',
        partition_by=['year', 'month', 'day']
    )
}}



-- Flatten user activity logs from bronze to silver with enhanced session-level aggregations
with raw_events as (
    select
        session_id,
        user_id,
        to_timestamp(timestamp) as timestamp,
        -- date(to_timestamp(timestamp)) as session_date,
        -- hour(to_timestamp(timestamp)) as hour_of_day,
        -- case 
        --     when dayofweek(to_timestamp(timestamp)) in (1, 7) then true  -- Sunday=1, Saturday=7
        --     else false 
        -- end as is_weekend,
        
        -- Device info
        device.type as device_type,
        device.os as device_os,
        device.browser as browser,
        
        -- Location
        location.city as city,
        location.country as country,
        location.coordinates.lat as latitude,
        location.coordinates.lon as longitude,
        
        -- Session info
        CAST(session_metrics.duration_seconds as INT) as duration_seconds,
        CAST(session_metrics.page_views as INT) as page_views,
        CAST(session_metrics.actions_count as INT) as actions_count,
        CAST(session_metrics.has_purchase as BOOLEAN) as has_purchase,
        CAST(session_metrics.revenue as FLOAT) as revenue,
        
        -- User segment
        user_segment,
        
        -- Traffic (source and campaign are top-level fields, not in properties)
        referrer,
        referrer_type,
        coalesce(source, 'direct') as utm_source,
        campaign as utm_campaign,
        
        -- Device properties
        CAST(properties.is_mobile as BOOLEAN) as is_mobile,
        properties.language as language,
        properties.ab_test as ab_test,
            
        year(ingest_date) as year,
        month(ingest_date) as month,
        day(ingest_date) as day
        
    from {{ source('bronze', 'user_activity_logs') }}
    

)

select * from raw_events
