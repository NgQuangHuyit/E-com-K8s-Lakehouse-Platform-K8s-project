# User Activity Data Pipeline - Simplified

## Architecture

```
Bronze (Raw JSON)
    ↓
Silver: user_events (flattened events)
    ↓
Gold:
  ├── fact_user_behavior (detailed actions)
  └── fact_daily_metrics (aggregated KPIs)
```

## Models Overview

### Silver Layer

**`user_events`** - Flattened event data
- **Purpose**: Parse nested JSON from bronze into clean columns
- **Grain**: One row per event
- **Key Fields**: event_id, user_id, session_id, device info, location, actions array
- **Partitioned by**: event_date

### Gold Layer

**`fact_user_behavior`** - User behavior fact table
- **Purpose**: Detailed action-level analytics
- **Grain**: One row per action (exploded from actions array)
- **Key Fields**: 
  - Action details: action_type, product_id, revenue
  - Context: device_type, city, traffic_source
  - Flags: is_view, is_add_to_cart, is_purchase, is_search
  - Foreign keys: product_sk, date_sk
- **Partitioned by**: action_date
- **Use Cases**: 
  - Product performance analysis
  - User journey tracking
  - Conversion funnel
  - Search behavior

**`fact_daily_metrics`** - Daily aggregated metrics
- **Purpose**: Pre-computed KPIs for dashboards
- **Grain**: One row per date, traffic_source, device_type
- **Key Metrics**:
  - active_users, total_sessions
  - total_views, total_searches, total_add_to_carts, total_purchases
  - total_revenue, avg_order_value
  - conversion_rate_pct
  - avg_session_duration, avg_page_views
- **Partitioned by**: metric_date
- **Use Cases**:
  - Executive dashboards
  - Traffic source comparison
  - Device performance
  - Daily trends

## Running the Pipeline

```bash
# Build silver layer
dbt run --models user_events

# Build gold layer
dbt run --models fact_user_behavior fact_daily_metrics

# Or run everything
dbt run --models user_events+

# Run tests
dbt test --models user_events+
```

## Example Queries

### 1. Conversion Funnel by Traffic Source
```sql
select
    traffic_source,
    sum(is_view) as views,
    sum(is_add_to_cart) as add_to_carts,
    sum(is_purchase) as purchases,
    round(100.0 * sum(is_add_to_cart) / nullif(sum(is_view), 0), 2) as view_to_cart_pct,
    round(100.0 * sum(is_purchase) / nullif(sum(is_add_to_cart), 0), 2) as cart_to_purchase_pct
from {{ ref('fact_user_behavior') }}
where action_date >= current_date - 7
group by traffic_source
order by purchases desc;
```

### 2. Top Products by Action Type
```sql
select
    p.product_name,
    p.category,
    sum(is_view) as views,
    sum(is_add_to_cart) as carts,
    sum(is_purchase) as purchases,
    sum(case when is_purchase = 1 then revenue else 0 end) as total_revenue
from {{ ref('fact_user_behavior') }} f
join {{ ref('dim_products') }} p on f.product_sk = p.product_sk
where f.action_date >= current_date - 30
group by p.product_name, p.category
order by total_revenue desc
limit 20;
```

### 3. Daily Performance Dashboard
```sql
select
    metric_date,
    sum(active_users) as daily_active_users,
    sum(total_sessions) as sessions,
    sum(total_purchases) as orders,
    sum(total_revenue) as revenue,
    round(avg(conversion_rate_pct), 2) as avg_conversion_rate,
    round(sum(total_revenue) / nullif(sum(total_purchases), 0), 2) as avg_order_value
from {{ ref('fact_daily_metrics') }}
where metric_date >= current_date - 30
group by metric_date
order by metric_date desc;
```

### 4. Device & Traffic Source Performance
```sql
select
    device_type,
    traffic_source,
    sum(active_users) as users,
    sum(total_sessions) as sessions,
    sum(total_purchases) as purchases,
    sum(total_revenue) as revenue,
    round(avg(conversion_rate_pct), 2) as conversion_rate,
    round(avg(avg_session_duration), 0) as avg_duration_sec
from {{ ref('fact_daily_metrics') }}
where metric_date >= current_date - 7
group by device_type, traffic_source
order by revenue desc;
```

## Integration with Existing Models

The pipeline integrates with your existing dimensional models:
- **`dim_products`**: Joined via product_id → product_sk
- **`dim_date`**: Joined via action_date → date_sk

This allows you to combine user behavior data with your existing orders, sales, and customer data.

## Performance Tips

1. **Partition Pruning**: Always filter by date columns (event_date, action_date, metric_date)
2. **Incremental Loads**: Models automatically process only new data
3. **Use Aggregates**: Use `fact_daily_metrics` for dashboards instead of aggregating `fact_user_behavior`
4. **Index Frequently Used Columns**: Consider Z-ordering on user_id, product_id, session_id

## Next Steps

1. Run `dbt run --models user_events+` to build all models
2. Run `dbt test` to validate data quality
3. Create your analytics dashboards using the example queries
4. Monitor query performance and add optimizations as needed
