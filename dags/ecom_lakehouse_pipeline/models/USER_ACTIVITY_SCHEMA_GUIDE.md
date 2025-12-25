# User Activity Data Schema Guide

## Overview

This document provides detailed schema definitions for the user activity data pipeline, including all columns, data types, and descriptions for each table in the Silver and Gold layers.

---

## Silver Layer

### `user_events`

**Purpose**: Flattened user activity events from bronze layer with parsed JSON structures.

**Materialization**: Incremental (merge strategy)  
**Partition Key**: `event_date`  
**Unique Key**: `event_id`  
**Grain**: One row per event

#### Schema

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `event_id` | STRING | NO | Unique identifier for the event (UUID) |
| `user_id` | BIGINT | NO | User identifier (range: 1-100000) |
| `timestamp` | TIMESTAMP | NO | Event timestamp in UTC |
| `event_date` | DATE | NO | Date extracted from timestamp (partition key) |
| `device_type` | STRING | YES | Device type (Desktop, Mobile, Tablet) |
| `device_os` | STRING | YES | Operating system (Windows, macOS, Linux, iOS, Android) |
| `browser` | STRING | YES | Browser name (Chrome, Firefox, Safari, Edge, etc.) |
| `city` | STRING | YES | City name (Ho Chi Minh, Hanoi, Da Nang, etc.) |
| `country` | STRING | YES | Country name (default: Vietnam) |
| `session_id` | STRING | NO | Session identifier (UUID) |
| `duration_seconds` | INTEGER | YES | Session duration in seconds |
| `page_views` | INTEGER | YES | Number of page views in the session |
| `referrer` | STRING | YES | Full referrer URL |
| `utm_source` | STRING | YES | UTM source parameter (google, facebook, email, etc.) |
| `utm_campaign` | STRING | YES | UTM campaign name |
| `actions` | ARRAY<STRUCT> | YES | Array of action objects (see structure below) |
| `ingest_date` | DATE | NO | Date when data was ingested into bronze layer |

#### Actions Array Structure

Each element in the `actions` array contains:

```sql
STRUCT<
  action: STRING,           -- Action type: view, search, add_to_cart, remove_from_cart, purchase, wishlist, review
  product_id: BIGINT,       -- Product ID (1-1000), present for: view, add_to_cart, remove_from_cart, purchase, wishlist, review
  search_term: STRING,      -- Search query, present only for: search
  order_id: STRING,         -- Order UUID, present only for: purchase
  revenue: DECIMAL(10,2)    -- Revenue amount, present only for: purchase
>
```

#### Sample Row

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": 12345,
  "timestamp": "2025-11-15T14:30:45.123Z",
  "event_date": "2025-11-15",
  "device_type": "Desktop",
  "device_os": "Windows",
  "browser": "Chrome",
  "city": "Ho Chi Minh",
  "country": "Vietnam",
  "session_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "duration_seconds": 180,
  "page_views": 5,
  "referrer": "https://google.com",
  "utm_source": "google",
  "utm_campaign": "spring_sale",
  "actions": [
    {"action": "view", "product_id": 456},
    {"action": "add_to_cart", "product_id": 456},
    {"action": "purchase", "product_id": 456, "order_id": "order-123", "revenue": 299.99}
  ],
  "ingest_date": "2025-11-15"
}
```

---

## Gold Layer

### `fact_user_behavior`

**Purpose**: Detailed action-level fact table for user behavior analytics.

**Materialization**: Incremental (merge strategy)  
**Partition Key**: `action_date`  
**Unique Key**: `behavior_id`  
**Grain**: One row per action (exploded from actions array)

#### Schema

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `behavior_id` | STRING | NO | Unique identifier (MD5 hash of event_id + action_index) |
| `event_id` | STRING | NO | Original event identifier from user_events |
| `user_id` | BIGINT | NO | User identifier |
| `session_id` | STRING | NO | Session identifier |
| `action_date` | DATE | NO | Date of the action (partition key) |
| `action_timestamp` | TIMESTAMP | NO | Exact timestamp of the action |
| `action_type` | STRING | NO | Type of action performed |
| `product_id` | BIGINT | YES | Product identifier (NULL for search actions) |
| `search_term` | STRING | YES | Search query (only for search actions) |
| `order_id` | STRING | YES | Order identifier (only for purchase actions) |
| `revenue` | DECIMAL(10,2) | YES | Revenue amount (only for purchase actions) |
| `session_duration` | INTEGER | YES | Session duration in seconds |
| `session_page_views` | INTEGER | YES | Number of page views in the session |
| `device_type` | STRING | YES | Device type |
| `device_os` | STRING | YES | Operating system |
| `city` | STRING | YES | City name |
| `country` | STRING | YES | Country name |
| `traffic_source` | STRING | NO | Traffic source (defaults to 'direct' if NULL) |
| `utm_campaign` | STRING | YES | UTM campaign name |
| `is_view` | INTEGER | NO | Flag: 1 if action is 'view', else 0 |
| `is_add_to_cart` | INTEGER | NO | Flag: 1 if action is 'add_to_cart', else 0 |
| `is_purchase` | INTEGER | NO | Flag: 1 if action is 'purchase', else 0 |
| `is_search` | INTEGER | NO | Flag: 1 if action is 'search', else 0 |
| `product_sk` | BIGINT | YES | Foreign key to dim_products.product_sk |
| `date_sk` | BIGINT | YES | Foreign key to dim_date.date_sk |
| `created_at` | TIMESTAMP | NO | Record creation timestamp |
| `ingest_date` | DATE | NO | Original ingest date from bronze |

#### Action Types

| Action Type | Description | Has product_id | Has search_term | Has order_id | Has revenue |
|------------|-------------|----------------|-----------------|--------------|-------------|
| `view` | User viewed a product | ✓ | ✗ | ✗ | ✗ |
| `search` | User performed a search | ✗ | ✓ | ✗ | ✗ |
| `add_to_cart` | User added product to cart | ✓ | ✗ | ✗ | ✗ |
| `remove_from_cart` | User removed product from cart | ✓ | ✗ | ✗ | ✗ |
| `purchase` | User completed a purchase | ✓ | ✗ | ✓ | ✓ |
| `wishlist` | User added product to wishlist | ✓ | ✗ | ✗ | ✗ |
| `review` | User reviewed a product | ✓ | ✗ | ✗ | ✗ |

#### Sample Row

```json
{
  "behavior_id": "5f2b8e7a9c1d3e4f6a8b9c0d1e2f3a4b",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": 12345,
  "session_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "action_date": "2025-11-15",
  "action_timestamp": "2025-11-15T14:30:45.123Z",
  "action_type": "purchase",
  "product_id": 456,
  "search_term": null,
  "order_id": "order-123",
  "revenue": 299.99,
  "session_duration": 180,
  "session_page_views": 5,
  "device_type": "Desktop",
  "device_os": "Windows",
  "city": "Ho Chi Minh",
  "country": "Vietnam",
  "traffic_source": "google",
  "utm_campaign": "spring_sale",
  "is_view": 0,
  "is_add_to_cart": 0,
  "is_purchase": 1,
  "is_search": 0,
  "product_sk": 789,
  "date_sk": 20251115,
  "created_at": "2025-11-15T15:00:00.000Z",
  "ingest_date": "2025-11-15"
}
```

---

### `fact_daily_metrics`

**Purpose**: Pre-aggregated daily KPIs for dashboard and reporting.

**Materialization**: Incremental (merge strategy)  
**Partition Key**: `metric_date`  
**Unique Key**: [`metric_date`, `traffic_source`, `device_type`]  
**Grain**: One row per date, traffic source, and device type combination

#### Schema

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `metric_date` | DATE | NO | Date of the metrics (partition key) |
| `traffic_source` | STRING | NO | Traffic source (part of composite key) |
| `device_type` | STRING | NO | Device type (part of composite key) |
| `active_users` | BIGINT | NO | Count of distinct users |
| `total_sessions` | BIGINT | NO | Count of distinct sessions |
| `total_views` | BIGINT | NO | Sum of view actions |
| `total_searches` | BIGINT | NO | Sum of search actions |
| `total_add_to_carts` | BIGINT | NO | Sum of add_to_cart actions |
| `total_purchases` | BIGINT | NO | Sum of purchase actions |
| `total_revenue` | DECIMAL(12,2) | NO | Sum of revenue from purchases |
| `avg_session_duration` | DOUBLE | YES | Average session duration in seconds |
| `avg_page_views` | DOUBLE | YES | Average page views per session |
| `converted_sessions` | BIGINT | NO | Count of sessions with at least one purchase |
| `created_at` | TIMESTAMP | NO | Record creation timestamp |
| `conversion_rate_pct` | DECIMAL(5,2) | YES | Percentage of sessions that converted (converted_sessions / total_sessions * 100) |
| `avg_order_value` | DECIMAL(10,2) | YES | Average revenue per purchase (total_revenue / total_purchases) |

#### Sample Row

```json
{
  "metric_date": "2025-11-15",
  "traffic_source": "google",
  "device_type": "Desktop",
  "active_users": 15234,
  "total_sessions": 23456,
  "total_views": 98765,
  "total_searches": 12345,
  "total_add_to_carts": 5432,
  "total_purchases": 876,
  "total_revenue": 262740.24,
  "avg_session_duration": 156.8,
  "avg_page_views": 4.2,
  "converted_sessions": 876,
  "created_at": "2025-11-15T23:59:59.000Z",
  "conversion_rate_pct": 3.73,
  "avg_order_value": 299.93
}
```

---

## Data Relationships

### Entity Relationship Diagram

```
┌─────────────────────┐
│   user_events       │
│   (Silver)          │
│                     │
│ PK: event_id        │
│     event_date      │
└──────────┬──────────┘
           │
           │ 1:N (explode actions array)
           │
           ▼
┌─────────────────────┐         ┌──────────────────┐
│ fact_user_behavior  │────────▶│  dim_products    │
│   (Gold)            │  N:1    │                  │
│                     │         │ PK: product_sk   │
│ PK: behavior_id     │         │ NK: product_id   │
│ FK: product_sk      │         └──────────────────┘
│ FK: date_sk         │
│     action_date     │         ┌──────────────────┐
└──────────┬──────────┘         │   dim_date       │
           │              N:1   │                  │
           └───────────────────▶│ PK: date_sk      │
                                │ NK: date         │
                                └──────────────────┘
           │
           │ Aggregates to
           │
           ▼
┌─────────────────────┐
│ fact_daily_metrics  │
│   (Gold)            │
│                     │
│ PK: metric_date     │
│     traffic_source  │
│     device_type     │
└─────────────────────┘
```

### Foreign Key Relationships

1. **fact_user_behavior → dim_products**
   - Join: `fact_user_behavior.product_sk = dim_products.product_sk`
   - Cardinality: Many-to-One
   - Note: product_sk can be NULL for search actions

2. **fact_user_behavior → dim_date**
   - Join: `fact_user_behavior.date_sk = dim_date.date_sk`
   - Cardinality: Many-to-One

---

## Data Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  Bronze Layer: bronze.user_activity_logs                         │
│  - Raw JSON events with nested structures                        │
│  - Partitioned by ingest_date                                    │
│  - Format: NDJSON (newline-delimited JSON)                       │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         │ Parse JSON, flatten device/location
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  Silver Layer: user_events                                       │
│  - Flattened events with clean columns                           │
│  - Device, location, session info extracted                      │
│  - Actions still in array format                                 │
│  - Incremental: merge new events by event_id                     │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         │ Explode actions array (posexplode)
                         │ Calculate flags (is_view, is_purchase, etc.)
                         │ Join to dim_products, dim_date
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  Gold Layer: fact_user_behavior                                  │
│  - One row per action                                            │
│  - Product and date foreign keys                                 │
│  - Action type flags for easy filtering                          │
│  - Incremental: merge new actions by behavior_id                 │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         │ GROUP BY date, traffic_source, device
                         │ Aggregate: COUNT, SUM, AVG
                         │ Calculate conversion_rate_pct, avg_order_value
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  Gold Layer: fact_daily_metrics                                  │
│  - Pre-computed daily KPIs                                       │
│  - Fast dashboard queries                                        │
│  - Incremental: merge by composite key                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## Incremental Processing Logic

### user_events
```sql
-- Only process new data since last run
WHERE ingest_date >= (SELECT MAX(ingest_date) FROM {{ this }})
```

### fact_user_behavior
```sql
-- Only process events from dates not yet processed
WHERE event_date >= (SELECT MAX(action_date) FROM {{ this }})
```

### fact_daily_metrics
```sql
-- Recompute metrics for dates with new data
WHERE action_date >= (SELECT MAX(metric_date) FROM {{ this }})
```

---

## Data Quality Rules

### user_events
- `event_id` must be unique
- `user_id` must not be NULL
- `event_date` must not be NULL
- `event_date` should match `date(timestamp)`

### fact_user_behavior
- `behavior_id` must be unique
- `action_type` must be one of: view, search, add_to_cart, remove_from_cart, purchase, wishlist, review
- `product_sk` must exist in `dim_products` (except for search actions)
- `date_sk` must exist in `dim_date`
- `is_view + is_add_to_cart + is_purchase + is_search` should equal 1 for main action types
- `revenue` should only be present when `is_purchase = 1`
- `product_id` should be NULL only for search actions

### fact_daily_metrics
- `metric_date` must not be NULL
- `conversion_rate_pct` should be between 0 and 100
- `converted_sessions` should be <= `total_sessions`
- `total_purchases` should be <= `total_add_to_carts`
- `avg_order_value` = `total_revenue / total_purchases` (when total_purchases > 0)

---

## Usage Examples

### Query 1: User Journey for a Specific Session
```sql
SELECT
    action_timestamp,
    action_type,
    product_id,
    search_term,
    revenue
FROM fact_user_behavior
WHERE session_id = '7c9e6679-7425-40de-944b-e07fc1f90ae7'
ORDER BY action_timestamp;
```

### Query 2: Daily Conversion Funnel
```sql
SELECT
    action_date,
    SUM(is_view) as total_views,
    SUM(is_add_to_cart) as total_add_to_carts,
    SUM(is_purchase) as total_purchases,
    ROUND(100.0 * SUM(is_add_to_cart) / NULLIF(SUM(is_view), 0), 2) as view_to_cart_pct,
    ROUND(100.0 * SUM(is_purchase) / NULLIF(SUM(is_add_to_cart), 0), 2) as cart_to_purchase_pct
FROM fact_user_behavior
WHERE action_date >= CURRENT_DATE - 30
GROUP BY action_date
ORDER BY action_date DESC;
```

### Query 3: Top Performing Traffic Sources
```sql
SELECT
    traffic_source,
    device_type,
    SUM(active_users) as users,
    SUM(total_revenue) as revenue,
    AVG(conversion_rate_pct) as avg_conversion_rate,
    SUM(total_revenue) / NULLIF(SUM(active_users), 0) as revenue_per_user
FROM fact_daily_metrics
WHERE metric_date >= CURRENT_DATE - 7
GROUP BY traffic_source, device_type
ORDER BY revenue DESC;
```

### Query 4: Product Performance Analysis
```sql
SELECT
    p.product_name,
    p.category,
    COUNT(DISTINCT f.user_id) as unique_viewers,
    SUM(f.is_view) as views,
    SUM(f.is_add_to_cart) as add_to_carts,
    SUM(f.is_purchase) as purchases,
    SUM(CASE WHEN f.is_purchase = 1 THEN f.revenue ELSE 0 END) as total_revenue,
    ROUND(100.0 * SUM(f.is_add_to_cart) / NULLIF(SUM(f.is_view), 0), 2) as view_to_cart_rate,
    ROUND(100.0 * SUM(f.is_purchase) / NULLIF(SUM(f.is_add_to_cart), 0), 2) as cart_to_purchase_rate
FROM fact_user_behavior f
JOIN dim_products p ON f.product_sk = p.product_sk
WHERE f.action_date >= CURRENT_DATE - 30
GROUP BY p.product_name, p.category
HAVING SUM(f.is_view) >= 100
ORDER BY total_revenue DESC
LIMIT 20;
```

---

## Performance Optimization

### Partitioning Strategy
All tables are partitioned by date columns for optimal query performance:
- `user_events`: Partitioned by `event_date`
- `fact_user_behavior`: Partitioned by `action_date`
- `fact_daily_metrics`: Partitioned by `metric_date`

**Best Practice**: Always include date filters in WHERE clauses to leverage partition pruning.

### Indexing Recommendations
Consider Z-ordering (Delta Lake optimization) on frequently filtered columns:
- `user_events`: Z-order by `user_id`, `session_id`
- `fact_user_behavior`: Z-order by `user_id`, `product_id`, `action_type`
- `fact_daily_metrics`: Already optimized by grain

### Query Performance Tips
1. **Use fact_daily_metrics for dashboards** instead of aggregating fact_user_behavior
2. **Filter by date first** to reduce data scan
3. **Use action type flags** (is_view, is_purchase) instead of string comparisons on action_type
4. **Leverage incremental processing** to avoid full table scans during dbt runs

---

## Maintenance

### Daily Operations
```bash
# Run incremental update (processes only new data)
dbt run --models user_events+

# Run data quality tests
dbt test --models user_events+

# Check for data freshness
dbt source freshness
```

### Full Refresh (if needed)
```bash
# Rebuild from scratch
dbt run --models user_events+ --full-refresh
```

### Monitoring Queries
```sql
-- Check data freshness
SELECT MAX(event_date) as latest_event_date FROM user_events;
SELECT MAX(action_date) as latest_action_date FROM fact_user_behavior;
SELECT MAX(metric_date) as latest_metric_date FROM fact_daily_metrics;

-- Check data volume
SELECT COUNT(*) as event_count FROM user_events;
SELECT COUNT(*) as behavior_count FROM fact_user_behavior;
SELECT COUNT(*) as metric_count FROM fact_daily_metrics;

-- Check data quality
SELECT 
    COUNT(*) as total_behaviors,
    SUM(CASE WHEN product_sk IS NULL AND action_type != 'search' THEN 1 ELSE 0 END) as missing_product_refs
FROM fact_user_behavior
WHERE action_date = CURRENT_DATE - 1;
```

---

## Change Log

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-11-15 | Initial schema documentation |

---

For questions or issues, please refer to the dbt project documentation or contact the data engineering team.
