# E-Commerce Lakehouse Platform - Business Requirements & Data Model

## Executive Summary

This document outlines the business requirements, use cases, and data model architecture for our E-Commerce Lakehouse Analytics Platform. The platform integrates transactional data from OLTP systems and behavioral data from user activity logs to provide comprehensive analytics across sales performance, customer behavior, and marketing effectiveness.

---

## Table of Contents

1. [Business Context](#business-context)
2. [Data Sources](#data-sources)
3. [End-User Requirements](#end-user-requirements)
4. [Business Processes](#business-processes)
5. [Data Model Architecture](#data-model-architecture)
6. [Fact & Dimension Tables](#fact--dimension-tables)
7. [Analytics Use Cases](#analytics-use-cases)
8. [Key Performance Indicators (KPIs)](#key-performance-indicators-kpis)

---

## Business Context

### Platform Overview

Our e-commerce platform serves customers across Vietnam, selling products from various brands and categories. The business generates two primary types of data:

1. **Transactional Data (OLTP)**: Orders, products, customers, payments - captured in operational databases
2. **Behavioral Data**: User activity logs including page views, searches, cart actions, and purchases - captured via web analytics

### Business Challenges

The organization needs to answer critical questions across multiple departments:

- **Sales Team**: Which products are performing best? What are our conversion rates?
- **Marketing Team**: Which channels drive the most revenue? How effective are our campaigns?
- **Product Team**: What's the customer journey from view to purchase? Where do users drop off?
- **Executive Team**: What are our daily/weekly/monthly trends? What's our AOV and customer LTV?
- **Customer Success**: Which customer segments are most valuable? Who's at risk of churn?

---

## Data Sources

### Source 1: OLTP Database (Transactional System)

**Tables Available:**
- `orders` - Order header information
- `order_items` - Line items for each order
- `products` - Product catalog with pricing
- `brands` - Brand reference data
- `category` - Product categories
- `customers` - Customer master data
- `payment_method` - Payment methods reference

**Data Characteristics:**
- **Update Frequency**: Real-time/Near real-time
- **Volume**: ~10K-50K orders per day
- **Data Quality**: High (validated by application logic)
- **Schema**: Normalized (3NF) for operational efficiency

### Source 2: User Activity Logs (Behavioral Data)

**Structure**: NDJSON (Newline-Delimited JSON)

**Event Types Captured:**
- `view` - Product page views
- `search` - Search queries
- `add_to_cart` - Items added to shopping cart
- `remove_from_cart` - Items removed from cart
- `purchase` - Completed transactions
- `wishlist` - Items added to wishlist
- `review` - Product reviews submitted

**Data Characteristics:**
- **Update Frequency**: Real-time streaming
- **Volume**: ~500K-2M events per day
- **Data Quality**: Variable (web tracking can have gaps)
- **Schema**: Semi-structured JSON with nested objects

**Key Attributes Captured:**
- User demographics and device information
- Traffic sources and UTM parameters
- Session metrics (duration, page views)
- Geographic location data
- Timestamp and sequence information

---

## End-User Requirements

### 1. Sales Analytics Team

**Primary Needs:**
- "I need to track daily/weekly/monthly sales trends by product, category, and brand"
- "I want to identify top-performing and underperforming products"
- "I need to analyze discount effectiveness and pricing strategies"
- "Show me average order value, order frequency, and revenue per customer"

**Key Questions:**
- What are our top 20 products by revenue this month?
- Which categories have the highest growth rate?
- What's the impact of discounts on sales volume?
- Which brands contribute most to total revenue?

### 2. Marketing & Growth Team

**Primary Needs:**
- "I need to understand which traffic sources drive the most conversions"
- "Show me campaign performance across different channels (Google, Facebook, Email, etc.)"
- "I want to calculate customer acquisition cost (CAC) by channel"
- "Track user engagement metrics: bounce rate, session duration, pages per session"

**Key Questions:**
- Which UTM campaigns have the highest ROI?
- What's the conversion rate by traffic source?
- Which devices (Desktop/Mobile) convert better?
- What's the cost per acquisition for each marketing channel?

### 3. Product & UX Team

**Primary Needs:**
- "Show me the complete user journey from first view to purchase"
- "Identify where users drop off in the conversion funnel"
- "Track search behavior: what are users looking for? Are they finding it?"
- "Analyze product page engagement: views, time spent, add-to-cart rate"

**Key Questions:**
- What's the view-to-cart conversion rate by product?
- What's the cart-to-purchase conversion rate?
- Which products have high views but low purchases?
- What are the most common search terms with no results?

### 4. Customer Analytics Team

**Primary Needs:**
- "Segment customers by purchase behavior and loyalty tier"
- "Identify high-value customers (VIP, repeat buyers)"
- "Track customer lifetime value (CLV) and churn risk"
- "Analyze repeat purchase rates and customer retention"

**Key Questions:**
- Who are our top 100 customers by lifetime value?
- What's the repeat purchase rate by customer tier?
- Which customer segments have the highest average order value?
- What's the churn rate for each loyalty tier?

### 5. Executive Dashboard Users

**Primary Needs:**
- "Give me a real-time view of key business metrics"
- "Show me daily active users, sessions, and conversion rates"
- "Track revenue, order volume, and average order value trends"
- "Compare performance week-over-week and year-over-year"

**Key Questions:**
- What's our daily/weekly/monthly revenue trend?
- How many active users do we have today/this week/this month?
- What's the overall conversion rate from visitor to buyer?
- Are we meeting our growth targets?

---

## Business Processes

Based on the requirements above, we've identified **4 core business processes** that drive our analytics:

### Process 1: Sales Order Management
**Scope**: Track and analyze all sales transactions  
**Input**: Orders, Order Items, Products, Customers, Payment Methods  
**Output**: Sales fact table with product, customer, payment, and time dimensions  
**Grain**: One row per order line item  
**Key Metrics**: Revenue, quantity sold, discount amount, average order value

### Process 2: Product Performance Analysis
**Scope**: Daily aggregated product sales metrics  
**Input**: Sales transactions aggregated by product and date  
**Output**: Daily product sales fact table  
**Grain**: One row per product per day  
**Key Metrics**: Total sales amount, order count, quantity sold, average unit price, discount amount

### Process 3: User Behavior & Engagement Tracking
**Scope**: Capture and analyze all user interactions on the platform  
**Input**: User activity logs (views, searches, cart actions, purchases)  
**Output**: User behavior fact table with action-level detail  
**Grain**: One row per user action  
**Key Metrics**: Action counts by type, session metrics, conversion flags, revenue attribution

### Process 4: Marketing Performance & Attribution
**Scope**: Daily aggregated metrics by traffic source and device  
**Input**: User behavior data aggregated by date, traffic source, device  
**Output**: Daily marketing metrics fact table  
**Grain**: One row per date, traffic source, and device combination  
**Key Metrics**: Active users, sessions, conversion rate, revenue, average order value

---

## Data Model Architecture

### Medallion Architecture

Our data platform follows the **Medallion Architecture** pattern with three layers:

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw Data)                                        │
│  - bronze.orders, bronze.order_items                            │
│  - bronze.products, bronze.brands, bronze.category              │
│  - bronze.customers, bronze.payment_method                      │
│  - bronze.user_activity_logs (NDJSON)                           │
│                                                                  │
│  Purpose: Persist raw data as-is for auditability              │
│  Format: Delta tables, partitioned by ingest_date              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ Data Cleaning, Validation, Type Casting
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Cleaned & Conformed)                            │
│  - silver.orders (joined with order_items)                      │
│  - silver.products (joined with brands, categories)             │
│  - silver.customers (cleaned & standardized)                    │
│  - silver.payment_method (reference data)                       │
│  - silver.user_events (flattened JSON)                          │
│                                                                  │
│  Purpose: Clean, validate, deduplicate, join related tables    │
│  Format: Delta tables, incremental processing                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ SCD Type 2 for Dimensions
                         │ Star Schema Transformation
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Analytics-Ready)                                   │
│                                                                  │
│  Dimensions (Type 2 SCD):                                       │
│  - dim_customers (with history)                                 │
│  - dim_products (with history)                                  │
│  - dim_payment_method                                           │
│  - dim_date (date dimension)                                    │
│                                                                  │
│  Facts:                                                         │
│  - fact_sales (order line items)                               │
│  - fact_sale_products_daily (daily aggregates)                  │
│  - fact_user_behavior (user actions)                            │
│  - fact_daily_metrics (marketing metrics)                       │
│                                                                  │
│  Purpose: Star schema optimized for BI tools and queries       │
│  Format: Delta tables, partitioned for performance             │
└─────────────────────────────────────────────────────────────────┘
```

### Star Schema Design

Our gold layer implements a **star schema** with fact tables at the center surrounded by dimension tables:

```
                    dim_date
                        ↑
                        │
    dim_customers ──→ fact_sales ←── dim_products
                        │
                        ↓
                  dim_payment_method


                    dim_date
                        ↑
                        │
                 fact_user_behavior ←── dim_products
                        


                 fact_daily_metrics
                    (denormalized)


             fact_sale_products_daily ←── dim_products
                        ↑
                        │
                    dim_date
```

---

## Fact & Dimension Tables

### Dimension Tables (Type 2 SCD)

#### `dim_customers`
**Purpose**: Customer master dimension with history tracking  
**Type**: Type 2 Slowly Changing Dimension  
**Business Key**: customer_id  
**Surrogate Key**: customer_sk  

**Columns:**
- `customer_sk` (BIGINT) - Surrogate key (auto-generated)
- `customer_nk` (BIGINT) - Natural key (customer_id)
- `first_name` (STRING) - Customer first name
- `last_name` (STRING) - Customer last name
- `email` (STRING) - Email address
- `phone_number` (STRING) - Contact phone
- `gender` (STRING) - Gender
- `customer_tier` (STRING) - Loyalty tier (Bronze/Silver/Gold/Platinum)
- `effective_start_date` (TIMESTAMP) - Valid from date
- `effective_end_date` (TIMESTAMP) - Valid to date (NULL = current)
- `is_current` (BOOLEAN) - Flag for current record

**SCD Logic**: Tracks changes in customer tier, contact information

---

#### `dim_products`
**Purpose**: Product catalog dimension with history tracking  
**Type**: Type 2 Slowly Changing Dimension  
**Business Key**: product_id  
**Surrogate Key**: product_sk  

**Columns:**
- `product_sk` (BIGINT) - Surrogate key
- `product_nk` (BIGINT) - Natural key (product_id)
- `product_name` (STRING) - Product name
- `product_description` (STRING) - Product description
- `unit_price` (DECIMAL) - Current unit price
- `category_id` (INT) - Category identifier
- `category_name` (STRING) - Category name
- `category_description` (STRING) - Category description
- `brand_id` (INT) - Brand identifier
- `brand_name` (STRING) - Brand name
- `brand_origin` (STRING) - Brand origin country
- `effective_start_date` (TIMESTAMP) - Valid from date
- `effective_end_date` (TIMESTAMP) - Valid to date
- `is_current` (BOOLEAN) - Current record flag

**SCD Logic**: Tracks changes in price, category, brand associations

---

#### `dim_payment_method`
**Purpose**: Payment method reference dimension  
**Type**: Type 1 (no history tracking)  
**Business Key**: payment_method_id  
**Surrogate Key**: payment_method_sk  

**Columns:**
- `payment_method_sk` (BIGINT) - Surrogate key
- `payment_method_nk` (INT) - Natural key
- `payment_method_name` (STRING) - Payment method name (Credit Card, Debit Card, E-Wallet, Cash on Delivery)
- `is_current` (BOOLEAN) - Active flag

---

#### `dim_date`
**Purpose**: Time dimension for date-based analysis  
**Type**: Pre-generated date dimension  
**Business Key**: date  
**Surrogate Key**: datekey (YYYYMMDD format as INT)  

**Columns:**
- `datekey` (INT) - Surrogate key in YYYYMMDD format
- `date` (DATE) - Actual date value
- `day` (INT) - Day of month (1-31)
- `month` (INT) - Month (1-12)
- `year` (INT) - Year (4 digits)
- `quarter` (INT) - Quarter (1-4)
- `day_of_week` (INT) - Day of week (1=Monday, 7=Sunday)
- `day_name` (STRING) - Day name (Monday, Tuesday, etc.)
- `month_name` (STRING) - Month name (January, February, etc.)
- `is_weekend` (BOOLEAN) - Weekend flag
- `is_holiday` (BOOLEAN) - Holiday flag (Vietnamese holidays)
- `fiscal_year` (INT) - Fiscal year
- `fiscal_quarter` (INT) - Fiscal quarter

---

### Fact Tables

#### `fact_sales`
**Business Process**: Sales Order Management  
**Grain**: One row per order line item  
**Type**: Transaction fact table  
**Update Strategy**: Incremental (append new order items)  

**Columns:**

**Keys:**
- `order_item_id` (STRING) - Primary key (unique key for incremental)
- `order_id` (STRING) - Order header identifier
- `customer_sk` (BIGINT) - FK to dim_customers
- `product_sk` (BIGINT) - FK to dim_products
- `payment_method_sk` (BIGINT) - FK to dim_payment_method
- `order_time_sk` (INT) - FK to dim_date

**Measures:**
- `quantity` (INT) - Quantity ordered
- `unit_price` (DECIMAL) - Price per unit at time of order
- `discount_amt` (DECIMAL) - Discount amount applied
- `sub_total` (DECIMAL) - Line item subtotal (quantity × unit_price)

**Business Rules:**
- Links to current dimension records (is_current = TRUE)
- Captures historical prices via unit_price in fact
- Discount captured as absolute amount
- Sub_total calculated as quantity × unit_price (before discount)

**Use Cases:**
- Sales performance analysis by product, customer, time
- Revenue trending and forecasting
- Discount effectiveness analysis
- Customer purchase behavior analysis

---

#### `fact_sale_products_daily`
**Business Process**: Product Performance Analysis  
**Grain**: One row per product per day  
**Type**: Aggregate fact table (rolled up from fact_sales)  
**Update Strategy**: Incremental (insert new date/product combinations)  

**Columns:**

**Keys:**
- `datekey_product_sk` (STRING) - Composite unique key (datekey_product_sk)
- `datekey` (INT) - FK to dim_date
- `product_sk` (BIGINT) - FK to dim_products

**Measures:**
- `total_sales_amount` (DECIMAL) - Sum of sub_total
- `total_order_count` (INT) - Count of distinct orders
- `total_quantity_sold` (INT) - Sum of quantity
- `avg_unit_price` (DECIMAL) - Average unit price
- `total_discount_amount` (DECIMAL) - Sum of discount_amt

**Business Rules:**
- Pre-aggregated for fast dashboard performance
- Eliminates need to aggregate fact_sales for daily reports
- Incremental: Only inserts new date/product combinations

**Use Cases:**
- Daily product sales dashboards
- Product performance trending
- Inventory planning and forecasting
- Promotional effectiveness measurement

---

#### `fact_user_behavior`
**Business Process**: User Behavior & Engagement Tracking  
**Grain**: One row per user action  
**Type**: Transaction fact table  
**Update Strategy**: Incremental (merge by behavior_id)  

**Columns:**

**Keys:**
- `behavior_id` (STRING) - Primary key (MD5 hash of event_id + action_index)
- `event_id` (STRING) - Original event identifier
- `user_id` (BIGINT) - User identifier (not a foreign key - user dimension doesn't exist yet)
- `session_id` (STRING) - Session identifier
- `action_date` (DATE) - Partition key
- `action_timestamp` (TIMESTAMP) - Exact timestamp of action
- `product_sk` (BIGINT) - FK to dim_products (NULL for search actions)
- `date_sk` (BIGINT) - FK to dim_date

**Action Details:**
- `action_type` (STRING) - Type of action (view, search, add_to_cart, remove_from_cart, purchase, wishlist, review)
- `product_id` (BIGINT) - Product natural key (NULL for search)
- `search_term` (STRING) - Search query (only for search actions)
- `order_id` (STRING) - Order identifier (only for purchase actions)
- `revenue` (DECIMAL) - Revenue amount (only for purchase actions)

**Session Context:**
- `session_duration` (INT) - Session duration in seconds
- `session_page_views` (INT) - Page views in session

**User Context (Denormalized):**
- `device_type` (STRING) - Desktop, Mobile, Tablet
- `device_os` (STRING) - Windows, macOS, Linux, iOS, Android
- `city` (STRING) - City name
- `country` (STRING) - Country name
- `traffic_source` (STRING) - UTM source (defaults to 'direct')
- `utm_campaign` (STRING) - Campaign name

**Action Flags (for fast filtering):**
- `is_view` (INT) - 1 if action = 'view', else 0
- `is_add_to_cart` (INT) - 1 if action = 'add_to_cart', else 0
- `is_purchase` (INT) - 1 if action = 'purchase', else 0
- `is_search` (INT) - 1 if action = 'search', else 0

**Metadata:**
- `created_at` (TIMESTAMP) - Record creation timestamp
- `ingest_date` (DATE) - Original ingest date

**Business Rules:**
- Exploded from actions array in user_events (1 event → multiple behavior rows)
- Device, location, traffic source denormalized for query performance
- Action flags enable fast filtering without string comparisons
- Links to dim_products for product-related actions

**Use Cases:**
- User journey analysis (funnel analysis)
- Product engagement tracking
- Search behavior analysis
- Conversion attribution
- Session analysis

---

#### `fact_daily_metrics`
**Business Process**: Marketing Performance & Attribution  
**Grain**: One row per date, traffic source, and device type  
**Type**: Aggregate fact table (rolled up from fact_user_behavior)  
**Update Strategy**: Incremental (merge by composite key)  

**Columns:**

**Keys:**
- `metric_date` (DATE) - Date of metrics (partition key)
- `traffic_source` (STRING) - Traffic source (part of composite key)
- `device_type` (STRING) - Device type (part of composite key)

**User & Session Metrics:**
- `active_users` (BIGINT) - Count of distinct users
- `total_sessions` (BIGINT) - Count of distinct sessions
- `converted_sessions` (BIGINT) - Sessions with at least one purchase

**Action Counts:**
- `total_views` (BIGINT) - Sum of view actions
- `total_searches` (BIGINT) - Sum of search actions
- `total_add_to_carts` (BIGINT) - Sum of add_to_cart actions
- `total_purchases` (BIGINT) - Sum of purchase actions

**Revenue Metrics:**
- `total_revenue` (DECIMAL) - Sum of revenue from purchases

**Session Quality Metrics:**
- `avg_session_duration` (DOUBLE) - Average session duration in seconds
- `avg_page_views` (DOUBLE) - Average page views per session

**Calculated KPIs:**
- `conversion_rate_pct` (DECIMAL) - Percentage of sessions that converted (converted_sessions / total_sessions × 100)
- `avg_order_value` (DECIMAL) - Average revenue per purchase (total_revenue / total_purchases)

**Metadata:**
- `created_at` (TIMESTAMP) - Record creation timestamp

**Business Rules:**
- Pre-computed daily KPIs for dashboard performance
- Eliminates need to aggregate fact_user_behavior for reports
- Conversion rate and AOV calculated during aggregation

**Use Cases:**
- Executive dashboards (daily KPIs)
- Traffic source performance comparison
- Device performance analysis
- Marketing campaign ROI calculation
- Trend analysis (day-over-day, week-over-week)

---

## Analytics Use Cases

### Use Case 1: Sales Performance Dashboard

**Business Question**: "What are our daily sales trends and which products are driving revenue?"

**Data Required:**
- `fact_sales` joined to `dim_products`, `dim_date`
- `fact_sale_products_daily` for daily aggregates

**Key Metrics:**
- Daily revenue trend
- Top 10 products by revenue
- Revenue by category and brand
- Average order value

**Sample Query:**
```sql
SELECT 
    d.date,
    d.day_name,
    SUM(f.sub_total) as daily_revenue,
    COUNT(DISTINCT f.order_id) as order_count,
    AVG(f.sub_total) as avg_line_item_value
FROM fact_sales f
JOIN dim_date d ON f.order_time_sk = d.datekey
WHERE d.date >= CURRENT_DATE - 30
GROUP BY d.date, d.day_name
ORDER BY d.date DESC;
```

---

### Use Case 2: Customer Segmentation & Lifetime Value

**Business Question**: "Who are our most valuable customers and how do different customer tiers perform?"

**Data Required:**
- `fact_sales` joined to `dim_customers`, `dim_date`

**Key Metrics:**
- Customer lifetime value (CLV)
- Average order value by customer tier
- Repeat purchase rate
- Revenue contribution by tier

**Sample Query:**
```sql
SELECT 
    c.customer_tier,
    COUNT(DISTINCT c.customer_sk) as customer_count,
    SUM(f.sub_total) as total_revenue,
    SUM(f.sub_total) / COUNT(DISTINCT c.customer_sk) as revenue_per_customer,
    COUNT(DISTINCT f.order_id) / COUNT(DISTINCT c.customer_sk) as avg_orders_per_customer
FROM fact_sales f
JOIN dim_customers c ON f.customer_sk = c.customer_sk
WHERE c.is_current = TRUE
GROUP BY c.customer_tier
ORDER BY total_revenue DESC;
```

---

### Use Case 3: Conversion Funnel Analysis

**Business Question**: "Where are users dropping off in the purchase journey?"

**Data Required:**
- `fact_user_behavior`

**Key Metrics:**
- Views → Add to Cart conversion rate
- Add to Cart → Purchase conversion rate
- Overall view → Purchase conversion rate
- Drop-off rates at each stage

**Sample Query:**
```sql
SELECT
    action_date,
    SUM(is_view) as total_views,
    SUM(is_add_to_cart) as total_add_to_carts,
    SUM(is_purchase) as total_purchases,
    ROUND(100.0 * SUM(is_add_to_cart) / NULLIF(SUM(is_view), 0), 2) as view_to_cart_pct,
    ROUND(100.0 * SUM(is_purchase) / NULLIF(SUM(is_add_to_cart), 0), 2) as cart_to_purchase_pct,
    ROUND(100.0 * SUM(is_purchase) / NULLIF(SUM(is_view), 0), 2) as overall_conversion_pct
FROM fact_user_behavior
WHERE action_date >= CURRENT_DATE - 7
GROUP BY action_date
ORDER BY action_date DESC;
```

---

### Use Case 4: Marketing Channel Performance

**Business Question**: "Which marketing channels deliver the best ROI?"

**Data Required:**
- `fact_daily_metrics`

**Key Metrics:**
- Active users by channel
- Conversion rate by channel
- Revenue by channel
- Customer acquisition cost (when joined with marketing spend data)

**Sample Query:**
```sql
SELECT
    traffic_source,
    device_type,
    SUM(active_users) as total_users,
    SUM(total_sessions) as sessions,
    SUM(total_purchases) as purchases,
    SUM(total_revenue) as revenue,
    AVG(conversion_rate_pct) as avg_conversion_rate,
    AVG(avg_order_value) as avg_order_value,
    SUM(total_revenue) / NULLIF(SUM(active_users), 0) as revenue_per_user
FROM fact_daily_metrics
WHERE metric_date >= CURRENT_DATE - 30
GROUP BY traffic_source, device_type
ORDER BY revenue DESC;
```

---

### Use Case 5: Product Performance & Engagement

**Business Question**: "Which products have high engagement but low conversion?"

**Data Required:**
- `fact_user_behavior` joined to `dim_products`

**Key Metrics:**
- Product view count
- Add to cart rate
- Purchase rate
- View-to-purchase conversion

**Sample Query:**
```sql
SELECT
    p.product_name,
    p.category_name,
    p.brand_name,
    SUM(f.is_view) as views,
    SUM(f.is_add_to_cart) as add_to_carts,
    SUM(f.is_purchase) as purchases,
    ROUND(100.0 * SUM(f.is_add_to_cart) / NULLIF(SUM(f.is_view), 0), 2) as view_to_cart_rate,
    ROUND(100.0 * SUM(f.is_purchase) / NULLIF(SUM(f.is_add_to_cart), 0), 2) as cart_to_purchase_rate,
    SUM(CASE WHEN f.is_purchase = 1 THEN f.revenue ELSE 0 END) as total_revenue
FROM fact_user_behavior f
JOIN dim_products p ON f.product_sk = p.product_sk
WHERE f.action_date >= CURRENT_DATE - 30
  AND p.is_current = TRUE
GROUP BY p.product_name, p.category_name, p.brand_name
HAVING SUM(f.is_view) >= 100
ORDER BY views DESC, cart_to_purchase_rate ASC
LIMIT 20;
```

---

### Use Case 6: Time-Based Performance Analysis

**Business Question**: "How does performance vary by day of week and time of day?"

**Data Required:**
- `fact_sales` joined to `dim_date`
- `fact_daily_metrics` joined to `dim_date`

**Key Metrics:**
- Revenue by day of week
- Conversion rate by day of week
- Weekend vs. weekday performance
- Holiday impact analysis

**Sample Query:**
```sql
SELECT
    d.day_name,
    d.is_weekend,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(f.sub_total) as total_revenue,
    AVG(f.sub_total) as avg_transaction_value
FROM fact_sales f
JOIN dim_date d ON f.order_time_sk = d.datekey
WHERE d.date >= CURRENT_DATE - 90
GROUP BY d.day_name, d.is_weekend, 
         CASE d.day_of_week 
           WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 
           WHEN 4 THEN 4 WHEN 5 THEN 5 WHEN 6 THEN 6 WHEN 7 THEN 7 
         END
ORDER BY CASE d.day_of_week 
           WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 
           WHEN 4 THEN 4 WHEN 5 THEN 5 WHEN 6 THEN 6 WHEN 7 THEN 7 
         END;
```

---

## Key Performance Indicators (KPIs)

### Revenue KPIs
- **Daily/Weekly/Monthly Revenue**: `SUM(sub_total)` from fact_sales or total_revenue from fact_daily_metrics
- **Average Order Value (AOV)**: `total_revenue / total_purchases`
- **Revenue per Customer**: `SUM(sub_total) / COUNT(DISTINCT customer_sk)`
- **Revenue per Session**: `total_revenue / total_sessions`

### Conversion KPIs
- **Overall Conversion Rate**: `(total_purchases / total_sessions) × 100`
- **View-to-Cart Rate**: `(total_add_to_carts / total_views) × 100`
- **Cart-to-Purchase Rate**: `(total_purchases / total_add_to_carts) × 100`
- **Checkout Abandonment Rate**: `100 - cart_to_purchase_rate`

### User Engagement KPIs
- **Daily Active Users (DAU)**: `COUNT(DISTINCT user_id)` where action_date = today
- **Session Duration**: `avg_session_duration` from fact_daily_metrics
- **Pages per Session**: `avg_page_views` from fact_daily_metrics
- **Bounce Rate**: Sessions with only 1 action / total sessions

### Product Performance KPIs
- **Best Sellers**: Products ranked by `total_quantity_sold` or `total_sales_amount`
- **Product View Count**: `SUM(is_view)` from fact_user_behavior
- **Add-to-Cart Rate**: Views that resulted in add_to_cart
- **Product Conversion Rate**: Views that resulted in purchase

### Customer Metrics
- **Customer Lifetime Value (CLV)**: `SUM(sub_total)` per customer across all time
- **Repeat Purchase Rate**: Customers with > 1 order / total customers
- **Customer Acquisition Cost (CAC)**: Marketing spend / new customers (requires external spend data)
- **Customer Retention Rate**: Customers who purchased in current + previous period / customers in previous period

### Marketing Attribution KPIs
- **Revenue by Traffic Source**: `SUM(total_revenue)` grouped by traffic_source
- **Conversion Rate by Channel**: `conversion_rate_pct` from fact_daily_metrics
- **Cost per Acquisition (CPA)**: Marketing spend by channel / conversions (requires external spend data)
- **Return on Ad Spend (ROAS)**: Revenue from channel / ad spend on channel

---

## Data Refresh Strategy

### Bronze Layer
- **Update Frequency**: Real-time or micro-batch (every 5-15 minutes)
- **Method**: CDC (Change Data Capture) from OLTP, streaming ingestion for user logs
- **Partitioning**: By `ingest_date`

### Silver Layer
- **Update Frequency**: Batch processing every 1-4 hours
- **Method**: Incremental processing using dbt, merge/upsert strategy
- **Data Quality**: Validation, deduplication, type casting

### Gold Layer - Dimensions
- **Update Frequency**: Daily (overnight batch)
- **Method**: Type 2 SCD snapshots via dbt snapshots
- **Strategy**: Track changes in customer tier, product pricing, etc.

### Gold Layer - Facts
- **fact_sales**: Incremental append (new orders only), runs every 1-4 hours
- **fact_sale_products_daily**: Daily aggregation, runs once per day after fact_sales
- **fact_user_behavior**: Incremental merge (micro-batch every 15-30 minutes)
- **fact_daily_metrics**: Daily aggregation, runs once per day after fact_user_behavior

---

## Data Quality & Governance

### Data Quality Checks

**Dimension Tables:**
- Unique surrogate keys
- No NULL in business keys
- Valid effective_start_date and effective_end_date
- Only one current record per business key (is_current = TRUE)

**Fact Tables:**
- Valid foreign keys (exist in dimension tables)
- No negative quantities or amounts (except discount)
- Required fields not NULL (e.g., order_id, product_sk, action_type)
- Date consistency (order_date <= current_date)

### Data Lineage

```
OLTP Database → Bronze (raw tables) → Silver (cleaned tables) → Gold (dimensions via snapshots)
                                                               → Gold (fact_sales)
                                                               → Gold (fact_sale_products_daily)

User Activity Logs → Bronze (JSON) → Silver (flattened) → Gold (fact_user_behavior)
                                                         → Gold (fact_daily_metrics)
```

### Data Retention Policy

- **Bronze**: 90 days (raw data retention for reprocessing)
- **Silver**: 1 year (cleaned data for analysis)
- **Gold**: Indefinite (business reporting data, compressed and optimized)

---

## Conclusion

This data model supports comprehensive analytics across all major business functions:

✅ **Sales Analytics**: Track revenue, product performance, and discount effectiveness  
✅ **Customer Analytics**: Segment customers, calculate LTV, track retention  
✅ **Marketing Attribution**: Measure channel performance, ROI, and campaign effectiveness  
✅ **Product Analytics**: Analyze engagement, conversion funnels, and user journeys  
✅ **Executive Reporting**: Monitor KPIs, trends, and business health  

The platform integrates both transactional (OLTP) and behavioral (user activity) data, providing a 360-degree view of the e-commerce business. The star schema design ensures query performance while the incremental processing strategy keeps data fresh and costs optimized.

### Next Steps

1. **Implementation**: Deploy dbt models to production environment
2. **Testing**: Validate data quality and reconcile with source systems
3. **BI Integration**: Connect Tableau/Power BI/Looker to gold layer
4. **Monitoring**: Set up data freshness alerts and quality checks
5. **Optimization**: Monitor query performance and add indexes/partitions as needed
6. **Enhancement**: Add customer dimension, geographic dimension, or time-of-day analysis as requirements evolve

---

**Document Version**: 1.0  
**Last Updated**: November 15, 2025  
**Maintained By**: Data Engineering Team
