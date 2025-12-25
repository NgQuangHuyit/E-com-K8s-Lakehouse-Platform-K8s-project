# Core Business Requirements and Analytical Mapping (Concise)

## Abstract

This document states the core, exam-ready business requirements and shows how they are already supported by the current Bronze → Silver → Gold pipeline and star schema. Scope is limited to transactional OLTP data and user activity logs; no data model changes are required.

---

## 1) Core Business Problem Statement

Provide a unified, analytics-ready view of commercial performance by: (i) quantifying revenue and product outcomes over time, (ii) attributing conversions and revenue to marketing channels and devices, and (iii) understanding the user journey from product view to purchase to reduce funnel drop-off—using only the existing facts and dimensions.

---

## 2) Current Data Flow (Aligned)

- Bronze: raw OLTP tables and raw JSON `bronze.user_activity_logs` (partitioned by ingest_date)
- Silver: cleaned/conformed tables; notably `silver.user_events` (flattened events)
- Gold: star schema (Delta, incremental) with:
  - Dimensions: `dim_products`, `dim_customers`, `dim_payment_method`, `dim_date`
  - Facts: `fact_sales`, `fact_sale_products_daily`, `fact_user_behavior`, `fact_daily_metrics`

---

## 3) Core Business Processes

1. Sales Order Management
   - Data: orders, order_items, customers, products, payment_method
   - Output: transaction-level sales and daily product aggregates

2. User Behavior and Engagement Tracking
   - Data: user activity logs → `silver.user_events` → actions
   - Output: action-level signals and session/traffic context

3. Marketing Performance and Attribution
   - Data: aggregated behavior by date × traffic_source × device
   - Output: daily channel/device KPIs (conversion, AOV, revenue)

---

## 4) Analytical Needs → Existing Model Mapping

- Revenue trends and order volumes → `fact_sales` + `dim_date`
- Top products/categories/brands → `fact_sales`, `fact_sale_products_daily` + `dim_products`
- Price/discount impact → `fact_sales` (unit_price, discount_amt) + `dim_date`
- AOV and order frequency → `fact_sales` (aggregate by order/date)
- Funnel: view → cart → purchase → `fact_user_behavior` (is_view, is_add_to_cart, is_purchase)
- Channel/device attribution → `fact_daily_metrics` (traffic_source, device_type)
- Engagement: DAU, sessions, pages/session → `fact_daily_metrics`

Note: Search analytics are covered at action-level via `fact_user_behavior` (search actions). Customer 360 analyses are limited to attributes present in `dim_customers` joined with sales facts.

---

## 5) Tables in Use (No Changes Required)

- Dimensions: `dim_products` (Type 2), `dim_customers` (Type 2), `dim_payment_method` (Type 1), `dim_date`
- Facts:
  - `fact_sales` (order line item grain)
  - `fact_sale_products_daily` (date × product aggregate)
  - `fact_user_behavior` (action grain with session/traffic context)
  - `fact_daily_metrics` (date × traffic_source × device aggregate)
- Silver support: `silver.user_events` → basis for behavior/marketing facts

---

## 6) Core KPIs

- Revenue: total revenue, D/W/M trend, revenue by product/category/brand
- Orders: order count, items per order, discount amount, average unit price
- AOV: total_revenue / total_purchases (`fact_daily_metrics`) or sales-derived equivalent
- Conversion: view→cart, cart→purchase, overall conversion rate
- Engagement: DAU, sessions, avg session duration, avg page views
- Marketing: revenue and conversion rate by traffic_source and device

---

## 7) Assumptions, Scope

Assumptions:
- Behavioral logs are sufficient to compute action-level funnels.
- Product SK exists for product actions (NULL for pure search is acceptable).

Scope:
- Sales/product performance, funnel, marketing channel/device KPIs using current facts/dims.

Out of scope (would need extra data/modeling): multi-touch attribution, advanced cohorts/retention beyond provided attributes, PII enrichment.

---

## 8) Conclusion

The existing model already supports the core commercial analytics needs. Stakeholders can answer revenue and product performance questions (`fact_sales`, `fact_sale_products_daily`), analyze journey and engagement (`fact_user_behavior`), and monitor marketing KPIs (`fact_daily_metrics`) while slicing with `dim_products`, `dim_customers`, `dim_payment_method`, and `dim_date`—with no schema changes.
