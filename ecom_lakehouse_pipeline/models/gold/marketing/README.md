# Marketing Campaign Model - Gold Layer

## Overview
Model đơn giản để tạo danh sách khách hàng target cho marketing campaigns dựa trên ML predictions.

## Model: `high_value_purchase_campaign`

**Purpose**: Lấy top customers có xác suất mua hàng cao nhất

**Output**: Danh sách email với phân khúc và recommendation đơn giản

**Key Features**:
- Purchase probability từ ML model
- Phân khúc: Hot Lead (>80%), Warm Lead (60-80%), Cold Lead (<60%)
- Campaign action recommendations
- Chỉ customers có email

**Use Cases**:
- Email marketing campaigns
- Push notifications
- Targeted promotions

---

## Data Flow

```
ML Inference (Spark Job)
         ↓
ml.next_day_purchase_prediction
         ↓
gold.marketing.high_value_purchase_campaign
         ↓
Export to Marketing Tools
```

## Dependencies

- `ml.next_day_purchase_prediction` - ML predictions từ Spark job
- `gold.dim_customers` - Customer master data

## Usage

```bash
# Run model
dbt run --select high_value_purchase_campaign

# Test
dbt test --select high_value_purchase_campaign
```

## Sample Query

```sql
-- Lấy top 100 Hot Leads hôm nay
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    purchase_probability,
    customer_segment,
    campaign_action
FROM lakehouse.marketing.high_value_purchase_campaign
WHERE customer_segment = 'Hot Lead'
ORDER BY purchase_probability DESC
LIMIT 100;
```

## Export to Marketing Tools

Có thể export sang:
- **Email**: Mailchimp, SendGrid
- **CRM**: Salesforce, HubSpot
- **Ads**: Google Ads, Facebook Ads

## Segments

| Segment | Probability | Action |
|---------|-------------|--------|
| Hot Lead | ≥80% | Send Premium Offer |
| Warm Lead | 60-80% | Send Discount Code |
| Cold Lead | 50-60% | Send General Newsletter |
