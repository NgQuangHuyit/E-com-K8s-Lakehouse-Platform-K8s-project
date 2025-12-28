# Gold ML Staging Models

## Purpose
Staging tables để cache dữ liệu từ `ml` schema, tránh OOM và optimize performance khi chạy downstream models.

## Models

### `stg_purchase_predictions.sql`
Cache ML predictions (7 ngày gần nhất) từ `ml.next_day_purchase_prediction`

### `stg_user_behavior_features.sql`
Cache behavioral features (7 ngày gần nhất) từ `ml.user_behavior_3d_agg_feature`

## Benefits
- ✅ Tránh Spark OOM khi join large tables
- ✅ Faster query performance (materialized tables)
- ✅ Reduce load on source ml schema
- ✅ Data retention control (7 days)

## Usage

```bash
# Run staging models
dbt run --select gold/ml

# Run with downstream
dbt run --select gold/ml+ 
```

## Refresh Schedule
Run daily sau khi Spark inference job hoàn thành
