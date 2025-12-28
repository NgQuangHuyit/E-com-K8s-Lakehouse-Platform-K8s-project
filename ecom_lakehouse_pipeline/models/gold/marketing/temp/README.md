# Marketing Temp Tables

## Purpose
External parquet tables để cache dữ liệu trung gian, tránh OOM khi query large ML tables.

## Models

### `temp_purchase_predictions.sql`
External parquet table cache ML predictions (7 ngày gần nhất)
- **Location**: `s3a://lakehouse/temp/marketing/purchase_predictions`
- **Format**: Parquet
- **Retention**: 7 days

### `temp_customers.sql`
External parquet table cache customer data có email
- **Location**: `s3a://lakehouse/temp/marketing/customers`
- **Format**: Parquet

## Benefits
- ✅ **Tránh OOM**: Query từ parquet thay vì Iceberg table lớn
- ✅ **Fast**: Parquet optimized cho analytical queries
- ✅ **Isolated**: Không ảnh hưởng source tables
- ✅ **External**: Data được lưu riêng trên S3

## Usage

```bash
# Run temp tables
dbt run --select gold/marketing/temp

# Run full pipeline
dbt run --select gold/marketing/temp+ 
```

## Data Flow

```
ml.next_day_purchase_prediction (Iceberg)
         ↓
temp_purchase_predictions (Parquet) 
         ↓
high_value_purchase_campaign
```

## Cleanup

Temp tables tự động refresh mỗi lần chạy DAG. Old parquet files được overwrite.
