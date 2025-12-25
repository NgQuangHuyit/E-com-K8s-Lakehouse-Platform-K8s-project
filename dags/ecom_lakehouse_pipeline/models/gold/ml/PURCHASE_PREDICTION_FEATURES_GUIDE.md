# Feature Engineering Guide - Purchase Prediction Model

## 📊 Tổng quan Bài toán

**Mục tiêu**: Dự đoán xác suất user sẽ purchase trong session  
**Loại bài toán**: Binary Classification  
**Label**: `has_purchase` (0 hoặc 1)  
**Ứng dụng**: 
- Targeting quảng cáo real-time
- Personalized promotion
- Optimize UX/UI dựa trên purchase probability

---

## 🎯 Feature Categories & Rationale

### 1️⃣ Core Behavior Features (Engagement)
Đo lường mức độ tương tác của user với website.

| Feature | Type | Rationale | Expected Impact |
|---------|------|-----------|-----------------|
| `duration_seconds` | Numeric | Session dài = engagement cao | ⭐⭐⭐ |
| `actions_count` | Numeric | Nhiều actions = user active | ⭐⭐⭐⭐ |
| `page_views` | Numeric | Nhiều pages nhưng không convert có thể là confusion | ⭐⭐ |
| `action_velocity` | Calculated | actions/duration - User quyết đoán vs dè dặt | ⭐⭐⭐⭐ |
| `time_to_first_action` | Numeric | Action nhanh = intent rõ ràng | ⭐⭐⭐ |
| `avg_time_per_page` | Calculated | Engagement depth | ⭐⭐ |

### 2️⃣ Action Type Features (Intent Signals)
Phân tích các hành động cụ thể thể hiện purchase intent.

| Feature | Type | Rationale | Expected Impact |
|---------|------|-----------|-----------------|
| `num_views` | Count | Baseline behavior | ⭐⭐ |
| `num_add_to_cart` | Count | **Strongest signal** cho purchase intent | ⭐⭐⭐⭐⭐ |
| `has_checkout_view` | Binary | Đến checkout = rất gần purchase | ⭐⭐⭐⭐⭐ |
| `num_searches` | Count | Goal-oriented behavior | ⭐⭐⭐ |
| `num_wishlist` | Count | Quan tâm nhưng defer decision | ⭐⭐ |
| `num_reviews` | Count | Research = serious buyer | ⭐⭐⭐ |
| `num_remove_from_cart` | Count | Indecisive = lower conversion | ⭐⭐ (negative) |
| `cart_conversion_rate` | Ratio | add_to_cart/views - Decisiveness | ⭐⭐⭐⭐ |

### 3️⃣ Product Diversity Features (Exploration)
Đo lường breadth của browsing behavior.

| Feature | Type | Rationale | Expected Impact |
|---------|------|-----------|-----------------|
| `unique_products_viewed` | Count | Focused (ít) vs window shopping (nhiều) | ⭐⭐⭐ |
| `unique_categories_viewed` | Count | Cross-category browsing pattern | ⭐⭐ |

### 4️⃣ Price Features (Economic Behavior)
Phân tích price sensitivity và willingness to pay.

| Feature | Type | Rationale | Expected Impact |
|---------|------|-----------|-----------------|
| `avg_product_price` | Numeric | Price tier preference | ⭐⭐⭐ |
| `price_range_interest` | Calculated | max - min: Price exploration breadth | ⭐⭐ |

### 5️⃣ Temporal Features (Timing)
Shopping patterns thay đổi theo thời gian.

| Feature | Type | Rationale | Expected Impact |
|---------|------|-----------|-----------------|
| `hour_of_day` | Categorical | Peak hours có conversion khác nhau | ⭐⭐⭐ |
| `is_weekend` | Binary | Weekend shopping behavior | ⭐⭐ |

### 6️⃣ Device Context
Device type ảnh hưởng đến behavior và conversion.

| Feature | Type | Rationale | Expected Impact |
|---------|------|-----------|-----------------|
| `is_mobile` | Binary | Mobile có conversion thấp hơn desktop | ⭐⭐⭐⭐ |

### 7️⃣ User & Marketing Features (Context)
Thông tin về user và nguồn traffic.

| Feature | Type | Rationale | Expected Impact |
|---------|------|-----------|-----------------|
| `user_segment` | Categorical | VIP > Loyal > Returning > New | ⭐⭐⭐⭐⭐ |
| `referrer_type` | Categorical | Traffic quality: Direct/Email > Social > Search | ⭐⭐⭐⭐ |

---

## 🔄 DBT Model Architecture

```
bronze/user_activity_logs (raw NDJSON)
    ↓
silver/user_sessions (flattened, session-level)
    ↓
gold/ml_purchase_prediction_features (engineered features)
    ↓
ML Model Training & Inference
```

### Silver Layer: `user_sessions.sql`
- Flatten nested JSON structure
- Extract basic fields
- Add temporal features (hour, is_weekend)
- Preserve actions array for downstream processing

### Gold Layer: `ml_purchase_prediction_features.sql`
- Explode actions array
- Calculate all engineered features
- Aggregate to session level
- Clean nulls and handle edge cases

---

## 📈 Feature Statistics & Validation

### Expected Feature Distributions

**Continuous Features:**
```sql
-- Check feature ranges
SELECT 
    -- Behavior
    avg(duration_seconds) as avg_duration,
    avg(action_velocity) as avg_velocity,
    
    -- Conversion indicators
    avg(num_add_to_cart) as avg_cart_adds,
    avg(cart_conversion_rate) as avg_cart_cvr,
    
    -- Diversity
    avg(unique_products_viewed) as avg_products,
    
    -- Label distribution
    avg(label) as purchase_rate
FROM {{ ref('ml_purchase_prediction_features') }}
```

**Categorical Features:**
```sql
-- User segment distribution
SELECT user_segment, count(*), avg(label) as purchase_rate
FROM {{ ref('ml_purchase_prediction_features') }}
GROUP BY user_segment
ORDER BY purchase_rate DESC

-- Expected order: vip > loyal > returning > new
```

---

## 🚀 Usage for Data Scientists

### 1. Feature Selection trong Python
```python
import pandas as pd
from pyspark.sql import SparkSession

# Đọc data từ Delta Lake
spark = SparkSession.builder.getOrCreate()
df = spark.read.format("delta").load("s3://lakehouse/gold/ml_purchase_prediction_features")

# Convert to Pandas
pdf = df.toPandas()

# Selected features (as specified)
selected_features = [
    # Core behavior
    "duration_seconds",
    "actions_count",
    "page_views",
    "action_velocity",
    "time_to_first_action",
    "num_views",
    "num_add_to_cart",
    "has_checkout_view",
    "num_searches",
    "num_wishlist",
    "cart_conversion_rate",
    
    # Diversity
    "unique_products_viewed",
    "unique_categories_viewed",
    
    # Price
    "avg_product_price",
    "price_range_interest",
    
    # Time
    "hour_of_day",
    "is_weekend",
    
    # Device
    "is_mobile",
    
    # User & marketing
    "user_segment",
    "referrer_type"
]

# Additional recommended features
additional_features = [
    "num_reviews",
    "num_remove_from_cart",
    "has_search",
    "avg_time_per_page"
]

X = pdf[selected_features]
y = pdf['label']
```

### 2. Feature Encoding
```python
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split

# Encode categorical features
le_segment = LabelEncoder()
le_referrer = LabelEncoder()

X['user_segment_encoded'] = le_segment.fit_transform(X['user_segment'])
X['referrer_type_encoded'] = le_referrer.fit_transform(X['referrer_type'])

# Drop original categorical columns
X = X.drop(['user_segment', 'referrer_type'], axis=1)

# Scale numerical features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X_scaled, y, test_size=0.2, random_state=42, stratify=y
)
```

### 3. Model Training Example
```python
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report

# Try multiple models
models = {
    'Logistic Regression': LogisticRegression(max_iter=1000),
    'Random Forest': RandomForestClassifier(n_estimators=100, random_state=42),
    'Gradient Boosting': GradientBoostingClassifier(n_estimators=100, random_state=42)
}

for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred_proba)
    print(f"{name} - AUC: {auc:.4f}")
```

---

## ⚠️ Important Notes

### Data Quality Checks
1. **Null handling**: All features have `coalesce()` với default values hợp lý
2. **Division by zero**: Tất cả calculations có check `when ... > 0`
3. **Outliers**: Cần check distribution và consider winsorization

### Feature Engineering Tips
1. **Interaction terms**: Consider `num_add_to_cart * is_mobile` 
2. **Polynomial features**: `duration_seconds^2` cho non-linear patterns
3. **Binning**: `hour_of_day` → morning/afternoon/evening/night
4. **Target encoding**: Cho categorical features với high cardinality

### Model Performance Expectations
- **Baseline** (random): AUC ~ 0.50
- **Simple Logistic**: AUC ~ 0.70-0.75
- **Tree-based models**: AUC ~ 0.75-0.82
- **Ensemble/Neural**: AUC ~ 0.82-0.88

Top features dự kiến có feature importance cao nhất:
1. `has_checkout_view` 
2. `num_add_to_cart`
3. `user_segment`
4. `action_velocity`
5. `referrer_type`

---

## 📝 Next Steps

1. ✅ Run DBT models: `dbt run --select user_sessions ml_purchase_prediction_features`
2. ✅ Validate data quality: `dbt test`
3. ✅ Check feature distributions và correlations
4. ⏳ Train baseline models
5. ⏳ Feature importance analysis
6. ⏳ Hyperparameter tuning
7. ⏳ Deploy model for real-time scoring

---

## 📚 References

- DBT documentation: [ecom_lakehouse_pipeline/README.md](../README.md)
- Schema definitions: [ml_purchase_prediction_schema.yml](ml_purchase_prediction_schema.yml)
- Source data: `bronze.user_activity_logs`
