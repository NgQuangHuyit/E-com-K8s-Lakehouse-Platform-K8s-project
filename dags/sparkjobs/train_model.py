import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_date
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when

def get_spark(app_name: str):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark



if __name__ == "__main__":
    spark = get_spark("train_next_day_purchase_model")
    df = spark.read.format("delta").table("ml.user_behavior_3d_agg_feature").filter(col("prediction_date") < to_date('2025-12-11'))
    # Đặc trưng về hành vi mua hàng (quan trọng nhất)
    purchase_behavior_features = [
        'purchase_count_3d',              # #1 - Số lần mua trong 3 ngày
        'add_to_cart_count_3d',           # #2 - Thêm vào giỏ
        'checkout_view_count_3d',         # #3 - Xem checkout
        'cart_conversion_rate_3d',        # #4 - Tỷ lệ chuyển đổi từ giỏ
        'purchase_conversion_rate_3d',    # #5 - Tỷ lệ mua hàng
    ]

    # Đặc trưng về engagement (tương tác)
    engagement_features = [
        'sessions_3d',                    # #6 - Số phiên
        'total_duration_3d',              # #7 - Tổng thời gian
        'avg_session_duration_3d',        # #8 - Thời gian TB/phiên
        'total_actions_3d',               # #9 - Tổng hành động
        'actions_per_session_3d',         # #10 - Hành động/phiên
    ]

    # Đặc trưng về sản phẩm & giá cả
    product_features = [
        'distinct_products_3d',           # #11 - Số sản phẩm unique xem
        'avg_product_price_3d',           # #12 - Giá TB sản phẩm quan tâm
    ]

    # Tổng hợp tất cả features
    selected_features = purchase_behavior_features + engagement_features + product_features
    # Xử lý null giá trị null
    for col_name in selected_features:
        df = df.withColumn(col_name, 
                        when(col(col_name).isNull(), 0.0)
                        .otherwise(col(col_name).cast("double")))

    # Đổi tên cột nhãn
    df = df.withColumn("label", col("label_purchase_tomorrow").cast("integer"))
    # Lọc bỏ bản ghi có nhãn null
    df = df.filter(col("label").isNotNull())
    stages = []

    # Bước 1: Kết hợp các đặng trưng
    assembler = VectorAssembler(
        inputCols=selected_features,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    stages.append(assembler)

    # Bước 2: chuẩn hóa đặc trưng 
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=False
    )
    stages.append(scaler)

    # Bước 3: Logistic Regression
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        regParam=0.01,           # L2 regularization
        elasticNetParam=0.0,     # 0 = L2 only
        family="binomial",
        threshold=0.5            # Threshold cho classification
    )
    stages.append(lr)

    # Tạo Pipeline
    pipeline = Pipeline(stages=stages)


    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    print("✓ Pipeline bao gồm 3 bước:")
    print("  1. VectorAssembler - Kết hợp features")
    print("  2. StandardScaler - Chuẩn hóa")
    print("  3. LogisticRegression - Mô hình")
    print("\n" + "="*70)
    print("HUẤN LUYỆN MÔ HÌNH")
    print("="*70)

    model = pipeline.fit(train_df)

    print("✓ Hoàn thành huấn luyện!")

    model_path = "s3a://lakehouse/models/next_day_purchase_prediction_lr_v3"
    model.write().overwrite().save(model_path)
    print(f"✓ Đã lưu model tại: {model_path}")