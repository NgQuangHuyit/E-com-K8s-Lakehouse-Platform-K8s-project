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

def get_spark(app_name: str):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark



if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="Next day purchase prediction inference")
    parser.add_argument("--execution-date", required=True, help="Execution date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    # Validate and parse execution date
    try:
        execution_date = datetime.strptime(args.execution_date, "%Y-%m-%d").date()
        print(f"Running inference for execution_date: {execution_date}")
    except ValueError:
        raise ValueError(f"Invalid date format: {args.execution_date}. Expected YYYY-MM-DD")
    
    spark = get_spark("predict_next_day_purchase")
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.sql.functions import col, when, to_date, lit

    df = spark.read.format("delta").table("ml.user_behavior_3d_agg_feature").filter(col("prediction_date") < to_date(lit('2025-12-11')))
    # Đặc trưng về hành vi mua hàng (quan trọng nhất)
    purchase_behavior_features = [
        'purchase_count_3d',              # #1 - Số lần mua trong 3 ngày
        'add_to_cart_count_3d',           # #2 - Thêm vào giỏ
        'checkout_view_count_3d',         # #3 - Xem checkout
        'cart_conversion_rate_3d',        # #4 - Tỷ lệ chuyển đổi từ giỏ
        'purchase_conversion_rate_3d'   # #5 - Tỷ lệ mua hàng
    ]

    # Đặc trưng về engagement (tương tác)
    engagement_features = [
        'sessions_3d',                    # #6 - Số phiên
        'total_duration_3d',              # #7 - Tổng thời gian
        'avg_session_duration_3d',        # #8 - Thời gian TB/phiên
        'total_actions_3d',               # #9 - Tổng hành động
        'actions_per_session_3d'       # #10 - Hành động/phiên
    ]

    # Đặc trưng về sản phẩm & giá cả
    product_features = [
        'distinct_products_3d',           # #11 - Số sản phẩm unique xem
        'avg_product_price_3d'        # #12 - Giá TB sản phẩm quan tâm
    ]

    # Tổng hợp tất cả features
    selected_features = purchase_behavior_features + engagement_features + product_features
    for col_name in selected_features:
        df = df.withColumn(col_name, 
                        when(col(col_name).isNull(), 0.0)
                        .otherwise(col(col_name).cast("double")))

    # Đổi tên cột nhãn
    df = df.withColumn("label", col("label_purchase_tomorrow").cast("integer"))
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

    model = pipeline.fit(df)

    df_all = spark.read.format("delta").table("ml.user_behavior_3d_agg_feature")

    df_to_predict = df_all.filter(col("prediction_date") == to_date(lit(execution_date)))
    required_features = [
        # Purchase behavior
        'purchase_count_3d',
        'add_to_cart_count_3d',
        'checkout_view_count_3d',
        'cart_conversion_rate_3d',
        'purchase_conversion_rate_3d',
        # Engagement
        'sessions_3d',
        'total_duration_3d',
        'avg_session_duration_3d',
        'total_actions_3d',
        'actions_per_session_3d',
        # Product
        'distinct_products_3d',
        'avg_product_price_3d'
    ]
    # ==============

    predictions = model.transform(df_to_predict)

    predictions = predictions.withColumn("prediction_timestamp", current_timestamp())
    from pyspark.ml.functions import vector_to_array
    key_cols = ["user_id", "prediction_date"]

    required_features = [
        # Purchase behavior
        'purchase_count_3d',
        'add_to_cart_count_3d',
        'checkout_view_count_3d',
        'cart_conversion_rate_3d',
        'purchase_conversion_rate_3d',

        # Engagement
        'sessions_3d',
        'total_duration_3d',
        'avg_session_duration_3d',
        'total_actions_3d',
        'actions_per_session_3d',

        # Product
        'distinct_products_3d',
        'avg_product_price_3d'
    ]

    df_predict_input = (
        df_to_predict.select(*(key_cols + required_features))
    )

    df_predict_input = df_predict_input.fillna(0)

    predictions = model.transform(df_predict_input)

    predictions_final = (
        predictions
        .withColumn("probability_arr", vector_to_array(col("probability")))
        .withColumn("prediction_timestamp", current_timestamp())
        .select(
            "user_id",
            "prediction_date",
            col("prediction").cast("int").alias("will_purchase_tomorrow"),
            col("probability_arr")[1].alias("purchase_probability"),
            "prediction_timestamp"
        )
    )
    predictions_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"prediction_date = DATE('{execution_date}')") \
        .saveAsTable("ml.next_day_purchase_prediction")
    # # ===============================
    # # 1. Load input feature table
    # # ===============================
    # df_all = (
    #     spark.read.format("delta")
    #     .table("ml.user_behavior_3d_agg_feature")
    #     .filter(col("prediction_date") == lit(execution_date))
    # )

    # key_cols = ["user_id", "prediction_date"]

    # required_features = [
    #     "purchase_count_3d",
    #     "add_to_cart_count_3d",
    #     "checkout_view_count_3d",
    #     "cart_conversion_rate_3d",
    #     "purchase_conversion_rate_3d",
    #     "sessions_3d",
    #     "total_duration_3d",
    #     "avg_session_duration_3d",
    #     "total_actions_3d",
    #     "actions_per_session_3d",
    #     "distinct_products_3d",
    #     "avg_product_price_3d",
    # ]

    # # ===============================
    # # 2. Filter records cần predict
    # # ===============================
    # df_predict_input = (
    #     df_all
    #     .filter(col("label_purchase_tomorrow").isNull())
    #     .select(*(key_cols + required_features))
    #     .fillna(0)
    # )

    # # ===============================
    # # 3. Load trained model
    # # ===============================
    # model_path = "s3a://lakehouse/models/next_day_purchase_prediction_lr_v3"

    # model: PipelineModel = PipelineModel.load(model_path)

    # # ===============================
    # # 4. Predict
    # # ===============================
    # predictions = model.transform(df_predict_input)

    # # ===============================
    # # 5. Normalize output
    # # ===============================
    # predictions_final = (
    #     predictions
    #     .withColumn("prediction_timestamp", current_timestamp())
    #     .withColumn(
    #         "purchase_probability",
    #         vector_to_array("probability")[1]
    #     )
    #     .select(
    #         "user_id",
    #         "prediction_date",
    #         col("prediction").cast("int").alias("will_purchase_tomorrow"),
    #         "purchase_probability",
    #         "prediction_timestamp",
    #     )
    # )

    # # ===============================
    # # 6. Write result to Delta
    # # ===============================
    # (
    #     predictions_final
    #     .write
    #     .format("delta")
    #     .mode("overwrite")  
    #     .option("overwriteSchema", "true")
    #     .saveAsTable("ml.next_day_purchase_prediction")
    # )

    spark.stop()

