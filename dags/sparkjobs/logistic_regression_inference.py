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

    # ===============================
    # 1. Load input feature table
    # ===============================
    df_all = (
        spark.read.format("delta")
        .table("ml.user_behavior_3d_agg_feature")
        .filter(col("prediction_date") == lit(execution_date))
    )

    key_cols = ["user_id", "prediction_date"]

    required_features = [
        "purchase_count_3d",
        "add_to_cart_count_3d",
        "checkout_view_count_3d",
        "cart_conversion_rate_3d",
        "purchase_conversion_rate_3d",
        "sessions_3d",
        "total_duration_3d",
        "avg_session_duration_3d",
        "total_actions_3d",
        "actions_per_session_3d",
        "distinct_products_3d",
        "avg_product_price_3d",
    ]

    # ===============================
    # 2. Filter records cần predict
    # ===============================
    df_predict_input = (
        df_all
        .filter(col("label_purchase_tomorrow").isNull())
        .select(*(key_cols + required_features))
        .fillna(0)
    )

    # ===============================
    # 3. Load trained model
    # ===============================
    model_path = "s3a://lakehouse/models/next_day_purchase_prediction_lr_v3"

    model: PipelineModel = PipelineModel.load(model_path)

    # ===============================
    # 4. Predict
    # ===============================
    predictions = model.transform(df_predict_input)

    # ===============================
    # 5. Normalize output
    # ===============================
    predictions_final = (
        predictions
        .withColumn("prediction_timestamp", current_timestamp())
        .withColumn(
            "purchase_probability",
            vector_to_array("probability")[1]
        )
        .select(
            "user_id",
            "prediction_date",
            col("prediction").cast("int").alias("will_purchase_tomorrow"),
            "purchase_probability",
            "prediction_timestamp",
        )
    )

    # ===============================
    # 6. Write result to Delta
    # ===============================
    (
        predictions_final
        .write
        .format("delta")
        .mode("overwrite")  
        .option("overwriteSchema", "true")
        .saveAsTable("ml.next_day_purchase_prediction")
    )

    spark.stop()

