#!/usr/bin/env python3
"""
Spark ML Inference Job for Purchase Prediction
Reads unpredicted customers from user_behavior_3d_agg_feature,
loads ML model from S3, performs inference, and writes results to ml.next_day_purchase_prediction
"""

import argparse
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType
import pickle
import boto3
from io import BytesIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PurchasePredictionInference:
    """Handle ML inference for purchase prediction"""
    
    def __init__(self, spark, model_s3_path, s3_endpoint, s3_access_key, s3_secret_key):
        self.spark = spark
        self.model_s3_path = model_s3_path
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.model = None
        
    def load_model_from_s3(self):
        """Load pickled ML model from S3/MinIO"""
        logger.info(f"Loading model from {self.model_s3_path}")
        
        try:
            # Parse S3 path
            path_parts = self.model_s3_path.replace("s3a://", "").split("/", 1)
            bucket_name = path_parts[0]
            object_key = path_parts[1]
            
            # Initialize S3 client
            s3_client = boto3.client(
                's3',
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key
            )
            
            # Download model
            buffer = BytesIO()
            s3_client.download_fileobj(bucket_name, object_key, buffer)
            buffer.seek(0)
            
            # Load model
            self.model = pickle.load(buffer)
            logger.info("Model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def get_unpredicted_customers(self, execution_date):
        """
        Query customers who haven't been predicted yet
        Logic: Get customers from user_behavior_3d_agg_feature who don't exist 
        in next_day_purchase_prediction for the target prediction date
        """
        logger.info(f"Fetching unpredicted customers for date: {execution_date}")
        
        # Target prediction date (next day)
        target_date = (datetime.strptime(execution_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        
        query = f"""
        SELECT 
            f.customer_id,
            f.feature_date,
            f.total_sessions_3d,
            f.total_page_views_3d,
            f.total_add_to_cart_3d,
            f.total_purchases_3d,
            f.total_session_duration_3d,
            f.avg_session_duration_3d,
            f.distinct_products_viewed_3d,
            f.distinct_categories_viewed_3d,
            f.cart_to_purchase_rate_3d,
            f.avg_time_on_product_page_3d,
            f.bounce_rate_3d,
            f.weekend_activity_rate_3d,
            f.evening_activity_rate_3d,
            f.mobile_usage_rate_3d,
            f.search_to_purchase_rate_3d,
            f.product_detail_view_rate_3d,
            f.days_since_last_purchase,
            f.recency_score,
            f.frequency_score,
            f.engagement_score
        FROM lakehouse.ml.user_behavior_3d_agg_feature f
        LEFT JOIN lakehouse.ml.next_day_purchase_prediction p
            ON f.customer_id = p.customer_id 
            AND p.prediction_date = '{target_date}'
        WHERE f.feature_date = '{execution_date}'
            AND p.customer_id IS NULL
        """
        
        df = self.spark.sql(query)
        count = df.count()
        logger.info(f"Found {count} customers to predict")
        
        return df, target_date
    
    def prepare_features(self, df):
        """Prepare feature vector for model inference"""
        logger.info("Preparing features for inference")
        
        # Define feature columns (must match training)
        feature_columns = [
            'total_sessions_3d',
            'total_page_views_3d',
            'total_add_to_cart_3d',
            'total_purchases_3d',
            'total_session_duration_3d',
            'avg_session_duration_3d',
            'distinct_products_viewed_3d',
            'distinct_categories_viewed_3d',
            'cart_to_purchase_rate_3d',
            'avg_time_on_product_page_3d',
            'bounce_rate_3d',
            'weekend_activity_rate_3d',
            'evening_activity_rate_3d',
            'mobile_usage_rate_3d',
            'search_to_purchase_rate_3d',
            'product_detail_view_rate_3d',
            'days_since_last_purchase',
            'recency_score',
            'frequency_score',
            'engagement_score'
        ]
        
        # Handle nulls - fill with 0
        for col in feature_columns:
            df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0.0)))
        
        return df, feature_columns
    
    def predict_batch(self, df, feature_columns):
        """Perform batch prediction"""
        logger.info("Performing batch prediction")
        
        # Collect data for prediction (for large datasets, consider using pandas_udf)
        pandas_df = df.select(['customer_id'] + feature_columns).toPandas()
        
        # Prepare features
        X = pandas_df[feature_columns].values
        
        # Predict
        predictions = self.model.predict(X)
        prediction_proba = self.model.predict_proba(X)[:, 1]  # Probability of class 1 (will purchase)
        
        # Add predictions to dataframe
        pandas_df['will_purchase_next_day'] = predictions
        pandas_df['purchase_probability'] = prediction_proba
        
        # Convert back to Spark DataFrame
        result_df = self.spark.createDataFrame(pandas_df)
        
        return result_df
    
    def write_predictions(self, df, target_date):
        """Write predictions to next_day_purchase_prediction table"""
        logger.info(f"Writing predictions to ml.next_day_purchase_prediction")
        
        # Add metadata columns
        df = df.withColumn('prediction_date', F.lit(target_date).cast('date'))
        df = df.withColumn('predicted_at', F.current_timestamp())
        df = df.withColumn('model_version', F.lit('v1.0'))  # Update with your versioning
        
        # Select final columns
        final_df = df.select(
            'customer_id',
            'prediction_date',
            'will_purchase_next_day',
            F.col('purchase_probability').cast(DoubleType()),
            'predicted_at',
            'model_version'
        )
        
        # Write to table (append mode)
        final_df.write \
            .mode('append') \
            .format('iceberg') \
            .saveAsTable('lakehouse.ml.next_day_purchase_prediction')
        
        count = final_df.count()
        logger.info(f"Successfully wrote {count} predictions")
        
        # Log summary statistics
        summary = final_df.agg(
            F.count('*').alias('total'),
            F.sum(F.when(F.col('will_purchase_next_day') == 1, 1).otherwise(0)).alias('predicted_buyers'),
            F.avg('purchase_probability').alias('avg_probability')
        ).collect()[0]
        
        logger.info(f"Prediction Summary - Total: {summary['total']}, "
                   f"Predicted Buyers: {summary['predicted_buyers']}, "
                   f"Avg Probability: {summary['avg_probability']:.4f}")
    
    def run(self, execution_date):
        """Main execution flow"""
        logger.info(f"Starting ML inference pipeline for {execution_date}")
        
        try:
            # Step 1: Load model
            self.load_model_from_s3()
            
            # Step 2: Get unpredicted customers
            df, target_date = self.get_unpredicted_customers(execution_date)
            
            if df.count() == 0:
                logger.info("No customers to predict. Exiting.")
                return
            
            # Step 3: Prepare features
            df, feature_columns = self.prepare_features(df)
            
            # Step 4: Perform prediction
            predictions_df = self.predict_batch(df, feature_columns)
            
            # Step 5: Write results
            self.write_predictions(predictions_df, target_date)
            
            logger.info("ML inference pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Purchase Prediction ML Inference')
    parser.add_argument('--execution-date', required=True, 
                       help='Execution date in YYYY-MM-DD format')
    parser.add_argument('--model-s3-path', 
                       default='s3a://lakehouse/models/purchase_prediction/model.pkl',
                       help='S3 path to the ML model')
    parser.add_argument('--s3-endpoint', 
                       default='http://minio.default.svc.cluster.local:9000',
                       help='S3/MinIO endpoint URL')
    parser.add_argument('--s3-access-key', 
                       default='minioadmin',
                       help='S3 access key')
    parser.add_argument('--s3-secret-key', 
                       default='minioadmin',
                       help='S3 secret key')
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("PurchasePredictionInference") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Run inference pipeline
        inference = PurchasePredictionInference(
            spark=spark,
            model_s3_path=args.model_s3_path,
            s3_endpoint=args.s3_endpoint,
            s3_access_key=args.s3_access_key,
            s3_secret_key=args.s3_secret_key
        )
        
        inference.run(args.execution_date)
        
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
