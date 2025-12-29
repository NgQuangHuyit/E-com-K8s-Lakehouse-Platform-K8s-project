from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ml-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="ml_train_purchase_prediction_model",
    start_date=datetime(2025, 12, 1),
    schedule_interval=None,  # Manual trigger only
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ml", "training", "model", "manual"],
    description="Train purchase prediction model using PySpark ML (Manual Run)"
) as dag:

    # Task 2: Train model with PySpark
    train_model_task = BashOperator(
        task_id="train_purchase_prediction_model",
        bash_command="""
            spark-submit \
                --deploy-mode client \
                --conf spark.dynamicAllocation.enabled=true \
                --conf spark.kubernetes.container.image=ngquanghuyit/spark-delta-lake:3.3 \
                --conf spark.kubernetes.driver.pod.name=spark-thrift-server-0 \
                --conf spark.kubernetes.executor.request.cores=500m \
                --conf spark.executor.instances=2 \
                --conf spark.dynamicAllocation.maxExecutors=4 \
                --conf spark.kubernetes.namespace=lakehouse \
                --conf spark.driver.host=spark-thrift-service \
                --conf spark.driver.bindAddress=spark-thrift-server-0 \
                --conf spark.driver.port=7078 \
                --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
                --conf spark.sql.adaptive.enabled=true \
                --conf spark.driver.memory=1400m \
                --conf spark.executor.memory=1400m \
                /opt/airflow/dags/repo/dags/sparkjobs/train_model.py
        """,
    )


    # Task 4: Log training completion
    def log_training_completion(**context):
        execution_date = context['ds']
        logger.info(f"🎉 Model training completed successfully!")
        logger.info(f"📅 Execution date: {execution_date}")
        logger.info(f"🔗 Model location: s3a://lakehouse/models/next_day_purchase_prediction_lr_v3")
        logger.info(f"🚀 Ready for inference pipeline!")
        return "Training pipeline complete"

    log_completion = PythonOperator(
        task_id="log_training_completion",
        python_callable=log_training_completion,
        provide_context=True,
    )

    # Task dependencies
    train_model_task >> log_completion
