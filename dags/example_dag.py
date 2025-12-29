from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Định nghĩa DAG
with DAG(
    dag_id='train_model_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['ml', 'training', 'model', 'manual'],
) as dag:

    task1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello from Airflow DAG!"'
    )

    # Task 2: Train model with PySpark
    # train_model_task = BashOperator(
    #     task_id="train_purchase_prediction_model",
    #     bash_command="""
    #             spark-submit \
    #             --deploy-mode client \
    #             --conf spark.dynamicAllocation.enabled=true \
    #             --conf spark.kubernetes.container.image=ngquanghuyit/spark-delta-lake:3.3 \
    #             --conf spark.kubernetes.executor.request.cores=500m \
    #             --conf spark.executor.instances=2 \
    #             --conf spark.dynamicAllocation.maxExecutors=3 \
    #             --conf spark.kubernetes.namespace=lakehouse \
    #             --conf spark.driver.host=airflow-scheduler.lakehouse.svc.cluster.local \
    #             --conf spark.driver.port=7078 \
    #             --conf spark.driver.bindAddress=0.0.0.0 \
    #             --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    #             --conf spark.driver.memory=1400m \
    #             --conf spark.executor.memory=1400m \
    #             /opt/airflow/dags/repo/dags/sparkjobs/train_model.py
    #     """,
    # )

    spark_submit_task = BashOperator(
        task_id="spark_ml_inference",
        bash_command="""
            spark-submit \
                --deploy-mode client \
                --conf spark.dynamicAllocation.enabled=true \
                --conf spark.kubernetes.container.image=ngquanghuyit/spark-delta-lake:3.3 \
                --conf spark.kubernetes.executor.request.cores=500m \
                --conf spark.executor.instances=2 \
                --conf spark.dynamicAllocation.maxExecutors=3 \
                --conf spark.kubernetes.namespace=lakehouse \
                --conf spark.driver.host=airflow-scheduler.lakehouse.svc.cluster.local \
                --conf spark.driver.port=7078 \
                --conf spark.driver.bindAddress=0.0.0.0 \
                --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
                --conf spark.driver.memory=1400m \
                --conf spark.executor.memory=1400m \
                /opt/airflow/dags/repo/dags/sparkjobs/train_model.py 
        """,
    )


    # Define thứ tự chạy
    task1 >> spark_submit_task
