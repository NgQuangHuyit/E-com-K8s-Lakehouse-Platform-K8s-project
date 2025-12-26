from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
import os
import json

logger = logging.getLogger(__name__)

# Spark configuration
SPARK_IMAGE = "your-registry/spark-ml-inference:latest"  # Update with your Docker registry
SPARK_NAMESPACE = "default"  # Update with your K8s namespace

# DBT configuration
DBT_PROJECT_DIR = os.getenv("DBT_HOME_PROJECT", "/opt/airflow/dags/repo/dags/ecom_lakehouse_pipeline")
DBT_MANIFEST_PATH = f"{DBT_PROJECT_DIR}/target/manifest.json"

# S3/MinIO configuration
MODEL_S3_PATH = "s3a://lakehouse/models/purchase_prediction/model.pkl"
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio.default.svc.cluster.local:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")


def parse_dbt_manifest(layer_selector):
    """Parse DBT manifest to get models and dependencies"""
    try:
        with open(DBT_MANIFEST_PATH, 'r') as f:
            manifest = json.load(f)
        
        models = {}
        nodes = manifest.get('nodes', {})
        
        for node_id, node_data in nodes.items():
            if not node_id.startswith('model.'):
                continue
            
            model_name = node_data.get('name')
            model_path = node_data.get('path', '')
            
            if layer_selector not in model_path:
                continue
            
            depends_on = node_data.get('depends_on', {}).get('nodes', [])
            upstream_models = [
                dep.split('.')[-1] for dep in depends_on 
                if dep.startswith('model.')
            ]
            
            models[model_name] = {
                'path': model_path,
                'upstream': upstream_models,
                'node_id': node_id
            }
        
        logger.info(f"Found {len(models)} models for layer '{layer_selector}'")
        return models
    
    except FileNotFoundError:
        logger.warning(f"Manifest not found at {DBT_MANIFEST_PATH}. Run 'dbt compile' first.")
        return {}
    except Exception as e:
        logger.error(f"Error parsing manifest: {e}")
        return {}


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="marketing_campain_datapipeline_with_inference_lr_model",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ml", "inference", "marketing"]
) as dag:

    # Spark ML Inference Job
    spark_inference_job = SparkKubernetesOperator(
        task_id="spark_ml_inference",
        namespace=SPARK_NAMESPACE,
        application_file="spark_ml_inference_job.yaml",  # We'll create this
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    # Alternative: Using BashOperator with spark-submit if SparkKubernetesOperator not available
    # spark_inference_job = BashOperator(
    #     task_id="spark_ml_inference",
    #     bash_command="""
    #     spark-submit \
    #         --master k8s://https://kubernetes.default.svc:443 \
    #         --deploy-mode cluster \
    #         --name purchase-prediction-inference \
    #         --conf spark.executor.instances=2 \
    #         --conf spark.kubernetes.container.image={{ params.spark_image }} \
    #         --conf spark.kubernetes.namespace={{ params.namespace }} \
    #         --conf spark.hadoop.fs.s3a.endpoint={{ params.s3_endpoint }} \
    #         --conf spark.hadoop.fs.s3a.access.key={{ params.s3_access_key }} \
    #         --conf spark.hadoop.fs.s3a.secret.key={{ params.s3_secret_key }} \
    #         --conf spark.hadoop.fs.s3a.path.style.access=true \
    #         --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    #         local:///opt/spark/jobs/ml_inference.py \
    #         --execution-date {{ ds }}
    #     """,
    #     params={
    #         "spark_image": SPARK_IMAGE,
    #         "namespace": SPARK_NAMESPACE,
    #         "s3_endpoint": S3_ENDPOINT,
    #         "s3_access_key": S3_ACCESS_KEY,
    #         "s3_secret_key": S3_SECRET_KEY,
    #     }
    # )

    # Compile DBT to generate manifest
    dbt_compile = BashOperator(
        task_id="dbt_compile",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt compile",
        env={
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            **os.environ
        },
        append_env=True
    )

    # Task Group: Gold Marketing layer
    with TaskGroup(group_id="gold_marketing") as marketing_group:
        marketing_models = parse_dbt_manifest("gold/marketing")
        marketing_tasks = {}
        
        # Create task for each marketing model
        for model_name, model_info in marketing_models.items():
            task = BashOperator(
                task_id=f"run_{model_name}",
                bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select {model_name}",
                env={
                    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
                    **os.environ
                },
                append_env=True
            )
            marketing_tasks[model_name] = task
        
        # Set dependencies based on DBT lineage
        for model_name, model_info in marketing_models.items():
            for upstream_model in model_info['upstream']:
                if upstream_model in marketing_tasks:
                    marketing_tasks[upstream_model] >> marketing_tasks[model_name]

    # DBT test marketing models
    dbt_test_marketing = BashOperator(
        task_id="dbt_test_marketing",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select gold/marketing",
        env={
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            **os.environ
        },
        append_env=True
    )

    # Task dependencies
    spark_inference_job >> dbt_compile >> marketing_group >> dbt_test_marketing
