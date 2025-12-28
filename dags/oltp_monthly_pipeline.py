from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from airflow.providers.jdbc.operators.jdbc import JdbcOperator

import logging
import os
import json

logger = logging.getLogger(__name__)

S3_CONN_ID = "minio_default"    
S3_BUCKET = "lakehouse"  
REMOTE_S3_PATH = "tmp/orders/ingest_date={{ ds }}/" 
MYSQL_CONN_ID = "mysql_oltp"

# DBT configuration
DBT_PROJECT_DIR = os.getenv("DBT_HOME_PROJECT", "/opt/airflow/dags/repo/dags/ecom_lakehouse_pipeline")
DBT_MANIFEST_PATH = f"{DBT_PROJECT_DIR}/target/manifest.json"


def parse_dbt_manifest(layer_selector):
    try:
        with open(DBT_MANIFEST_PATH, 'r') as f:
            manifest = json.load(f)
        
        models = {}
        nodes = manifest.get('nodes', {})
        
        for node_id, node_data in nodes.items():
            # Filter only models (not tests, seeds, etc.)
            if not node_id.startswith('model.'):
                continue
            
            model_name = node_data.get('name')
            model_path = node_data.get('path', '')
            
            # Filter by layer selector (e.g., "silver/oltp" or "gold/sale_mart")
            if layer_selector not in model_path:
                continue
            
            # Get dependencies (upstream models)
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
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='oltp_monthly_pipeline_2',
    start_date=datetime(2025, 11, 15),
    schedule_interval=None,   
    default_args=default_args,
    catchup=False,     
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    with TaskGroup(group_id="ingest_transactional") as transactional_group:
        order_ingestion = SqlToS3Operator(
            task_id='transfer_orders',
            sql_conn_id=MYSQL_CONN_ID,
            aws_conn_id=S3_CONN_ID,
            query="""
                SELECT *
                FROM orders
                WHERE DATE_FORMAT(order_date, '%Y-%m') = '{{ ds[:7] }}'
                """,
            s3_bucket=S3_BUCKET,
            s3_key="bronze/orders/year={{ ds[:4] }}/month={{ ds[5:7] }}/orders_{{ ds[:4] }}{{ ds[5:7] }}.csv",
            replace=True,
            file_format='csv',
            pd_kwargs={
                "index": False,
                "header": False,
                "encoding": "utf-8"
            }
        )

        order_items_ingestion = SqlToS3Operator(
            task_id='transfer_order_items',
            sql_conn_id=MYSQL_CONN_ID,
            aws_conn_id=S3_CONN_ID,
            query="""
                WITH monthly_orders AS (
                    SELECT order_id
                    FROM orders
                    WHERE DATE_FORMAT(order_date, '%Y-%m') = '{{ ds[:7] }}'
                )
                SELECT oi.*
                FROM order_items oi
                JOIN monthly_orders mo ON oi.order_id = mo.order_id
                """,
            s3_bucket=S3_BUCKET,
            s3_key="bronze/order-items/year={{ ds[:4] }}/month={{ ds[5:7] }}/order_items_{{ ds[:4] }}{{ ds[5:7] }}.csv",
            replace=True,
            file_format='csv',
            pd_kwargs={
                "index": False,
                "header": False,
                "encoding": "utf-8"
            }
        )

    # Task Group: Master data snapshots ingestion
    with TaskGroup(group_id="ingest_snapshots") as snapshots_group:
        products_snapshot = SqlToS3Operator(
            task_id='transfer_products',
            sql_conn_id=MYSQL_CONN_ID,
            aws_conn_id=S3_CONN_ID,
            query='SELECT * FROM products;',
            s3_bucket=S3_BUCKET,
            s3_key="bronze/products/products_snapshot.csv",
            replace=True,
            file_format='csv',
            pd_kwargs={
                "index": False,
                "header": False,
                "encoding": "utf-8"
            }
        )

        categories_snapshot = SqlToS3Operator(
            task_id='transfer_categories',
            sql_conn_id=MYSQL_CONN_ID,
            aws_conn_id=S3_CONN_ID,
            query='SELECT * FROM categories;',
            s3_bucket=S3_BUCKET,
            s3_key="bronze/category/categories_snapshot.csv",
            replace=True,
            file_format='csv',
            pd_kwargs={
                "index": False,
                "header": False,
                "encoding": "utf-8"
            }
        )

        customers_snapshot = SqlToS3Operator(
            task_id='transfer_customers',
            sql_conn_id=MYSQL_CONN_ID,
            aws_conn_id=S3_CONN_ID,
            query='SELECT * FROM customers;',
            s3_bucket=S3_BUCKET,
            s3_key="bronze/customer/customers_snapshot.csv",
            replace=True,
            file_format='csv',
            pd_kwargs={
                "index": False,
                "header": False,
                "encoding": "utf-8"
            }
        )

        payment_methods_snapshot = SqlToS3Operator(
            task_id='transfer_payment_methods',
            sql_conn_id=MYSQL_CONN_ID,
            aws_conn_id=S3_CONN_ID,
            query='SELECT * FROM payment_methods;',
            s3_bucket=S3_BUCKET,
            s3_key="bronze/payment-method/payment_methods_snapshot.csv",
            replace=True,
            file_format='csv',
            pd_kwargs={
                "index": False,
                "header": False,
                "encoding": "utf-8"
            }
        )

        brands_snapshot = SqlToS3Operator(
            task_id='transfer_brands',
            sql_conn_id=MYSQL_CONN_ID,
            aws_conn_id=S3_CONN_ID,
            query='SELECT * FROM brands;',
            s3_bucket=S3_BUCKET,
            s3_key="bronze/brands/brands_snapshot.csv",
            replace=True,
            file_format='csv',
            pd_kwargs={
                "index": False,
                "header": False,
                "encoding": "utf-8"
            }
        )

    # Ingestion coordination
    start_ingest = DummyOperator(task_id='start_ingestion')
    done_ingest = DummyOperator(task_id='done_ingestion')
    
    with TaskGroup(group_id="spark_partition_update") as spark_partition_update:

        spark_partition_update_orders = JdbcOperator(
                                    task_id="update_spark_partition",
                                    jdbc_conn_id="spark_thrift_default",
                                    sql="MSCK repair table bronze.orders",
                                    hook_params={
                                        "driver_class": "org.apache.hive.jdbc.HiveDriver",
                                        "driver_path": "/opt/airflow/jars/hive-jdbc-3.1.3-standalone.jar"
                                    }
                                )
        spark_partition_update_order_items = JdbcOperator(
                                    task_id="update_spark_partition_order_items",
                                    jdbc_conn_id="spark_thrift_default",
                                    sql="MSCK repair table bronze.order_items",
                                    hook_params={
                                        "driver_class": "org.apache.hive.jdbc.HiveDriver",
                                        "driver_path": "/opt/airflow/jars/hive-jdbc-3.1.3-standalone.jar"
                                    }
                                )
        spark_partition_update_orders >> spark_partition_update_order_items
    
    start_ingest >> [transactional_group, snapshots_group] >> done_ingest >> spark_partition_update


    # # Compile DBT to generate manifest
    # dbt_compile = BashOperator(
    #     task_id="dbt_compile",
    #     bash_command=f"cd {DBT_PROJECT_DIR} && dbt compile",
    #     env={
    #         "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    #         **os.environ
    #     },
    #     append_env=True
    # )

    # # Task Group: Silver OLTP layer with auto-generated tasks per model
    # with TaskGroup(group_id="silver_oltp") as silver_group:
    #     silver_models = parse_dbt_manifest("silver/oltp")
    #     silver_tasks = {}
        
    #     # Create task for each silver model
    #     for model_name, model_info in silver_models.items():
    #         task = BashOperator(
    #             task_id=f"run_{model_name}",
    #             bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select {model_name}",
    #             env={
    #                 "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    #                 **os.environ
    #             },
    #             append_env=True
    #         )
    #         silver_tasks[model_name] = task
        
    #     # Set dependencies based on DBT lineage
    #     for model_name, model_info in silver_models.items():
    #         for upstream_model in model_info['upstream']:
    #             if upstream_model in silver_tasks:
    #                 silver_tasks[upstream_model] >> silver_tasks[model_name]

    # # Task Group: Gold sale_mart layer with auto-generated tasks
    # with TaskGroup(group_id="gold_sale_mart") as gold_group:
    #     gold_models = parse_dbt_manifest("gold/sale_mart")
    #     gold_tasks = {}
        
    #     # Create task for each gold model
    #     for model_name, model_info in gold_models.items():
    #         task = BashOperator(
    #             task_id=f"run_{model_name}",
    #             bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select {model_name}",
    #             env={
    #                 "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    #                 **os.environ
    #             },
    #             append_env=True
    #         )
    #         gold_tasks[model_name] = task
        
    #     # Set dependencies within gold layer
    #     for model_name, model_info in gold_models.items():
    #         for upstream_model in model_info['upstream']:
    #             if upstream_model in gold_tasks:
    #                 gold_tasks[upstream_model] >> gold_tasks[model_name]

    # # DBT test tasks
    # dbt_test_silver = BashOperator(
    #     task_id="dbt_test_silver",
    #     bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select orders orders_items customers products categories brands payment_method",
    #     env={
    #         "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    #         **os.environ
    #     },
    #     append_env=True
    # )

    # dbt_test_gold = BashOperator(
    #     task_id="dbt_test_gold",
    #     bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select fact_orders dim_date dim_customers dim_payment_methods",
    #     env={
    #         "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    #         **os.environ
    #     },
    #     append_env=True
    # )

    # # Task dependencies across layers
    # done_ingest >> dbt_compile >> silver_group >> dbt_test_silver >> gold_group >> dbt_test_gold
