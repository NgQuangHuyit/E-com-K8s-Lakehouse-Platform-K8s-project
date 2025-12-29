from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from datetime import datetime, timedelta
import logging
import os
import json

logger = logging.getLogger(__name__)

SFTP_CONN_ID = "web-server-log"
S3_CONN_ID = "minio_default"    
S3_BUCKET = "lakehouse"  
SFTP_REMOTE_PATH = "/home/dev/logs/ingest_date={{ ds }}/" 
REMOTE_S3_PATH = "bronze/user_activity_logs/ingest_date={{ ds }}/" 

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
            
            # Filter by layer selector (e.g., "silver/activity_logs" or "gold/ml")
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

def get_sftp_files_to_transfer(sftp_conn_id, sftp_remote_path,s3_remote_path, **kwargs):
    """
    Lists files in a given SFTP directory and returns their names.
    """
    logger.info(f"Listing files in SFTP path: {sftp_remote_path}")
    sftp_hook = SFTPHook(ssh_conn_id=sftp_conn_id)
    try:
        file_list = sftp_hook.list_directory(sftp_remote_path)
    except FileNotFoundError:
        file_list = []
    if not file_list:
        logging.info(f"No files found in {sftp_remote_path}. Skipping transfer.")
        return [] 

    source_target_pairs = [
        {
            "sftp_path": f"{sftp_remote_path}{file_name}",
            "s3_key": f"{s3_remote_path}{file_name}"
        }
        for file_name in file_list
    ]

    logger.info(f"Files to transfer: {source_target_pairs}")

    return source_target_pairs  
    # source_files = list(map(lambda file_name: f"{sftp_remote_path}{file_name}", file_list))
    # logger.info(f"Files found: {source_files}")
    # target_file_s3_key = list(map(lambda file_name: f"{REMOTE_S3_PATH}{file_name}", file_list))
    # logger.info(f"Target S3 keys: {target_file_s3_key}")
    # return {
    #     "source_files": source_files,
    #     "target_file_s3_key": target_file_s3_key
    # }


default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="user_activity_logs_data_pipeline",
    start_date=datetime(2025, 12, 15),
    schedule_interval='@daily',  
    default_args=default_args,
    catchup=True,     
    max_active_runs=1,
    max_active_tasks=1

) as dag:

    list_sftp_files_task = PythonOperator(
        task_id="list_sftp_files",
        python_callable=get_sftp_files_to_transfer,
        op_kwargs={
            "sftp_conn_id": SFTP_CONN_ID,
            "sftp_remote_path": SFTP_REMOTE_PATH,
            "s3_remote_path": REMOTE_S3_PATH
        },
    )

    transfer_file_to_s3 = SFTPToS3Operator.partial(
        task_id="transfer_file_to_s3",
        sftp_conn_id=SFTP_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        s3_bucket=S3_BUCKET,
    ).expand_kwargs(
        list_sftp_files_task.output
    )
    spark_partition_update_user_activity_logs = JdbcOperator(
                                    task_id="update_spark_partition_user_activity_logs",
                                    jdbc_conn_id="spark_thrift_default",
                                    sql="MSCK repair table bronze.user_activity_logs;",
                                    hook_params={
                                        "driver_class": "org.apache.hive.jdbc.HiveDriver",
                                        "driver_path": "/opt/airflow/jars/hive-jdbc-3.1.3-standalone.jar"
                                    }
                                )
    with TaskGroup(group_id="drop_tmp_tables") as drop_tmp_tables_group:
        spark_drop_actions_daily_tmp_tbl = JdbcOperator(
                                        task_id="drop_actions_daily_tmp_tbl",
                                        jdbc_conn_id="spark_thrift_default",
                                        sql="DROP TABLE IF EXISTS default.actions_daily_tmp;",
                                        hook_params={
                                            "driver_class": "org.apache.hive.jdbc.HiveDriver",
                                            "driver_path": "/opt/airflow/jars/hive-jdbc-3.1.3-standalone.jar"
                                        }
                                    )
        
        spark_drop_sessions_daily_tmp_tbl = JdbcOperator(
                                        task_id="drop_sessions_daily_tmp_tbl",
                                        jdbc_conn_id="spark_thrift_default",
                                        sql="DROP TABLE IF EXISTS default.sessions_daily_tmp;",
                                        hook_params={
                                            "driver_class": "org.apache.hive.jdbc.HiveDriver",
                                            "driver_path": "/opt/airflow/jars/hive-jdbc-3.1.3-standalone.jar"
                                        }
                                    )
        spark_drop_user_daily_metric_tmp_tbl = JdbcOperator(
                                        task_id="drop_user_daily_metric_tmp_tbl",
                                        jdbc_conn_id="spark_thrift_default",
                                        sql="DROP TABLE IF EXISTS default.user_daily_metric_tmp;",
                                        hook_params={
                                            "driver_class": "org.apache.hive.jdbc.HiveDriver",
                                            "driver_path": "/opt/airflow/jars/hive-jdbc-3.1.3-standalone.jar"
                                        }
                                    )
        

        
    # Compile DBT to generate manifest
    dbt_compile = BashOperator(
        task_id="dbt_compile",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt compile  "
                     f"--vars '{{\"etl_date\": \"{{{{ ds }}}}\", "
                            f"\"etl_year\": \"{{{{ execution_date.strftime('%Y') }}}}\", "
                            f"\"etl_month\": \"{{{{ execution_date.strftime('%m') }}}}\"}}'",
        env={
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            **os.environ
        },
        append_env=True
    )

    # Task Group: Silver layer with auto-generated tasks per model
    with TaskGroup(group_id="silver_activity_logs") as silver_group:
        silver_models = parse_dbt_manifest("silver/activity_logs")
        silver_tasks = {}
        
        # Create task for each silver model
        for model_name, model_info in silver_models.items():
            task = BashOperator(
                task_id=f"run_{model_name}",
                bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select {model_name} "
                            f" --vars '{{\"etl_date\": \"{{{{ ds }}}}\", "
                            f"\"etl_year\": \"{{{{ execution_date.strftime('%Y') }}}}\", "
                            f"\"etl_month\": \"{{{{ execution_date.strftime('%m') }}}}\"}}'",
                env={
                    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
                    **os.environ
                },
                append_env=True
            )
            silver_tasks[model_name] = task
        
        # Set dependencies based on DBT lineage
        for model_name, model_info in silver_models.items():
            for upstream_model in model_info['upstream']:
                if upstream_model in silver_tasks:
                    silver_tasks[upstream_model] >> silver_tasks[model_name]

    # Task Group: Gold ML layer with auto-generated tasks
    with TaskGroup(group_id="gold_ml") as gold_group:
        gold_models = parse_dbt_manifest("gold/ml")
        gold_tasks = {}
        
        # Create task for each gold model
        for model_name, model_info in gold_models.items():
            task = BashOperator(
                task_id=f"run_{model_name}",
                bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select {model_name} "
                            f" --vars '{{\"etl_date\": \"{{{{ ds }}}}\", "
                            f"\"etl_year\": \"{{{{ execution_date.strftime('%Y') }}}}\", "
                            f"\"etl_month\": \"{{{{ execution_date.strftime('%m') }}}}\"}}'",
                env={
                    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
                    **os.environ
                },
                append_env=True
            )
            gold_tasks[model_name] = task
        
        # Set dependencies within gold layer
        for model_name, model_info in gold_models.items():
            for upstream_model in model_info['upstream']:
                if upstream_model in gold_tasks:
                    gold_tasks[upstream_model] >> gold_tasks[model_name]

    # DBT test tasks
    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select silver/activity_logs",
        env={
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            **os.environ
        },
        append_env=True
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select gold/ml",
        env={
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            **os.environ
        },
        append_env=True
    )

    # Task dependencies across layers
    list_sftp_files_task >> transfer_file_to_s3 >> spark_partition_update_user_activity_logs >> dbt_compile >> silver_group >> dbt_test_silver >> drop_tmp_tables_group >> gold_group >> dbt_test_gold >> drop_tmp_tables_group
