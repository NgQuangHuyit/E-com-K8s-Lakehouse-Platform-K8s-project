from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.operators.email import EmailOperator
from datetime import timedelta

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_sftp_ingest_log",
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",   
    default_args=default_args,
    catchup=True,     
    max_active_runs=1 
) as dag:

    ingest_log = SFTPToS3Operator(
        task_id="ingest_log_file",
        sftp_conn_id="web-server-log",       
        s3_conn_id ="minio_default",    
       
        sftp_path="/home/dev/logs/ingest_date={{ ds }}/*.ndjson",
        s3_bucket="lakehouse",
        s3_key="bronze/user_activity_logs/ingest_date={{ ds }}/{{ filename }}",
        use_temp_file=True
    )

    ingest_log
