from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.operators.email import EmailOperator
from datetime import timedelta


SFTP_CONN_ID = "web-server-log"  
S3_CONN_ID = "minio_default"    
S3_BUCKET = "lakehouse"  
SFTP_REMOTE_PATH = "/home/dev/logs/ingest_date={{ ds }}/" 
REMOTE_S3_PATH = "bronze/user_activity_logs/ingest_date={{ ds }}/" 

def get_sftp_files_to_transfer(sftp_conn_id, sftp_remote_path, **kwargs):
    """
    Lists files in a given SFTP directory and returns their names.
    """
    sftp_hook = SFTPHook(sftp_conn_id=sftp_conn_id)
    file_list = sftp_hook.list_directory(sftp_remote_path)
    return file_list


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

    list_sftp_files_task = PythonOperator(
        task_id="list_sftp_files",
        python_callable=get_sftp_files_to_transfer,
        op_kwargs={
            "sftp_conn_id": SFTP_CONN_ID,
            "sftp_remote_path": SFTP_REMOTE_PATH,
        },
    )

    transfer_file_to_s3 = SFTPToS3Operator.partial(
        task_id="transfer_file_to_s3",
        sftp_conn_id=SFTP_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        s3_bucket=S3_BUCKET,
    ).expand(
        sftp_path=list_sftp_files_task.output.map(lambda file_name: f"{SFTP_REMOTE_PATH}{file_name}"),
        s3_key=list_sftp_files_task.output.map(lambda file_name: f"{REMOTE_S3_PATH}{file_name}"),
    )


    # ingest_log = SFTPToS3Operator(
    #     task_id="ingest_log_file",
    #     sftp_conn_id="web-server-log",       
    #     s3_conn_id ="minio_default",    
       
    #     sftp_path="/home/dev/logs/ingest_date={{ ds }}/*.ndjson",
    #     s3_bucket="lakehouse",
    #     s3_key="bronze/user_activity_logs/ingest_date={{ ds }}/{{ filename }}",
    #     use_temp_file=True
    # )

    list_sftp_files_task >> transfer_file_to_s3
