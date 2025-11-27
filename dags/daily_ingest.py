from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from datetime import datetime
from airflow.utils.dates import days_ago

S3_CONN_ID = "minio_default"    
S3_BUCKET = "lakehouse"  
REMOTE_S3_PATH = "tmp/orders/ingest_date={{ ds }}/" 
MYSQL_CONN_ID = "mysql_oltp"

with DAG(
    dag_id='daily_ingestion',
    start_date=days_ago(5),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2
) as dag:
    # mysql_to_s3_task = SqlToS3Operator(
    #     task_id='transfer_mysql_data_to_s3',
    #     sql_conn_id='mysql_default',  # Reference to your MySQL connection
    #     aws_conn_id='aws_default',    # Reference to your AWS connection
    #     query='SELECT * FROM your_table;',  # Your SQL query to extract data
    #     s3_bucket='your-s3-bucket-name', # Your S3 bucket name
    #     s3_key='data/your_table_data.csv', # The S3 key (path and filename) for the output
    #     replace=True, # Whether to replace the file if it already exists
    #     file_format='csv', # Output file format (e.g., 'csv', 'json')
    # )
    query = 'SELECT * FROM orders Limit 100000'

    order_ingestion = SqlToS3Operator(
        task_id='transfer_orders_data_to_s3',
        sql_conn_id=MYSQL_CONN_ID,
        aws_conn_id=S3_CONN_ID,
        query=query,
        s3_bucket=S3_BUCKET,
        s3_key=f"{REMOTE_S3_PATH}orders_data.csv",
        replace=True,
        file_format='csv',
    )