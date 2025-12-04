from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator

S3_CONN_ID = "minio_default"    
S3_BUCKET = "lakehouse"  
REMOTE_S3_PATH = "tmp/orders/ingest_date={{ ds }}/" 
MYSQL_CONN_ID = "mysql_oltp"

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='daily_ingestion',
    start_date=datetime(2025, 11, 15),
    schedule_interval="@daily",   
    default_args=default_args,
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
    query_order = "SELECT * FROM orders where order_date = '{{ ds }}';"
    
    query_order_items = "SELECT oi.* FROM order_items oi JOIN orders o ON oi.order_id = o.order_id where o.order_date = '{{ ds }}';"

    query_snapshot_brands = 'SELECT * FROM brands;'

    query_snapshot_products = 'SELECT * FROM products;'

    query_snapshot_categories = 'SELECT * FROM categories;'

    query_snapshot_customers = 'SELECT * FROM customers;'

    query_snapshot_payment_methods = 'SELECT * FROM payment_methods;'


    order_ingestion = SqlToS3Operator(
        task_id='transfer_orders_data_to_s3',
        sql_conn_id=MYSQL_CONN_ID,
        aws_conn_id=S3_CONN_ID,
        query=query_order,
        s3_bucket=S3_BUCKET,
        s3_key="bronze/orders/ingest_date={{ ds }}/orders_data.csv",
        replace=True,
        file_format='csv',
        pd_kwargs={
        "index": False,   # Bỏ số thứ tự dòng (0, 1, 2...)
        "header": False,  # Bỏ tên cột (id, name, date...)
        "encoding": "utf-8" # Đảm bảo file csv đầu ra cũng chuẩn utf-8
    }

    )

    order_items_ingestion = SqlToS3Operator(
        task_id='transfer_order_items_data_to_s3',
        sql_conn_id=MYSQL_CONN_ID,
        aws_conn_id=S3_CONN_ID,
        query=query_order_items,
        s3_bucket=S3_BUCKET,
        s3_key="bronze/order-items/ingest_date={{ ds }}/order_items_data.csv",
        replace=True,
        file_format='csv',
        pd_kwargs={
        "index": False,   # Bỏ số thứ tự dòng (0, 1, 2...)
        "header": False,  # Bỏ tên cột (id, name, date...)
        "encoding": "utf-8" # Đảm bảo file csv đầu ra cũng chuẩn utf-8
    }

    )

    products_snapshot_ingestion = SqlToS3Operator(
        task_id='transfer_products_snapshot_to_s3',
        sql_conn_id=MYSQL_CONN_ID,
        aws_conn_id=S3_CONN_ID,
        query=query_snapshot_products,
        s3_bucket=S3_BUCKET,
        s3_key="bronze/products/ingest_date={{ ds }}/products_snapshot.csv",
        replace=True,
        file_format='csv',
        pd_kwargs={
        "index": False,   # Bỏ số thứ tự dòng (0, 1, 2...)
        "header": False,  # Bỏ tên cột (id, name, date...)
        "encoding": "utf-8" # Đảm bảo file csv đầu ra cũng chuẩn utf-8
    }

    )

    categories_snapshot_ingestion = SqlToS3Operator(
        task_id='transfer_categories_snapshot_to_s3',
        sql_conn_id=MYSQL_CONN_ID,
        aws_conn_id=S3_CONN_ID,
        query=query_snapshot_categories,
        s3_bucket=S3_BUCKET,
        s3_key="bronze/category/ingest_date={{ ds }}/categories_snapshot.csv",
        replace=True,
        file_format='csv',
        pd_kwargs={
        "index": False,   # Bỏ số thứ tự dòng (0, 1, 2...)
        "header": False,  # Bỏ tên cột (id, name, date...)
        "encoding": "utf-8" # Đảm bảo file csv đầu ra cũng chuẩn utf-8
    }

    )

    customers_snapshot_ingestion = SqlToS3Operator(
        task_id='transfer_customers_snapshot_to_s3',
        sql_conn_id=MYSQL_CONN_ID,
        aws_conn_id=S3_CONN_ID,
        query=query_snapshot_customers,
        s3_bucket=S3_BUCKET,
        s3_key="bronze/customer/ingest_date={{ ds }}/customers_snapshot.csv",
        replace=True,
        file_format='csv',
        pd_kwargs={
        "index": False,   # Bỏ số thứ tự dòng (0, 1, 2...)
        "header": False,  # Bỏ tên cột (id, name, date...)
        "encoding": "utf-8" # Đảm bảo file csv đầu ra cũng chuẩn utf-8
    }

    )

    payment_methods_snapshot_ingestion = SqlToS3Operator(
        task_id='transfer_payment_methods_snapshot_to_s3',
        sql_conn_id=MYSQL_CONN_ID,
        aws_conn_id=S3_CONN_ID,
        query=query_snapshot_payment_methods,
        s3_bucket=S3_BUCKET,
        s3_key="bronze/payment-method/ingest_date={{ ds }}/payment_methods_snapshot.csv",
        replace=True,
        file_format='csv',
        pd_kwargs={
        "index": False,   # Bỏ số thứ tự dòng (0, 1, 2...)
        "header": False,  # Bỏ tên cột (id, name, date...)
        "encoding": "utf-8" # Đảm bảo file csv đầu ra cũng chuẩn utf-8
    }
    )   

    brand_snapshot_ingestion = SqlToS3Operator(
        task_id='transfer_brands_snapshot_to_s3',
        sql_conn_id=MYSQL_CONN_ID,
        aws_conn_id=S3_CONN_ID,
        query=query_snapshot_brands,
        s3_bucket=S3_BUCKET,
        s3_key="bronze/brands/ingest_date={{ ds }}/brands_snapshot.csv",
        replace=True,
        file_format='csv',
        pd_kwargs={
        "index": False,   # Bỏ số thứ tự dòng (0, 1, 2...)
        "header": False,  # Bỏ tên cột (id, name, date...)
        "encoding": "utf-8" # Đảm bảo file csv đầu ra cũng chuẩn utf-8
    }
    )

    start_ingest = DummyOperator(
        task_id='start_ingestion'
    )
    done_ingest = DummyOperator(
        task_id='done_ingestion'
    )
    start_ingest >> [
        order_ingestion, 
        order_items_ingestion,
        products_snapshot_ingestion,
        categories_snapshot_ingestion,
        customers_snapshot_ingestion,
        payment_methods_snapshot_ingestion,
        brand_snapshot_ingestion
    ] >> done_ingest


    
