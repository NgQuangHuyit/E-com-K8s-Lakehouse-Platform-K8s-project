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
    dag_id='example_hello_world',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='*/5 * * * *',  # Chạy mỗi 5 phút
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['example'],
) as dag:

    task1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello from Airflow DAG!"'
    )

    task2 = BashOperator(
        task_id='print_time',
        bash_command='date'
    )

    task3 = BashOperator(
        task_id='print_goodbye',
        bash_command='echo "Goodbye from Airflow DAG!"'
    )

    # Define thứ tự chạy
    task1 >> task2 >> task3
