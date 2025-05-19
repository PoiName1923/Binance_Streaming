from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator 
from code_python import loading_process


default_args = {
    'owner': 'ndtien',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'Project_With_Streaming_Data',
    default_args=default_args,
    description='Loading Training EDA Hourly',
    schedule_interval='0 * * * *',  # Chạy hàng giờ vào phút 0
    catchup=False,
    tags=['hourly_processing'],
) as dag:

    loading_task = BashOperator(
        task_id='loading_hourly_data',
        bash_command="python /opt/airflow/dags/code_python/loading_process.py",
        dag=dag
    )

    # Thêm các task khác nếu cần
    # run_this >> another_task