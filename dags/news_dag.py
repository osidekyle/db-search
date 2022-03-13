from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from news_etl import get_news_data

default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,1,0,0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'news_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(minutes=1)
)

run_etl = PythonOperator(
    task_id = 'Getting_news_data',
    python_callable=get_news_data,
    dag=dag
)

store_data = BashOperator(
    task_id = 'store_in_cassandra',
    bash_command = 'echo Writing to Database'
)



run_etl >> store_data