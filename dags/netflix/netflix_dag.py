from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dummy import DummyOperator
import sys

sys.path.append('/home/airflow/airflow-code/dags/netflix/source_load')

from data_load import run_script


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='Netflix_Data_Analytics',
    default_args=default_args,
    description='This dag runs data analytics on top of netflix datasets',
    schedule_interval=timedelta(days=1),
)

credits_sensor = S3KeySensor(
    task_id='credits_rawfile_sensor',
    poke_interval=60 * 5,
    timeout=60 * 60 * 24 * 7,
    bucket_key='raw_files/credits.csv',
    wildcard_match=True,
    bucket_name='netflix-data-analytics',
    aws_conn_id='aws_default',
    dag=dag
)

titles_rawfile_sensor = S3KeySensor(
    task_id='titles_rawfile_sensor',
    poke_interval=60 * 5,
    timeout=60 * 60 * 24 * 7,
    bucket_key='raw_files/titles.csv',
    wildcard_match=True,
    bucket_name='netflix-data-analytics',
    aws_conn_id='aws_default',
    dag=dag
)
load_data_snowflake = PythonOperator(task_id='my_task'
    ,python_callable=run_script, 
    dag=dag)






start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> credits_rawfile_sensor >> titles_rawfile_sensor >> load_data_snowflake  >> end_task
