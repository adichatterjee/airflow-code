from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dummy import DummyOperator
from source_load.data_load import run_script


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

s3_sensor = S3KeySensor(
    task_id='s3_sensor_task',
    poke_interval=60 * 5,
    timeout=60 * 60 * 24 * 7,
    bucket_key='dags/test-file.csv',
    wildcard_match=True,
    bucket_name='s3-mwaa-airflow',
    aws_conn_id='aws_default',
    dag=dag
)

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> s3_sensor >> end_task
