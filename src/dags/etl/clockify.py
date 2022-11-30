import airflow
from airflow import DAG
from datetime import timedelta
from utils.hooks.clockify_hook import ClockifyHook
from airflow.operators.python import PythonOperator
import sys
import os

dag_name = os.path.basename(sys.argv[0][:-3])

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

clockify_dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    description='Integration with clockify to extract detailed reports to BQ',
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=20))


def clockify_to_fs(**kwargs):
    clockify = ClockifyHook()

    report_df = clockify.get_detailed_report_df(
        start_date='2022-10-01 00:00:00',
        end_date='2022-11-15 23:59:59'
    )

    report_df.to_csv('test_save.csv', index=False)


def fs_to_bq(**kwargs):
    print('fs_to_bq')


def bq_transform(**kwargs):
    print('bq_transform')


with clockify_dag as dag:
    extract = PythonOperator(
        task_id='extract',
        provide_context=True,
        python_callable=clockify_to_fs)

    load = PythonOperator(
        task_id='load',
        provide_context=True,
        python_callable=fs_to_bq)

    transform = PythonOperator(
        task_id='transform',
        provide_context=True,
        python_callable=bq_transform)

    extract >> load >> transform
