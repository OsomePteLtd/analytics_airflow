import airflow
from airflow import DAG
from datetime import timedelta
from utils.hooks.clockify_hook import ClockifyHook
from utils.config import get_dag_folder_path
from airflow.operators.python import PythonOperator

import sys
import os

DAG_NAME = 'clockify'
WORKING_DIRECTORY = get_dag_folder_path(DAG_NAME) + 'temp_extracts/'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

clockify_dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    description='Integration with clockify to extract detailed reports to BQ',
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=20))


def clockify_to_fs(**kwargs):

    context = kwargs
    run_id = context['dag_run'].run_id

    clockify = ClockifyHook()

    report_df = clockify.get_detailed_report_df(
        start_date='2022-10-01 00:00:00',
        end_date='2022-11-15 23:59:59'
    )

    filename = f'{WORKING_DIRECTORY}{run_id}.csv'
    report_df.to_csv(filename, index=False)


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
