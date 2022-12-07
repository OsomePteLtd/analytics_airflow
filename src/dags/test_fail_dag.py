import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from utils.utils import task_fail_slack_alert

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert,
}


def failed():
    print(0 / 0)


dag = DAG(
    'test_fail',
    default_args=default_args,
    description='test for fail notification',
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=20))

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = PythonOperator(
    task_id='test_fail',
    python_callable=failed
)