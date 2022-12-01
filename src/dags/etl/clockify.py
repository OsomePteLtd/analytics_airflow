import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from utils.hooks.clockify_hook import ClockifyHook
from utils.utils import get_dag_workdir_path_from_context

import logging
from datetime import timedelta, datetime

SUB_PATH = 'temp_extracts/'
END_DATE_KEY = 'end_date'

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

clockify_dag = DAG(
    dag_id='clockify',
    default_args=default_args,
    description='Integration with clockify to extract detailed reports to BQ',
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=20))


def clockify_to_fs(**kwargs):
    # getting things from context
    context = kwargs
    run_id = context['dag_run'].run_id
    ti = context['ti']

    # initializing workdir and hook
    workdir = get_dag_workdir_path_from_context(context, SUB_PATH)
    clockify = ClockifyHook()

    # getting previous end date and creating a new one
    prev_end_date = ti.xcom_pull(key=END_DATE_KEY, include_prior_dates=True)

    logging.info(f'prev date = {prev_end_date}')

    if prev_end_date:
        start_date = prev_end_date if type(prev_end_date) == str else prev_end_date[0]
    else:
        start_date = '2022-10-01 00:00:00'  # default start_date

    end_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S') + timedelta(days=5)
    end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')
    # end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

    if start_date == end_date:

        logging.warning('Start date and end date are the same, skipping')
        raise AirflowSkipException

    # requesting a report

    report_df = clockify.get_detailed_report_df(
        start_date=start_date,
        end_date=end_date
    )

    # saving to workdir
    filename = f'{workdir}{run_id}.csv'
    report_df.to_csv(filename, index=False)
    logging.info(f'Saved extracted df to {filename}')

    # passing current end date to xcom
    ti.xcom_push(key=END_DATE_KEY, value=end_date)


def fs_to_bq(**kwargs):
    print('fs_to_bq')


def bq_transform(**kwargs):
    print('bq_transform')


def cleaner(**kwargs):
    # remove temp files that were uploaded and xcom variables
    pass


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

    cleaning_trails = PythonOperator(
        task_id='cleaning_trails',
        provide_context=True,
        python_callable=cleaner
    )

    extract >> load >> transform >> cleaning_trails
