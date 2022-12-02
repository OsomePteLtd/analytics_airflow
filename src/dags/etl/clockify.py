import os

import airflow
from airflow import DAG
from airflow.models import XCom
from airflow.utils.session import provide_session
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.api.common.trigger_dag import trigger_dag
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from utils.hooks.clockify_hook import ClockifyHook
from utils.utils import get_dag_workdir_path_from_context
from utils.config import AIRFLOW_DATASET_ID, AIRFLOW_TMP_DATASET_ID, COMPOSER_BUCKET_NAME, PROJECT_ID

import logging
from datetime import timedelta, datetime

SUB_PATH = 'temp_extracts/'
END_DATE_KEY = 'end_date'
DETAILED_REPORT_TABLE_NAME = 'clockify_detailed_report'

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


@provide_session
def clockify_to_fs(session, **kwargs):
    # getting things from context
    context = kwargs
    run_id = context['dag_run'].run_id
    ti = context['ti']

    # initializing workdir and hook
    workdir = get_dag_workdir_path_from_context(context, SUB_PATH)
    clockify = ClockifyHook()

    # getting previous end date and creating a new one
    prev_end_date = ti.xcom_pull(key=END_DATE_KEY, include_prior_dates=True)

    logging.info(f'Extracted prev date = {prev_end_date}')

    if prev_end_date:
        start_date = prev_end_date
    else:
        start_date = '2022-01-01 00:00:00'  # default start_date

    days_since_prev_end_date = (datetime.now() - datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')).days

    if days_since_prev_end_date > 30:
        end_date = (datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S') + timedelta(days=30))
    else:
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')

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

    # removing previous xcom values
    session.query(XCom).filter(
        XCom.execution_date < ti.execution_date,
        XCom.task_id == ti.task_id,
        XCom.dag_id == ti.dag_id
    ).delete()


def fs_to_bq(**kwargs):
    context = kwargs
    workdir = get_dag_workdir_path_from_context(context, SUB_PATH)
    dag_name = context['dag'].dag_id

    hook = BigQueryHook(use_legacy_sql=False)

    dfs = os.listdir(workdir)

    job_configuration = {
        "load": {
            "destinationTable": {
                "project_id": PROJECT_ID,
                "datasetId": AIRFLOW_TMP_DATASET_ID,
                "tableId": DETAILED_REPORT_TABLE_NAME,
            },
            "sourceUris": [f"gs://{COMPOSER_BUCKET_NAME}/data/{dag_name}/{SUB_PATH}*"],
            "writeDisposition": "WRITE_APPEND",
            "skipLeadingRows": 1,
            "allowJaggedRows": True,
            "allowQuotedNewlines": True,
            "autodetect": True,
        }
    }

    hook.insert_job(configuration=job_configuration)

    for df in dfs:
        # removing dfs from temp folder
        logging.info(f'Removing {workdir + df}')
        os.remove(workdir + df)


def bq_transform(**kwargs):
    # move from temp table to prod table
    # check if table exists
    hook = BigQueryHook(use_legacy_sql=False)
    temp_table_name = f'`{PROJECT_ID}.{AIRFLOW_TMP_DATASET_ID}.{DETAILED_REPORT_TABLE_NAME}`'
    destination_table_name = f'`{PROJECT_ID}.{AIRFLOW_DATASET_ID}.{DETAILED_REPORT_TABLE_NAME}`'

    if hook.table_exists(dataset_id='airflow', table_id=DETAILED_REPORT_TABLE_NAME):
        # if exists insert from temp table
        query = f'''
        INSERT INTO {destination_table_name}
        
        SELECT 
            *,
            CURRENT_TIMESTAMP() as _airflow_synced_at 
        FROM {temp_table_name};
        '''
        hook.run(query, autocommit=True)
    else:
        # if no - create as + datetime.now + clusterization
        query = f'''
        CREATE TABLE {destination_table_name} 
        CLUSTER BY email
        AS
 
        SELECT 
            *,
            CURRENT_TIMESTAMP() as _airflow_synced_at 
        FROM {temp_table_name};
        '''
        hook.run(query, autocommit=True)

    # drop temp table
    hook.run(f'DROP TABLE {temp_table_name};', autocommit=True)


def create_new_dagrun(**kwargs):
    # check if end_date is the last possible, if no, create new dagrun
    context = kwargs
    ti = context['ti']
    end_date = ti.xcom_pull(key='end_date')
    end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    last_possible_end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if end_date < last_possible_end_date:
        trigger_dag(context['dag'].dag_id)
    else:
        logging.info(f'End date equals to last possible end date, no need in new run ')


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

    next_run = PythonOperator(
        task_id='check_end_date_and_create_new_dagrun',
        provide_context=True,
        python_callable=create_new_dagrun)

    extract >> load >> transform >> next_run
