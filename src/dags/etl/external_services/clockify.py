import os

import pendulum
from airflow import DAG
from airflow.models import XCom
from airflow.utils.session import provide_session
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.api.common.trigger_dag import trigger_dag
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from utils.hooks.clockify_hook import ClockifyHook
from utils.utils import get_dag_workdir_path_from_context, task_fail_slack_alert
from utils.config import AIRFLOW_TMP_DATASET_ID, COMPOSER_BUCKET_NAME, PROJECT_ID, SYNCED_AT_FIELD

import logging
from datetime import timedelta, datetime

SUB_PATH = 'temp_extracts/'
END_DATE_KEY = 'end_date'

CLOCKIFY_DATASET_ID = 'clockify'
DETAILED_REPORT_TABLE_NAME = 'clockify_detailed_report'

default_args = {
    'start_date': pendulum.datetime(2022, 12, 1, tz="UTC"),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on-failure-callback': task_fail_slack_alert
}

clockify_dag = DAG(
    dag_id='clockify',
    default_args=default_args,
    description='Integration with clockify to extract detailed reports to BQ',
    schedule_interval='0 2 * * *',
    dagrun_timeout=timedelta(minutes=20))


def get_clockify_schema_fields() -> list:
    schema_fields = [
        {"name": "Project", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Client", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Task", "type": "STRING", "mode": "NULLABLE"},
        {"name": "User", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Group", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Tags", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Billable", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "Start_Date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Start_Time", "type": "TIME", "mode": "NULLABLE"},
        {"name": "End_Date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "End_Time", "type": "TIME", "mode": "NULLABLE"},
        {"name": "Duration__h_", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Duration__decimal_", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "Billable_Rate__SGD_", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "Billable_Amount__SGD_", "type": "FLOAT", "mode": "NULLABLE"},
    ]

    return schema_fields


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
            "clustering": {
                'fields': ['parent_company', 'child_company']
            },
            "schema": {
                "fields": get_clockify_schema_fields()
            }
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

    destination_table_name = f'`{PROJECT_ID}.{CLOCKIFY_DATASET_ID}.{DETAILED_REPORT_TABLE_NAME}`'

    table_schema = get_clockify_schema_fields()
    table_schema.append({"name": SYNCED_AT_FIELD, "type": "TIMESTAMP", "mode": "REQUIRED"})

    if not hook.table_exists(dataset_id=CLOCKIFY_DATASET_ID, table_id=DETAILED_REPORT_TABLE_NAME):
        # if tables doesn't exist - create one
        hook.create_empty_table(
            dataset_id=CLOCKIFY_DATASET_ID,
            table_id=DETAILED_REPORT_TABLE_NAME,
            schema_fields=table_schema,
            cluster_fields=['email']
        )

    # appending new rows and dropping temp table
    logging.info(f'Destination table found, appending new rows')
    query = f'''
           INSERT INTO {destination_table_name}

           SELECT 
               *,
               CURRENT_TIMESTAMP() as {SYNCED_AT_FIELD} 
           FROM {temp_table_name};
           
           DROP TABLE {temp_table_name};
           '''
    hook.run(query, autocommit=True)


def create_new_dagrun(**kwargs):
    # check if end_date is the last possible, if no, create new dagrun
    context = kwargs
    ti = context['ti']
    end_date = ti.xcom_pull(key='end_date')
    end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    last_possible_end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if end_date < last_possible_end_date:
        trigger_dag(dag_id=context['dag'].dag_id,
                    execution_date=context['execution_date'] + timedelta(seconds=1)

                    )
        logging.info(f'End date not reached - triggering new run ')
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
