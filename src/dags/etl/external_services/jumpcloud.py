from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import json
import logging
import pendulum
from datetime import timedelta, datetime

from utils.config import SYNCED_AT_FIELD
from utils.utils import task_fail_slack_alert
from utils.hooks.jumpcloud_hook import JumpcloudHook

DATASET_ID = 'jumpcloud'
SYSTEMS_TABLE_NAME = 'systems'
SYSTEM_USERS_TABLE_NAME = 'system_users'

default_args = {
    'start_date': pendulum.datetime(2022, 12, 6, tz="UTC"),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

jumpcloud_dag = DAG(
    dag_id='jumpcloud',
    default_args=default_args,
    description='Integration with jumpcloud',
    schedule_interval='0 2 * * *',
    dagrun_timeout=timedelta(minutes=20))


def upload_systems(**kwargs):
    # initializing hooks
    jumpcloud_hook = JumpcloudHook()
    bigquery_hook = BigQueryHook(use_legacy_sql=False)
    logging.info(f'Initialized hooks')

    # extracting data from jumpcloud
    extracted_systems = jumpcloud_hook.get_full_systems_list()
    logging.info(f'Extracted data from jumpcloud')

    # prepare
    data_column_name = 'system_json'
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f'Current date is - {current_date}')

    prepared_rows = []
    for system in extracted_systems:
        prepared_row = {
            data_column_name: json.dumps(system),
            SYNCED_AT_FIELD: current_date
        }

        prepared_rows.append(prepared_row)

    logging.info(f'Transformed data and inserted a timestamp')

    # check and create bq table
    # -must have partitions by SYNCED_AT_FIELD
    if not bigquery_hook.table_exists(dataset_id=DATASET_ID, table_id=SYSTEMS_TABLE_NAME):
        logging.info(f'Table wasn\'t found, creating one')
        bigquery_hook.create_empty_table(
            dataset_id=DATASET_ID,
            table_id=SYSTEMS_TABLE_NAME,
            schema_fields=[
                {"name": data_column_name, "type": "JSON", "mode": "REQUIRED"},
                {"name": SYNCED_AT_FIELD, "type": "TIMESTAMP", "mode": "REQUIRED"},
            ],
            time_partitioning={
                "type": 'DAY',
                "field": SYNCED_AT_FIELD,
            },
            cluster_fields=[SYNCED_AT_FIELD]
        )
        logging.info(f'Table was created')

    logging.info(f'Uploading rows to BigQuery')

    # upload rows
    bigquery_hook.insert_all(
        dataset_id=DATASET_ID,
        table_id=SYSTEMS_TABLE_NAME,
        rows=prepared_rows
    )

    logging.info(f'Done successfully')


def upload_system_users(**kwargs):
    # initializing hooks
    jumpcloud_hook = JumpcloudHook()
    bigquery_hook = BigQueryHook(use_legacy_sql=False)
    logging.info(f'Initialized hooks')

    # extracting data from jumpcloud
    extracted_system_users = jumpcloud_hook.get_full_system_users_list()
    logging.info(f'Extracted data from jumpcloud')

    # prepare
    data_column_name = 'system_users_json'
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f'Current date is - {current_date}')

    prepared_rows = []
    for system_user in extracted_system_users:
        prepared_row = {
            data_column_name: json.dumps(system_user),
            SYNCED_AT_FIELD: current_date
        }

        prepared_rows.append(prepared_row)

    logging.info(f'Transformed data and inserted a timestamp')

    # check and create bq table
    # -must have partitions by SYNCED_AT_FIELD
    if not bigquery_hook.table_exists(dataset_id=DATASET_ID, table_id=SYSTEM_USERS_TABLE_NAME):
        logging.info(f'Table wasn\'t found, creating one')
        bigquery_hook.create_empty_table(
            dataset_id=DATASET_ID,
            table_id=SYSTEM_USERS_TABLE_NAME,
            schema_fields=[
                {"name": data_column_name, "type": "JSON", "mode": "REQUIRED"},
                {"name": SYNCED_AT_FIELD, "type": "TIMESTAMP", "mode": "REQUIRED"},
            ],
            time_partitioning={
                "type": 'DAY',
                "field": SYNCED_AT_FIELD,
            },
            cluster_fields=[SYNCED_AT_FIELD]
        )
        logging.info(f'Table was created')

    logging.info(f'Uploading rows to BigQuery')

    # upload rows
    bigquery_hook.insert_all(
        dataset_id=DATASET_ID,
        table_id=SYSTEM_USERS_TABLE_NAME,
        rows=prepared_rows
    )

    logging.info(f'Done successfully')


with jumpcloud_dag as dag:

    init = DummyOperator(task_id='init')

    systems = PythonOperator(
        task_id='systems',
        provide_context=True,
        python_callable=upload_systems)

    system_users = PythonOperator(
        task_id='system_users',
        provide_context=True,
        python_callable=upload_system_users)

    init >> [systems, system_users]
