from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import logging
import pendulum
from datetime import timedelta, datetime

from utils.utils import task_fail_slack_alert

SNAPSHOT_DATASET_ID = 'snapshots_backup'

default_args = {
    'start_date': pendulum.datetime(2022, 12, 12, tz="UTC"),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

snapshots_dag = DAG(
    dag_id='bigquery_snapshots',
    default_args=default_args,
    description=f'Snapshotting to {SNAPSHOT_DATASET_ID} schema',
    schedule_interval='10 3 * * *',
    dagrun_timeout=timedelta(minutes=20))


def save_snapshot(hook, project_id: str, dataset_id: str, table_name: str):

    current_datetime = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    expiration_timestamp = (current_datetime + timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
    dest_table_name = f'{dataset_id}__{table_name}{current_datetime.strftime("%Y%m%d")}'

    logging.info(f'Snapshotting {project_id}.{dataset_id}.{table_name} as \n{project_id}.{SNAPSHOT_DATASET_ID}.'
                 f'{dest_table_name} with expiration set to {expiration_timestamp}'
                 )

    query = f'''
    CREATE SNAPSHOT TABLE IF NOT EXISTS `{project_id}`.`{SNAPSHOT_DATASET_ID}`.`{dest_table_name}`
    CLONE `{project_id}`.`{dataset_id}`.`{table_name}`
    OPTIONS (expiration_timestamp = TIMESTAMP '{expiration_timestamp}');
    '''

    hook.run_query(sql=query, use_legacy_sql=False)

    logging.info('Done')


def snapshot_snapshot_schema(**kwargs):
    hook = BigQueryHook()

    logging.info(f'Getting list of tables from snapshots schema')
    snapshots_list = hook.get_dataset_tables(dataset_id='dbt_snapshots')
    logging.info(f'Received {len(snapshots_list)} tables')

    for table in snapshots_list:
        save_snapshot(
            hook,
            table['projectId'],
            table['datasetId'],
            table['tableId']
        )

    logging.info(f'Successfully finished snapshotting tables.')


with snapshots_dag as dag:
    dbt_snapshots = PythonOperator(
        task_id='dbt_snapshots',
        provide_context=True,
        python_callable=snapshot_snapshot_schema)

    dbt_snapshots
