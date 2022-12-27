from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

import json
import logging
import pendulum
from datetime import timedelta, datetime

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from utils.config import COMPOSER_BUCKET_NAME
from utils.utils import task_fail_slack_alert, get_dag_workdir_path_from_context, get_gcs_path_from_local_path
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook

default_args = {
    'start_date': pendulum.datetime(2022, 12, 25, tz="UTC"),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

jumpcloud_dag = DAG(
    dag_id='dbt_docs_and_logs',
    default_args=default_args,
    description='Automation for dbt cloud that generates docs and stores them on GCS and sends logs to GitHub '
                'PR comments',
    schedule_interval='0 2 * * *',
    dagrun_timeout=timedelta(minutes=20))


def generate_docs(**kwargs):
    # init hooks
    logging.info(f'Initializing hooks')

    dbt_hook = DbtCloudHook()
    gcs_hook = GCSHook()
    logging.info(f'Hooks are initialized')

    # get manifest.json and catalog.json from API
    logging.info(f'Getting list of runs')
    run_id = None

    account_id = dbt_hook.connection.login
    runs = dbt_hook._run_and_get_response(
        endpoint=f"{account_id}/runs/",
        payload={
            "job_definition_id": 40171,
            "order_by": '-id',
        },
        paginate=False,
    )

    logging.info(f'Received response list of runs from dbt cloud')
    logging.info(f'Parsing the runs')

    for run in runs.json()['data']:
        logging.info(f'Checking run_id {run["id"]}')
        if run['is_success'] is True:
            run_id = run['id']
            break

    logging.info(f'Cycle done, run_id is {run_id}')

    if run_id is None:
        raise ValueError(f'Failed to find successful run withing last 100 runs')

    logging.info(f'Requesting artifacts')
    manifest_json = dbt_hook.get_job_run_artifact(run_id=run_id, path='manifest.json').json()
    catalog_json = dbt_hook.get_job_run_artifact(run_id=run_id, path='catalog.json').json()
    logging.info(f'Artifacts received')

    # read index.html from GCS
    logging.info(f'Getting source index')
    source_workdir = get_dag_workdir_path_from_context(context=kwargs, sub_path='source_files')

    with open(source_workdir + 'index.html', 'r') as f:
        content_index = f.read()

    # generate docs
    logging.info(f'Generating docs')

    search_str = 'o=[i("manifest","manifest.json"+t),i("catalog","catalog.json"+t)]'
    new_str = "o=[{label: 'manifest', data: " + json.dumps(
        manifest_json) + "},{label: 'catalog', data: " + json.dumps(catalog_json) + "}]"
    new_content = content_index.replace(search_str, new_str)

    # save them to GCS and give access
    logging.info(f'Saving docs to GCS')

    target_file_name = 'index.html'
    target_workdir = get_dag_workdir_path_from_context(context=kwargs, sub_path='generated_docs')
    with open(target_workdir + target_file_name, 'w') as f:
        f.write(new_content)

    logging.info(f'Providing access')
    target_object_name = get_gcs_path_from_local_path(target_workdir + target_file_name)
    target_object_name = target_object_name[
        target_object_name.find(COMPOSER_BUCKET_NAME) + len(COMPOSER_BUCKET_NAME) + 1:]

    acl_entries = ['group-bigquery-analysts@osome.com', 'group-bigquery-others@osome.com']

    for entry in acl_entries:
        gcs_hook.insert_object_acl(
            bucket_name=COMPOSER_BUCKET_NAME,
            object_name=target_object_name,
            entity=entry,
            role='READER'
        )

    logging.info(f'All done')


with jumpcloud_dag as dag:
    init = DummyOperator(task_id='init')

    docs = PythonOperator(
        task_id='docs',
        provide_context=True,
        python_callable=generate_docs)

    init >> [docs]
