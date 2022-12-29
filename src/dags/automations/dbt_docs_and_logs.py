from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

import json
import logging
import pendulum
from datetime import timedelta

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from utils.config import COMPOSER_BUCKET_NAME
from utils.hooks.github_hook import GitHubHook
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
    # schedule_interval='*/5 * * * *',
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=20))


def generate_docs(**kwargs):
    # init hooks and etc
    logging.info(f'Initializing hooks and etc')

    dbt_hook = DbtCloudHook()
    gcs_hook = GCSHook()
    logging.info(f'Hooks are initialized')
    source_workdir = get_dag_workdir_path_from_context(context=kwargs, sub_path='source_files')

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

    try:
        with open(source_workdir + 'state.json', 'r') as f:
            state = json.load(f)
    except IOError:
        state = {'last_run_id': -1}

    if state['last_run_id'] == run_id:
        logging.info(f'Current run id is the same as previous one')
        raise AirflowSkipException
    else:
        state['last_run_id'] = run_id

    logging.info(f'Requesting artifacts')
    manifest_json = dbt_hook.get_job_run_artifact(run_id=run_id, path='manifest.json').json()
    catalog_json = dbt_hook.get_job_run_artifact(run_id=run_id, path='catalog.json').json()
    logging.info(f'Artifacts received')

    # read index.html from GCS
    logging.info(f'Getting source index')

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

    #  Saving last run_id
    with open(source_workdir + 'state.json', 'w+') as f:
        json.dump(state, f)

    logging.info(f'All done')


def send_logs(**kwargs):
    def process_and_send(run_id: int, dbt: DbtCloudHook, gcs: GCSHook, gh: GitHubHook):
        run_dict = dbt.get_job_run(
            run_id=run_id,
            include_related=['run_steps', 'job', 'trigger', 'debug_logs']
        ).json()['data']

        logging.info(f'Received info about run_id: {run_id}')

        github_pr_id = run_dict['trigger']['github_pull_request_id']
        execute_steps = run_dict['job']['execute_steps']
        run_steps = run_dict['run_steps'][3:]
        file_name = f'PR-{github_pr_id}-RUN-{run_id}-STEP-'

        message = f'''DBT Cloud PR check launched at {run_dict['created_at'][:-13]} was failed.
        You can find run steps statuses and logs below. 
    
        '''

        logs = None
        debug_logs = None

        logging.info(f'Building message...')
        for i in range(0, len(execute_steps)):
            status = f'[{run_steps[i]["status_humanized"]}]'
            step = f'''{status:<10} {execute_steps}\n'''
            message += step

            if run_steps[i]['status_humanized'] == 'Error':
                file_name += run_steps[i]['id']
                logs = run_steps[i]['logs']
                debug_logs = run_steps[i]['debug_logs']

        # upload logs to gsc
        bucket_name = 'osome-dbt-failed-logs'
        logs_file_name = f'logs/{file_name}-logs.log'
        debug_logs_file_name = f'logs/{file_name}-debug_logs.log'

        logging.info(f'Uploading files to GCS, filepath:\n{logs_file_name}\n{debug_logs_file_name}')

        gcs.upload(
            bucket_name=bucket_name,
            object_name=logs_file_name,
            data=logs,
        )
        gcs.upload(
            bucket_name=bucket_name,
            object_name=debug_logs_file_name,
            data=debug_logs,
        )

        logging.info(f'Upload done')

        logs_gcs_url = f'https://storage.cloud.google.com/{bucket_name}/{logs_file_name}'
        debug_logs_gcs_url = f'https://storage.cloud.google.com/{bucket_name}/{debug_logs_file_name}'

        message += f'\n(Download logs)[{logs_gcs_url}]' \
                   f'\n(Download debug_logs)[{debug_logs_gcs_url}]'

        logging.info(f'Sending message to PR {github_pr_id}, message:\n{message}')

        # send message to PR with link
        response = gh.create_issue_comment(
            repo='analytics_dbt',
            issue_number=github_pr_id,
            body=message
        )

        logging.info(f'Response completed')

        if response.ok is False:
            logging.error(f'While trying to create PR message - response was returned with not ok status:'
                          f'{response}\n{response.reason}\n{response.text}\n')

            raise ValueError('Response not ok')

    # init hooks
    logging.info(f'Initializing hooks and etc')
    dbt_hook = DbtCloudHook()
    gcs_hook = GCSHook()
    github_hook = GitHubHook()

    source_workdir = get_dag_workdir_path_from_context(context=kwargs, sub_path='source_files')
    state_file_path = source_workdir + 'logs_state.json'

    # get last run_id from state
    try:
        with open(state_file_path, 'r') as f:
            state = json.load(f)
    except IOError:
        state = {'last_run_id': 107622624}  # one of the last runs

    # paginate through runs, stop when see last run_id
    account_id = dbt_hook.connection.login

    last_prev_run_id = state['last_run_id']
    logging.info(f'Last found run_id - {last_prev_run_id}')

    current_last_run_id = None
    cycle_trigger = True
    limit_size = 5
    offset = 0

    failed_runs = []

    while cycle_trigger is True:
        runs = dbt_hook._run_and_get_response(
            endpoint=f"{account_id}/runs/",
            payload={
                "job_definition_id": 40560,
                "order_by": '-id',
                "limit": limit_size,
                "offset": offset
            },
            paginate=False,
        )

        offset += limit_size

        runs_list = runs.json()['data']
        logging.info(f'Received {len(runs_list)}, iterating over them')

        for run in runs_list:
            logging.info(f'Checking {run["id"]}')
            if run['is_complete'] is False:
                logging.info(f'Run is incomplete, skipping')
                continue

            if run['id'] <= last_prev_run_id:
                logging.info(f'Reached last prev run_id, breaking')
                cycle_trigger = False
                break

            if run['is_complete'] is True and current_last_run_id is None:
                logging.info(f'Preliminary saved current last run_id')
                current_last_run_id = run['id']

            if run['is_error'] is True:
                logging.info(f'Run is failed, adding to the failed list')
                failed_runs.append(run['id'])

    if current_last_run_id is None:
        logging.info(f'Found 0 new runs, skipping')
        raise AirflowSkipException

    for run in failed_runs[::-1]:
        process_and_send(run_id=run, dbt=dbt_hook, gcs=gcs_hook, gh=github_hook)

        # save state for the last chronologically processed run
        state['last_run_id'] = run
        with open(state_file_path, 'w+') as f:
            json.dump(state, f)

    # save last processed run id
    state['last_run_id'] = current_last_run_id
    with open(state_file_path, 'w+') as f:
        json.dump(state, f)


with jumpcloud_dag as dag:
    init = DummyOperator(task_id='init')

    docs = PythonOperator(
        task_id='docs',
        provide_context=True,
        python_callable=generate_docs)

    logs = PythonOperator(
        task_id='logs',
        provide_context=True,
        python_callable=send_logs)

    init >> [docs, logs]
