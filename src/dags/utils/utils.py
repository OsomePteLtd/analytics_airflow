import logging
import os
from pathlib import Path
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

from utils.config import DATA_FOLDER_PATH, SLACK_CONN_ID, COMPOSER_BUCKET_NAME


def get_dag_workdir_path_from_dag_obj(dag_obj, sub_path: str = '') -> str:
    """
    Function that creates workdir and returns its path from the DAG object

    :param dag_obj: dag object that is inside context['dag']
    :param sub_path: any sub path that you want to have inside dag's workdir
    :return: returns str path ended with '/'
    """

    dag_id = dag_obj.dag_id

    if len(sub_path) > 0 and sub_path[0] == '/':
        sub_path = sub_path[1:]

    full_path = os.path.join(DATA_FOLDER_PATH, dag_id, sub_path)

    if full_path[-1] != '/':
        full_path += '/'

    if not os.path.exists(full_path):
        path = Path(full_path)
        path.mkdir(parents=True, exist_ok=True)

    return full_path


def get_dag_workdir_path_from_context(context, sub_path: str = '') -> str:
    """
    Function that creates workdir and returns its path from the context

    :param context: context that is passed by Airflow when provide_context=True
    :param sub_path: any sub path that you want to have inside dag's workdir
    :return: returns str path ended with '/'
    """

    dag_obj = context['dag']

    return get_dag_workdir_path_from_dag_obj(dag_obj, sub_path=sub_path)


def task_fail_slack_alert(context):
    """
    Sends a message to the channel with fail info link to the log.

    Usage:
    -add "'on_failure_callback': task_fail_slack_alert," to default_arguments in your dag


    reference:
    https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
    """

    logging.info('Sending slack alert about failed dag')

    slack_msg = """
    :red_circle: Task Failed. 
    *Task*: {task}  
    *Dag*: {dag} 
    *Execution Time*: {exec_date}  
    *Log Url*: {log_url} 
    """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('data_interval_start'),
        log_url=context.get('task_instance').log_url,
    )

    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    hook.send(text=slack_msg)


def local_path_to_gs_uri(path: str) -> str:
    """
    Returns GCS uri instead of local path
    :param path:
    :return:
    """
    path = path.replace(DATA_FOLDER_PATH, f'gs://{COMPOSER_BUCKET_NAME}/data/')
    return path
