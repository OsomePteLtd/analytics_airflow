import os
from pathlib import Path

from .config import DATA_FOLDER_PATH


def get_dag_workdir_path_from_dag_obj(dag_obj, sub_path: str = '') -> str:
    """
    Function that creates workdir and returns its path from the DAG object

    :param dag_obj: dag object that is inside context['dag']
    :param sub_path: any sub path that you want to have inside dag's workdir
    :return: returns str path ended with '/'
    """

    dag_id = dag_obj.dag_id

    if sub_path[0] == '/':
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
