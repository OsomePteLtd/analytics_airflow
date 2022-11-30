DATA_FOLDER_PATH = f'/home/airflow/gcs/data/'


def get_dag_folder_path(dag_name):
    return DATA_FOLDER_PATH + dag_name + '/'
