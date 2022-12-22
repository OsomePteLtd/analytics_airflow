import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

import logging
import pendulum
from datetime import timedelta, datetime
from utils.config import AIRFLOW_DATASET_ID, PROJECT_ID, SYNCED_AT_FIELD
from utils.utils import task_fail_slack_alert, get_dag_workdir_path_from_context, local_path_to_gs_uri

DAG_NAME = 'second_companies'

SOURCE_SCHEMA_NAME = 'dbt_atsybaeva'
SOURCE_TABLE_NAME = 'parent_child_companies_all'

DESTINATION_SCHEMA_NAME = AIRFLOW_DATASET_ID
DESTINATION_TABLE_NAME = 'processed_parent_child_companies'

default_args = {
    'start_date': pendulum.datetime(2022, 12, 21, tz="UTC"),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on-failure-callback': task_fail_slack_alert
}

second_companies_dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    description=f'Integration that finds parent companies for every child, requested here:'
                f'https://reallyosome.atlassian.net/browse/DE-80',
    schedule_interval='10 4 * * *',
    dagrun_timeout=timedelta(minutes=20)
)


def process(**kwargs):
    # initializing
    logging.info(f'Initializing packages')
    import pandas as pd
    hook = BigQueryHook(use_legacy_sql=False)
    path = get_dag_workdir_path_from_context(kwargs)
    saved_df_name = path + 'parent_child.csv'

    # extracting df from BQ
    logging.info(f'Extracting df from BQ')
    global all_connections

    all_connections = hook.get_pandas_df(
        sql=f'SELECT * FROM `{SOURCE_SCHEMA_NAME}.{SOURCE_TABLE_NAME}`'
    )

    logging.info(f'Received DF, it has {len(all_connections)} rows')

    # transforming df
    logging.info(f'Starting transform')
    global list_of_children

    parent_creations = all_connections[['parent_company', 'parent_created']].drop_duplicates()
    children_creations = all_connections[['child_company', 'child_created']].drop_duplicates()
    absolute_parents = all_connections[all_connections.is_first_for_all_users == 1].parent_company.unique()

    def find_children(parent_id):
        all_children = all_connections[all_connections.parent_company == parent_id].child_company.unique()

        if len(all_children) == 0:
            return
        else:
            for child in all_children:
                if child in checked:
                    continue
                list_of_children.add(child)

                checked.add(child)
                find_children(child)

    final_pairs = []
    for parent in absolute_parents:
        list_of_children = set()
        checked = set()
        a = find_children(parent)
        for child in list_of_children:
            final_pairs.append({
                'parent_company': parent,
                'child_company': child,
                'parent_created': parent_creations[parent_creations['parent_company'] == parent].iloc[0][
                    'parent_created'],
                'child_created': children_creations[children_creations['child_company'] == child].iloc[0][
                    'child_created']
            })
    final_df = pd.DataFrame(final_pairs)
    final_df['parent_created'] = pd.to_datetime(final_df['parent_created'])
    final_df["rank"] = final_df.groupby("child_company")["parent_created"].rank(method="first", ascending=True)
    final_df = final_df[final_df['rank'] == 1]
    final_df[SYNCED_AT_FIELD] = datetime.now()
    logging.info(f'Transform done, result df has {len(final_df)} rows')

    # saving to s3
    logging.info(f'Saving df to GCS, path: {saved_df_name}')
    final_df.to_csv(saved_df_name, index=False)
    logging.info(f'Successfully saved df')

    # uploading df by overwriting existing table
    gcs_path = local_path_to_gs_uri(saved_df_name)
    logging.info(f'Starting BQ upload, gcs path: {gcs_path}')
    job_configuration = {
        "load": {
            "destinationTable": {
                "project_id": PROJECT_ID,
                "datasetId": DESTINATION_SCHEMA_NAME,
                "tableId": DESTINATION_TABLE_NAME,
            },
            "sourceUris": [gcs_path],
            "writeDisposition": "WRITE_TRUNCATE",
            "skipLeadingRows": 1,
            "allowJaggedRows": True,
            "clustering": {
                'fields': ['parent_company', 'child_company']
            },
            "allowQuotedNewlines": True,
            "autodetect": True,
        }
    }

    hook.insert_job(configuration=job_configuration)
    logging.info(f'Upload job done')

    # remove df from gcs
    os.remove(saved_df_name)
    logging.info(f'Removed saved df')

    logging.info(f'All done')


with second_companies_dag as dag:
    process = PythonOperator(
        task_id='process',
        provide_context=True,
        python_callable=process)

    process
