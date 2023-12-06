import os
from pathlib import Path
from datetime import datetime
from airflow import DAG

from airflow.configuration import conf
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

comp_home_path = Path(conf.get("core", "dags_folder")).parent.absolute()
comp_bucket_path = "dags/input/"
comp_local_path = os.path.join(comp_home_path, comp_bucket_path)

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", "dummy_json")
MARTS_OUTPUT = os.environ.get("GCP_TABLE_NAME", "users")

def print():
    return print("testing")

with DAG(
    'credix-batch',
    description='Credix Batch Processing Pipeline',
    start_date=datetime(2023, 10, 31),
    schedule_interval=None,
    tags=['DF11-Final Project']
):

    applicationrecord_to_gcs = LocalFilesystemToGCSOperator(
        task_id="applicationrecord_to_gcs",
        src=comp_local_path + "application_record.csv",
        dst="credix/batch-processing/application_record.csv",
        bucket="fellowship-yogi"  # Specify your GCS bucket name
        )

    dbt_init_task = BashOperator(
        task_id="dbt_init_task",
        bash_command= "cd /opt/airflow/dbt/credit_card_dwh && dbt deps && dbt seed --profiles-dir ."
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test_task",
        bash_command= "cd /opt/airflow/dbt/credit_card_dwh && dbt deps && dbt seed --profiles-dir ."
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run_task",
        bash_command= "cd /opt/airflow/dbt/credit_card_dwh && dbt deps && dbt run --profiles-dir ."
    )

    marts_to_dwh = GCSToBigQueryOperator(
        task_id='marts_to_dwh',
        bucket='fellowship-yogi',
        source_objects=['dummy_json_task/todos.csv'],
        destination_project_dataset_table=f"{DATASET_NAME}.{MARTS_OUTPUT}",
        schema_fields=[
                        ],
        write_disposition='WRITE_TRUNCATE',
    )

    applicationrecord_to_gcs >> dbt_init_task >> dbt_test_task >> dbt_run_task >> marts_to_dwh
