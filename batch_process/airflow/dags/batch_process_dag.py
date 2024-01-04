import os
# from pathlib import Path

from airflow import DAG
# from airflow.configuration import conf
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

# Set environtment
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", 'long-memory-400610')
BUCKET = os.environ.get("GCP_GCS_BUCKET", 'projectcredix_group3_datalake')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'projectcredix_group3_bronze')

# Set the file paths for the local files
PATH_TO_APPLICATION_RECORD = '/opt/airflow/dags/dataset/credit_application/'
PATH_TO_CREDIT_RECORD = '/opt/airflow/dags/dataset/credit_history/'

# Set the destination paths in GCS
DESTINATION_APPLICATION_RECORD = 'credit_application/'
DESTINATION_CREDIT_RECORD = 'credit_history/'

# Get a list of all CSV files in the local directories
application_csv_files = [os.path.join(PATH_TO_APPLICATION_RECORD, file) for file in os.listdir(PATH_TO_APPLICATION_RECORD) if file.endswith('.csv')]
credit_csv_files = [os.path.join(PATH_TO_CREDIT_RECORD, file) for file in os.listdir(PATH_TO_CREDIT_RECORD) if file.endswith('.csv')]

dataset_file = "credit_record.csv"
#dbt_loc = "/opt/airflow/dbt"

default_args = {
    "owner": "Group 3",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="batchprocess_projectcredix",
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['DF11'],
) as dag:

    start = DummyOperator(task_id="start")
    
    upload_application_record = LocalFilesystemToGCSOperator(
        task_id="upload_application_record",
        src=application_csv_files,
        dst=DESTINATION_APPLICATION_RECORD,
        bucket=BUCKET,
    )

    upload_credit_record = LocalFilesystemToGCSOperator(
        task_id="upload_credit_record",
        src=credit_csv_files,
        dst=DESTINATION_CREDIT_RECORD,
        bucket=BUCKET,
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "raw_credithistory_data",
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{BUCKET}/credit_history/{dataset_file}"],
                "autodetect": True
            },
        },
    )
    
    dbt_init_task = BashOperator(
        task_id="dbt_init_task",
        bash_command= "cd /opt/airflow/dags/final_dbt && dbt deps && dbt seed --profiles-dir ."
    )

    run_dbt_task = BashOperator(
        task_id="run_dbt_task",
        bash_command= "cd /opt/airflow/dags/final_dbt && dbt deps && dbt run --profiles-dir ."
    )

    end = DummyOperator(task_id="end")

    start >> \
    (upload_application_record, upload_credit_record) >> bigquery_external_table_task >> \
    dbt_init_task >> run_dbt_task >> \
    end
