from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG

default_args = {
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
}

with DAG(
    'retail',
    default_args=default_args,
    schedule=None,
    tags=['retail'],
) as dag:
    # Task 1: Upload CSV file to GCS
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket='retail-gcp-2024-01',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    # Task 2: Create BigQuery dataset
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        project_id='onlineretail-413310',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )

    # Task 3: Load data from GCS to BigQuery
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket='retail-gcp-2024-01',
        source_objects=['raw/online_retail.csv'],
        destination_project_dataset_table='onlineretail-413310:retail.raw_invoices',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='gcp',
    )

    # Task 4: Dbt Transform Task Group
    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    # Task 5: Dbt report Task Group
    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    # Define task dependencies
    upload_csv_to_gcs >> create_retail_dataset >> gcs_to_bigquery >> transform >> report
