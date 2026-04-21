from airflow import DAG
from datetime import datetime

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


PROJECT_ID = "your-project-id"
DATASET_ID = "your_dataset"
SOURCE_TABLE = "your_table"
BUCKET = "your-bucket"
LOCATION = "asia-southeast1"


with DAG(
    dag_id="bq_select_columns_to_parquet_gcs",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bigquery", "parquet", "export"],
) as dag:

    export_query_to_parquet = BigQueryInsertJobOperator(
        task_id="export_query_to_parquet",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                    SELECT
                        id,
                        name
                    FROM `{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE}`
                """,
                "useLegacySql": False,

                # 🔥 Export query result directly to GCS as Parquet
                "destinationUri": f"gs://{BUCKET}/exports/{SOURCE_TABLE}-*.parquet",
                "destinationFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )
