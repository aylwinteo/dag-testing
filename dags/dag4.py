import logging
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from functools import lru_cache

logger = logging.getLogger(__name__)

SOURCE_PROJECT = "quarantine-project"
TARGET_PROJECT = "prod-project"
DATASET = "your_dataset"
LOCATION = "asia-southeast1"
TABLE_PREFIX = "events"
LOOKBACK_DAYS = 4
SGT = ZoneInfo("Asia/Singapore")

def generate_table_name(prefix, date_str):
    return f"{prefix}_{date_str.replace('-', '')}"

@lru_cache()
def get_bq_client():
    hook = BigQueryHook()
    return hook.get_client()

def get_table_last_modified_time(bq_client, project_id, dataset_id, table_id):
    try:
        table = bq_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        return datetime.fromtimestamp(table.last_modified_time / 1000, tz=SGT)
    except NotFound:
        return None

def check_source_table(logical_date, offset_days, prefix, **context):
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(prefix, table_date)
    bq_client = get_bq_client()
    try:
        bq_client.get_table(f"{SOURCE_PROJECT}.{DATASET}.{table_name}")
        context['ti'].xcom_push(key=f"table_exists_{table_name}", value=True)
    except NotFound:
        context['ti'].xcom_push(key=f"table_exists_{table_name}", value=False)
        raise AirflowSkipException(f"{table_name} missing")

def build_merge_sql(logical_date, offset_days=0, direct_transfer=False, **context):
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(TABLE_PREFIX, table_date)

    source = f"{SOURCE_PROJECT}.{DATASET}.{table_name}"
    target = f"{TARGET_PROJECT}.{DATASET}.{table_name}"

    bq_client = get_bq_client()

    source_schema = {f.name: f.field_type for f in bq_client.get_table(source).schema}

    target_exists = True
    try:
        target_schema = {f.name: f.field_type for f in bq_client.get_table(target).schema}
        alter_sql = "\n".join(
            [f"ALTER TABLE `{target}` ADD COLUMN {c} {t};" for c, t in source_schema.items() if c not in target_schema]
        )
    except NotFound:
        target_exists = False
        alter_sql = ""
        create_sql = f"CREATE TABLE `{target}` AS SELECT * FROM `{source}` WHERE 1=0;"
    else:
        create_sql = ""

    if not direct_transfer:
        if target_exists:
            src_time = get_table_last_modified_time(bq_client, SOURCE_PROJECT, DATASET, table_name)
            tgt_time = get_table_last_modified_time(bq_client, TARGET_PROJECT, DATASET, table_name)
            if src_time is None or (tgt_time and src_time <= tgt_time):
                raise AirflowSkipException("no change")

    if "id" in source_schema:
        key = "T.id = S.id"
        cols = [c for c in source_schema if c != "id"]
    else:
        keys = ["user_pseudo_id", "event_timestamp", "event_name"]
        key = " AND ".join([f"T.{c}=S.{c}" for c in keys])
        cols = [c for c in source_schema if c not in keys]

    update_clause = ",\n".join([f"{c}=S.{c}" for c in cols])
    insert_cols = ", ".join(source_schema.keys())
    insert_vals = ", ".join([f"S.{c}" for c in source_schema.keys()])

    sql = f"""
    {create_sql}
    {alter_sql}
    CREATE TEMP TABLE staging AS SELECT * FROM `{source}`;

    MERGE `{target}` T
    USING staging S
    ON {key}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    context['ti'].xcom_push(key=f"merge_sql_{offset_days}", value=sql)
    return sql

def daily_report(**context):
    ti = context['ti']
    logical_date = context['logical_date']
    for i in range(1, LOOKBACK_DAYS + 1):
        table_date = (logical_date - timedelta(days=i)).strftime("%Y-%m-%d")
        table_name = generate_table_name(TABLE_PREFIX, table_date)
        status = ti.xcom_pull(task_ids=f"check_source_{i}", key=f"table_exists_{table_name}")
        logger.info(f"{table_name}: {'READY' if status else 'SKIPPED'}")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bq_quarantine_transfer_raw_safe_full",
    start_date=datetime(2024, 1, 1, tzinfo=SGT),
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=LOOKBACK_DAYS + 1,
    default_args=default_args,
) as dag:

    report = PythonOperator(
        task_id="report",
        python_callable=daily_report,
        trigger_rule=TriggerRule.ALL_DONE
    )

    tasks = []

    for i in range(1, LOOKBACK_DAYS + 1):
        check = PythonOperator(
            task_id=f"check_source_{i}",
            python_callable=check_source_table,
            op_kwargs={"prefix": TABLE_PREFIX, "offset_days": i},
        )

        build = PythonOperator(
            task_id=f"build_sql_{i}",
            python_callable=build_merge_sql,
            op_kwargs={"offset_days": i, "direct_transfer": i == 1},
        )

        run = BigQueryInsertJobOperator(
            task_id=f"run_{i}",
            location=LOCATION,
            configuration={
                "query": {
                    "query": "{{ ti.xcom_pull(task_ids='build_sql_" + str(i) + "', key='merge_sql_" + str(i) + "') }}",
                    "useLegacySql": False
                }
            }
        )

        check >> build >> run
        tasks.append(run)

    tasks >> report
