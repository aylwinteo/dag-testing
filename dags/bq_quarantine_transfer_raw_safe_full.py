from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ---------------- Configuration ----------------
SOURCE_PROJECT = "quarantine-project"
TARGET_PROJECT = "prod-project"
DATASET = "your_dataset"
LOCATION = "asia-southeast1"
TABLE_PREFIX = "events"
LOOKBACK_DAYS = 4
TIMESTAMP_COLUMN = "last_modified"
SGT = ZoneInfo("Asia/Singapore")

# ---------------- Helper Functions ----------------
def generate_table_name(prefix, date_str):
    clean_date = date_str.replace('-', '')
    return f"{prefix}_{clean_date}"

def get_bq_client():
    """Get a fresh BigQuery client"""
    hook = BigQueryHook()
    return hook.get_client()

def check_source_table(logical_date, prefix, **context):
    table_date = logical_date.strftime("%Y-%m-%d")
    table_name = generate_table_name(prefix, table_date)
    bq_client = get_bq_client()
    
    try:
        bq_client.get_table(f"{SOURCE_PROJECT}.{DATASET}.{table_name}")
        context['ti'].xcom_push(key=f"table_exists_{table_name}", value=True)
        print(f"[TRANSFER READY] Source table {table_name} exists")
    except Exception:
        context['ti'].xcom_push(key=f"table_exists_{table_name}", value=False)
        print(f"[SKIPPED] Source table {table_name} does not exist")
        raise AirflowSkipException(f"Source table {table_name} missing.")

def build_merge_sql(logical_date, offset_days=0, **context):
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(TABLE_PREFIX, table_date)
    source_table_full = f"{SOURCE_PROJECT}.{DATASET}.{table_name}"
    target_table_full = f"{TARGET_PROJECT}.{DATASET}.{table_name}"
    
    bq_client = get_bq_client()
    bq_table_source = bq_client.get_table(source_table_full)
    source_fields = {f.name: f.field_type for f in bq_table_source.schema}

    try:
        bq_table_target = bq_client.get_table(target_table_full)
        target_fields = {f.name: f.field_type for f in bq_table_target.schema}
        alter_sql = ""
        for col, col_type in source_fields.items():
            if col not in target_fields:
                alter_sql += f"ALTER TABLE `{target_table_full}` ADD COLUMN {col} {col_type};\n"
        create_target_sql = ""
    except Exception:
        alter_sql = ""
        create_target_sql = f"CREATE TABLE `{target_table_full}` AS SELECT * FROM `{source_table_full}` WHERE 1=0;\n"

    if 'id' in source_fields:
        merge_key = "T.id = S.id"
        update_fields = [f for f in source_fields.keys() if f not in ['id', TIMESTAMP_COLUMN]]
    else:
        key_cols = [f for f in source_fields.keys() if f != TIMESTAMP_COLUMN]
        merge_key = (
            "FARM_FINGERPRINT(CONCAT(" +
            ",".join([f"IFNULL(CAST(T.{c} AS STRING),'NULL')" for c in key_cols]) +
            ")) = FARM_FINGERPRINT(CONCAT(" +
            ",".join([f"IFNULL(CAST(S.{c} AS STRING),'NULL')" for c in key_cols]) +
            "))"
        )
        update_fields = key_cols

    update_clause = ",\n".join([f"{col} = S.{col}" for col in update_fields])

    merge_sql = f"""
    {create_target_sql}
    {alter_sql}
    CREATE TEMP TABLE staging_data AS
    SELECT * FROM `{source_table_full}`
    WHERE {TIMESTAMP_COLUMN} > IFNULL(
        (SELECT MAX({TIMESTAMP_COLUMN}) FROM `{target_table_full}`),
        TIMESTAMP('1970-01-01 00:00:00'));
    MERGE `{target_table_full}` T
    USING staging_data S
    ON {merge_key}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ROW;
    """
    context['ti'].xcom_push(key=f"merge_sql_{offset_days}", value=merge_sql)
    return merge_sql

def daily_report(**context):
    ti = context['ti']
    print("\n========== Daily Quarantine Transfer Report ==========")
    for i in range(1, LOOKBACK_DAYS + 1):
        table_date = (context['execution_date'] - timedelta(days=i)).strftime("%Y-%m-%d")
        table_name = generate_table_name(TABLE_PREFIX, table_date)
        status = ti.xcom_pull(task_ids=f"check_source_{i}", key=f"table_exists_{table_name}")
        print(f"{table_name}: {'READY' if status else 'SKIPPED'}")
    print("=====================================================")

# ---------------- DAG ----------------
with DAG(
    dag_id="bq_quarantine_transfer_raw_safe_full",
    start_date=datetime(2024, 1, 1, tzinfo=SGT),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=LOOKBACK_DAYS + 1
) as dag:

    report_task = PythonOperator(
        task_id='daily_report',
        python_callable=daily_report,
        trigger_rule=TriggerRule.ALL_DONE
    )

    for i in range(1, LOOKBACK_DAYS + 1):
        check_task = PythonOperator(
            task_id=f"check_source_{i}",
            python_callable=check_source_table,
            op_kwargs={"prefix": TABLE_PREFIX},
        )

        build_sql_task = PythonOperator(
            task_id=f"build_merge_sql_{i}",
            python_callable=build_merge_sql,
            op_kwargs={"offset_days": i},
        )

        transfer_task = BigQueryInsertJobOperator(
            task_id=f"execute_transfer_{i}",
            location=LOCATION,
            configuration={
                "query": {
                    "query": "{{ ti.xcom_pull(task_ids='build_merge_sql_" + str(i) + "', key='merge_sql_" + str(i) + "') }}",
                    "useLegacySql": False
                }
            },
            trigger_rule=TriggerRule.ALL_DONE
        )

        check_task >> build_sql_task >> transfer_task >> report_task

