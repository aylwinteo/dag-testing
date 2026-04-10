import logging
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.cloud.exceptions import NotFound
from functools import lru_cache

logger = logging.getLogger(__name__)

# -----------------------------
# CONFIGURATION
# -----------------------------

SOURCE_PROJECT = "ac-project-485203"   # Where raw GA4/event tables come from
TARGET_PROJECT = "team-485203"         # Where processed tables will be stored
DATASET = "analytics_514446807"        # Shared dataset name in both projects
LOCATION = "asia-southeast1"           # BigQuery region
TABLE_PREFIX = "events"                # GA4-style table prefix
LOOKBACK_DAYS = 4                      # How many past days to process
SGT = ZoneInfo("Asia/Singapore")       # DAG timezone


# -----------------------------
# TABLE NAME GENERATION
# -----------------------------
def generate_table_name(prefix, date_str):
    # Converts YYYY-MM-DD → events_YYYYMMDD format
    return f"{prefix}_{date_str.replace('-', '')}"


# -----------------------------
# BIGQUERY CLIENT (CACHED)
# -----------------------------
@lru_cache()
def get_bq_client():
    # Reuse BigQuery client to avoid reconnect overhead
    hook = BigQueryHook()
    return hook.get_client()


# -----------------------------
# TABLE METADATA HELPERS
# -----------------------------
def get_table_last_modified_time(bq_client, project_id, dataset_id, table_id):
    # Returns last modified timestamp of a table (used for change detection)
    try:
        table = bq_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        return table.modified
    except NotFound:
        return None


def get_table_row_count(bq_client, project, dataset, table):
    # Returns row count for a table (used as lightweight "data signature")
    try:
        query = f"SELECT COUNT(*) c FROM `{project}.{dataset}.{table}`"
        return list(bq_client.query(query).result())[0]["c"]
    except Exception:
        return None


# -----------------------------
# CHECK IF SOURCE TABLE EXISTS
# -----------------------------
def check_source_table(logical_date, offset_days, prefix, **context):
    # Computes which historical table to check
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(prefix, table_date)

    bq_client = get_bq_client()

    try:
        # Validate source table exists in BigQuery
        bq_client.get_table(f"{SOURCE_PROJECT}.{DATASET}.{table_name}")
        context['ti'].xcom_push(key=f"table_exists_{table_name}", value=True)
    except NotFound:
        # If missing → skip DAG path for that day
        context['ti'].xcom_push(key=f"table_exists_{table_name}", value=False)
        raise AirflowSkipException(f"{table_name} missing")


# -----------------------------
# BUILD SQL (CORE LOGIC)
# -----------------------------
def build_merge_sql(logical_date, offset_days=0, direct_transfer=False, **context):

    # Compute target date table (D-1, D-2, etc.)
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(TABLE_PREFIX, table_date)

    # Full source and target table paths
    source = f"{SOURCE_PROJECT}.{DATASET}.{table_name}"
    target = f"{TARGET_PROJECT}.{DATASET}.{table_name}"

    bq_client = get_bq_client()

    # Load source schema (column names + types)
    source_table = bq_client.get_table(source)
    source_schema = {f.name: f.field_type for f in source_table.schema}

    # Get row count (used for change detection)
    source_rows = get_table_row_count(bq_client, SOURCE_PROJECT, DATASET, table_name)

    # -----------------------------
    # DIRECT TRANSFER MODE (D-1)
    # -----------------------------
    if direct_transfer:

        # Check if target already has same data size → skip duplicate transfer
        target_rows = get_table_row_count(bq_client, TARGET_PROJECT, DATASET, table_name)

        if source_rows is not None and target_rows == source_rows:
            logger.info(f"{table_name} already transferred. Skipping.")
            raise AirflowSkipException("already transferred")

        # Full overwrite copy
        sql = f"CREATE OR REPLACE TABLE `{target}` AS SELECT * FROM `{source}`;"

        # Store status for reporting
        context['ti'].xcom_push(key=f"direct_transfer_success_{offset_days}", value=True)
        context['ti'].xcom_push(key=f"sql_to_execute_{offset_days}", value=sql)

        return sql

    # -----------------------------
    # MERGE MODE (D-2, D-3, etc.)
    # -----------------------------
    target_exists = True

    try:
        # Load target schema if table exists
        target_schema = {f.name: f.field_type for f in bq_client.get_table(target).schema}

        # Add missing columns to target table
        alter_sql = "\n".join(
            [
                f"ALTER TABLE `{target}` ADD COLUMN {c} {t};"
                for c, t in source_schema.items()
                if c not in target_schema
            ]
        )

    except NotFound:
        # If target doesn't exist → create empty structure
        target_exists = False
        alter_sql = ""
        create_sql = f"CREATE TABLE `{target}` AS SELECT * FROM `{source}` WHERE 1=0;"
    else:
        create_sql = ""

    # Skip merge if data already fully matches
    if target_exists:
        tgt_rows = get_table_row_count(bq_client, TARGET_PROJECT, DATASET, table_name)
        if source_rows is not None and tgt_rows == source_rows:
            raise AirflowSkipException("no change")

    # -----------------------------
    # PRIMARY KEY LOGIC
    # -----------------------------
    # If GA4-style id exists → use it
    if "id" in source_schema:
        key = "T.id = S.id"
        cols = [c for c in source_schema if c != "id"]

    # Otherwise fallback to GA4 composite key
    else:
        keys = ["user_pseudo_id", "event_timestamp", "event_name"]
        key = " AND ".join([f"T.{c}=S.{c}" for c in keys])
        cols = [c for c in source_schema if c not in keys]

    # Build UPDATE clause for MERGE
    update_clause = ",\n".join([f"{c}=S.{c}" for c in cols])

    # Build INSERT clause
    insert_cols = ", ".join(source_schema.keys())
    insert_vals = ", ".join([f"S.{c}" for c in source_schema.keys()])

    # -----------------------------
    # FINAL MERGE SQL
    # -----------------------------
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

    # Store outputs for Airflow task execution
    context['ti'].xcom_push(key=f"merge_success_{offset_days}", value=True)
    context['ti'].xcom_push(key=f"sql_to_execute_{offset_days}", value=sql)

    return sql


# -----------------------------
# DAILY SUMMARY REPORT
# -----------------------------
def daily_report(**context):
    ti = context['ti']
    logical_date = context['logical_date']

    merged = []
    transferred = []

    for i in range(1, LOOKBACK_DAYS + 1):

        table_date = (logical_date - timedelta(days=i)).strftime("%Y-%m-%d")
        table_name = generate_table_name(TABLE_PREFIX, table_date)

        # Collect successful direct transfers
        if ti.xcom_pull(task_ids=f"build_sql_{i}", key=f"direct_transfer_success_{i}"):
            transferred.append(table_name)

        # Collect successful merges
        if ti.xcom_pull(task_ids=f"build_sql_{i}", key=f"merge_success_{i}"):
            merged.append(table_name)

    # Log summary output
    if transferred:
        logger.info(f"Transferred: {', '.join(transferred)}")
    else:
        logger.info("No transfers")

    if merged:
        logger.info(f"Merged: {', '.join(merged)}")
    else:
        logger.info("No merges")


# -----------------------------
# AIRFLOW DEFAULT CONFIG
# -----------------------------
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    dag_id="bq_quarantine_transfer_raw_safe_full",
    start_date=datetime(2024, 1, 1, tzinfo=SGT),
    schedule="0 0 * * *",     # runs daily at midnight SG time
    catchup=False,            # do not backfill automatically
    max_active_runs=1,        # prevent overlapping runs
    concurrency=LOOKBACK_DAYS + 1,
    default_args=default_args,
) as dag:

    # Final reporting task (runs after all others)
    report = PythonOperator(
        task_id="report",
        python_callable=daily_report,
        trigger_rule=TriggerRule.ALL_DONE
    )

    tasks = []

    # Loop over lookback days (D-1 to D-4)
    for i in range(1, LOOKBACK_DAYS + 1):

        # Step 1: ensure source table exists
        check = PythonOperator(
            task_id=f"check_source_{i}",
            python_callable=check_source_table,
            op_kwargs={"prefix": TABLE_PREFIX, "offset_days": i},
        )

        # Step 2: build SQL (merge or transfer)
        build = PythonOperator(
            task_id=f"build_sql_{i}",
            python_callable=build_merge_sql,
            op_kwargs={"offset_days": i, "direct_transfer": i == 1},
        )

        # Step 3: execute SQL in BigQuery
        run = BigQueryInsertJobOperator(
            task_id=f"run_{i}",
            location=LOCATION,
            configuration={
                "query": {
                    "query": "{{ ti.xcom_pull(task_ids='build_sql_" + str(i) + "', key='sql_to_execute_" + str(i) + "') }}",
                    "useLegacySql": False
                }
            }
        )

        # Define dependency chain
        check >> build >> run
        tasks.append(run)

    # Reporting runs after all data tasks finish
    tasks >> report
