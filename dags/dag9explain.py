import logging
import time
import random
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import ServiceUnavailable, InternalServerError, TooManyRequests

# -----------------------------
# LOGGER SETUP
# -----------------------------
logger = logging.getLogger(__name__)

# -----------------------------
# CONFIGURATION (PIPELINE SETTINGS)
# -----------------------------

# Source project where raw event tables live (e.g. GA4 export)
SOURCE_PROJECT = "ac-project-485203"

# Target project where cleaned/merged tables will be written
TARGET_PROJECT = "team-485203"

# Shared dataset name in both projects
DATASET = "analytics_514446807"

# BigQuery region (important for job execution location)
LOCATION = "asia-southeast1"

# Table naming prefix (GA4-style tables like events_YYYYMMDD)
TABLE_PREFIX = "events"

# How many past days to process (D-1 to D-4)
LOOKBACK_DAYS = 4

# Timezone used for DAG scheduling
SGT = ZoneInfo("Asia/Singapore")

# Errors that should be retried automatically (transient GCP failures)
RETRYABLE_ERRORS = (ServiceUnavailable, InternalServerError, TooManyRequests)


# -----------------------------
# RETRY WRAPPER (RESILIENCE LAYER)
# -----------------------------
def bq_retry(func, max_retries=5, base_delay=2):
    """
    Executes a BigQuery operation with exponential backoff retry.
    Helps handle temporary GCP outages or throttling.
    """
    for attempt in range(max_retries):
        try:
            return func()
        except RETRYABLE_ERRORS:
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 1))


# -----------------------------
# TABLE NAME GENERATION
# -----------------------------
def generate_table_name(prefix, date_str):
    """
    Converts date format YYYY-MM-DD → events_YYYYMMDD
    Example:
        events_2026-04-10 → events_20260410
    """
    return f"{prefix}_{date_str.replace('-', '')}"


# -----------------------------
# BIGQUERY CLIENT
# -----------------------------
def get_bq_client():
    """
    Creates a BigQuery client using Airflow hook.
    Note: no caching used (fresh client per task execution).
    """
    hook = BigQueryHook()
    return hook.get_client()


# -----------------------------
# ROW COUNT UTILITY
# -----------------------------
def get_table_row_count(bq_client, project, dataset, table):
    """
    Returns row count of a table.
    Used as a lightweight signal for detecting changes.
    """
    try:
        query = f"SELECT COUNT(*) c FROM `{project}.{dataset}.{table}`"
        return list(bq_retry(lambda: bq_client.query(query).result()))[0]["c"]
    except Exception:
        return None


# -----------------------------
# SCHEMA DRIFT DETECTION
# -----------------------------
def detect_schema_drift(source_schema, target_schema):
    """
    Compares source vs target schema:
    - missing_in_target → new columns in source
    - extra_in_target → columns removed or outdated in source
    """
    source_set = {f"{k}:{v}" for k, v in source_schema.items()}
    target_set = {f"{k}:{v}" for k, v in target_schema.items()}
    return source_set - target_set, target_set - source_set


# -----------------------------
# STEP 1: CHECK SOURCE TABLE EXISTS
# -----------------------------
def check_source_table(logical_date, offset_days, prefix, **context):
    """
    Verifies that the source table exists before processing.
    If missing → skip this DAG branch safely.
    """
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(prefix, table_date)

    bq_client = get_bq_client()

    try:
        bq_retry(lambda: bq_client.get_table(f"{SOURCE_PROJECT}.{DATASET}.{table_name}"))

        # Mark table exists in XCom for reporting
        context["ti"].xcom_push(key=f"table_exists_{table_name}", value=True)

    except NotFound:
        context["ti"].xcom_push(key=f"table_exists_{table_name}", value=False)

        # Skip downstream tasks for this date
        raise AirflowSkipException(f"{table_name} missing")


# -----------------------------
# STEP 2: BUILD SQL (TRANSFER OR MERGE)
# -----------------------------
def build_merge_sql(logical_date, offset_days=0, direct_transfer=False, **context):

    # Compute target date table
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(TABLE_PREFIX, table_date)

    source = f"{SOURCE_PROJECT}.{DATASET}.{table_name}"
    target = f"{TARGET_PROJECT}.{DATASET}.{table_name}"

    bq_client = get_bq_client()

    # Load schema from source table
    source_table = bq_retry(lambda: bq_client.get_table(source))
    source_schema = {f.name: f.field_type for f in source_table.schema}

    # Get row count for change detection
    source_rows = get_table_row_count(bq_client, SOURCE_PROJECT, DATASET, table_name)

    # -----------------------------
    # DIRECT TRANSFER MODE (D-1 ONLY)
    # -----------------------------
    if direct_transfer:

        target_rows = get_table_row_count(bq_client, TARGET_PROJECT, DATASET, table_name)

        # Skip if already fully synced
        if source_rows is not None and target_rows == source_rows:
            raise AirflowSkipException("already transferred")

        # Full overwrite snapshot copy
        sql = f"CREATE OR REPLACE TABLE `{target}` AS SELECT * FROM `{source}`;"

        context["ti"].xcom_push(key=f"direct_transfer_success_{offset_days}", value=True)
        context["ti"].xcom_push(key=f"sql_to_execute_{offset_days}", value=sql)

        return sql

    # -----------------------------
    # MERGE MODE (D-2, D-3, D-4...)
    # -----------------------------
    try:
        target_table = bq_retry(lambda: bq_client.get_table(target))
        target_schema = {f.name: f.field_type for f in target_table.schema}

        # Detect schema differences
        missing, extra = detect_schema_drift(source_schema, target_schema)

        # Add new columns if needed
        alter_sql = "\n".join(
            f"ALTER TABLE `{target}` ADD COLUMN {c} {t};"
            for c, t in source_schema.items()
            if c not in target_schema
        )

        target_exists = True

    except NotFound:
        # If target doesn't exist, create empty table structure
        alter_sql = ""
        target_exists = False
        create_sql = f"CREATE TABLE `{target}` AS SELECT * FROM `{source}` WHERE 1=0;"
    else:
        create_sql = ""

    # Skip if row counts match (simple change detection optimization)
    if target_exists:
        tgt_rows = get_table_row_count(bq_client, TARGET_PROJECT, DATASET, table_name)
        if source_rows is not None and tgt_rows == source_rows:
            raise AirflowSkipException("no change")

    # -----------------------------
    # PRIMARY KEY SELECTION LOGIC
    # -----------------------------

    # If GA4-style ID exists → use it
    if "id" in source_schema:
        key = "T.id = S.id"
        cols = [c for c in source_schema if c != "id"]

    # Otherwise fallback to composite GA4 identifiers
    else:
        keys = ["user_pseudo_id", "event_timestamp", "event_name"]
        keys = [k for k in keys if k in source_schema]
        key = " AND ".join([f"T.{c}=S.{c}" for c in keys])
        cols = [c for c in source_schema if c not in keys]

    # Build UPDATE clause for matched rows
    update_clause = ",\n".join([f"{c}=S.{c}" for c in cols])

    # Build INSERT clause for new rows
    insert_cols = ", ".join(source_schema.keys())
    insert_vals = ", ".join([f"S.{c}" for c in source_schema.keys()])

    # -----------------------------
    # FINAL MERGE SQL
    # -----------------------------
    sql = f"""
    {create_sql}
    {alter_sql}

    MERGE `{target}` T
    USING (
        SELECT * FROM `{source}`
    ) S
    ON {key}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    # Save status to Airflow XCom for reporting
    context["ti"].xcom_push(key=f"merge_success_{offset_days}", value=True)
    context["ti"].xcom_push(key=f"sql_to_execute_{offset_days}", value=sql)

    return sql


# -----------------------------
# STEP 3: DAILY SUMMARY REPORT
# -----------------------------
def daily_report(**context):
    """
    Aggregates results of all processed days:
    - Which tables were transferred
    - Which tables were merged
    """
    ti = context["ti"]
    logical_date = context["logical_date"]

    merged = []
    transferred = []

    for i in range(1, LOOKBACK_DAYS + 1):

        table_date = (logical_date - timedelta(days=i)).strftime("%Y-%m-%d")
        table_name = generate_table_name(TABLE_PREFIX, table_date)

        if ti.xcom_pull(task_ids=f"build_sql_{i}", key=f"direct_transfer_success_{i}"):
            transferred.append(table_name)

        if ti.xcom_pull(task_ids=f"build_sql_{i}", key=f"merge_success_{i}"):
            merged.append(table_name)

    logger.info(f"Transferred: {', '.join(transferred) if transferred else 'No transfers'}")
    logger.info(f"Merged: {', '.join(merged) if merged else 'No merges'}")


# -----------------------------
# DAG DEFAULT CONFIG
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
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=LOOKBACK_DAYS + 1,
    default_args=default_args,
) as dag:

    # Final reporting task (runs after everything else completes)
    report = PythonOperator(
        task_id="report",
        python_callable=daily_report,
        trigger_rule=TriggerRule.ALL_DONE
    )

    tasks = []

    # Loop through D-1 to D-4
    for i in range(1, LOOKBACK_DAYS + 1):

        # Step 1: ensure source exists
        check = PythonOperator(
            task_id=f"check_source_{i}",
            python_callable=check_source_table,
            op_kwargs={"prefix": TABLE_PREFIX, "offset_days": i},
        )

        # Step 2: build SQL logic (transfer or merge)
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

        # Task dependency chain
        check >> build >> run
        tasks.append(run)

    # Run report after all processing
    tasks >> report
