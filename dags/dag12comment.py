import logging
import time
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException

from google.cloud.exceptions import NotFound
from google.api_core.exceptions import ServiceUnavailable, InternalServerError, TooManyRequests


# Logger for tracking pipeline activity
logger = logging.getLogger(__name__)


# Core configuration for source/target projects and dataset
SOURCE_PROJECT = "ac-project-485203"
TARGET_PROJECT = "team-485203"
DATASET = "analytics_514446807"
LOCATION = "asia-southeast1"

# GA-style table naming pattern: events_YYYYMMDD
TABLE_PREFIX = "events"

# Number of past days to process (D-1 to D-N)
LOOKBACK_DAYS = 4

# DAG timezone (Singapore time)
SGT = ZoneInfo("Asia/Singapore")


# Retryable BigQuery API errors (transient issues)
RETRYABLE_ERRORS = (ServiceUnavailable, InternalServerError, TooManyRequests)


# -------------------------------------------------------
# Helper: Retry wrapper for BigQuery API calls
# Prevents DAG failure due to temporary BQ issues
# -------------------------------------------------------
def bq_retry(func, max_retries=5, base_delay=2):
    for attempt in range(max_retries):
        try:
            return func()
        except RETRYABLE_ERRORS:
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 1))


# -------------------------------------------------------
# Helper: Generate table name from date
# Example: 2026-04-12 → events_20260412
# -------------------------------------------------------
def generate_table_name(prefix, date_str):
    return f"{prefix}_{date_str.replace('-', '')}"


# -------------------------------------------------------
# Helper: Get BigQuery client
# -------------------------------------------------------
def get_bq_client():
    return BigQueryHook().get_client()


# -------------------------------------------------------
# Helper: Get last modified timestamp of a table
# Used to determine if data has changed
# -------------------------------------------------------
def get_last_modified(client, project, dataset, table):
    try:
        t = bq_retry(lambda: client.get_table(f"{project}.{dataset}.{table}"))
        return t.modified
    except NotFound:
        return None


# -------------------------------------------------------
# Task 1: Validate source table exists
# Skips execution if table is missing
# -------------------------------------------------------
def check_source_table(logical_date, offset_days, prefix, **context):
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(prefix, table_date)

    client = get_bq_client()

    try:
        bq_retry(lambda: client.get_table(f"{SOURCE_PROJECT}.{DATASET}.{table_name}"))
    except NotFound:
        raise AirflowSkipException(f"Source table {table_name} not found.")


# -------------------------------------------------------
# Task 2: Build SQL dynamically based on scenario
# Handles:
# - D-1 full overwrite
# - Missing target backfill
# - Schema drift
# - Incremental MERGE
# -------------------------------------------------------
def build_sql(logical_date, offset_days=0, direct_transfer=False, **context):

    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(TABLE_PREFIX, table_date)

    source = f"{SOURCE_PROJECT}.{DATASET}.{table_name}"
    target = f"{TARGET_PROJECT}.{DATASET}.{table_name}"

    ti = context["ti"]
    client = get_bq_client()

    # Load source metadata and schema
    source_table = bq_retry(lambda: client.get_table(source))
    source_schema = {f.name: f.field_type for f in source_table.schema}
    source_last_modified = source_table.modified


    # -------------------------------------------------------
    # Case 1: D-1 → Always full overwrite (latest data)
    # -------------------------------------------------------
    if direct_transfer:
        sql = f"CREATE OR REPLACE TABLE `{target}` AS SELECT * FROM `{source}`;"
        ti.xcom_push(key=f"op_{offset_days}", value="direct_transfer")
        ti.xcom_push(key=f"sql_{offset_days}", value=sql)
        return sql


    # -------------------------------------------------------
    # Case 2: Check if target exists + schema drift handling
    # -------------------------------------------------------
    alter_sql = ""
    target_last_modified = get_last_modified(client, TARGET_PROJECT, DATASET, table_name)

    try:
        target_table = bq_retry(lambda: client.get_table(target))
        target_schema = {f.name: f.field_type for f in target_table.schema}

        # Add any new columns found in source
        new_cols = [
            f"ALTER TABLE `{target}` ADD COLUMN {c} {t};"
            for c, t in source_schema.items()
            if c not in target_schema
        ]
        alter_sql = "\n".join(new_cols)

    except NotFound:
        # Target missing → full backfill copy
        sql = f"CREATE TABLE `{target}` AS SELECT * FROM `{source}`;"
        ti.xcom_push(key=f"op_{offset_days}", value="backfill_copy")
        ti.xcom_push(key=f"sql_{offset_days}", value=sql)
        return sql


    # -------------------------------------------------------
    # Case 3: Skip if no data change detected
    # -------------------------------------------------------
    if target_last_modified and source_last_modified <= target_last_modified:
        ti.xcom_push(key=f"op_{offset_days}", value="skipped")
        raise AirflowSkipException(f"No changes for {table_name}")


    # -------------------------------------------------------
    # Case 4: MERGE (incremental update)
    # Deduplicates source using ROW_NUMBER
    # -------------------------------------------------------
    key_clause = "T.event_time = S.event_time AND T.event_name = S.event_name"

    cols_to_update = list(source_schema.keys())

    update_clause = ",\n".join([f"{c}=S.{c}" for c in cols_to_update])
    insert_cols = ", ".join(source_schema.keys())
    insert_vals = ", ".join([f"S.{c}" for c in source_schema.keys()])

    sql = f"""
    {alter_sql}

    MERGE `{target}` T
    USING (
        SELECT * EXCEPT(rn)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY event_time, event_name
                       ORDER BY event_time DESC
                   ) rn
            FROM `{source}`
        )
        WHERE rn = 1
    ) S
    ON {key_clause}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    ti.xcom_push(key=f"op_{offset_days}", value="merge")
    ti.xcom_push(key=f"sql_{offset_days}", value=sql)

    return sql


# -------------------------------------------------------
# Task 3: Reporting
# Summarizes pipeline actions for each day
# -------------------------------------------------------
def daily_report(**context):
    ti = context["ti"]
    logical_date = context["logical_date"]

    summary = {
        "direct_transfer": [],
        "backfill_copy": [],
        "merge": [],
        "skipped": []
    }

    for i in range(1, LOOKBACK_DAYS + 1):
        table_date = (logical_date - timedelta(days=i)).strftime("%Y-%m-%d")
        table_name = generate_table_name(TABLE_PREFIX, table_date)

        op = ti.xcom_pull(task_ids=f"build_sql_{i}", key=f"op_{i}")

        if op in summary:
            summary[op].append(table_name)
        else:
            summary["skipped"].append(table_name)

    for op_type, tables in summary.items():
        logger.info(f"{op_type}: {', '.join(tables) if tables else 'None'}")


# -------------------------------------------------------
# DAG Configuration
# -------------------------------------------------------
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="bq_hybrid_optimized_transfer",
    start_date=datetime(2024, 1, 1, tzinfo=SGT),
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:

    report = PythonOperator(
        task_id="report",
        python_callable=daily_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    for i in range(1, LOOKBACK_DAYS + 1):

        check = PythonOperator(
            task_id=f"check_source_{i}",
            python_callable=check_source_table,
            op_kwargs={"prefix": TABLE_PREFIX, "offset_days": i},
        )

        build = PythonOperator(
            task_id=f"build_sql_{i}",
            python_callable=build_sql,
            op_kwargs={"offset_days": i, "direct_transfer": i == 1},
        )

        run = BigQueryInsertJobOperator(
            task_id=f"run_{i}",
            location=LOCATION,
            configuration={
                "query": {
                    "query": "{{ ti.xcom_pull(task_ids='build_sql_" + str(i) + "', key='sql_" + str(i) + "') }}",
                    "useLegacySql": False,
                }
            },
        )

        check >> build >> run >> report
