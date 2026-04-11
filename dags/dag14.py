import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

SOURCE_PROJECT = "ac-project-485203"
TARGET_PROJECT = "team-485203"
DATASET = "analytics_514446807"
TABLE_PREFIX = "events"
LOOKBACK_DAYS = 4
SGT = ZoneInfo("Asia/Singapore")


# -------------------------
# Helpers (CLEANED)
# -------------------------

def table_name(date_str):
    return f"{TABLE_PREFIX}_{date_str.replace('-', '')}"


def resolve_table(logical_date, offset_days):
    date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    return table_name(date)


def get_client():
    return BigQueryHook().get_client()


def get_table(client, project, dataset, table):
    try:
        return client.get_table(f"{project}.{dataset}.{table}")
    except NotFound:
        return None


def get_meta(table):
    if not table:
        return None, None

    schema = "|".join([f"{f.name}:{f.field_type}" for f in table.schema])
    return table.modified, schema


# -------------------------
# Source check
# -------------------------

def check_source(logical_date, offset_days, **context):
    table = resolve_table(logical_date, offset_days)

    client = get_client()
    get_table(client, SOURCE_PROJECT, DATASET, table)


# -------------------------
# Build SQL (CORE LOGIC CLEANED)
# -------------------------

def build_sql(logical_date, offset_days, direct_transfer=False, **context):
    ti = context["ti"]

    table = resolve_table(logical_date, offset_days)

    source_ref = f"{SOURCE_PROJECT}.{DATASET}.{table}"
    target_ref = f"{TARGET_PROJECT}.{DATASET}.{table}"

    client = get_client()

    # -------------------------
    # SOURCE
    # -------------------------
    source_table = get_table(client, SOURCE_PROJECT, DATASET, table)
    if not source_table:
        ti.xcom_push(key=f"op_{offset_days}", value="missing_source")
        raise AirflowSkipException("source missing")

    source_modified, source_schema = get_meta(source_table)

    # -------------------------
    # TARGET
    # -------------------------
    target_table = get_table(client, TARGET_PROJECT, DATASET, table)
    target_modified, target_schema = get_meta(target_table)

    # CASE: no target → full copy
    if not target_table:
        sql = f"""
        CREATE TABLE `{target_ref}` AS
        SELECT * FROM `{source_ref}`;
        """

        ti.xcom_push(key=f"op_{offset_days}", value="sync_done")
        ti.xcom_push(key=f"sql_{offset_days}", value=sql)
        return sql

    # -------------------------
    # D-1 RULE
    # -------------------------
    if direct_transfer:
        sql = f"""
        CREATE OR REPLACE TABLE `{target_ref}` AS
        SELECT * FROM `{source_ref}`;
        """

        ti.xcom_push(key=f"op_{offset_days}", value="sync_done")
        ti.xcom_push(key=f"sql_{offset_days}", value=sql)
        return sql

    # -------------------------
    # D-2 to D-N RULE
    # -------------------------
    schema_changed = source_schema != target_schema

    data_changed = (
        target_modified is None
        or source_modified > target_modified
    )

    if not schema_changed and not data_changed:
        ti.xcom_push(key=f"op_{offset_days}", value="skipped")
        raise AirflowSkipException("no change detected")

    sql = f"""
    CREATE OR REPLACE TABLE `{target_ref}` AS
    SELECT * FROM `{source_ref}`;
    """

    ti.xcom_push(
        key=f"op_{offset_days}",
        value="schema_change" if schema_changed else "data_change"
    )

    ti.xcom_push(key=f"sql_{offset_days}", value=sql)
    return sql


# -------------------------
# REPORT TASK (UNCHANGED LOGIC, CLEAN INPUT)
# -------------------------

def daily_report(**context):
    ti = context["ti"]

    summary = {
        "sync_done": [],
        "skipped": [],
        "missing_source": []
    }

    for i in range(1, LOOKBACK_DAYS + 1):
        op = ti.xcom_pull(task_ids=f"build_{i}", key=f"op_{i}")
        table = f"events_{i}"

        if op == "sync_done":
            summary["sync_done"].append(table)
        elif op == "missing_source":
            summary["missing_source"].append(table)
        else:
            summary["skipped"].append(table)

    logger.info("========= DAILY SYNC REPORT =========")
    for k, v in summary.items():
        logger.info(f"{k}: {v if v else 'None'}")


# -------------------------
# DAG
# -------------------------

with DAG(
    dag_id="bq_metadata_replication_senior_clean",
    start_date=datetime(2024, 1, 1, tzinfo=SGT),
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    report = PythonOperator(
        task_id="daily_report",
        python_callable=daily_report,
        trigger_rule="all_done",
    )

    for i in range(1, LOOKBACK_DAYS + 1):

        check = PythonOperator(
            task_id=f"check_{i}",
            python_callable=check_source,
            op_kwargs={"offset_days": i},
        )

        build = PythonOperator(
            task_id=f"build_{i}",
            python_callable=build_sql,
            op_kwargs={
                "offset_days": i,
                "direct_transfer": i == 1
            },
        )

        run = BigQueryInsertJobOperator(
            task_id=f"run_{i}",
            location="asia-southeast1",
            configuration={
                "query": {
                    "query": "{{ ti.xcom_pull(task_ids='build_" + str(i) + "', key='sql_" + str(i) + "') }}",
                    "useLegacySql": False
                }
            },
        )

        check >> build >> run >> report
