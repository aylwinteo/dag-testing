import logging
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

SOURCE_PROJECT = "quarantine-project"
TARGET_PROJECT = "prod-project"
DATASET = "your_dataset"
LOCATION = "asia-southeast1"
TABLE_PREFIX = "events"
LOOKBACK_DAYS = 4
SGT = ZoneInfo("Asia/Singapore")

def generate_table_name(prefix, date_str):
    clean_date = date_str.replace('-', '')
    return f"{prefix}_{clean_date}"

def get_bq_client():
    hook = BigQueryHook()
    return hook.get_client()

def get_table_last_modified_time(bq_client, project_id, dataset_id, table_id):
    try:
        table_ref = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}")
        table = bq_client.get_table(table_ref)
        return datetime.fromtimestamp(table.last_modified_time / 1000, tz=SGT)
    except NotFound:
        return None
    except Exception as e:
        logger.error(f"Error fetching last_modified_time for {project_id}.{dataset_id}.{table_id}: {e}", exc_info=True)
        raise

def check_source_table(logical_date, offset_days, prefix, **context):
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(prefix, table_date)
    bq_client = get_bq_client()

    try:
        bq_client.get_table(f"{SOURCE_PROJECT}.{DATASET}.{table_name}")
        context['ti'].xcom_push(key=f"table_exists_{table_name}", value=True)
        logger.info(f"[TRANSFER READY] Source table {table_name} exists")
    except NotFound:
        context['ti'].xcom_push(key=f"table_exists_{table_name}", value=False)
        logger.warning(f"[SKIPPED] Source table {table_name} does not exist. Skipping transfer for this table.")
        raise AirflowSkipException(f"Source table {table_name} missing.")
    except Exception as e:
        logger.error(f"An unexpected error occurred while checking source table {table_name}: {e}", exc_info=True)
        raise

def build_merge_sql(logical_date, offset_days=0, direct_transfer=False, **context):
    table_date = (logical_date - timedelta(days=offset_days)).strftime("%Y-%m-%d")
    table_name = generate_table_name(TABLE_PREFIX, table_date)
    source_table_full = f"{SOURCE_PROJECT}.{DATASET}.{table_name}"
    target_table_full = f"{TARGET_PROJECT}.{DATASET}.{table_name}"

    bq_client = get_bq_client()

    try:
        bq_table_source = bq_client.get_table(source_table_full)
        source_fields = {f.name: f.field_type for f in bq_table_source.schema}
    except NotFound:
        logger.error(f"Source table {source_table_full} not found when building merge SQL. This should have been caught by check_source_table.")
        raise
    except Exception as e:
        logger.error(f"Error retrieving schema for source table {source_table_full}: {e}", exc_info=True)
        raise

    alter_sql = ""
    create_target_sql = ""
    target_table_exists = False

    try:
        bq_table_target = bq_client.get_table(target_table_full)
        target_fields = {f.name: f.field_type for f in bq_table_target.schema}
        target_table_exists = True

        for col, col_type in source_fields.items():
            if col not in target_fields:
                alter_sql += f"ALTER TABLE `{target_table_full}` ADD COLUMN {col} {col_type};\n"
                logger.info(f"Adding new column: {col} {col_type} to target table {target_table_full}")
    except NotFound:
        logger.info(f"Target table {target_table_full} does not exist. Generating CREATE TABLE statement.")
        create_target_sql = f"CREATE TABLE `{target_table_full}` AS SELECT * FROM `{source_table_full}` WHERE 1=0;\n"
    except Exception as e:
        logger.error(f"Error checking target schema for {target_table_full}: {e}", exc_info=True)
        raise

    if not direct_transfer:
        source_last_modified = get_table_last_modified_time(bq_client, SOURCE_PROJECT, DATASET, table_name)
        target_last_modified = get_table_last_modified_time(bq_client, TARGET_PROJECT, DATASET, table_name)

        if source_last_modified is None:
            logger.warning(f"Source table {source_table_full} not found or accessible. Skipping merge SQL generation.")
            context['ti'].xcom_push(key=f"merge_sql_{offset_days}", value="SELECT 1;")
            return "SELECT 1;"

        if target_last_modified is None:
            logger.info(f"Target table {target_table_full} does not exist. Will create and merge all data.")
            pass
        elif source_last_modified > target_last_modified:
            logger.info(f"Source table {source_table_full} is newer ({source_last_modified}) than target ({target_last_modified}). Will perform merge.")
            pass
        else:
            logger.info(f"Source table {source_table_full} ({source_last_modified}) is not newer than target ({target_last_modified}). Skipping merge.")
            context['ti'].xcom_push(key=f"merge_sql_{offset_days}", value="SELECT 1;")
            return "SELECT 1;"
    else:
        logger.info(f"Performing direct transfer for D-1 table {source_table_full}. Bypassing last_modified_time check.")

    merge_key = ""
    update_fields = []

    if 'id' in source_fields:
        merge_key = "T.id = S.id"
        update_fields = [f for f in source_fields.keys() if f not in ['id']]
        logger.debug(f"Using 'id' as merge key for {source_table_full}.")
    else:
        composite_event_key_cols = ["user_pseudo_id", "event_timestamp", "event_name"]

        if not all(col in source_fields for col in composite_event_key_cols):
            missing_cols = [col for col in composite_event_key_cols if col not in source_fields]
            logger.error(f"One or more columns in the defined composite event key ({composite_event_key_cols}) "
                         f"are not found in the source schema of {source_table_full}. Missing: {missing_cols}")
            raise ValueError(f"Missing composite key columns in source schema: {missing_cols}")

        merge_key = " AND ".join([f"T.{c} = S.{c}" for c in composite_event_key_cols])

        update_fields = [f for f in source_fields.keys() if f not in composite_event_key_cols]
        logger.debug(f"Using composite key {composite_event_key_cols} as merge key for {source_table_full}.")

        if not merge_key: 
             key_cols_for_fingerprint = [f for f in source_fields.keys()]
             merge_key = (
                 "FARM_FINGERPRINT(CONCAT(" +
                 ",".join([f"IFNULL(CAST(T.{c} AS STRING),'NULL')" for c in key_cols_for_fingerprint]) +
                 ")) = FARM_FINGERPRINT(CONCAT(" +
                 ",".join([f"IFNULL(CAST(S.{c} AS STRING),'NULL')" for c in key_cols_for_fingerprint]) +
                 "))"
             )
             update_fields = [f for f in source_fields.keys() if f not in key_cols_for_fingerprint]
             logger.debug(f"Falling back to FARM_FINGERPRINT as merge key for {source_table_full}.")

    update_clause = ",\n".join([f"{col} = S.{col}" for col in update_fields])
    if not update_clause: 
        update_clause = "T.DUMMY_FIELD = T.DUMMY_FIELD"

    merge_sql = f"""
    {create_target_sql}
    {alter_sql}
    CREATE TEMP TABLE staging_data AS
    SELECT * FROM `{source_table_full}`;

    MERGE `{target_table_full}` T
    USING staging_data S
    ON {merge_key}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ROW;
    """
    context['ti'].xcom_push(key=f"merge_sql_{offset_days}", value=merge_sql)
    logger.debug(f"Generated merge SQL for {source_table_full}:\n{merge_sql}")
    return merge_sql

def daily_report(**context):
    ti = context['ti']
    logger.info("\n========== Daily Quarantine Transfer Report ==========")

    table_date_d1 = (context['execution_date'] - timedelta(days=1)).strftime("%Y-%m-%d")
    table_name_d1 = generate_table_name(TABLE_PREFIX, table_date_d1)
    status_d1 = ti.xcom_pull(task_ids=f"check_source_d1", key=f"table_exists_{table_name_d1}")
    merge_sql_d1 = ti.xcom_pull(task_ids=f"build_merge_sql_d1", key=f"merge_sql_d1")
    action_d1 = "MERGED (Direct Transfer)" if merge_sql_d1 != "SELECT 1;" else "SKIPPED (no changes or source missing)"
    logger.info(f"{table_name_d1}: {'READY' if status_d1 else 'SKIPPED'} | Action: {action_d1}")

    for i in range(2, LOOKBACK_DAYS + 1):
        table_date = (context['execution_date'] - timedelta(days=i)).strftime("%Y-%m-%d")
        table_name = generate_table_name(TABLE_PREFIX, table_date)
        status = ti.xcom_pull(task_ids=f"check_source_{i}", key=f"table_exists_{table_name}")
        merge_sql_generated = ti.xcom_pull(task_ids=f"build_merge_sql_{i}", key=f"merge_sql_{i}")

        action = "MERGED (Conditional Update)" if merge_sql_generated != "SELECT 1;" else "SKIPPED (no changes)"

        logger.info(f"{table_name}: {'READY' if status else 'SKIPPED'} | Action: {action}")
    logger.info("=====================================================")

with DAG(
    dag_id="bq_quarantine_transfer_raw_safe_full",
    start_date=datetime(2024, 1, 1, tzinfo=SGT),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=LOOKBACK_DAYS + 1,
    tags=['bigquery', 'transfer', 'events', 'full_merge_on_metadata'],
) as dag:

    report_task = PythonOperator(
        task_id='daily_report',
        python_callable=daily_report,
        trigger_rule=TriggerRule.ALL_DONE
    )

    check_task_d1 = PythonOperator(
        task_id=f"check_source_d1",
        python_callable=check_source_table,
        op_kwargs={"prefix": TABLE_PREFIX, "offset_days": 1},
    )

    build_sql_task_d1 = PythonOperator(
        task_id=f"build_merge_sql_d1",
        python_callable=build_merge_sql,
        op_kwargs={"offset_days": 1, "direct_transfer": True},
    )

    transfer_task_d1 = BigQueryInsertJobOperator(
        task_id=f"execute_transfer_d1",
        location=LOCATION,
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='build_merge_sql_d1', key='merge_sql_d1') }}",
                "useLegacySql": False
            }
        },
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    for i in range(2, LOOKBACK_DAYS + 1):
        check_task = PythonOperator(
            task_id=f"check_source_{i}",
            python_callable=check_source_table,
            op_kwargs={"prefix": TABLE_PREFIX, "offset_days": i},
        )

        build_sql_task = PythonOperator(
            task_id=f"build_merge_sql_{i}",
            python_callable=build_merge_sql,
            op_kwargs={"offset_days": i, "direct_transfer": False}, # direct_transfer=False for D-2 to D-N
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
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        check_task >> build_sql_task >> transfer_task

    all_transfer_tasks = [transfer_task_d1] + [dag.get_task(f"execute_transfer_{j}") for j in range(2, LOOKBACK_DAYS + 1)]
    all_transfer_tasks >> report_task
