#!/usr/bin/env python3
import argparse
import os
import sys
import subprocess
import json
import re
import hashlib
import uuid
import time
import logging
from datetime import datetime, timedelta
from jinja2 import Template

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Shared Utilities ---

def run_command(cmd, check=True, capture_output=True):
    """Executes a shell command and returns the output."""
    logger.debug(f"Running: {cmd}")
    try:
        # If cmd is a list, shell=False by default (safer), if string shell=True is needed (or preferred for complex pipes)
        # We'll support both but default to list execution if possible for safety, unless shell=True is explicit or cmd is string
        shell_mode = isinstance(cmd, str)
        result = subprocess.run(
            cmd, 
            shell=shell_mode, 
            check=check, 
            capture_output=capture_output, 
            text=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {cmd}")
        if capture_output and e.stderr:
            logger.error(f"Error output: {e.stderr}")
        if check:
            raise
        return ""

def get_identity_hash(email):
    """Generates a short, consistent hash for an identity (email)."""
    return hashlib.md5(email.lower().strip().encode()).hexdigest()[:10]

def get_current_user_email():
    """Gets the current gcloud user email."""
    try:
        return run_command(["gcloud", "config", "get", "account"], check=True)
    except Exception:
        return None

def get_composer_bucket(project, region, env_name):
    """Fetches the Composer bucket name dynamically using gcloud."""
    logger.info(f"Detecting GCS Bucket for environment '{env_name}'...")
    try:
        cmd = [
            "gcloud", "composer", "environments", "describe", env_name,
            "--location", region,
            "--project", project,
            "--format", "value(config.dagGcsPrefix)"
        ]
        dag_path = run_command(cmd)
        
        if dag_path.startswith("gs://"):
            bucket = dag_path.replace("gs://", "").split("/")[0]
            logger.info(f"  > Found Bucket: {bucket}")
            return bucket
        else:
            raise ValueError(f"Unexpected DAG Path format: {dag_path}")
    except Exception as e:
        logger.error(f"Failed to detect Composer bucket: {e}")
        sys.exit(1)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket using gcloud storage cp."""
    destination_uri = f"gs://{bucket_name}/{destination_blob_name}"
    try:
        run_command(["gcloud", "storage", "cp", source_file_name, destination_uri], check=True)
        logger.info(f"File {source_file_name} uploaded to {destination_uri}.")
        return destination_uri
    except subprocess.CalledProcessError as e:
        logger.error(f"Error uploading {source_file_name}: {e.stderr}")
        raise e

# --- Command: Create (Manual Chain Scheduler) ---

DAG_TEMPLATE = """
from datetime import datetime, timedelta
import json
import hashlib
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from google.cloud import storage
import re

# --- SECURE IDENTITY CHECK ---
try:
    dag_filename = "{{ dag_id }}"
    owner_hash = dag_filename.split('_')[0]
    
    authority_map = Variable.get("RBAC_AUTHORITY_MAPPING", default_var={}, deserialize_json=True)
    target_sa = authority_map.get(owner_hash)
    
    if not target_sa:
        raise ValueError(f"Security Error: Hash '{owner_hash}' is not authorized in RBAC_AUTHORITY_MAPPING. Please contact Admin.")
        
    IMPERSONATION_ACCOUNT = target_sa
    print(f"Secure Identity Verified: {IMPERSONATION_ACCOUNT}")
    
except Exception as e:
    raise RuntimeError(f"DAG Security Initialization Failed: {e}")
# -----------------------------

default_args = {
    'owner': '{{ owner }}',
    'start_date': {{ start_date }},
    'retries': 1,
    'retry_delay': timedelta(minutes=int('5')),
    'email_on_failure': False,
    'email_on_retry': False,
}

def merge_notebooks(input_paths, output_path, **kwargs):
    print(f"Merging {len(input_paths)} notebooks into {output_path}")
    storage_client = storage.Client()
    merged_cells = []
    metadata = {}
    nbformat = 4
    nbformat_minor = 5

    for i, path in enumerate(input_paths):
        print(f"Processing {path}")
        bucket_name, blob_name = path.replace("gs://", "").split("/", 1)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        try:
            content = blob.download_as_text()
            nb = json.loads(content)
            
            if i == 0:
                metadata = nb.get('metadata', {})
                nbformat = nb.get('nbformat', 4)
                nbformat_minor = nb.get('nbformat_minor', 5)
            
            merged_cells.extend(nb.get('cells', []))
        except Exception as e:
            print(f"Error processing {path}: {e}")
            raise e

    merged_nb = {
        "cells": merged_cells,
        "metadata": metadata,
        "nbformat": nbformat,
        "nbformat_minor": nbformat_minor
    }

    output_bucket_name, output_blob_name = output_path.replace("gs://", "").split("/", 1)
    output_bucket = storage_client.bucket(output_bucket_name)
    output_blob = output_bucket.blob(output_blob_name)
    output_blob.upload_from_string(json.dumps(merged_nb, indent=1))
    print(f"Merged notebook uploaded to {output_path}")

dag = DAG(
    '{{ dag_id }}',
    default_args=default_args,
    description='Chained Notebooks: {{ notebook_names }}',
    schedule_interval='{{ schedule_interval }}',
    catchup=False,
    tags=['scheduler_jupyter_plugin', 'chained_notebooks'],
)

# Parsing Hooks for Scheduler Plugin
input_notebook = '{{ tasks[0].input_path }}'

WRAPPER_PATH = '{{ wrapper_path }}'

previous_task = None
last_task = None

{% for task in tasks %}
task_{{ loop.index }} = DataprocSubmitJobOperator(
    task_id='submit_job_{{ loop.index }}_{{ task.safe_name }}',
    project_id='{{ project_id }}',
    region='{{ region }}',
    job={
        'reference': {'project_id': '{{ project_id }}'},
        'placement': {'cluster_name': '{{ cluster_name }}'},
        'labels': {'client': 'scheduler-jupyter-plugin-chained'},
        'pyspark_job': {
            'main_python_file_uri': WRAPPER_PATH,
            'args': [
                '{{ task.input_path }}',
                '{{ task.output_path }}'{% if parameters %},
                '--parameters',
                {{ parameters_repr }}{% endif %}
            ]
        },
    },
    gcp_conn_id='google_cloud_default',
    {% if impersonate_service_account %}
    impersonation_chain=[IMPERSONATION_ACCOUNT],
    {% endif %}
    dag=dag,
)

if previous_task:
    previous_task >> task_{{ loop.index }}

previous_task = task_{{ loop.index }}
last_task = task_{{ loop.index }}
{% endfor %}

# Merge Task
input_notebooks = [
    {% for task in tasks %}
    '{{ task.output_path }}',
    {% endfor %}
]

merge_task = PythonOperator(
    task_id='merge_notebooks',
    python_callable=merge_notebooks,
    op_kwargs={
        'input_paths': input_notebooks,
        'output_path': '{{ merged_output_base }}' + '{{ '{{' }} run_id {{ '}}' }}' + '.ipynb'
    },
    dag=dag
)

if last_task:
    last_task >> merge_task
"""

def get_cluster_impersonation_account(project, region, cluster_name):
    try:
        cmd = [
            "gcloud", "dataproc", "clusters", "describe", cluster_name,
            "--region", region,
            "--project", project,
            "--format", "json"
        ]
        result = run_command(cmd)
        cluster_data = json.loads(result)
        
        config = cluster_data.get("config", {})
        multi_tenant = config.get("softwareConfig", {}).get("properties", {}).get("dataproc:dataproc.dynamic.multi.tenancy.enabled", "false")
        
        if multi_tenant == "true":
            user_email = get_current_user_email()
            if not user_email: return None
            
            mapping = config.get("securityConfig", {}).get("identityConfig", {}).get("userServiceAccountMapping", {})
            return mapping.get(user_email)
            
        return None
    except Exception as e:
        logger.warning(f"Failed to check cluster impersonation mapping: {e}")
        return None

def ensure_wrapper_in_gcs(bucket, local_wrapper_path=None):
    target_path = "dataproc-notebooks/wrapper_papermill.py"
    target_uri = f"gs://{bucket}/{target_path}"
    
    logger.info(f"Checking if wrapper exists at {target_uri}...")
    try:
        run_command(["gcloud", "storage", "ls", target_uri], check=True, capture_output=True)
        logger.info("Wrapper script found in GCS.")
        return target_uri
    except subprocess.CalledProcessError:
        logger.warning("Wrapper script NOT found in GCS.")
        # If local_wrapper_path provided, could try upload, but usually user lacks permissions.
        # Check fallback
        if local_wrapper_path and os.path.exists(local_wrapper_path):
             logger.info(f"Attempting to upload wrapper from {local_wrapper_path}...")
             try:
                 upload_to_gcs(bucket, local_wrapper_path, target_path)
                 return target_uri
             except Exception:
                 pass
                 
        logger.warning(f"Could not verify or upload wrapper. Continuing with path {target_uri} hoping it exists or will exist.")
        return target_uri

def cmd_create(args):
    """Handles the 'create' subcommand."""
    logger.info("Starting DAG Creation...")
    
    # 1. Resolve Configs
    bucket_name = get_composer_bucket(args.project, args.region, args.composer_env)
    impersonation_account = get_cluster_impersonation_account(args.project, args.region, args.cluster)
    
    # 2. Determine Identity
    derived_identity = impersonation_account or get_current_user_email()
    if not derived_identity:
         logger.error("Could not determine Identity! Provide mapped cluster or authenticate with gcloud.")
         sys.exit(1)
         
    identity_hash = get_identity_hash(derived_identity)
    owner = derived_identity
    prefix = identity_hash
    
    # 3. Setup IDs
    run_id = args.name if args.name else uuid.uuid4().hex[:8]
    dag_id = f"{prefix}_{run_id}"
    logger.info(f"Preparing DAG: {dag_id} (Prefix: {prefix})")
    
    # 4. Resolve Wrapper
    # Try to find local wrapper if not specified
    local_wrapper = args.local_wrapper_path
    if not local_wrapper:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        local_wrapper = os.path.join(script_dir, "wrapper_papermill.py")
        
    wrapper_gcs_path = ensure_wrapper_in_gcs(bucket_name, local_wrapper)
    
    # 5. Process Notebooks
    tasks = []
    for i, notebook_path in enumerate(args.notebooks):
        if not os.path.exists(notebook_path):
            logger.error(f"Notebook not found: {notebook_path}")
            sys.exit(1)
            
        filename = os.path.basename(notebook_path)
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', os.path.splitext(filename)[0])
        
        # Upload
        input_gcs = upload_to_gcs(
            bucket_name,
            notebook_path,
            f"dataproc-notebooks/{dag_id}/input/{i}_{filename}"
        )
        
        output_gcs = f"gs://{bucket_name}/dataproc-output/{dag_id}/output/{i}_{filename}"
        
        tasks.append({
            'safe_name': safe_name,
            'input_path': input_gcs,
            'output_path': output_gcs
        })
        
    # 6. Generate DAG
    template = Template(DAG_TEMPLATE)
    dag_content = template.render(
        dag_id=dag_id,
        owner=owner,
        start_date="datetime.now() - timedelta(days=1)",
        schedule_interval=args.schedule_value,
        notebook_names=",".join([t['safe_name'] for t in tasks]),
        project_id=args.project,
        region=args.region,
        cluster_name=args.cluster,
        wrapper_path=wrapper_gcs_path,
        tasks=tasks,
        impersonate_service_account=True,
        merged_output_base=f"gs://{bucket_name}/dataproc-output/{dag_id}/output-notebooks/{dag_id}_",
        parameters=args.parameters,
        parameters_repr=repr(args.parameters) if args.parameters else "''",
    )
    
    dag_filename = f"{dag_id}.py"
    with open(dag_filename, "w") as f:
        f.write(dag_content)
        
    # 7. Upload DAG
    upload_to_gcs(bucket_name, dag_filename, f"dags/dag_{dag_filename}")
    os.remove(dag_filename)
    logger.info(f"DAG uploaded successfully. Check Composer UI for {dag_id}")
    
    # 8. Submit Access Request
    cmd_permissions(
        argparse.Namespace(
            action='add',
            project=args.project,
            region=args.region,
            composer_env=args.composer_env,
            dag_id=dag_id,
            identity=derived_identity,
            identity_type='serviceAccount' if derived_identity.endswith(".gserviceaccount.com") else 'user',
            access_level='edit',
            requester=derived_identity
        )
    )

# --- Command: Update Schedule ---

def cmd_update_schedule(args):
    """Handles the 'update-schedule' subcommand."""
    bucket = get_composer_bucket(args.project, args.region, args.composer_env)
    
    gcs_path = f"gs://{bucket}/dags/dag_{args.dag_id}.py"
    local_path = f"dag_{args.dag_id}.py"
    
    logger.info(f"Downloading {gcs_path}...")
    try:
        run_command(["gcloud", "storage", "cp", gcs_path, local_path], check=True)
    except Exception:
        logger.error("Failed to download DAG. Check DAG ID.")
        sys.exit(1)
        
    try:
        with open(local_path, 'r') as f: content = f.read()
        
        pattern = r"(schedule_interval\s*=\s*)(['\"])(.*?)(['\"])"
        if not re.search(pattern, content):
            logger.error("Could not find 'schedule_interval' in DAG file.")
            sys.exit(1)
            
        new_content = re.sub(pattern, f"\\1'{args.new_schedule}'", content, count=1)
        
        with open(local_path, 'w') as f: f.write(new_content)
        
        logger.info(f"Uploading updated DAG to {gcs_path}...")
        run_command(["gcloud", "storage", "cp", local_path, gcs_path], check=True)
        logger.info("Schedule updated successfully.")
        
    except Exception as e:
        logger.error(f"Error updating schedule: {e}")
        sys.exit(1)
    finally:
        if os.path.exists(local_path): os.remove(local_path)

# --- Command: Permissions ---

def cmd_permissions(args):
    """Handles the 'permissions' subcommand."""
    bucket = get_composer_bucket(args.project, args.region, args.composer_env)
    
    req_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()
    
    requester = args.requester or get_current_user_email() or "unknown"
    requester_hash = get_identity_hash(requester)
    
    filename = f"{requester_hash}_req_{req_id}.json"
    
    payload = {
        "request_id": req_id,
        "timestamp": timestamp,
        "action": args.action,
        "dag_id": args.dag_id,
        "target_identity": args.identity,
        "requester_identity": requester,
        "identity_type": args.identity_type,
        "access_level": args.access_level,
        "project": args.project,
        "bucket": bucket
    }
    
    local_file = f"/tmp/{filename}"
    with open(local_file, 'w') as f: json.dump(payload, f, indent=2)
    
    remote_path = f"gs://{bucket}/permission_requests/pending/{filename}"
    try:
        logger.info(f"Submitting Permission Request to: {remote_path}")
        run_command(["gcloud", "storage", "cp", local_file, remote_path], check=True)
        logger.info("Request submitted successfully.")
    except Exception as e:
        logger.error(f"Failed to submit request: {e}")
    finally:
        if os.path.exists(local_file): os.remove(local_file)

# --- Main Entry Point ---

def main():
    parser = argparse.ArgumentParser(description="Unified DAG Manager for Composer")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Common Args
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--project", required=True, help="GCP Project ID")
    common.add_argument("--region", default="us-central1", help="GCP Region")
    common.add_argument("--composer-env", required=True, help="Composer Environment Name")
    
    # Create
    p_create = subparsers.add_parser("create", parents=[common], help="Create new DAG from notebooks")
    p_create.add_argument("--notebooks", required=True, nargs='+', help="Input notebook paths")
    p_create.add_argument("--cluster", required=True, help="Dataproc Cluster Name")
    p_create.add_argument("--name", help="Custom DAG suffix")
    p_create.add_argument("--schedule-value", default="@once", help="Schedule interval")
    p_create.add_argument("--parameters", help="JSON parameters")
    p_create.add_argument("--local-wrapper-path", help="Path to wrapper_papermill.py")
    p_create.set_defaults(func=cmd_create)
    
    # Update Schedule
    p_update = subparsers.add_parser("update-schedule", parents=[common], help="Update DAG schedule")
    p_update.add_argument("--dag-id", required=True, help="DAG ID")
    p_update.add_argument("--new-schedule", required=True, help="New schedule interval")
    p_update.set_defaults(func=cmd_update_schedule)
    
    # Permissions
    p_perm = subparsers.add_parser("permissions", parents=[common], help="Manage DAG permissions")
    p_perm.add_argument("--action", required=True, choices=['add', 'remove'])
    p_perm.add_argument("--dag-id", required=True, help="DAG ID")
    p_perm.add_argument("--identity", required=True, help="Target User/Group email")
    p_perm.add_argument("--identity-type", choices=['user', 'group', 'serviceAccount'], default='user')
    p_perm.add_argument("--access-level", choices=['read', 'edit'], default='read')
    p_perm.add_argument("--requester", help="Requester identity (optional)")
    p_perm.set_defaults(func=cmd_permissions)
    
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
