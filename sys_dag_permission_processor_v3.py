
import json
import logging
import os
import subprocess
import hashlib
import time
from datetime import datetime, timedelta
# REFRESH_TRIGGER_V3_FORCE_UPDATE
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.www.app import cached_app
from airflow import settings
from google.cloud import storage
import re
import ast

# Configuration
DAG_ID = "sys_dag_permission_processor_v3"
# BUCKET_NAME = "us-central1-my-airflow-2601-2f14fcdc-bucket"
# BUCKET_NAME - Enforce Airflow Variable
try:
    BUCKET_NAME = Variable.get("composer_bucket")
except KeyError:
    raise ValueError("Critical Error: 'composer_bucket' Airflow Variable is not set. This DAG requires the bucket name to be configured via the Provisioning Script or manually.")
REQUEST_PREFIX = "permission_requests/"
PENDING_PREFIX = "permission_requests/pending/"
PROCESSED_PREFIX = "permission_requests/processed/"
FAILED_PREFIX = "permission_requests/failed/"

debug_log = '/home/airflow/gcs/data/sys_dag_debug_v2.log'

def log_debug(msg):
    try:
        now = datetime.now()
        with open(debug_log, 'a') as f:
            f.write(f"{now}: {msg}\n")
            f.flush()
            os.fsync(f.fileno())
    except:
        pass

def get_identity_hash(email):
    return hashlib.md5(email.lower().strip().encode()).hexdigest()[:10]

def run_command(cmd, check=True):
    """Executes a shell command (used for GCS CLI)."""
    log_debug(f"Running: {cmd}")
    try:
        result = subprocess.run(
            cmd, shell=True, check=check, capture_output=True, text=True, timeout=60
        )
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        log_debug(f"Command timed out: {cmd}")
        if check: raise
        return ""
    except subprocess.CalledProcessError as e:
        log_debug(f"Command failed: {e.stderr}")
        if check: raise
        return ""

def patch_dag_access_control(bucket_obj, target_dag_id, role_key, action_type, access_level='read'):
    """
    Patches the DAG python file in GCS to update access_control dict.
    """
    blob_path = f"dags/dag_{target_dag_id}.py"
    blob = bucket_obj.blob(blob_path)
    
    if not blob.exists():
        log_debug(f"DAG file not found at {blob_path}, skipping patch.")
        return

    content = blob.download_as_text()
    
    start_anchor = "access_control="
    start_idx = content.find(start_anchor)
    if start_idx == -1:
        log_debug(f"No access_control found in {blob_path}. Skipping patch.")
        return

    # Find the opening brace
    brace_open_idx = content.find('{', start_idx)
    if brace_open_idx == -1:
         log_debug("No opening brace for access_control.")
         return

    # Count braces to find end
    balance = 1
    i = brace_open_idx + 1
    while i < len(content) and balance > 0:
        char = content[i]
        if char == '{':
            balance += 1
        elif char == '}':
            balance -= 1
        i += 1
    
    if balance != 0:
        log_debug("Could not find balanced closing brace.")
        return
    
    brace_close_idx = i - 1
    
    prefix = content[start_idx:brace_open_idx+1] # access_control={
    inner = content[brace_open_idx+1:brace_close_idx] # content inside
    
    # Auto-Repair: Fix missing commas between entries (e.g. } 'role_)
    # This handles legacy corruption where a role was added without a preceding comma
    repaired_inner = re.sub(r"(\})\s*(['\"]role_)", r"\1, \2", inner)
    if repaired_inner != inner:
        log_debug(f"Auto-Repaired missing comma in {blob_path}")
        inner = repaired_inner
    
    
    # Check if role is present
    role_entry_pattern = f"'{role_key}':"
    has_role = role_entry_pattern in inner or f'"{role_key}":' in inner
    
    new_inner = inner
    if action_type == 'add':
        perms_str = "{'can_read'}"
        if access_level == 'edit':
            perms_str = "{'can_read', 'can_edit'}"

        if has_role:
            # Update existing entry
            update_pattern = f"(['\"]{role_key}['\"]\s*:\s*\{{.*?\}})"
            new_entry = f"'{role_key}': {perms_str}"
            # Use regex substitution with DOTALL to catch newlines
            new_inner = re.sub(update_pattern, new_entry, inner, count=1, flags=re.DOTALL)
            log_debug(f"Updated existing role {role_key} to {perms_str}")
        else:
            entry = f" '{role_key}': {perms_str},"
            if '\n' in inner:
                stripped_inner = inner.rstrip()
                if not stripped_inner.strip().endswith(','):
                     stripped_inner += ','
                new_inner = stripped_inner + f"\n       {entry}\n    "
            else:
                if inner.strip() and not inner.strip().endswith(','):
                    inner += ","
                new_inner = inner + entry
                
    elif action_type == 'remove':
        if has_role:
             remove_pattern = f"['\"]{role_key}['\"]\s*:\s*\{{.*?\}}s*,?"
             new_inner = re.sub(remove_pattern, "", inner, flags=re.DOTALL)

    if new_inner != inner:
        new_content = content[:brace_open_idx+1] + new_inner + content[brace_close_idx:]
        blob.upload_from_string(new_content)
        log_debug(f"Patched [v2-brace] {blob_path}: {action_type} {role_key}")
    else:
        log_debug(f"No patch needed [v2-brace] for {blob_path} ({action_type})")

def process_requests(**context):
    log_debug("STARTING process_requests (Session Merge + Commit)")
    
    # 1. Setup Airflow Security Manager
    try:
        log_debug("Calling cached_app()")
        app = cached_app()
        log_debug("cached_app() done. Getting sm")
        sm = app.appbuilder.sm
        log_debug(f"SecurityManager loaded: {sm}. Creating Session")
        session = settings.Session()
        log_debug("Session created.")
    except Exception as e:
        log_debug(f"Failed to load SecurityManager: {e}")
        import traceback
        log_debug(traceback.format_exc())
        raise

    # Helper: Find/Create Permission
    def find_perm(action_name, resource_name):
        # Try different methods for compatibility
        if hasattr(sm, 'get_permission'):
            return sm.get_permission(action_name, resource_name)
        if hasattr(sm, 'find_permission_view_menu'):
            return sm.find_permission_view_menu(action_name, resource_name)
        if hasattr(sm, 'find_permission'):
             return sm.find_permission(action_name, resource_name)
        log_debug("ERROR: No suitable method to find permission!")
        return None

    def add_perm_resource(action_name, resource_name):
        if hasattr(sm, 'add_permission_view_menu'):
             sm.add_permission_view_menu(action_name, resource_name)
        elif hasattr(sm, 'create_permission'):
             sm.create_permission(action_name, resource_name)

    # Helper: Patch DAG File (Uses module level function)
    # patch_dag_access_control is defined at module level


    # 2. Setup GCS Client
    conf = context['dag_run'].conf or {}
    bucket_name = conf.get('bucket') or BUCKET_NAME
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    start_time = time.time()
    duration_limit = 110 

    while time.time() - start_time < duration_limit:
        try:
            blobs = list(bucket.list_blobs(prefix=PENDING_PREFIX))
            found_any = False
            
            for blob in blobs:
                if blob.name.endswith('/'): continue
                
                found_any = True
                blob_name = blob.name
                log_debug(f"Found request: {blob_name}")
                
                try:
                    content = blob.download_as_text()
                    req = json.loads(content)
                    
                    action = req.get('action')
                    identity = req.get('target_identity')
                    dag_id = req.get('dag_id')
                    id_type = req.get('identity_type')
                    access_level = req.get('access_level', 'read') # Default to read
                    
                    if not (action and identity and dag_id):
                            raise ValueError("Missing fields")

                    identity_hash = get_identity_hash(identity)
                    dag_hash = get_identity_hash(dag_id)
                    
                    # 3. Apply GCS IAM
                    title = f"share-{dag_hash}-{identity_hash}"
                    description = f"Shared access to DAG {dag_hash} for {identity_hash}"
                    
                    # Access Level Logic (GCS)
                    role = "roles/storage.objectViewer" # Default Read
                    if access_level == 'edit':
                        role = "roles/storage.objectAdmin"

                    if id_type == 'group':
                        member = f"group:{identity}"
                    elif id_type == 'serviceAccount':
                        member = f"serviceAccount:{identity}"
                    else:
                        member = f"user:{identity}"
                    
                    if identity.startswith("group:") or identity.startswith("user:") or identity.startswith("serviceAccount:"):
                        member = identity

                    prefixes = [
                        f"dags/dag_{dag_id}.py",
                        f"dataproc-output/{dag_id}/",
                        f"dataproc-notebooks/{dag_id}/"
                    ]
                    conditions = [f"resource.name == 'projects/_/buckets/{bucket_name}/objects/{p}'" if not p.endswith('/') else f"resource.name.startsWith('projects/_/buckets/{bucket_name}/objects/{p}')" for p in prefixes]
                    expression = " || ".join(conditions)
                    
                    cmd = f"gcloud storage buckets {action}-iam-policy-binding gs://{bucket_name} --member='{member}' --role='{role}' \"--condition=expression={expression},title={title},description={description}\""
                    run_command(cmd)
                    
                    log_debug("GCS Command Finished.")

                    # 4. Apply Airflow RBAC
                    role_name = f"role_{identity_hash}"
                    resource_name = f"DAG:{dag_id}"
                    
                    log_debug(f"Applying RBAC: {action} {resource_name} to {role_name}")
                    
                    try:
                        if action == 'add':
                            target_role = sm.find_role(role_name)
                            if not target_role:
                                log_debug(f"Creating Role {role_name}")
                                sm.add_role(role_name)
                                target_role = sm.find_role(role_name)
                            
                            target_role = session.merge(target_role)
                            
                            # Access Level Logic (RBAC)
                            allowed_ops = ['can_read']
                            if access_level == 'edit':
                                allowed_ops.append('can_edit')

                            # 1. Base Viewer Permissions
                            viewer = sm.find_role('Viewer')
                            if viewer:
                                for p in viewer.permissions:
                                    if p not in target_role.permissions:
                                        target_role.permissions.append(p)
                            
                            # 1.5 Revoke Global Access (Isolation)
                            # Remove global DAG read/edit/delete if present (inherited from Viewer)
                            revoke = [('can_read', 'DAGs'), ('can_edit', 'DAGs'), ('can_delete', 'DAGs')]
                            for p in list(target_role.permissions):
                                if (p.action.name, p.resource.name) in revoke:
                                    target_role.permissions.remove(p)
                                    log_debug(f"Revoked global {p.action.name} on {p.resource.name}")

                            # 2. Extra Permissions (Trigger/Retry/Clear)
                            extra_perms = [
                                ('can_create', 'DAG Runs'),
                                ('can_read', 'DAG Runs'),
                                ('can_edit', 'DAG Runs'),
                                ('can_delete', 'DAG Runs'), 
                                ('can_read', 'Task Instances'),
                                ('can_edit', 'Task Instances'),
                                ('can_read', 'ImportErrors'),
                                ('menu_access', 'DAGs')
                            ]
                            for action_n, resource_n in extra_perms:
                                perm = find_perm(action_n, resource_n)
                                if perm and perm not in target_role.permissions:
                                    target_role.permissions.append(perm)

                            # 3. DAG Specific Permissions

                            for act_name in allowed_ops:
                                perm = find_perm(act_name, resource_name)
                                if not perm:
                                    add_perm_resource(act_name, resource_name)
                                    perm = find_perm(act_name, resource_name)
                                
                                if perm and perm not in target_role.permissions:
                                    # sm.add_permission_to_role might use different session. 
                                    # We do it manually if possible or use SM but commit OUR session.
                                    # sm.add_permission_to_role(target_role, perm) usually does: role.permissions.append(perm)
                                    target_role.permissions.append(perm)
                                    log_debug(f"Appended {act_name} on {resource_name}")
                            
                            session.commit()
                            log_debug("Session committed after add.")
                                    
                        elif action == 'remove':
                             target_role = sm.find_role(role_name)
                             if target_role:
                                 target_role = session.merge(target_role)
                                 for act_name in ['can_read', 'can_edit']:
                                     perm = find_perm(act_name, resource_name)
                                     if perm and perm in target_role.permissions:
                                         target_role.permissions.remove(perm)
                                         log_debug(f"Removed {act_name} on {resource_name}")
                                 
                                 session.commit()
                                 log_debug("Session committed after remove.")
                    except Exception as rbac_e:
                        log_debug(f"RBAC Error: {rbac_e}")
                        session.rollback()
                        raise

                    # 5. Patch DAG File (GCS) - DISABLED to match plugin behavior
                    # try:
                    #     patch_dag_access_control(bucket, dag_id, role_name, action, access_level)
                    # except Exception as patch_e:
                    #     log_debug(f"DAG Patch Warning: {patch_e}")

                    # Move to Processed
                    new_name = blob_name.replace(PENDING_PREFIX, PROCESSED_PREFIX)
                    bucket.rename_blob(blob, new_name)
                    log_debug(f"Moved to {new_name}")



                except Exception as e:
                    log_debug(f"Failed to process {blob_name}: {e}")
                    try:
                        bucket.rename_blob(blob, blob_name.replace(PENDING_PREFIX, FAILED_PREFIX))
                    except: pass
            
            if not found_any:
                log_debug("No pending requests found.")

        except Exception as loop_error:
            log_debug(f"Loop error: {loop_error}")
        
        # Sleep
        elapsed = time.time() - start_time
        if elapsed < duration_limit:
            time.sleep(10)
    
    session.close()

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(DAG_ID, default_args=default_args, schedule_interval="*/2 * * * *", catchup=False, max_active_runs=1) as dag:
    processor = PythonOperator(
        task_id='process_permission_requests',
        python_callable=process_requests,
        provide_context=True
    )
