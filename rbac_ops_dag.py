from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow import settings
from airflow.models import DagModel
from airflow.www.app import cached_app
from airflow.utils.trigger_rule import TriggerRule
import logging
import json
import os
import traceback

# --- Shared Helpers ---
def get_session():
    return settings.Session()

def get_sm():
    app = cached_app()
    return app.appbuilder.sm

def log_to_file(filename, msg):
    print(msg)
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'a') as f:
            f.write(msg + "\n")
    except Exception:
        pass

# --- Logic: Sync (Provisioning) ---
def sync_rbac_logic(**context):
    conf = context['dag_run'].conf or {}
    log_file = '/home/airflow/gcs/data/rbac_ops_log.txt'
    
    def log(m): log_to_file(log_file, m)
    
    log("\n=== Starting RBAC Sync ===")
    
    # 1. Read Manifest
    local_manifest_path = '/home/airflow/gcs/data/rbac_manifest.json'
    try:
        with open(local_manifest_path, 'r') as f:
            manifest = json.load(f)
    except Exception as e:
        log(f"CRITICAL: Failed to read manifest: {e}")
        return

    sm = get_sm()
    session = get_session()
    roles_to_configure = set()

    # 2. Sync Groups
    groups = manifest.get('groups', {})
    for group_email, val in groups.items():
        role_name = val.get('role_name', group_email.split('@')[0])
        members = val.get('members', [])
        
        roles_to_configure.add(role_name)
        log(f"Group: {group_email} -> Role: {role_name}")
        
        if not sm.find_role(role_name):
            sm.add_role(role_name)
            log(f"  + Created Role: {role_name}")
            
        target_role = sm.find_role(role_name)
        
        for m in members:
            username = m.get('username')
            email = m.get('email')
            
            user = sm.find_user(username=username)
            if user and any(r.name == 'Admin' for r in user.roles):
                continue # Skip Admin
                
            if not user:
                log(f"  + Creating User: {username}")
                user = sm.add_user(username=username, first_name="Synced", last_name="User", email=email, role=target_role)
            else:
                if target_role not in user.roles:
                    user.roles.append(target_role)
                    session.commit()
                    log(f"  + Added role to {username}")

    # 3. Sync SAs
    for sa in manifest.get('service_accounts_detailed', []):
        role_name = sa.get('role_name')
        username = sa.get('username')
        email = sa.get('email')
        
        if not role_name or not username: continue
        
        roles_to_configure.add(role_name)
        log(f"SA: {email} -> Role: {role_name}")
        
        if not sm.find_role(role_name):
            sm.add_role(role_name)
        
        target_role = sm.find_role(role_name)
        user = sm.find_user(username=username)
        
        if not user:
            sm.add_user(username=username, first_name="Auto", last_name="SA", email=email, role=target_role)
        else:
            if target_role not in user.roles:
                user.roles.append(target_role)
                session.commit()
            
            # Remove Op
            op = sm.find_role('Op')
            if op and op in user.roles:
                user.roles.remove(op)
                session.commit()

    # 4. Configure Permissions
    verify_permissions(sm, session, roles_to_configure, log)
    log("=== Sync Complete ===")

def verify_permissions(sm, session, roles, log):
    """Applies Standard Policy to roles."""
    viewer = sm.find_role('Viewer')
    extra = [
        ('can_create', 'DAG Runs'),
        ('can_read', 'DAG Runs'),
        ('can_edit', 'DAG Runs'),
        ('can_delete', 'DAG Runs'), 
        ('can_read', 'Task Instances'),
        ('can_edit', 'Task Instances'),
        ('can_read', 'ImportErrors'),
        ('menu_access', 'DAGs')
    ]
    revoke = [('can_read', 'DAGs'), ('can_edit', 'DAGs'), ('can_delete', 'DAGs')]
    
    for role_name in roles:
        role = sm.find_role(role_name)
        if not role: continue
        
        # Base Viewer
        for p in viewer.permissions:
            if p not in role.permissions:
                sm.add_permission_to_role(role, p)
        
        # Grant Extra (DAG Runs - Trigger/Clear)
        def find_perm(action, resource):
            """
            Compatibility helper for Airflow RBAC.
            - get_permission: Used in newer Airflow versions (2.10+ / FAB 4.x).
            - find_permission_view_menu: Deprecated/Legacy method for older Airflow versions.
            """
            if hasattr(sm, 'get_permission'):
                return sm.get_permission(action, resource)
            if hasattr(sm, 'find_permission_view_menu'):
                return sm.find_permission_view_menu(action, resource)
            return None

        for action, resource in extra:
            perm = find_perm(action, resource)
            if perm and perm not in role.permissions:
                sm.add_permission_to_role(role, perm)
        
        # Revoke Global
        for p in list(role.permissions):
            if (p.action.name, p.resource.name) in revoke:
                sm.remove_permission_from_role(role, p)
        session.commit()
        
        # Prefix Access
        prefix = role_name.replace('role_', '') + '_'
        
        # RETRY LOGIC for Hash-based Roles
        # If looks like a hash role AND no dags found, wait.
        # Heuristic: Hash roles are exactly len 15 (role_ + 10 chars)
        max_retries = 6 
        db_dags = []
        for attempt in range(max_retries):
            db_dags = [d.dag_id for d in session.query(DagModel).all() if d.dag_id.startswith(prefix)]
            
            if db_dags:
                break
            
            if len(role_name) == 15 and role_name.startswith('role_'):
                if attempt < max_retries - 1:
                    log(f"  > [Attempt {attempt+1}/{max_retries}] No DAGs found for {prefix}... waiting for parsing.")
                    time.sleep(10)
                else:
                    log(f"  > WARNING: Timed out waiting for DAGs with prefix {prefix}.")
            else:
                break

        if db_dags:
            log(f"  > Granting access to {len(db_dags)} DAGs for {role_name}...")
            for dag_id in db_dags:
                res = f"DAG:{dag_id}"
                for act in ['can_read', 'can_edit']:
                     # standard FAB lookup
                    perm = find_perm(act, res)
                    if not perm:
                        sm.add_permission_view_menu(act, res)
                        perm = find_perm(act, res)
                    if perm and perm not in role.permissions:
                        sm.add_permission_to_role(role, perm)


# --- Logic: Cleanup (Deprovisioning) ---
def cleanup_rbac_logic(**context):
    log_file = '/home/airflow/gcs/data/rbac_ops_log.txt'
    def log(m): log_to_file(log_file, m)
    
    log("\n=== Starting RBAC Cleanup ===")
    
    local_manifest_path = '/home/airflow/gcs/data/cleanup_manifest.json'
    try:
        with open(local_manifest_path, 'r') as f:
            manifest = json.load(f)
    except Exception as e:
        log(f"CRITICAL: Failed to read cleanup manifest: {e}")
        return

    sm = get_sm()
    session = get_session()

    # Users
    for u in manifest.get('users_to_delete', []):
        username = u.get('username')
        email = u.get('email')
        user = sm.find_user(username=username) or sm.find_user(email=email)
        if user:
            log(f"  > Deleting User: {user.username}")
            try:
                if hasattr(sm, 'del_user'): sm.del_user(user.id)
                else: 
                    session.delete(user)
                    session.commit()
            except Exception as e:
                log(f"    ! Error: {e}")

    # Roles
    for r_name in manifest.get('roles_to_delete', []):
        role = sm.find_role(r_name)
        if role:
            log(f"  > Deleting Role: {r_name}")
            try:
                session.delete(role)
                session.commit()
            except Exception as e:
                log(f"    ! Error: {e}")
    
    log("=== Cleanup Complete ===")


# --- DAG Definition ---
def decide_action(**context):
    conf = context['dag_run'].conf or {}
    action = conf.get('action', 'sync') # Default to sync
    if action == 'cleanup':
        return 'do_cleanup'
    return 'do_sync'

with DAG(
    'rbac_ops_dag',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['security', 'ops'],
) as dag:
    
    branch = BranchPythonOperator(
        task_id='decide_action',
        python_callable=decide_action,
        provide_context=True,
    )

    sync_task = PythonOperator(
        task_id='do_sync',
        python_callable=sync_rbac_logic,
        provide_context=True,
    )

    cleanup_task = PythonOperator(
        task_id='do_cleanup',
        python_callable=cleanup_rbac_logic,
        provide_context=True,
    )

    branch >> [sync_task, cleanup_task]
