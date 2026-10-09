
import os
import argparse
import subprocess
import time
import json
import logging
import sys
import hashlib
import re

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Utilities ---
def run_command(cmd, check=True, capture_output=True):
    logger.debug(f"Running: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=check, capture_output=capture_output, text=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {cmd}")
        logger.error(f"Error output: {e.stderr}")
        if check: raise
        return ""

def get_identity_hash(email):
    return hashlib.md5(email.lower().strip().encode()).hexdigest()[:10]

# --- Main Manager Class ---
class ComposerAccessManager:
    def __init__(self, config):
        self.config = config
        self.project = config['project']
        self.region = config['region']
        self.env_name = config['composer_env']
        self.dry_run = config.get('dry_run', False)
        self.bucket = self._detect_bucket()
        self.group_cache = {}

    def _detect_bucket(self):
        logger.info(f"Detecting Bucket for {self.env_name}...")
        res = run_command(f"gcloud composer environments describe {self.env_name} --location {self.region} --project {self.project} --format='value(config.dagGcsPrefix)'")
        if res.startswith("gs://"):
            return res.replace("gs://", "").split("/")[0]
        logger.error("Failed to detect bucket.")
        sys.exit(1)

    def _get_full_email(self, identity):
        """
        Validate and return the full email address.
        """
        if "@" not in identity:
            raise ValueError(f"Invalid Identity: '{identity}'. You must provide the FULL email address (e.g., service-account@project.iam.gserviceaccount.com).")
        return identity.strip()

    def fetch_group_members(self, group_email):
        if group_email in self.group_cache: return self.group_cache[group_email]
        logger.info(f"Fetching members for {group_email}...")
        try:
            res = run_command(f"gcloud identity groups memberships list --group-email='{group_email}' --format='json'")
            members = []
            for m in json.loads(res):
                email = m.get('preferredMemberKey', {}).get('id', '')
                if not email or 'admin@' in email: continue
                unique_id = m.get('name', '').split('/')[-1]
                members.append({"email": email, "username": f"accounts.google.com:{unique_id}"})
            self.group_cache[group_email] = members
            return members
        except Exception as e:
            logger.warning(f"Error fetching group {group_email}: {e}")
            return []

    # --- Core Actions ---
    def provision(self):
        logger.info(">>> STARTING PROVISIONING <<<")
        self._setup_infra()
        self._set_airflow_variables()
        self._deploy_dags()
        self._trigger_rbac_ops(action='sync')
        logger.info(">>> PROVISIONING COMPLETE <<<")
    
    def _set_airflow_variables(self):
        logger.info("Setting Airflow Variables...")
        
        # 1. Bucket Variable
        run_command(f"gcloud composer environments run {self.env_name} --location {self.region} --project {self.project} variables set -- composer_bucket {self.bucket}", check=False)

        # 2. RBAC Authority Mapping (Hash -> Email)
        logger.info("  Calculcating RBAC Authority Mapping...")
        mapping = {}
        for sa in self.config['service_accounts']:
            email = self._get_full_email(sa)
            h = get_identity_hash(email)
            mapping[h] = email
        
        mapping_json = json.dumps(mapping)
        logger.info(f"  Setting RBAC_AUTHORITY_MAPPING: {mapping_json}")
        # Use single quotes for shell safety, but escape inner json quotes? 
        # Actually gcloud variables set takes the value as a separate arg.
        # We need to be careful with shell escaping.
        # Safest is to use temporary file or careful escaping. 
        # Let's try direct set first, usually handles JSON if quoted properly.
        # Wait, 'variables set' key value.
        
        # We'll write to a temp file to avoid shell escaping hell with JSON
        try:
             with open("temp_vars.json", "w") as f:
                 f.write(mapping_json)
             
             # Use 'variables import' with a file? No, 'variables set' takes KEY VALUE.
             # We will use subprocess with list args to avoid shell injection
             cmd = [
                 "gcloud", "composer", "environments", "run", self.env_name,
                 "--location", self.region,
                 "--project", self.project,
                 "variables", "set", "--", "RBAC_AUTHORITY_MAPPING", mapping_json
             ]
             # subprocess.run with list args handles escaping better than shell=True
             subprocess.run(cmd, check=True, text=True, capture_output=True)
             logger.info("  > SUCCESS: RBAC_AUTHORITY_MAPPING updated.")
        except Exception as e:
            logger.error(f"  > FAILED to set RBAC mapping: {e}")


    def deprovision(self):
        logger.info(">>> STARTING DEPROVISIONING <<<")
        # 1. Cleanup Airflow first
        # self._trigger_rbac_ops(action='cleanup') # Deprecated: DAG cleanup is unreliable for Users.
        self._cleanup_airflow_cli()
        
        # 2. Cleanup Infra
        self._cleanup_infra()
        
        # 3. Prune Manifest
        self._prune_manifest()
        logger.info(">>> DEPROVISIONING COMPLETE <<<")

    # --- Infrastructure Logic ---
    def _setup_infra(self):
        logger.info("Configuring Infrastructure IAM & Storage...")
        
        # 1. Project IAM (Discovery)
        role = "ComposerDiscovery"
        try:
            run_command(f"gcloud iam roles describe {role} --project {self.project}", check=True)
        except:
            run_command(f"gcloud iam roles create {role} --project {self.project} --title 'Composer Discovery' --permissions 'composer.environments.list,composer.operations.list' --stage GA")

        # 2. Bucket UBLA & Composer SA Admin
        run_command(f"gcloud storage buckets update gs://{self.bucket} --uniform-bucket-level-access", check=False)
        
        # Grant Storage Admin to Composer Environment SA (Required for sys_dag to manage IAM)
        c_sa_env = run_command(f"gcloud composer environments describe {self.env_name} --location {self.region} --format='value(config.nodeConfig.serviceAccount)'", check=True)
        if c_sa_env:
            logger.info(f"Granting Storage Admin to Composer SA: {c_sa_env}")
            run_command(f"gcloud projects add-iam-policy-binding {self.project} --member='serviceAccount:{c_sa_env}' --role='roles/storage.admin' --condition=None", check=False)

        # 3. Service Accounts & Storage
        for sa in self.config['service_accounts']:
            email = self._get_full_email(sa)
            logger.info(f"Processing SA: {email}")
            
            # Impersonation trust
            c_sa = run_command(f"gcloud composer environments describe {self.env_name} --location {self.region} --format='value(config.nodeConfig.serviceAccount)'")
            run_command(f"gcloud iam service-accounts add-iam-policy-binding {email} --member='serviceAccount:{c_sa}' --role='roles/iam.serviceAccountTokenCreator'", check=False)
            run_command(f"gcloud iam service-accounts add-iam-policy-binding {email} --member='serviceAccount:{c_sa}' --role='roles/iam.serviceAccountUser'", check=False)
            
            # Custom Project Role
            run_command(f"gcloud projects add-iam-policy-binding {self.project} --member='serviceAccount:{email}' --role='projects/{self.project}/roles/{role}' --condition=None", check=False)
            
            # Bucket Reader
            run_command(f"gcloud storage buckets add-iam-policy-binding gs://{self.bucket} --member='serviceAccount:{email}' --role='roles/storage.legacyBucketReader' --condition=None", check=False)
            
            # Object Admin (Conditional)
            h = get_identity_hash(email)
            cond = self._get_user_storage_condition(h)
            # Use check=True to fail on error, and sleep to avoid race conditions
            run_command(f"gcloud storage buckets add-iam-policy-binding gs://{self.bucket} --member='serviceAccount:{email}' --role='roles/storage.objectAdmin' --condition=\"{cond}\"", check=True)
            time.sleep(2) # Avoid GCS 1-update-per-second limit / Race Conditions
    


    def _get_user_storage_condition(self, h):
        prefixes = [
            f"dags/dag_{h}_", 
            f"dataproc-notebooks/{h}_", 
            f"dataproc-output/{h}_", 
            f"permission_requests/pending/{h}_",
            f"permission_requests/processed/{h}_",
            f"permission_requests/failed/{h}_"
        ]
        expr = " || ".join([f"resource.name.startsWith('projects/_/buckets/{self.bucket}/objects/{p}')" for p in prefixes])
        return f"expression={expr},title=share-{h}"



    def _cleanup_infra(self):
        logger.info("Cleaning up Infrastructure...")
        if self.dry_run: return
        
        # Revoke Composer SA Storage Admin
        c_sa_env = run_command(f"gcloud composer environments describe {self.env_name} --location {self.region} --format='value(config.nodeConfig.serviceAccount)'", check=False)
        if c_sa_env:
            logger.info(f"Revoking Storage Admin from Composer SA: {c_sa_env}")
            run_command(f"gcloud projects remove-iam-policy-binding {self.project} --member='serviceAccount:{c_sa_env}' --role='projects/{self.project}/roles/ComposerDiscovery' --condition=None", check=False)
            run_command(f"gcloud projects remove-iam-policy-binding {self.project} --member='serviceAccount:{c_sa_env}' --role='roles/storage.admin' --condition=None", check=False)

        # Revoke User Project IAM & SA Bindings
        roles_to_revoke = [
            f"projects/{self.project}/roles/ComposerDiscovery",
            f"projects/{self.project}/roles/CustomComposerUser",
            "roles/composer.user",
            "roles/composer.worker", # unlikely but good to clean
            f"projects/{self.project}/roles/ComposerIAP" # If custom role for IAP exists, or standard role check?
            # Standard IAP role is usually roles/iap.httpsResourceAccessor but might be custom here
        ]
        # Check standard IAP role too?
        # Actually, let's just revoke what we suspect.
        # User output showed: projects/kenly-lakehouse-dev-1/roles/ComposerIAP
        
        roles_to_revoke = [
            f"projects/{self.project}/roles/ComposerDiscovery",
            f"projects/{self.project}/roles/CustomComposerUser",
            f"projects/{self.project}/roles/ComposerIAP",
            "roles/composer.user"
        ]

        for sa in self.config['service_accounts']:
            email = self._get_full_email(sa)
            logger.info(f"Revoking IAM for SA: {email}")
            
            for role_str in roles_to_revoke:
                 # Use --all to remove conditional bindings as well (e.g. CustomComposerUser limit)
                 # This ensures clean state for these specific roles.
                 run_command(f"gcloud projects remove-iam-policy-binding {self.project} --member='serviceAccount:{email}' --role='{role_str}' --all", check=False)
            
            # SA Impersonation (Reverse of provision: SA trusts Composer SA?)
            # Wait, provision was: `add-iam-policy-binding {email} --member='serviceAccount:{c_sa}'`
            # This means "Composer SA" can act as "User SA". 
            # So we remove {c_sa} from {email}'s policy.
            if c_sa_env:
                run_command(f"gcloud iam service-accounts remove-iam-policy-binding {email} --member='serviceAccount:{c_sa_env}' --role='roles/iam.serviceAccountTokenCreator'", check=False)
                run_command(f"gcloud iam service-accounts remove-iam-policy-binding {email} --member='serviceAccount:{c_sa_env}' --role='roles/iam.serviceAccountUser'", check=False)

        # Remove Manifest to prevent resurrection
        logger.info("Removing RBAC Sync Manifest...")
        run_command(f"gcloud storage rm gs://{self.bucket}/data/rbac_manifest.json", check=False)
        
        # Robust IAM Cleanup (Get-Filter-Set)
        self._nuke_bucket_iam()

    def _nuke_bucket_iam(self):
        logger.info("Nuking Service Account bindings from Bucket Policy...")
        json_file = "bucket_policy_temp.json"
        
        # 1. Get Policy
        run_command(f"gcloud storage buckets get-iam-policy gs://{self.bucket} --format=json > {json_file}", check=True)
        
        with open(json_file, 'r') as f:
            policy = json.load(f)
            
        bindings = policy.get('bindings', [])
        new_bindings = []
        
        target_emails = [self._get_full_email(sa) for sa in self.config['service_accounts']]
        
        for b in bindings:
            role = b.get('role')
            members = b.get('members', [])
            condition = b.get('condition')
            
            # Filter members
            new_members = []
            for m in members:
                is_target = False
                if m.startswith("serviceAccount:"):
                    email = m.split(":", 1)[1]
                    if email in target_emails:
                        is_target = True
                
                if not is_target:
                    new_members.append(m)
            
            if new_members:
                # If members remain, keep the binding
                b['members'] = new_members
                new_bindings.append(b)
            # Else: drop the binding completely if no members left
            
        policy['bindings'] = new_bindings
        
        with open(json_file, 'w') as f:
            json.dump(policy, f, indent=2)
            
        # 2. Set Policy
        logger.info("Applying cleaned IAM policy...")
        run_command(f"gcloud storage buckets set-iam-policy gs://{self.bucket} {json_file}", check=True)
        
        if os.path.exists(json_file):
            os.remove(json_file)







    # --- Airflow Logic ---
    def _deploy_dags(self):
        logger.info("Deploying DAGs...")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 1. Ops DAG
        ops_src = os.path.join(script_dir, "rbac_ops_dag.py")
        run_command(f"gcloud storage cp {ops_src} gs://{self.bucket}/dags/rbac_ops_dag.py")
        
        # 2. System DAG (Dynamic)
        # 2. System DAG (Dynamic - Uses Airflow Variable 'composer_bucket')
        sys_src = os.path.join(script_dir, "sys_dag_permission_processor_v3.py")
        if os.path.exists(sys_src):
            run_command(f"gcloud storage cp {sys_src} gs://{self.bucket}/dags/sys_dag_permission_processor_v3.py")
            
        # 3. Diagnostics
        diag_dir = os.path.join(script_dir, "diagnostics")
        if os.path.exists(diag_dir):
            run_command(f"gcloud storage cp {diag_dir}/*.py gs://{self.bucket}/dags/diagnostics/")

    def _trigger_rbac_ops(self, action):
        logger.info(f"Triggering rbac_ops_dag (Action: {action})...")
        
        manifest_uri = None
        if action == 'sync':
            manifest_uri = self._upload_rbac_manifest()
            conf = {"action": "sync", "member_file_uri": manifest_uri}
        else: # cleanup
            manifest_uri = self._upload_cleanup_manifest()
            conf = {"action": "cleanup", "cleanup_file_uri": manifest_uri}
            
        if self.dry_run: return
        
        # Trigger
        conf_str = json.dumps(conf)
        cmd = f"gcloud composer environments run {self.env_name} --location {self.region} --project {self.project} dags trigger -- rbac_ops_dag --conf '{conf_str}'"
        
        # Retry logic for deployment delay
        for i in range(10):
            try:
                run_command(cmd, check=True)
                return
            except:
                logger.info("  Waiting for DAG to appear...")
                time.sleep(10)

    def _cleanup_airflow_cli(self):
        """
        Robust cleanup using direct Airflow CLI via gcloud.
        Parses the table output of 'users list' to find targets.
        """
        logger.info("Cleaning up Airflow Users via CLI (Robust Method)...")
        
        # 1. Get List (Table format)
        logger.info("  > Fetching current Airflow users...")
        # 'users list' is the valid subcommand for 'gcloud composer ... run'
        cmd = f"gcloud composer environments run {self.env_name} --location {self.region} --project {self.project} users list"
        
        table_output = run_command(cmd, check=False)
        if not table_output:
            logger.warning("Failed to list users (or empty output).")
            return

        # 2. Parse Table
        targets = []
        lines = table_output.splitlines()
        
        # Heuristic: Find header line starting with "id"
        start_idx = -1
        for i, line in enumerate(lines):
            if line.strip().lower().startswith("id") and "username" in line:
                start_idx = i
                break
        
        if start_idx == -1:
            logger.debug(f"Could not find header in output: {table_output[:200]}...")
        else:
            # Process data lines (skip header and separator)
            for line in lines[start_idx+2:]:
                if "|" not in line: continue
                parts = [p.strip() for p in line.split("|")]
                if len(parts) < 3: continue
                
                username = parts[1]
                email = parts[2]
                
                # Check match (SAs or Group members)
                is_target = False
                # SAs (end with iam.gserviceaccount.com)
                if email.endswith(".iam.gserviceaccount.com"):
                    is_target = True
                
                if is_target:
                    targets.append(username)

        logger.info(f"DEBUG: users list output length: {len(table_output)}")
        logger.info(f"DEBUG: Targets: {targets}")
        
        # 3. Delete Targets
        for username in targets:
            logger.info(f"  > Deleting User manually: {username}")
            # 'users delete'
            del_cmd = f"gcloud composer environments run {self.env_name} --location {self.region} --project {self.project} users delete -- --username '{username}'"
            run_command(del_cmd, check=False)


    def _upload_rbac_manifest(self):
        # Build Sync Manifest
        m = {"groups": {}, "service_accounts_detailed": []}
        for g in self.config['sync_groups']:
            gh = get_identity_hash(g)
            m["groups"][g] = {"role_name": f"role_{gh}", "members": self.fetch_group_members(g)}
            
        for sa in self.config['service_accounts']:
            email = self._get_full_email(sa)
            uid = run_command(f"gcloud iam service-accounts describe {email} --project {self.project} --format='value(uniqueId)'")
            m["service_accounts_detailed"].append({
                "role_name": f"role_{get_identity_hash(email)}",
                "email": email,
                "username": f"accounts.google.com:{uid}"
            })
            
        with open("rbac_manifest.json", 'w') as f: json.dump(m, f, indent=2)
        dest = f"gs://{self.bucket}/data/rbac_manifest.json"
        run_command(f"gcloud storage cp rbac_manifest.json {dest}")
        os.remove("rbac_manifest.json")
        return dest

    def _upload_cleanup_manifest(self):
        # Build Cleanup Manifest
        m = {"users_to_delete": [], "roles_to_delete": []}
        
        # SAs
        for sa in self.config['service_accounts']:
            email = f"{sa}@{self.project}.iam.gserviceaccount.com"
            m["users_to_delete"].append({"email": email})
            m["roles_to_delete"].append(sa) # Usually roles are just 'ds-user-1-svc' in old naming, or role_HASH in new?
            # Warning: Deletion needs to know the exact Role Name used during creation!
            # New creation uses role_{HASH}. Old creation used {sa_name}? 
            # We should probably try to delete BOTH or calculate hash.
            h = get_identity_hash(email)
            m["roles_to_delete"].append(f"role_{h}")

        # Groups
        for g in self.config['remove_groups']:
            gh = get_identity_hash(g)
            m["roles_to_delete"].append(f"role_{gh}")
            members = self.fetch_group_members(g)
            for mem in members:
                m["users_to_delete"].append({"username": mem["username"]})

        with open("cleanup_manifest.json", 'w') as f: json.dump(m, f, indent=2)
        dest = f"gs://{self.bucket}/data/cleanup_manifest.json"
        run_command(f"gcloud storage cp cleanup_manifest.json {dest}")
        os.remove("cleanup_manifest.json")
        return dest

    def _prune_manifest(self):
        # Similar logic to deprovision_platform.py prune
        path = f"gs://{self.bucket}/data/rbac_manifest.json"
        run_command(f"gcloud storage cp {path} current_m.json", check=False)
        if not os.path.exists("current_m.json"): return
        
        with open("current_m.json") as f: data = json.load(f)
        
        # Filter Logic (Simplified)
        if 'groups' in data:
            for g in self.config['remove_groups']:
                data['groups'].pop(g, None)
                
        # Prune Service Accounts
        # 1. target_service_accounts (Legacy/Simple List)
        if 'target_service_accounts' in data:
            data['target_service_accounts'] = [
                sa for sa in data['target_service_accounts']
                if sa not in self.config['service_accounts']
            ]
            
        # 2. service_accounts_detailed (Main)
        if 'service_accounts_detailed' in data:
            def is_removed(sa_entry):
                email = sa_entry.get('email', '')
                for remove_sa in self.config['service_accounts']:
                    # Check if 'remove_sa' (e.g. ds-user-1-svc) is part of the email
                    if remove_sa in email: return True
                return False

            original_count = len(data['service_accounts_detailed'])
            data['service_accounts_detailed'] = [
                sa for sa in data['service_accounts_detailed']
                if not is_removed(sa)
            ]
            if len(data['service_accounts_detailed']) < original_count:
                logger.info(f"  > Pruned {original_count - len(data['service_accounts_detailed'])} Service Accounts from manifest.")
        
        with open("current_m.json", 'w') as f: json.dump(data, f)
        run_command(f"gcloud storage cp current_m.json {path}")
        os.remove("current_m.json")

    def _setup_dataproc(self):
        cluster = self.config.get('dataproc_cluster')
        if not cluster:
            logger.info("No Dataproc Cluster specified. Skipping Dataproc integration.")
            return

        # 1. Upload Wrapper Script (Once)
        wrapper_local = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wrapper_papermill.py")
        wrapper_remote = "dataproc-notebooks/wrapper_papermill.py"
        if os.path.exists(wrapper_local):
            logger.info(f"  > Uploading {wrapper_local} to gs://{self.bucket}/{wrapper_remote}...")
            run_command(f"gcloud storage cp {wrapper_local} gs://{self.bucket}/{wrapper_remote}", check=True)
        else:
            logger.warning(f"  > Local wrapper script not found at {wrapper_local}. Skipping upload.")

        # 2. Grant SA Access for Single Cluster
        logger.info(f"Configuring Access for Cluster: {cluster}")
        dp_sa = run_command(f"gcloud dataproc clusters describe {cluster} --region {self.region} --format='value(config.gceClusterConfig.serviceAccount)'")
        
        if not dp_sa:
            logger.warning(f"  > No explicit Service Account found for {cluster}. Assuming Default Compute Engine SA.")
            # Fetch Project Number
            proj_num = run_command(f"gcloud projects describe {self.project} --format='value(projectNumber)'")
            if proj_num:
                dp_sa = f"{proj_num}-compute@developer.gserviceaccount.com"
            else:
                logger.error("  > Failed to determine Project Number for default SA.")
                return 

        logger.info(f"  > Target Dataproc SA: {dp_sa}")
        logger.info(f"  > Granting SA Read Access to Wrapper...")
        run_command(
            f"gcloud storage buckets add-iam-policy-binding gs://{self.bucket} "
            f"--member='serviceAccount:{dp_sa}' "
            f"--role='roles/storage.objectViewer' "
            f"--condition=\"expression=resource.name.startsWith('projects/_/buckets/{self.bucket}/objects/{wrapper_remote}'),title=wrapper_access_dp_{get_identity_hash(dp_sa)},description=Read Wrapper\""
        , check=False)

        # 3. Grant Users Read Access to Wrapper & Write Access to Notebooks
        for sa in self.config['service_accounts']:
            email = self._get_full_email(sa)
            h = get_identity_hash(email)
            
            # Read Wrapper
            run_command(
                f"gcloud storage buckets add-iam-policy-binding gs://{self.bucket} "
                f"--member='serviceAccount:{email}' "
                f"--role='roles/storage.objectViewer' "
                f"--condition=\"expression=resource.name.startsWith('projects/_/buckets/{self.bucket}/objects/{wrapper_remote}'),title=wrapper_access_{h},description=Read Wrapper\""
            , check=False)
            
            # Write Notebooks (Strict Isolation)
            prefix = f"dataproc-notebooks/{h}_"
            run_command(
                f"gcloud storage buckets add-iam-policy-binding gs://{self.bucket} "
                f"--member='serviceAccount:{email}' "
                f"--role='roles/storage.objectAdmin' "
                f"--condition=\"expression=resource.name.startsWith('projects/_/buckets/{self.bucket}/objects/{prefix}'),title=notebooks_access_{h},description=Write Notebooks\""
            , check=True)
            time.sleep(2)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--action", required=True, choices=['provision', 'deprovision'])
    p.add_argument("--project", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--composer-env", required=True)
    p.add_argument("--service-accounts", nargs="+", default=[])
    p.add_argument("--groups", nargs="+", default=[], help="Groups to Sync (provision) or Remove (deprovision)")
    p.add_argument("--dataproc-cluster", help="Dataproc Cluster for integration (Single)")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    
    cfg = {
        "project": args.project, "region": args.region, "composer_env": args.composer_env,
        "service_accounts": args.service_accounts,
        "sync_groups": args.groups, # Aliased for provision
        "remove_groups": args.groups, # Aliased for deprovision
        "dataproc_cluster": args.dataproc_cluster,
        "dry_run": args.dry_run
    }
    
    mgr = ComposerAccessManager(cfg)
    if args.action == 'provision': 
        mgr.provision()
        mgr._setup_dataproc()
    else: 
        mgr.deprovision() # Deprovision should also cleanup dataproc specific bits now handled in CLEANUP INFRA

if __name__ == "__main__":
    main()
