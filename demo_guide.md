# Demo Guide

This guide provides a step-by-step walkthrough for (1) setting up a secure, multi-tenant Composer environment, and (2) running a comprehensive demo using a unified CLI tool to create and update notebook schedules that are capable of linear chaining multiple notebooks; as well as to manage permissions on the schedule jobs (i.e. sharing with other users/groups).

The instructions below reflect the specific **demo environment**. Please adapt to your own environment for testing.

## 1. Prerequisite Setup

**Project & Region:**
*   `project`: `kenly-lakehouse-dev-1`
*   `region`: `us-central1`

**Service Accounts:**
*   `ds-user-1-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com`
*   `ds-user-2-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com`
*   `ds-user-3-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com`

**Groups & Membership:**
*   `ds-grp-1@kenly.altostrat.com`
    *   Contains: 
        * `ds-user-1-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com`
        * `ds-user-2-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com`
*   `ds-grp-3@kenly.altostrat.com`
    *   Contains:
        * `ds-user-2-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com`
        * `ds-user-3-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com`

**Infrastructure:**
*   **Composer Environment:** `my-airflow-26010741`
*   **Dataproc Multitenant GCE Cluster:** `pyspark-cluster-dev-multitenant-20260124073836`
    *   **Identity Mapping:**
        ```yaml
        user_service_account_mapping:
          ds-user-1-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com: ds-user-1-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com
          ds-user-2-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com: ds-user-2-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com
          ds-user-3-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com: ds-user-3-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com
        ```

*   **Workbench Instances:**
    *   `instance-20251203-071523-single-svc` (for **ds-user-1-svc**)
    *   `instance-20251203-151903-single-svc-2` (for **ds-user-2-svc**)
    *   `instance-20251208-220655-single-svc-3` (for **ds-user-3-svc**)

---

## 2. Setup for Composer

*Perform this step as a Project Owner or User with administrative permissions to GCS and Composer.*

1.  **Distribute the folder** `composer_access_management` to your environment.
    *   This folder contains `setup_composer_access.py` and necessary utilities files.
2.  **Execute the Provisioning Script:**

```bash
# === Common Configuration ===
export PROJECT_ID="kenly-lakehouse-dev-1"
export REGION="us-central1"
export COMPOSER_ENV="my-airflow-26010741"
export CLUSTER_NAME="pyspark-cluster-dev-multitenant-20260124073836"

python3 composer_access_management/setup_composer_access.py \
  --action provision \
  --project $PROJECT_ID \
  --region $REGION \
  --composer-env $COMPOSER_ENV \
  --dataproc-cluster $CLUSTER_NAME \
  --service-accounts \
      ds-user-1-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com \
      ds-user-2-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com \
      ds-user-3-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com \
  --groups \
      ds-grp-1@kenly.altostrat.com \
      ds-grp-3@kenly.altostrat.com
```
    
---

## 3. Demo Scenarios

In this demo, we verify:
1.  **User 1:** Schedules an individual job + Schedules a shared job for `ds-grp-1` (Edit Access).
2.  **User 3:** Schedules an individual job + Schedules a shared job for `ds-grp-3` (View Access).
3.  **User 2:** Verifies access to both shared jobs (as a member of both groups).

### Scenario 1: User 1 (`ds-user-1-svc`)
*Member of: `ds-grp-1`*

1.  **Distribute the Scheduler:** Ensure the `my_scheduler` folder is available on the Workbench instance.
2.  **Access Terminal:** Open the terminal on **`instance-20251203-071523-single-svc`** (serving `ds-user-1-svc`).

#### Step 1: Schedule Individual Job
```bash
python3 my_scheduler/manual_dag_manager.py create \
    --project $PROJECT_ID \
    --region $REGION \
    --cluster $CLUSTER_NAME \
    --composer-env $COMPOSER_ENV \
    --notebooks "notebooks/Basic Spark.ipynb" "notebooks/my_folder_1/my_sub_folder_1/Basic Spark.ipynb" \
    --schedule-value "@once" \
    --name "user1_private_$(date +%Y%m%d%H%M%S)"
```

**Result**
1.  The DAG should show up in the plugin UI within a short delay.

#### Step 2: Schedule & Share with `ds-grp-1` (EDIT Access)
**A. Schedule the Job:**
```bash
python3 my_scheduler/manual_dag_manager.py create \
    --project $PROJECT_ID \
    --region $REGION \
    --cluster $CLUSTER_NAME \
    --composer-env $COMPOSER_ENV \
    --notebooks "notebooks/Basic Spark.ipynb" "notebooks/my_folder_1/my_sub_folder_1/Basic Spark.ipynb" \
    --schedule-value "@once" \
    --name "user1_shared_grp1_$(date +%Y%m%d%H%M%S)"
```
*(Copy the DAG ID from the output, e.g., `4308b450df_user1_shared_grp1_...`)*

**B. Share the Job:**
```bash
python3 my_scheduler/manual_dag_manager.py permissions \
    --action add \
    --dag-id <DAG_ID_FROM_ABOVE> \
    --identity ds-grp-1@kenly.altostrat.com \
    --identity-type "group" \
    --project $PROJECT_ID \
    --region $REGION \
    --composer-env $COMPOSER_ENV \
    --access-level edit
```

**Result**
1.  Members of `ds-grp-1` (like User 2) can now **Edit** this DAG.
2.  Perform the "Edit" action
    * "Pause" / "Unpause" / "Trigger" / "Delete" the job can be done from the plugin UI
    * But to "Update" the schedule, refer to Scenario 4 below

---

### Scenario 2: User 3 (`ds-user-3-svc`)
*Member of: `ds-grp-3` only*

1.  **Distribute the Scheduler:**
    *   Ensure the `my_scheduler` folder is available on the Workbench instance.
2.  **Access Terminal:**
    *   Open the terminal on **`instance-20251208-220655-single-svc-3`** (serving `ds-user-3-svc`).

#### Step 1: Schedule Individual Job
```bash
python3 my_scheduler/manual_dag_manager.py create \
    --project $PROJECT_ID \
    --region $REGION \
    --cluster $CLUSTER_NAME \
    --composer-env $COMPOSER_ENV \
    --notebooks "notebooks/Basic Spark.ipynb" "notebooks/my_folder_1/my_sub_folder_1/Basic Spark.ipynb" \
    --schedule-value "@once" \
    --name "user3_private_$(date +%Y%m%d%H%M%S)"
```

#### Step 2: Schedule & Share with `ds-grp-3` (VIEW Access)
**A. Schedule the Job:**
```bash
python3 my_scheduler/manual_dag_manager.py create \
    --project $PROJECT_ID \
    --region $REGION \
    --cluster $CLUSTER_NAME \
    --composer-env $COMPOSER_ENV \
    --notebooks "notebooks/Basic Spark.ipynb" "notebooks/my_folder_1/my_sub_folder_1/Basic Spark.ipynb" \
    --schedule-value "@once" \
    --name "user3_shared_grp3_$(date +%Y%m%d%H%M%S)"
```
*(Copy the DAG ID from the output, e.g., `4308b450df_user3_shared_grp3_...`)*

**B. Share the Job:**
```bash
python3 my_scheduler/manual_dag_manager.py permissions \
    --action add \
    --dag-id <DAG_ID_FROM_ABOVE> \
    --identity ds-grp-3@kenly.altostrat.com \
    --identity-type "group" \
    --project $PROJECT_ID \
    --region $REGION \
    --composer-env $COMPOSER_ENV \
    --access-level read
```

**Result**
1.  Members of `ds-grp-3` (like User 2) can now **View** this DAG.
2.  User cannot "Pause" / "Unpause" / "Trigger" / "Delete" / "Update" the job, it is only visible in their plugin UI page, and can see the job runs and job results.


---

### Scenario 3: User 2 (`ds-user-2-svc`) / Verification
*Member of: `ds-grp-1` AND `ds-grp-3`*

#### Step 1: Verify Access
1.  **Login/Act as User 2** (`ds-user-2-svc`).
2.  **Check Visibility**:
    *   **User 1's Private DAG**: Should **NOT** be visible.
    *   **User 1's Shared DAG**: Should be visible **(Edit Access)**.
    *   **User 3's Private DAG**: Should **NOT** be visible.
    *   **User 3's Shared DAG**: Should be visible **(Read/View Only Access)**.

---

### Scenario 4: Collaborative - Schedule Update
*Demonstrates that "Edit" (group) permission allows modifying the DAG schedule.*

#### Step 1: Login as Editor (User 2)
*(Assuming you are impersonating or logged in as `ds-user-2-svc`, who is a member of `ds-grp-1`)*.

#### Step 2: Update User 1's DAG Schedule
User 2 decides the shared DAG needs to run Daily instead of Once.

```bash
# 1. Identify the Shared DAG ID (from Airflow UI or list)
# e.g., dag_df13958c56_shared_run1

# 2. Update Schedule
python3 my_scheduler/manual_dag_manager.py update-schedule \
    --project $PROJECT_ID \
    --region $REGION \
    --composer-env $COMPOSER_ENV \
    --dag-id "dag_df13958c56_shared_run1" \
    --new-schedule "@daily"
```
> **Expectation:** Success. GCS allows the overwrite because `ds-grp-1` has `storage.objectAdmin` on this specific file.

---

## Part 3: Clean up

### Clean up
To remove these test users/groups from Airflow and revoke infrastructure access:

```bash
# Run Deprovisioner (Unified)
python3 composer_access_management/setup_composer_access.py \
  --action deprovision \
  --project $PROJECT_ID \
  --region $REGION \
  --composer-env $COMPOSER_ENV \
  --service-accounts \
      ds-user-1-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com \
      ds-user-2-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com \
      ds-user-3-svc@kenly-lakehouse-dev-1.iam.gserviceaccount.com \
  --groups \
      ds-grp-1@kenly.altostrat.com \
      ds-grp-3@kenly.altostrat.com
```

**What happens?**
1.  **Airflow Cleanup:** Triggers `rbac_ops_dag` with `{"action": "cleanup"}`.
2.  **Infra Cleanup:** Revokes legacy bucket permissions.
3.  **Manifest Pruning:** Updates `rbac_manifest.json` to prevent zombie entities.
