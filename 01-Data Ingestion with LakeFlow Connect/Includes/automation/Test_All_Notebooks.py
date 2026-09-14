# Databricks notebook source
# MAGIC %md
# MAGIC # Automated Test Runner — Single Course
# MAGIC
# MAGIC Creates a **single multi-task Lakeflow Job** for one course, with optional QA checks,
# MAGIC so the full run is visible in one timeline.
# MAGIC
# MAGIC **Run structure:**
# MAGIC - Course notebooks run in the dependency order you define in `COURSE_TASKS`.
# MAGIC - **QA Content Checker** can run independently (no dependencies) alongside the course tasks.
# MAGIC - The separate **Course-specific inputs** cell makes it easy to reuse this notebook for another course.
# MAGIC
# MAGIC ```
# MAGIC  QA Check      :  qa_content_checker  (optional, parallel)
# MAGIC  Course Tasks  :  task_01  ──►  task_02  ──►  task_03
# MAGIC ```
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Reads course-specific settings from a dedicated configuration cell (`COURSE_NAME`, `LAB_NOTEBOOKS`, `COURSE_TASKS`).
# MAGIC 2. Auto-fills all `<FILL_IN>` placeholders in the configured lab notebooks using the inline solution blocks.
# MAGIC 3. Resolves all notebook paths relative to this notebook's location.
# MAGIC 4. Creates (or re-creates) a single persistent Lakeflow Job for the configured course.
# MAGIC 5. Optionally adds `qa_content_checker` as an **independent task** (no dependencies).
# MAGIC 6. Triggers the job with `jobs.run_now` and polls until completion.
# MAGIC 7. Appends one result row per task to a generic results table.
# MAGIC 8. Displays a combined summary table with clickable `run_page_url` links.
# MAGIC 9. Creates and publishes a **Lakeview dashboard** from the run results and QA findings.
# MAGIC 10. Raises if any task failed — triggering Lakeflow Job failure email.

# COMMAND ----------

# MAGIC %run ./Configuration

# COMMAND ----------

# ── Patch Known Placeholders ─────────────────────────────────────────────────
# Some course notebooks use informal placeholders (e.g. 'your-labuser-name')
# that are NOT marked with <FILL_IN> and therefore missed by the auto-fill.
# This cell performs direct string replacement on those known patterns.
# ──────────────────────────────────────────────────────────────────────────────

import base64
import json
import re
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

# Derive the actual schema name (same logic as RESULTS_SCHEMA)
_user_email = spark.sql("SELECT current_user()").collect()[0][0]
_schema_name = _user_email.split("@")[0].replace(".", "_").replace("-", "_")

# Known placeholder patterns to replace with the actual schema name
PLACEHOLDER_REPLACEMENTS = [
    ("your-labuser-name", _schema_name),
    ("your-lab-user-schema", _schema_name),
    ("your-lab-user-name", _schema_name),
]

# Notebooks known to contain these informal placeholders
NOTEBOOKS_TO_PATCH = [
    "../../05 Demo -  Create Streaming Tables with SQL using Auto Loader",
]

# ── Fix intentional error cells ──────────────────────────────────────────────
# Some demo notebooks have cells that INTENTIONALLY fail (teaching moments).
# For automated testing, we patch those to not break the full run.
# Each entry: (relative_path, old_text, new_text)
# ── Streaming table → regular table patches (notebook 05) ────────────────
# CREATE OR REFRESH STREAMING TABLE is not supported on classic clusters
# or serverless generic compute (requires SQL Warehouse or feature preview).
# We convert it to a regular CTAS so the setup + queries still run.
STREAMING_TABLE_PATCHES = [
    (
        "../../05 Demo -  Create Streaming Tables with SQL using Auto Loader",
        [
            # Patch 1: Convert CREATE OR REFRESH STREAMING TABLE to CREATE OR REPLACE TABLE
            (
                "CREATE OR REFRESH STREAMING TABLE sql_csv_autoloader\nSCHEDULE EVERY 1 WEEK     -- Scheduling the refresh is optional\nAS\nSELECT *\nFROM STREAM read_files(",
                "-- Converted to regular table for automated testing (streaming tables require SQL warehouse)\nCREATE OR REPLACE TABLE sql_csv_autoloader\nAS\nSELECT *\nFROM read_files(",
            ),
            # Patch 2: Convert REFRESH STREAMING TABLE to a no-op SELECT
            (
                "REFRESH STREAMING TABLE sql_csv_autoloader;",
                "-- REFRESH STREAMING TABLE sql_csv_autoloader; -- Skipped: requires SQL warehouse\nSELECT 'Refresh skipped in automated test mode' AS status;",
            ),
        ],
    ),
]

INTENTIONAL_ERROR_PATCHES = [
    (
        "../../04 Demo - Data Ingestion with CREATE TABLE AS and COPY INTO",
        "COPY INTO historical_users_bronze_ci\n  FROM '/Volumes/dbacademy_ecommerce/v01/raw/users-historical'\n  FILEFORMAT = parquet;",
        "COPY INTO historical_users_bronze_ci\n  FROM '/Volumes/dbacademy_ecommerce/v01/raw/users-historical'\n  FILEFORMAT = parquet\n  COPY_OPTIONS ('mergeSchema' = 'true');  -- Added for automated testing",
    ),
    (
        "../../17 Demo - BONUS - Data Ingestion with MERGE INTO",
        "MERGE INTO main_users_target target\nUSING new_users_source source\nON target.id = source.id\nWHEN MATCHED AND source.status = 'update' THEN\n  UPDATE SET \n    target.email = source.email,\n    target.status = source.status\nWHEN MATCHED AND source.status = 'delete' THEN\n  DELETE\nWHEN NOT MATCHED AND source.status = 'new' THEN\n  INSERT (id, first_name, email, sign_up_date, status, country)\n  VALUES (source.id, source.first_name, source.email, source.sign_up_date, source.status, source.country);",
        "MERGE WITH SCHEMA EVOLUTION INTO main_users_target target  -- Added SCHEMA EVOLUTION for automated testing\nUSING new_users_source source\nON target.id = source.id\nWHEN MATCHED AND source.status = 'update' THEN\n  UPDATE SET \n    target.email = source.email,\n    target.status = source.status\nWHEN MATCHED AND source.status = 'delete' THEN\n  DELETE\nWHEN NOT MATCHED AND source.status = 'new' THEN\n  INSERT (id, first_name, email, sign_up_date, status, country)\n  VALUES (source.id, source.first_name, source.email, source.sign_up_date, source.status, source.country);",
    ),
]

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_course_root = "/".join(_nb_path.split("/")[:-1])

print("═" * 70)
print("PATCH KNOWN PLACEHOLDERS")
print("═" * 70)

for rel_path in NOTEBOOKS_TO_PATCH:
    abs_path = os.path.normpath(f"{_course_root}/{rel_path}")
    nb_name = abs_path.split("/")[-1]
    print(f"\n▶ Patching: {nb_name}")

    try:
        export_resp = w.workspace.export(path=abs_path, format=ExportFormat.JUPYTER)
        nb_json = json.loads(base64.b64decode(export_resp.content))
        cells = nb_json.get("cells", [])

        total_replacements = 0
        for cell in cells:
            source = ''.join(cell.get("source", []))
            new_source = source
            for placeholder, replacement in PLACEHOLDER_REPLACEMENTS:
                if placeholder in new_source:
                    new_source = new_source.replace(placeholder, replacement)
            if new_source != source:
                cell["source"] = [new_source]
                total_replacements += 1

        if total_replacements > 0:
            patched_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
            w.workspace.import_(
                path=abs_path,
                content=patched_content,
                format=ImportFormat.JUPYTER,
                overwrite=True,
                language=Language.PYTHON,
            )
            print(f"   ✅ Patched {total_replacements} cell(s) — replaced placeholders with '{_schema_name}'")
        else:
            print(f"   ℹ️  No placeholders found (already patched or not present)")

    except Exception as e:
        print(f"   ❌ ERROR: {e}")

# ── Apply streaming table patches ───────────────────────────────────────────
print("\n" + "─" * 70)
print("PATCH STREAMING TABLE DDL → REGULAR TABLE")
print("─" * 70)

for rel_path, patches in STREAMING_TABLE_PATCHES:
    abs_path = os.path.normpath(f"{_course_root}/{rel_path}")
    nb_name = abs_path.split("/")[-1]
    print(f"\n▶ Patching: {nb_name}")

    try:
        export_resp = w.workspace.export(path=abs_path, format=ExportFormat.JUPYTER)
        nb_json = json.loads(base64.b64decode(export_resp.content))
        cells = nb_json.get("cells", [])

        patched = 0
        for cell in cells:
            source = ''.join(cell.get("source", []))
            new_source = source
            for old_text, new_text in patches:
                if old_text in new_source:
                    new_source = new_source.replace(old_text, new_text)
            if new_source != source:
                cell["source"] = [new_source]
                patched += 1

        if patched > 0:
            patched_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
            w.workspace.import_(
                path=abs_path,
                content=patched_content,
                format=ImportFormat.JUPYTER,
                overwrite=True,
                language=Language.PYTHON,
            )
            print(f"   ✅ Patched {patched} cell(s) — converted streaming table to regular table")
        else:
            print(f"   ℹ️  No streaming table patterns found (already patched)")

    except Exception as e:
        print(f"   ❌ ERROR: {e}")

# ── Apply intentional-error patches ──────────────────────────────────────────
print("\n" + "─" * 70)
print("PATCH INTENTIONAL ERROR CELLS")
print("─" * 70)

for rel_path, old_text, new_text in INTENTIONAL_ERROR_PATCHES:
    abs_path = os.path.normpath(f"{_course_root}/{rel_path}")
    nb_name = abs_path.split("/")[-1]
    print(f"\n▶ Patching: {nb_name}")

    try:
        export_resp = w.workspace.export(path=abs_path, format=ExportFormat.JUPYTER)
        nb_json = json.loads(base64.b64decode(export_resp.content))
        cells = nb_json.get("cells", [])

        patched = 0
        for cell in cells:
            source = ''.join(cell.get("source", []))
            if old_text in source:
                cell["source"] = [source.replace(old_text, new_text)]
                patched += 1

        if patched > 0:
            patched_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
            w.workspace.import_(
                path=abs_path,
                content=patched_content,
                format=ImportFormat.JUPYTER,
                overwrite=True,
                language=Language.PYTHON,
            )
            print(f"   ✅ Patched {patched} cell(s) — fixed intentional error for automated run")
        else:
            print(f"   ℹ️  Pattern not found (already patched or content changed)")

    except Exception as e:
        print(f"   ❌ ERROR: {e}")

print("\n" + "═" * 70)
print("ALL PATCHING COMPLETE")
print("═" * 70)

# COMMAND ----------

# Catalog / schema / table names
RESULTS_CATALOG   = "dbacademy"
RESULTS_SCHEMA    = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
RESULTS_TABLE     = "notebook_run_results"
QA_FINDINGS_TABLE = "qa_content_findings"

# How long to wait for the entire job run (seconds)
TOTAL_RUN_TIMEOUT_SECONDS = 60 * 60 * 2   # 2 hour ceiling
POLL_INTERVAL_SECONDS     = 15

# Job and dashboard names
JOB_NAME       = f"[Course Validation] {COURSE_NAME}"
DASHBOARD_NAME = f"[Course Validation] {COURSE_NAME} Results Dashboard"

# Build the task list from the reusable course configuration above.
TASKS = []

if RUN_QA_CHECKER:
    TASKS.append({
        "task_key": "qa_content_checker",
        "course": "QA",
        "name": QA_TASK_NAME,
        "relative_path": QA_TASK_RELATIVE_PATH,
        "depends_on": [],
        "env_key": "qa_env",
    })

for task in COURSE_TASKS:
    TASKS.append({
        "task_key": task["task_key"],
        "course": COURSE_NAME,
        "name": task["name"],
        "relative_path": task["relative_path"],
        "depends_on": task.get("depends_on", []),
        "env_key": task["env_key"],
        "use_classic": task.get("use_classic", False),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and workspace client

# COMMAND ----------

import json
import os
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    JobEmailNotifications,
    JobEnvironment,
    NotebookTask,
    RunLifeCycleState,
    RunResultState,
    Task,
    TaskDependency,
)

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve workspace paths

# COMMAND ----------

this_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
course_root = "/".join(this_notebook_path.split("/")[:-1])

for t in TASKS:
    # os.path.normpath resolves '../' so Databricks gets a clean absolute path
    t["notebook_path"] = os.path.normpath(f"{course_root}/{t['relative_path']}")

print(f"Course root: {course_root}\n")
for t in TASKS:
    print(f"  [{t['task_key']}]  {t['name']}\n      {t['notebook_path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure results table exists

# COMMAND ----------

results_schema_name = RESULTS_SCHEMA if RESULTS_SCHEMA != "information_schema" else "default"
results_fqn = f"{RESULTS_CATALOG}.{results_schema_name}.{RESULTS_TABLE}"

results_schema = StructType([
    StructField("run_timestamp",    TimestampType(), nullable=False),
    StructField("job_id",           LongType(),      nullable=True),
    StructField("job_run_id",       LongType(),      nullable=True),
    StructField("course",           StringType(),    nullable=True),
    StructField("task_key",         StringType(),    nullable=False),
    StructField("demo_name",        StringType(),    nullable=False),
    StructField("notebook_path",    StringType(),    nullable=False),
    StructField("status",           StringType(),    nullable=False),  # PASS / FAIL / TIMEOUT
    StructField("result_state",     StringType(),    nullable=True),
    StructField("life_cycle_state", StringType(),    nullable=True),
    StructField("duration_seconds", DoubleType(),    nullable=True),
    StructField("run_id",           LongType(),      nullable=True),
    StructField("run_page_url",     StringType(),    nullable=True),
    StructField("error_message",    StringType(),    nullable=True),
])

if not spark.catalog.tableExists(results_fqn):
    spark.createDataFrame([], results_schema).write.format("delta").saveAsTable(results_fqn)
    print(f"Created results table: {results_fqn}")
else:
    print(f"Results table exists: {results_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create (or re-create) the course Lakeflow Job

# COMMAND ----------

# Delete any existing job with the same name so we always get a clean definition.
ENVIRONMENT_VERSION = "5"

existing = [j for j in w.jobs.list(name=JOB_NAME)]
for j in existing:
    w.jobs.delete(job_id=j.job_id)
    print(f"Deleted existing job: {j.job_id} ({j.settings.name})")

# Build Task objects from TASKS config.
import re

def _safe_key(k):
    return re.sub(r'[^a-zA-Z0-9_-]', '_', k)

# All tasks share one environment (identical spec — consolidates to stay within the 10-env API limit).
SHARED_ENV_KEY = "shared_env"

job_tasks = []
for t in TASKS:
    job_tasks.append(
        Task(
            task_key=_safe_key(t["task_key"]),
            description=t["name"],
            notebook_task=NotebookTask(notebook_path=t["notebook_path"]),
            environment_key=SHARED_ENV_KEY,
            depends_on=[TaskDependency(task_key=_safe_key(dep)) for dep in t["depends_on"]],
        )
    )

# ── Route classic-compute tasks to the existing cluster ────────────────────────
# Dynamically fetch the user's classic cluster
current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = current_user.split("@")[0]

CLASSIC_CLUSTER_ID = None
for c in w.clusters.list():
    if c.creator_user_name == current_user or c.cluster_name == user_prefix:
        CLASSIC_CLUSTER_ID = c.cluster_id
        print(f"Found classic cluster: {c.cluster_name} (ID: {c.cluster_id})")
        break

if not CLASSIC_CLUSTER_ID:
    print("⚠️  No classic cluster found for user. Classic-compute tasks will fall back to serverless.")

if CLASSIC_CLUSTER_ID:
    for i, t in enumerate(TASKS):
        if t.get("use_classic"):
            job_tasks[i].environment_key = None
            job_tasks[i].existing_cluster_id = CLASSIC_CLUSTER_ID

job_environments = [
    JobEnvironment(
        environment_key=SHARED_ENV_KEY,
        spec=Environment(environment_version=ENVIRONMENT_VERSION),
    )
]

created_job = w.jobs.create(
    name=JOB_NAME,
    tasks=job_tasks,
    environments=job_environments,
    email_notifications=JobEmailNotifications(
        on_failure=TESTER_EMAILS,
        on_success=TESTER_EMAILS,
    ),
)
job_id = created_job.job_id
print(f"Created Lakeflow Job: {job_id}  ({JOB_NAME})")
print(f"\nTask DAG:")
for t in TASKS:
    deps = " → depends on: " + ", ".join(t["depends_on"]) if t["depends_on"] else " (starts immediately)"
    compute_label = "[CLASSIC]" if t.get("use_classic") else "[SERVERLESS]"
    print(f"  {compute_label} {t['task_key']}{deps}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger the job run and poll until all tasks complete

# COMMAND ----------

run_response  = w.jobs.run_now(job_id=job_id)
job_run_id    = run_response.run_id
run_timestamp = datetime.now(timezone.utc)

print(f"Triggered job run: {job_run_id}")
print(f"Polling every {POLL_INTERVAL_SECONDS}s (timeout={TOTAL_RUN_TIMEOUT_SECONDS}s)...\n")

deadline        = time.time() + TOTAL_RUN_TIMEOUT_SECONDS
final_run       = None
terminal_states = {RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR}

while time.time() < deadline:
    run = w.jobs.get_run(run_id=job_run_id)
    lc  = run.state.life_cycle_state if run.state else None

    task_states = {
        tk.task_key: (
            tk.state.life_cycle_state.value if tk.state and tk.state.life_cycle_state else "PENDING"
        )
        for tk in (run.tasks or [])
    }
    print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] run={lc}  tasks={task_states}")

    if lc in terminal_states:
        final_run = run
        break

    time.sleep(POLL_INTERVAL_SECONDS)
else:
    print("TIMEOUT — cancelling the run.")
    try:
        w.jobs.cancel_run(run_id=job_run_id)
    except Exception:
        pass
    final_run = w.jobs.get_run(run_id=job_run_id)

print(f"\nFinal run state: {final_run.state.life_cycle_state if final_run.state else 'UNKNOWN'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect per-task results

# COMMAND ----------

task_cfg = {t["task_key"]: t for t in TASKS}

results = []
for task_run in (final_run.tasks or []):
    cfg   = task_cfg.get(task_run.task_key, {})
    state = task_run.state

    result_state = state.result_state.value     if state and state.result_state     else None
    life_cycle   = state.life_cycle_state.value  if state and state.life_cycle_state  else None
    error_msg    = state.state_message           if state                             else None

    duration = None
    if task_run.start_time and task_run.end_time:
        duration = float((task_run.end_time - task_run.start_time) / 1000.0)

    if life_cycle == RunLifeCycleState.TERMINATED.value and result_state == RunResultState.SUCCESS.value:
        status = "PASS"
    elif life_cycle in (RunLifeCycleState.SKIPPED.value, RunLifeCycleState.INTERNAL_ERROR.value):
        status = "FAIL"
    elif life_cycle == RunLifeCycleState.TERMINATED.value:
        status = "FAIL"
    else:
        status = "TIMEOUT"

    results.append({
        "job_id":           job_id,
        "job_run_id":       job_run_id,
        "course":           cfg.get("course", ""),
        "task_key":         task_run.task_key,
        "demo_name":        cfg.get("name", task_run.task_key),
        "notebook_path":    cfg.get("notebook_path", ""),
        "status":           status,
        "result_state":     result_state,
        "life_cycle_state": life_cycle,
        "duration_seconds": duration,
        "run_id":           task_run.run_id,
        "run_page_url":     task_run.run_page_url,
        "error_message":    error_msg,
    })

# Sort back into configured TASKS order
order = {t["task_key"]: i for i, t in enumerate(TASKS)}
results.sort(key=lambda r: order.get(r["task_key"], 999))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append results to Delta + display summary

# COMMAND ----------

rows       = [Row(run_timestamp=run_timestamp, **r) for r in results]
results_df = spark.createDataFrame(rows, schema=results_schema)

results_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(results_fqn)

print(f"Appended {results_df.count()} rows to {results_fqn}\n")

# Summary for the configured single-course run
summary_groups = [COURSE_NAME]
if RUN_QA_CHECKER:
    summary_groups.append("QA")

for group_name in summary_groups:
    group_results = [r for r in results if r["course"] == group_name]
    if not group_results:
        continue

    passed = sum(1 for r in group_results if r["status"] == "PASS")
    failed = sum(1 for r in group_results if r["status"] != "PASS")
    icon = "✅" if failed == 0 else "❌"
    label = "QA Checks" if group_name == "QA" else COURSE_NAME
    print(f"  {icon} {label}:  {passed}/{len(group_results)} passed")

print(f"\nOverall: {sum(1 for r in results if r['status'] == 'PASS')}/{len(results)} tasks passed")

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakeview Dashboard

# COMMAND ----------

import json
from databricks.sdk.service.dashboards import Dashboard

# ── Dashboard Configuration ────────────────────────────────────────────────────
DASHBOARD_FOLDER = "/".join(this_notebook_path.split("/")[:-2])  # Up from CourseRunner/
qa_findings_fqn = f"{RESULTS_CATALOG}.{RESULTS_SCHEMA}.{QA_FINDINGS_TABLE}"
course_label_sql = COURSE_NAME.replace("'", "''")

# ── Dataset SQL (always filters to the latest run) ────────────────────────────
run_filter = f"job_run_id = (SELECT MAX(job_run_id) FROM {results_fqn})"
qa_filter  = f"run_timestamp = (SELECT MAX(run_timestamp) FROM {qa_findings_fqn})"

status_summary_sql = (
    f"SELECT status, COUNT(*) AS task_count "
    f"FROM {results_fqn} WHERE {run_filter} "
    f"GROUP BY status ORDER BY status"
)

task_detail_sql = (
    f"SELECT CASE WHEN course = 'QA' THEN 'QA Checks' ELSE '{course_label_sql}' END AS run_group, "
    f"task_key, demo_name, status, ROUND(duration_seconds, 1) AS duration_seconds, "
    f"run_page_url, error_message "
    f"FROM {results_fqn} WHERE {run_filter} "
    f"ORDER BY CASE WHEN course = 'QA' THEN 0 ELSE 1 END, task_key"
)

qa_sev_sql = (
    f"SELECT severity, COUNT(*) AS issue_count "
    f"FROM {qa_findings_fqn} WHERE {qa_filter} "
    f"GROUP BY severity ORDER BY issue_count DESC"
) if spark.catalog.tableExists(qa_findings_fqn) else (
    "SELECT 'No Data' AS severity, 0 AS issue_count WHERE 1=0"
)

qa_detail_sql = (
    f"SELECT notebook_name, cell_index, issue_type, severity, "
    f"offending_text, suggested_fix, check_source "
    f"FROM {qa_findings_fqn} WHERE {qa_filter} "
    f"ORDER BY severity, notebook_name, cell_index"
) if spark.catalog.tableExists(qa_findings_fqn) else (
    "SELECT '' AS notebook_name, 0 AS cell_index, '' AS issue_type, "
    "'' AS severity, '' AS offending_text, '' AS suggested_fix, '' AS check_source WHERE 1=0"
)

datasets = [
    {
        "name": "ds_status_summary",
        "displayName": "Task Status Summary",
        "query": status_summary_sql,
    },
    {
        "name": "ds_task_detail",
        "displayName": "Task Results Detail",
        "query": task_detail_sql,
    },
]

pages = [
    {
        "name": "pg_runs",
        "displayName": f"{COURSE_NAME} Run Results",
        "layout": [
            {
                "widget": {
                    "name": "w_bar",
                    "title": "Task Status — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_status_summary",
                            "fields": [
                                {"name": "status", "expression": "`status`"},
                                {"name": "task_count", "expression": "`task_count`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 2,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "status", "displayName": "Status"},
                            "y": {"fieldName": "task_count", "displayName": "Tasks"},
                            "color": {"fieldName": "status", "displayName": "Status"}
                        }
                    }
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 6}
            },
            {
                "widget": {
                    "name": "w_task_table",
                    "title": "Task Results — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_task_detail",
                            "fields": [
                                {"name": "run_group", "expression": "`run_group`"},
                                {"name": "task_key", "expression": "`task_key`"},
                                {"name": "demo_name", "expression": "`demo_name`"},
                                {"name": "status", "expression": "`status`"},
                                {"name": "duration_seconds", "expression": "`duration_seconds`"},
                                {"name": "run_page_url", "expression": "`run_page_url`"},
                                {"name": "error_message", "expression": "`error_message`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 2,
                        "widgetType": "table",
                        "encodings": {
                            "columns": [
                                {"fieldName": "run_group", "visible": True, "title": "Run Group"},
                                {"fieldName": "task_key", "visible": True, "title": "Task Key"},
                                {"fieldName": "demo_name", "visible": True, "title": "Task Name"},
                                {"fieldName": "status", "visible": True, "title": "Status"},
                                {"fieldName": "duration_seconds", "visible": True, "title": "Duration (s)"},
                                {"fieldName": "run_page_url", "visible": True, "title": "Run URL"},
                                {"fieldName": "error_message", "visible": True, "title": "Error"}
                            ]
                        }
                    }
                },
                "position": {"x": 0, "y": 6, "width": 12, "height": 8}
            }
        ]
    }
]

if RUN_QA_CHECKER:
    datasets.extend([
        {
            "name": "ds_qa_severity",
            "displayName": "QA Issues by Severity",
            "query": qa_sev_sql,
        },
        {
            "name": "ds_qa_detail",
            "displayName": "QA Findings Detail",
            "query": qa_detail_sql,
        },
    ])

    pages.append({
        "name": "pg_qa",
        "displayName": "QA Findings",
        "layout": [
            {
                "widget": {
                    "name": "w_qa_bar",
                    "title": "QA Issues by Severity — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_qa_severity",
                            "fields": [
                                {"name": "severity", "expression": "`severity`"},
                                {"name": "issue_count", "expression": "`issue_count`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 2,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "severity", "displayName": "Severity"},
                            "y": {"fieldName": "issue_count", "displayName": "Issue Count"}
                        }
                    }
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 6}
            },
            {
                "widget": {
                    "name": "w_qa_table",
                    "title": "QA Findings Detail — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_qa_detail",
                            "fields": [
                                {"name": "notebook_name", "expression": "`notebook_name`"},
                                {"name": "cell_index", "expression": "`cell_index`"},
                                {"name": "issue_type", "expression": "`issue_type`"},
                                {"name": "severity", "expression": "`severity`"},
                                {"name": "offending_text", "expression": "`offending_text`"},
                                {"name": "suggested_fix", "expression": "`suggested_fix`"},
                                {"name": "check_source", "expression": "`check_source`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 2,
                        "widgetType": "table",
                        "encodings": {
                            "columns": [
                                {"fieldName": "notebook_name", "visible": True, "title": "Notebook"},
                                {"fieldName": "cell_index", "visible": True, "title": "Cell #"},
                                {"fieldName": "issue_type", "visible": True, "title": "Issue Type"},
                                {"fieldName": "severity", "visible": True, "title": "Severity"},
                                {"fieldName": "offending_text", "visible": True, "title": "Offending Text"},
                                {"fieldName": "suggested_fix", "visible": True, "title": "Suggested Fix"},
                                {"fieldName": "check_source", "visible": True, "title": "Source"}
                            ]
                        }
                    }
                },
                "position": {"x": 0, "y": 6, "width": 12, "height": 8}
            }
        ]
    })

spec = {
    "datasets": datasets,
    "pages": pages,
}

# ── Create / re-create the Lakeview Dashboard ─────────────────────────────────
try:
    for d in w.lakeview.list():
        if d.display_name == DASHBOARD_NAME:
            w.lakeview.trash(dashboard_id=d.dashboard_id)
            print(f"Replaced existing dashboard: {d.dashboard_id}")
            break
except Exception:
    pass  # No existing dashboard — proceed to create

dashboard = w.lakeview.create(Dashboard(
    display_name=DASHBOARD_NAME,
    serialized_dashboard=json.dumps(spec),
    parent_path=DASHBOARD_FOLDER,
))
w.lakeview.publish(dashboard_id=dashboard.dashboard_id)

workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
dashboard_url  = f"https://{workspace_host}/dashboardsv3/{dashboard.dashboard_id}"

print(f"✅  Lakeview Dashboard created & published!")
print(f"    Name : {DASHBOARD_NAME}")
print(f"    ID   : {dashboard.dashboard_id}")
print(f"    URL  : {dashboard_url}")

# COMMAND ----------

# Display summary of failed tasks (deduplicated) with detailed error description
failed_rows = []
for r in results:
    if r["status"] != "PASS":
        name = r["demo_name"]
        if not any(row["Task"] == name for row in failed_rows):
            # Fetch detailed error from run output
            error_desc = ""
            try:
                run_output = w.jobs.get_run_output(run_id=r["run_id"])
                error_desc = (run_output.error or "").strip()
                if not error_desc and run_output.error_trace:
                    error_desc = run_output.error_trace.strip().split("\n")[-1]
            except Exception:
                pass
            failed_rows.append({
                "Task": name,
                "Error": r["error_message"] or "Workload failed",
                "Error Description": error_desc or "See run output for details",
            })

if failed_rows:
    failed_df = spark.createDataFrame(failed_rows)
    display(failed_df.select("Task", "Error", "Error Description"))
else:
    print("All tasks passed — no failures to report.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fail the notebook if any task failed

# COMMAND ----------

failed = [r for r in results if r["status"] != "PASS"]
if failed:
    summary = "\n".join(
        f"  - [{r['task_key']}] {r['demo_name']}: {r['status']} ({r['result_state']}) — {r['run_page_url']}"
        for r in failed
    )
    raise RuntimeError(
        f"{len(failed)} of {len(results)} task(s) failed on Serverless v{ENVIRONMENT_VERSION}:\n{summary}"
    )

print(f"All {len(results)} tasks passed on Serverless v{ENVIRONMENT_VERSION}.")
dbutils.notebook.exit(json.dumps({
    "total":  len(results),
    "passed": len(results),
    "failed": 0,
}))