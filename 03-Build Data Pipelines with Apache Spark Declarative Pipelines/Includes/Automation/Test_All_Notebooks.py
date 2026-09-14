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

# MAGIC %md
# MAGIC ## Notebooks with UI Instructions (Auto-Scanned)
# MAGIC
# MAGIC The following cell **dynamically scans** all course notebooks (demos and labs) to detect manual UI steps that cannot be fully automated by the test runner.
# MAGIC
# MAGIC It looks for markdown cells containing UI action patterns (e.g., "Select...", "Click...", "Navigate to...", icon references, etc.) and reports which notebooks have them.
# MAGIC
# MAGIC **This runs fresh on every execution** — no hardcoded values — so it always reflects the latest course content.

# COMMAND ----------

# MAGIC %run ./notebook_path

# COMMAND ----------

# ── Auto-Scan: Notebooks with UI Instructions ───────────────────────────────
# Dynamically scans ALL course notebooks (demos and labs) to detect cells
# containing manual UI steps that cannot be automated by the test runner.
#
# NOT HARDCODED — runs fresh every time so it reflects the latest course content.
# ─────────────────────────────────────────────────────────────────────────────

import base64
import json
import re
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

w = WorkspaceClient()

# Derive course_root from this notebook's own path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
course_root = "/".join(_nb_path.split("/")[:-1])

# ── UI Detection Patterns ─────────────────────────────────────────────────────
# These regex patterns identify markdown cells that contain UI interaction steps.
# They match common instructional language used in Databricks Academy notebooks.

UI_ACTION_PATTERNS = [
    # Direct UI actions
    re.compile(r'\bSelect\s+(the\s+)?\*\*', re.IGNORECASE),          # "Select the **Catalog** icon"
    re.compile(r'\bClick\s+(the\s+|on\s+)?\*\*', re.IGNORECASE),    # "Click the **Run pipeline** button"
    re.compile(r'\bright-click\b', re.IGNORECASE),                    # "right-click on Jobs & Pipelines"
    re.compile(r'\bOpen\s+in\s+(a\s+)?(New|new)\s+Tab\b'),           # "Open in New Tab"
    re.compile(r'\bOpen\s+(Link\s+)?in\s+New\s+(Browser\s+)?Tab\b', re.IGNORECASE),
    # Navigation instructions
    re.compile(r'\b(left|top|main|far-left)\s+navigation\s+(bar|pane)\b', re.IGNORECASE),
    re.compile(r'\bNavigate\s+to\b', re.IGNORECASE),
    re.compile(r'\bExpand\s+(your|the)\s+\*\*', re.IGNORECASE),       # "Expand the **sdp_1_bronze** schema"
    # Icon references (images in markdown = UI screenshot)
    re.compile(r'!\[.*?Icon.*?\]\('),                                 # ![Catalog Icon](./path)
    re.compile(r'!\[.*?(icon|button|select|settings).*?\]\(', re.IGNORECASE),
    # Pipeline editor / Jobs & Pipelines UI
    re.compile(r'\bJobs\s*(&|and)\s*Pipelines\b', re.IGNORECASE),
    re.compile(r'\bLakeflow\s+(Pipelines?\s+)?Editor\b', re.IGNORECASE),
    re.compile(r'\bPipeline\s+(graph|details|settings)\b', re.IGNORECASE),
    re.compile(r'\bOpen\s+in\s+Editor\b', re.IGNORECASE),
    re.compile(r'\bRun\s+pipeline\b', re.IGNORECASE),
    re.compile(r'\bDry\s+Run\b', re.IGNORECASE),
    # Settings and configuration via UI
    re.compile(r'\bgear\s+icon\b', re.IGNORECASE),
    re.compile(r'\bellipsis\s+icon\b', re.IGNORECASE),
    re.compile(r'\bthree-dot\s+menu\b', re.IGNORECASE),
    re.compile(r'\bSelect\s+\*\*Create\*\*', re.IGNORECASE),
    re.compile(r'\bSelect\s+\*\*Settings\*\*', re.IGNORECASE),
    re.compile(r'\bAdd\s+configuration\b', re.IGNORECASE),
    re.compile(r'\bSelect\s+\*\*Save\*\*', re.IGNORECASE),
]

# Minimum number of distinct pattern matches in a cell to qualify as a UI step
MIN_PATTERN_MATCHES = 2


def extract_section_header(content: str) -> str:
    """Extract the markdown section header (##, ###) from a cell's content."""
    # Look for markdown headers
    match = re.search(r'^#{1,4}\s+(.+?)$', content, re.MULTILINE)
    if match:
        # Clean markdown formatting
        header = match.group(1).strip()
        header = re.sub(r'\*\*(.+?)\*\*', r'\1', header)  # Remove bold
        header = re.sub(r'!\[.*?\]\(.*?\)', '', header)    # Remove images
        return header.strip()
    return ""


def get_ui_step_summary(content: str) -> str:
    """Generate a short summary of what UI actions are described in the cell."""
    actions = []
    if re.search(r'\bSelect\s+(the\s+)?\*\*Catalog\*\*', content, re.IGNORECASE):
        actions.append("Navigate Catalog")
    if re.search(r'\bJobs\s*(&|and)\s*Pipelines\b', content, re.IGNORECASE):
        actions.append("Jobs & Pipelines")
    if re.search(r'\bRun\s+pipeline\b', content, re.IGNORECASE):
        actions.append("Run pipeline")
    if re.search(r'\bDry\s+Run\b', content, re.IGNORECASE):
        actions.append("Dry run")
    if re.search(r'\bOpen\s+in\s+Editor\b', content, re.IGNORECASE):
        actions.append("Open in Editor")
    if re.search(r'\bSchedule\b', content, re.IGNORECASE) and re.search(r'\bpipeline\b', content, re.IGNORECASE):
        actions.append("Schedule pipeline")
    if re.search(r'\bSettings\b', content) and re.search(r'\b(Select|gear|configure)\b', content, re.IGNORECASE):
        actions.append("Configure settings")
    if re.search(r'\bExpand\b', content, re.IGNORECASE) and re.search(r'\b(schema|catalog|volume|folder)\b', content, re.IGNORECASE):
        actions.append("Explore catalog/schema")
    if re.search(r'\bCreate\b.*\b(ETL|Pipeline)\b', content, re.IGNORECASE):
        actions.append("Create pipeline")
    if re.search(r'\bright-click\b', content, re.IGNORECASE):
        actions.append("Right-click menu")
    if re.search(r'\bPipeline\s+graph\b', content, re.IGNORECASE):
        actions.append("Pipeline graph")
    if re.search(r'\bevent.*log\b', content, re.IGNORECASE):
        actions.append("Event log")
    if re.search(r'\bEnable\b.*\b(Lakeflow|Editor|feature)\b', content, re.IGNORECASE):
        actions.append("Enable feature")
    return ", ".join(actions) if actions else "UI interaction"


def scan_notebook_for_ui_steps(notebook_path: str) -> list:
    """Scan a notebook for markdown cells containing UI instructions.
    
    Returns a list of dicts with section header and summary for each UI step found.
    """
    ui_steps = []
    
    try:
        export_resp = w.workspace.export(path=notebook_path, format=ExportFormat.JUPYTER)
        nb_json = json.loads(base64.b64decode(export_resp.content))
    except Exception as e:
        return [{"section": "ERROR", "summary": str(e)}]
    
    cells = nb_json.get("cells", [])
    
    for cell in cells:
        source = ''.join(cell.get("source", []))
        
        # Only check markdown cells and code cells with %md magic (used in Databricks)
        is_markdown = cell.get("cell_type") == "markdown"
        is_md_magic = cell.get("cell_type") == "code" and bool(re.match(r'\s*%md', source))
        
        if not (is_markdown or is_md_magic):
            continue
        
        # Count how many distinct UI patterns match in this cell
        matched_patterns = sum(1 for p in UI_ACTION_PATTERNS if p.search(source))
        
        if matched_patterns >= MIN_PATTERN_MATCHES:
            section = extract_section_header(source)
            summary = get_ui_step_summary(source)
            # Avoid duplicate entries for the same section
            if not any(s["section"] == section and section for s in ui_steps):
                ui_steps.append({
                    "section": section or "(unlabeled section)",
                    "summary": summary,
                    "pattern_matches": matched_patterns,
                })
    
    return ui_steps


# ── Scan All Course Notebooks ─────────────────────────────────────────────────
# Only scan Demos and Labs (lectures are presentation-only, no UI steps expected)

print("═" * 70)
print("AUTO-SCAN: NOTEBOOKS WITH UI INSTRUCTIONS")
print("═" * 70)
print("\nScanning all Demo and Lab notebooks for UI instruction patterns...\n")

UI_INSTRUCTION_NOTEBOOKS = {}
total_scanned = 0

for task in COURSE_TASKS:
    notebook_name = task["name"]
    # Scan Demos and Labs (skip Lectures and overview/summary notebooks)
    if not any(keyword in notebook_name for keyword in ["Demo", "Lab"]):
        continue
    
    notebook_path = os.path.normpath(f"{course_root}/{task['relative_path']}")
    total_scanned += 1
    
    ui_steps = scan_notebook_for_ui_steps(notebook_path)
    
    if ui_steps:
        UI_INSTRUCTION_NOTEBOOKS[notebook_name] = ui_steps

# ── Display Results ───────────────────────────────────────────────────────────
print(f"{len(UI_INSTRUCTION_NOTEBOOKS)} of {total_scanned} Demo/Lab notebooks contain UI steps:\n")

for notebook_name, steps in UI_INSTRUCTION_NOTEBOOKS.items():
    print(f"📋 {notebook_name}  ({len(steps)} UI sections)")
    for step in steps:
        section = step['section']
        summary = step['summary']
        print(f"     • {section} — {summary}")
    print()

total_steps = sum(len(s) for s in UI_INSTRUCTION_NOTEBOOKS.values())
print("═" * 70)
print(f"SCANNED: {total_scanned} notebooks  |  WITH UI STEPS: {len(UI_INSTRUCTION_NOTEBOOKS)}  |  TOTAL UI SECTIONS: {total_steps}")
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

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RESULTS_CATALOG}.{results_schema_name}")
except Exception as e:
    if "UNAUTHORIZED_ACCESS" in str(e) or "PERMISSION_DENIED" in str(e):
        # Fall back to user's own labuser catalog (derived from username)
        user_catalog = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
        print(f"⚠️  No permission to create schema in '{RESULTS_CATALOG}'. Falling back to catalog: {user_catalog}")
        RESULTS_CATALOG = user_catalog
        results_fqn = f"{RESULTS_CATALOG}.{results_schema_name}.{RESULTS_TABLE}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RESULTS_CATALOG}.{results_schema_name}")
    else:
        raise

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
            depends_on=[TaskDependency(task_key=_safe_key(dep['task_key'])) for dep in t["depends_on"]],
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
    deps = " → depends on: " + ", ".join(dep["task_key"] for dep in t["depends_on"]) if t["depends_on"] else " (starts immediately)"
    compute_label = "[CLASSIC]" if t.get("use_classic") else "[SERVERLESS]"
    print(f"  {compute_label} {t['task_key']}{deps}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Created 5 Demo pipeline

# COMMAND ----------

# ── Create Lakeflow Spark Declarative Pipeline ─────────────────────────────────
# Creates the "05 - Developing a Simple Pipeline" pipeline programmatically.
# This cell also creates the project folder and writes the pipeline SQL source
# (bronze/silver/gold layers) so the pipeline has code to execute.
# ───────────────────────────────────────────────────────────────────────────────

import base64
from databricks.sdk.service.pipelines import PipelineLibrary
from databricks.sdk.service.workspace import ImportFormat

PIPELINE_NAME = "05 - Developing a Simple Pipeline"

# Derive user catalog and paths
user_catalog = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
# Robustly derive course content root by locating 'Includes' in the notebook path
_this_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_parts = _this_path.split("/")
_includes_idx = next(i for i, p in enumerate(_parts) if p == "Includes")
_course_content_root = "/".join(_parts[:_includes_idx])
pipeline_root = "/Workspace" + _course_content_root + "/05 - Developing a Simple Pipeline Project"
orders_source = f"{pipeline_root}/orders"
volume_path = f"/Volumes/{user_catalog}/sdp_1_bronze/source"

# ── Create project folder and write pipeline SQL ──────────────────────────────
orders_folder_ws = _course_content_root + "/05 - Developing a Simple Pipeline Project/orders"
try:
    w.workspace.mkdirs(orders_folder_ws)
except Exception:
    pass  # folder may already exist

orders_sql = f'''--------------------------------------------------------------------------------------------------------------
-- A. Create the bronze streaming table from the raw JSON files in your source volume
--------------------------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_1_bronze.orders_bronze_demo05
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
    "${{source}}/orders",
    format => 'JSON'
);

--------------------------------------------------------------------------------------------------------------
-- B. Create the silver streaming table from the bronze streaming table
--------------------------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_2_silver.orders_silver_demo05
AS
SELECT
  order_id,
  timestamp(order_timestamp) AS order_timestamp,
  customer_id,
  notifications
FROM STREAM sdp_1_bronze.orders_bronze_demo05;

--------------------------------------------------------------------------------------------------------------
-- C. Create the gold materialized view aggregating the silver table by order date
--------------------------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW sdp_3_gold.gold_orders_by_date_demo05
AS
SELECT
  date(order_timestamp) AS order_date,
  count(*) AS total_daily_orders
FROM sdp_2_silver.orders_silver_demo05
GROUP BY date(order_timestamp);
'''

orders_sql_path = f"{orders_folder_ws}/orders_pipeline.sql"
w.workspace.import_(
    path=orders_sql_path,
    content=base64.b64encode(orders_sql.encode()).decode(),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print(f"Wrote pipeline SQL to: {orders_sql_path}")

# ── Delete existing pipeline with the same name (clean slate) ─────────────────
existing_pipelines = list(w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'"))
for p in existing_pipelines:
    if p.name == PIPELINE_NAME:
        w.pipelines.delete(pipeline_id=p.pipeline_id)
        print(f"Deleted existing pipeline: {p.pipeline_id} ({p.name})")

# ── Create the pipeline ───────────────────────────────────────────────────────
response = w.pipelines.create(
    name=PIPELINE_NAME,
    catalog=user_catalog,
    schema="sdp_1_bronze",
    serverless=True,
    photon=True,
    channel="CURRENT",
    continuous=False,
    root_path=pipeline_root,
    libraries=[PipelineLibrary.from_dict({"glob": {"include": f"{orders_source}/**"}})],
    configuration={"source": volume_path},
)

pipeline_id = response.pipeline_id
print(f"\n✅ Created pipeline: {pipeline_id}")
print(f"   Name: {PIPELINE_NAME}")
print(f"   Catalog: {user_catalog}")
print(f"   Schema: sdp_1_bronze")
print(f"   Root path: {pipeline_root}")
print(f"   Source code: {orders_source}/**")
print(f"   Configuration: source = {volume_path}")
print(f"\n   Pipeline URL: /pipelines/{pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding auto-trigger command for demo 5

# COMMAND ----------

# ── Patch 05 Demo Notebook: Insert Pipeline Trigger Cells ────────────────────
# The 05 Demo notebook has UI instructions like "Go to pipeline editor and click
# Run Pipeline". For automated job execution, we insert Python cells that
# programmatically trigger the pipeline and wait for completion.
#
# Inserts TWO trigger cells:
#   1. Before Section D ("Add a New File") → first pipeline run (processes 00.json)
#   2. After the cell that views 01.json → second pipeline run (processes 01.json)
# ─────────────────────────────────────────────────────────────────────────────────

import base64
import json
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

# The pipeline trigger code to inject
PIPELINE_TRIGGER_CODE = f'''
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PIPELINE_NAME = "{PIPELINE_NAME}"
pipelines_list = list(w.pipelines.list_pipelines(filter=f"name LIKE '{{PIPELINE_NAME}}'"))
pipeline_id = next(p.pipeline_id for p in pipelines_list if p.name == PIPELINE_NAME)

print(f"Triggering pipeline update for: {{PIPELINE_NAME}} ({{pipeline_id}})")
update_response = w.pipelines.start_update(pipeline_id=pipeline_id)
update_id = update_response.update_id
print(f"Update ID: {{update_id}} - waiting for completion...")

while True:
    status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    state = status.update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)

assert state == "COMPLETED", f"Pipeline update failed with state: {{state}}"
print(f"✅ Pipeline update {{update_id}} completed successfully")
'''

# Path to 05 Demo notebook
demo_05_path = os.path.normpath(f"{course_root}/../../05 Demo - Developing a Simple Pipeline")

# Export the notebook as .ipynb
print(f"Exporting: {demo_05_path}")
export_resp = w.workspace.export(path=demo_05_path, format=ExportFormat.JUPYTER)
nb_json = json.loads(base64.b64decode(export_resp.content))
cells = nb_json["cells"]

# Helper: find cell index by content substring
def find_cell_index(cells, marker):
    for i, cell in enumerate(cells):
        if marker in "".join(cell.get("source", [])):
            return i
    return None

# Build the trigger cell template
def make_trigger_cell(comment):
    return {
        "cell_type": "code",
        "source": ["%python\n", f"# {comment}\n"] + [line + "\n" for line in PIPELINE_TRIGGER_CODE.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }

# Find insertion points
# 1. Before "## D. Add a New File" → insert first pipeline trigger
marker_section_d = "## D. Add a New File"
idx_section_d = find_cell_index(cells, marker_section_d)
# ── Language-agnostic fallbacks (for translated course versions) ──────────
if idx_section_d is None:
    idx_section_d = find_cell_index(cells, "## D.")  # Section prefix only
if idx_section_d is None:
    for i, cell in enumerate(cells):
        if cell.get("cell_type") == "code" and "orders_bronze_demo05" in "".join(cell.get("source", [])):
            idx_section_d = i
            print(f"  ℹ️ Fallback: found 'orders_bronze_demo05' query cell at index {i}")
            break

# 2. After the cell that reads 01.json → insert second pipeline trigger
marker_01_json = "orders/01.json"
# Find the LAST code cell referencing 01.json (Cell 33)
idx_01_json = None
for i, cell in enumerate(cells):
    if cell.get("cell_type") == "code" and marker_01_json in "".join(cell.get("source", [])):
        idx_01_json = i

inserted = 0

if idx_section_d is not None:
    trigger_cell_1 = make_trigger_cell("AUTO-INSERTED: Run pipeline first time (processes 00.json → 174 rows)")
    cells.insert(idx_section_d, trigger_cell_1)
    inserted += 1
    print(f"  Inserted first pipeline trigger before Section D (index {idx_section_d})")
else:
    print("  ⚠️ Could not find Section D marker")

if idx_01_json is not None:
    # Adjust index due to first insertion
    adjusted_idx = idx_01_json + inserted + 1  # +1 to insert AFTER the cell
    trigger_cell_2 = make_trigger_cell("AUTO-INSERTED: Run pipeline second time (processes 01.json → +25 rows)")
    cells.insert(adjusted_idx, trigger_cell_2)
    inserted += 1
    print(f"  Inserted second pipeline trigger after 01.json cell (index {adjusted_idx})")
else:
    print("  ⚠️ Could not find 01.json cell marker")

# Re-import the modified notebook
if inserted > 0:
    nb_json["cells"] = cells
    modified_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
    w.workspace.import_(
        path=demo_05_path,
        content=modified_content,
        format=ImportFormat.JUPYTER,
        language=Language.SQL,
        overwrite=True,
    )
    print(f"\n✅ Patched 05 Demo notebook with {inserted} pipeline trigger cell(s)")
else:
    print("\n⚠️ No cells inserted — check markers")

# COMMAND ----------

# ── Patch 07 Demo Notebook: Insert Pipeline Trigger Cell ─────────────────────
# The 07 Demo notebook creates a pipeline in cmd 10 and then has UI instructions
# to "Click Run pipeline". For automated job execution, we insert a Python cell
# after the pipeline creation that triggers the pipeline and waits for completion.
#
# Inserts ONE trigger cell after create_declarative_pipeline() call.
# ─────────────────────────────────────────────────────────────────────────────────

# The pipeline name used in 07 Demo (uses my_catalog which resolves to user_catalog)
PIPELINE_NAME_07 = f"07 - Adding Data Quality Expectations Project - {user_catalog}"

# The pipeline trigger code to inject for 07 Demo
PIPELINE_TRIGGER_CODE_07 = f'''
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PIPELINE_NAME = "{PIPELINE_NAME_07}"
pipelines_list = list(w.pipelines.list_pipelines(filter=f"name LIKE '{{PIPELINE_NAME}}'"))
pipeline_id = next(p.pipeline_id for p in pipelines_list if p.name == PIPELINE_NAME)

print(f"Triggering pipeline update for: {{PIPELINE_NAME}} ({{pipeline_id}})")
update_response = w.pipelines.start_update(pipeline_id=pipeline_id)
update_id = update_response.update_id
print(f"Update ID: {{update_id}} - waiting for completion...")

while True:
    status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    state = status.update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)

assert state == "COMPLETED", f"Pipeline update failed with state: {{state}}"
print(f"\u2705 Pipeline update {{update_id}} completed successfully")
'''

# Path to 07 Demo notebook
demo_07_path = os.path.normpath(f"{course_root}/../../07 Demo - Adding Data Quality Expectations")

# Export the notebook as .ipynb
print(f"Exporting: {demo_07_path}")
export_resp_07 = w.workspace.export(path=demo_07_path, format=ExportFormat.JUPYTER)
nb_json_07 = json.loads(base64.b64decode(export_resp_07.content))
cells_07 = nb_json_07["cells"]

# Find the cell that creates the pipeline (contains create_declarative_pipeline)
idx_create_pipeline = None
for i, cell in enumerate(cells_07):
    if cell.get("cell_type") == "code" and "create_declarative_pipeline(" in "".join(cell.get("source", [])):
        idx_create_pipeline = i
        break

if idx_create_pipeline is not None:
    trigger_cell = {
        "cell_type": "code",
        "source": ["%python\n", "# AUTO-INSERTED: Run pipeline after creation (processes 00.json \u2192 174 rows bronze, 148 silver)\n"] + [line + "\n" for line in PIPELINE_TRIGGER_CODE_07.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }
    cells_07.insert(idx_create_pipeline + 1, trigger_cell)  # Insert AFTER create_declarative_pipeline
    print(f"  Inserted pipeline trigger after create_declarative_pipeline (index {idx_create_pipeline + 1})")

    # Re-import the modified notebook
    nb_json_07["cells"] = cells_07
    modified_content_07 = base64.b64encode(json.dumps(nb_json_07).encode()).decode()
    w.workspace.import_(
        path=demo_07_path,
        content=modified_content_07,
        format=ImportFormat.JUPYTER,
        language=Language.SQL,
        overwrite=True,
    )
    print(f"\n\u2705 Patched 07 Demo notebook with pipeline trigger cell")
else:
    print("\n\u26a0\ufe0f Could not find create_declarative_pipeline() cell in 07 Demo")

# COMMAND ----------

# ── Create 08 Lab Pipeline & Patch Notebook with Auto-Trigger ────────────────
# The 08 Lab has UI instructions to create a pipeline and run it manually.
# This cell:
#   1. Creates the pipeline programmatically with the solution SQL
#   2. Patches the 08 Lab notebook with trigger cells at two points:
#      - Before Section D (first run: processes employees_1.csv → 6 rows bronze, 5 silver)
#      - After Cell 49 adds employees_2.csv (second run: +4 rows bronze, +4 silver)
# ─────────────────────────────────────────────────────────────────────────────────

# ── 1. Create the pipeline source SQL file ────────────────────────────────────
PIPELINE_NAME_08 = f"lab08 - {user_catalog} pipeline project"

# Solution SQL with user's catalog substituted
lab08_sql = f'''-- Bronze: Ingest raw CSV from volume
CREATE OR REFRESH STREAMING TABLE sdp_lab_1_bronze.employees_bronze_lab08
AS
SELECT 
  *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name AS raw_file_name
FROM STREAM read_files(
  '/Volumes/{user_catalog}/sdp_lab_1_bronze/lab_files',
  format => 'CSV',
  schema => '
    EmployeeID STRING,
    FirstName STRING,
    Country STRING,
    Department STRING,
    Salary DOUBLE,
    HireDate DATE,
    Operation STRING,
    ProcessDate DATE
  ',
  header => 'true',
  inferSchema => 'false'
);

-- Silver: Clean and transform with data quality expectations
CREATE OR REFRESH STREAMING TABLE sdp_lab_2_silver.employees_silver_lab08
(
    CONSTRAINT check_country EXPECT (Country IN ('US','GR')),
    CONSTRAINT check_salary EXPECT (Salary > 0),
    CONSTRAINT check_null_id EXPECT (EmployeeID IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
  EmployeeID,
  FirstName,
  upper(Country) AS Country,
  Department,
  Salary,
  HireDate,
  date_format(HireDate, 'MMMM') AS HireMonthName,
  year(HireDate) AS HireYear, 
  Operation
FROM STREAM sdp_lab_1_bronze.employees_bronze_lab08;

-- Gold: Employees by Country
CREATE OR REFRESH MATERIALIZED VIEW sdp_lab_3_gold.employees_by_country_gold_lab08
AS
SELECT 
  Country,
  count(*) AS TotalEmployees,
  sum(Salary) AS TotalSalary
FROM sdp_lab_2_silver.employees_silver_lab08
GROUP BY Country;

-- Gold: Salary by Department
CREATE OR REFRESH MATERIALIZED VIEW sdp_lab_3_gold.salary_by_department_gold_lab08
AS
SELECT
  Department,
  sum(Salary) AS TotalSalary
FROM sdp_lab_2_silver.employees_silver_lab08
GROUP BY Department;
'''

# Create project folder and write SQL file
lab08_project_path = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/lab08_pipeline_project"
lab08_sql_path = f"{lab08_project_path}/my_transformation.sql"

# Ensure folder exists
try:
    w.workspace.mkdirs(lab08_project_path)
except Exception:
    pass  # folder may already exist

# Write the SQL file
import base64
w.workspace.import_(
    path=lab08_sql_path,
    content=base64.b64encode(lab08_sql.encode()).decode(),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print(f"Wrote solution SQL to: {lab08_sql_path}")

# ── 2. Create the pipeline ────────────────────────────────────────────────────
# Delete existing pipeline with the same name
existing = list(w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME_08}'"))
for p in existing:
    if p.name == PIPELINE_NAME_08:
        w.pipelines.delete(pipeline_id=p.pipeline_id)
        print(f"Deleted existing pipeline: {p.pipeline_id}")

response_08 = w.pipelines.create(
    name=PIPELINE_NAME_08,
    catalog=user_catalog,
    schema="sdp_lab_1_bronze",
    serverless=True,
    photon=True,
    channel="CURRENT",
    continuous=False,
    root_path="/Workspace" + lab08_project_path,
    libraries=[PipelineLibrary.from_dict({"glob": {"include": "/Workspace" + lab08_sql_path}})],
    configuration={},
)
pipeline_id_08 = response_08.pipeline_id
print(f"\n\u2705 Created pipeline: {pipeline_id_08} ({PIPELINE_NAME_08})")

# ── 3. Patch 08 Lab notebook with auto-trigger cells ─────────────────────────
PIPELINE_TRIGGER_CODE_08 = f'''
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PIPELINE_NAME = "{PIPELINE_NAME_08}"
pipelines_list = list(w.pipelines.list_pipelines(filter=f"name LIKE '{{PIPELINE_NAME}}'"))
pipeline_id = next(p.pipeline_id for p in pipelines_list if p.name == PIPELINE_NAME)

print(f"Triggering pipeline update for: {{PIPELINE_NAME}} ({{pipeline_id}})")
update_response = w.pipelines.start_update(pipeline_id=pipeline_id)
update_id = update_response.update_id
print(f"Update ID: {{update_id}} - waiting for completion...")

while True:
    status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    state = status.update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)

assert state == "COMPLETED", f"Pipeline update failed with state: {{state}}"
print(f"\u2705 Pipeline update {{update_id}} completed successfully")
'''

# Path to 08 Lab notebook
lab08_path = os.path.normpath(f"{course_root}/../../08 Lab - Create a Pipeline")

print(f"\nExporting: {lab08_path}")
export_resp_08 = w.workspace.export(path=lab08_path, format=ExportFormat.JUPYTER)
nb_json_08 = json.loads(base64.b64decode(export_resp_08.content))
cells_08 = nb_json_08["cells"]

# Find insertion points
# 1. Before Section D: "## D. Explore the Streaming Tables"
marker_section_d_08 = "## D. Explore the Streaming Tables"
idx_section_d_08 = find_cell_index(cells_08, marker_section_d_08)
# ── Language-agnostic fallbacks (for translated course versions) ──────────
if idx_section_d_08 is None:
    idx_section_d_08 = find_cell_index(cells_08, "## D.")  # Section prefix only
if idx_section_d_08 is None:
    for i, cell in enumerate(cells_08):
        if cell.get("cell_type") == "code" and "employees_bronze_lab08" in "".join(cell.get("source", [])):
            idx_section_d_08 = i
            print(f"  ℹ️ Fallback: found 'employees_bronze_lab08' query cell at index {i}")
            break

# 2. After the cell that adds employees_2.csv (copy_workspace_files_to_volume with n=2)
marker_add_file_08 = "copy_workspace_files_to_volume"
idx_add_file_08 = None
for i, cell in enumerate(cells_08):
    if cell.get("cell_type") == "code" and marker_add_file_08 in "".join(cell.get("source", [])):
        idx_add_file_08 = i

inserted_08 = 0

if idx_section_d_08 is not None:
    trigger_1 = {
        "cell_type": "code",
        "source": ["%python\n", "# AUTO-INSERTED: Run pipeline first time (processes employees_1.csv)\n"] + [line + "\n" for line in PIPELINE_TRIGGER_CODE_08.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }
    cells_08.insert(idx_section_d_08, trigger_1)
    inserted_08 += 1
    print(f"  Inserted first pipeline trigger before Section D (index {idx_section_d_08})")
else:
    print("  \u26a0\ufe0f Could not find Section D marker")

if idx_add_file_08 is not None:
    adjusted_idx = idx_add_file_08 + inserted_08 + 1
    trigger_2 = {
        "cell_type": "code",
        "source": ["%python\n", "# AUTO-INSERTED: Run pipeline second time (processes employees_2.csv)\n"] + [line + "\n" for line in PIPELINE_TRIGGER_CODE_08.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }
    cells_08.insert(adjusted_idx, trigger_2)
    inserted_08 += 1
    print(f"  Inserted second pipeline trigger after add-file cell (index {adjusted_idx})")
else:
    print("  \u26a0\ufe0f Could not find copy_workspace_files_to_volume cell")

if inserted_08 > 0:
    nb_json_08["cells"] = cells_08
    modified_content_08 = base64.b64encode(json.dumps(nb_json_08).encode()).decode()
    w.workspace.import_(
        path=lab08_path,
        content=modified_content_08,
        format=ImportFormat.JUPYTER,
        language=Language.SQL,
        overwrite=True,
    )
    print(f"\n\u2705 Patched 08 Lab notebook with {inserted_08} pipeline trigger cell(s)")
else:
    print("\n\u26a0\ufe0f No cells inserted — check markers")

# COMMAND ----------

# ── Patch 10 Demo Notebook: Event Log Config + Pipeline Trigger Cells ────────
# The 10 Demo notebook creates a pipeline via create_declarative_pipeline() in
# cell 15 and has UI instructions to configure event log settings and run the
# pipeline manually. For automated job execution, we insert:
#   1. After create_declarative_pipeline() → update pipeline with event log
#      settings + trigger first pipeline run
#   2. After the copy command → trigger second pipeline run (incremental processing)
# ──────────────────────────────────────────────────────────────────────────────

import os
import base64
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()

PIPELINE_NAME_10 = f"10 - Deploying a Pipeline to Production Project - {user_catalog}"

# ------------------------------------------------------------------------------
# Code injected AFTER create_declarative_pipeline()
# ------------------------------------------------------------------------------
PIPELINE_SETUP_AND_TRIGGER_CODE_10 = f'''
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import EventLogSpec

w = WorkspaceClient()

PIPELINE_NAME = "{PIPELINE_NAME_10}"
pipelines_list = list(w.pipelines.list_pipelines(filter=f"name LIKE '{{PIPELINE_NAME}}'"))
pipeline_id = next(p.pipeline_id for p in pipelines_list if p.name == PIPELINE_NAME)
pipeline_spec = w.pipelines.get(pipeline_id=pipeline_id)
spec = pipeline_spec.spec

# ── Update pipeline with Event Log settings ──────────────────────────────────
print(f"Updating pipeline event log settings for: {{PIPELINE_NAME}} ({{pipeline_id}})")
w.pipelines.update(
    pipeline_id=pipeline_id,
    name=PIPELINE_NAME,
    catalog=my_catalog,
    schema='default',
    serverless=spec.serverless,      # preserve serverless
    root_path=spec.root_path,
    libraries=spec.libraries,
    configuration=spec.configuration,
    channel=spec.channel,
    continuous=spec.continuous,
    event_log=EventLogSpec(
        catalog=my_catalog,
        schema="sdp_1_bronze",
        name="event_log_demo10"
    )
)
print(f"✅ Event log configured: {{my_catalog}}.sdp_1_bronze.event_log_demo10")

# ── Trigger pipeline first run ───────────────────────────────────────────────
print(f"Triggering pipeline update for: {{PIPELINE_NAME}} ({{pipeline_id}})")
update_response = w.pipelines.start_update(pipeline_id=pipeline_id)
update_id = update_response.update_id
print(f"Update ID: {{update_id}} - waiting for completion...")

while True:
    status = w.pipelines.get_update(
        pipeline_id=pipeline_id,
        update_id=update_id
    )
    state = status.update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)

assert state == "COMPLETED", f"Pipeline update failed with state: {{state}}"
print(f"✅ Pipeline update {{update_id}} completed successfully")
'''

# ------------------------------------------------------------------------------
# Code injected AFTER copy_workspace_files_to_volume()
# ------------------------------------------------------------------------------
PIPELINE_TRIGGER_CODE_10 = f'''
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PIPELINE_NAME = "{PIPELINE_NAME_10}"
pipelines_list = list(w.pipelines.list_pipelines(filter=f"name LIKE '{{PIPELINE_NAME}}'"))
pipeline_id = next(p.pipeline_id for p in pipelines_list if p.name == PIPELINE_NAME)

print(f"Triggering pipeline update for: {{PIPELINE_NAME}} ({{pipeline_id}})")
update_response = w.pipelines.start_update(pipeline_id=pipeline_id)
update_id = update_response.update_id
print(f"Update ID: {{update_id}} - waiting for completion...")

while True:
    status = w.pipelines.get_update(
        pipeline_id=pipeline_id,
        update_id=update_id
    )
    state = status.update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)

assert state == "COMPLETED", f"Pipeline update failed with state: {{state}}"
print(f"✅ Pipeline update {{update_id}} completed successfully")
'''

# ------------------------------------------------------------------------------
# Export notebook
# ------------------------------------------------------------------------------
demo_10_path = os.path.normpath(
    f"{course_root}/../../10 Demo - Deploying a Pipeline to Production"
)

print(f"Exporting: {demo_10_path}")

export_resp_10 = w.workspace.export(
    path=demo_10_path,
    format=ExportFormat.JUPYTER
)

nb_json_10 = json.loads(base64.b64decode(export_resp_10.content))
cells_10 = nb_json_10["cells"]

# ------------------------------------------------------------------------------
# Locate insertion points
# ------------------------------------------------------------------------------

idx_create_pipeline_10 = None
for i, cell in enumerate(cells_10):
    if (
        cell.get("cell_type") == "code"
        and "create_declarative_pipeline(" in "".join(cell.get("source", []))
    ):
        idx_create_pipeline_10 = i
        break

idx_copy_10 = None
for i, cell in enumerate(cells_10):
    if (
        cell.get("cell_type") == "code"
        and "copy_workspace_files_to_volume" in "".join(cell.get("source", []))
    ):
        idx_copy_10 = i

inserted_10 = 0

# ------------------------------------------------------------------------------
# Insert first auto-generated cell
# ------------------------------------------------------------------------------
if idx_create_pipeline_10 is not None:
    setup_trigger_cell = {
        "cell_type": "code",
        "source": [
            "%python\n",
            "# AUTO-INSERTED: Configure event log + Run pipeline first time\n",
        ] + [line + "\n" for line in PIPELINE_SETUP_AND_TRIGGER_CODE_10.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }

    cells_10.insert(idx_create_pipeline_10 + 1, setup_trigger_cell)
    inserted_10 += 1

    print(
        f"Inserted event log config + first pipeline trigger after create_declarative_pipeline (index {idx_create_pipeline_10 + 1})"
    )
else:
    print("⚠️ Could not find create_declarative_pipeline() cell.")

# ------------------------------------------------------------------------------
# Insert second auto-generated cell
# ------------------------------------------------------------------------------
if idx_copy_10 is not None:
    adjusted_idx = idx_copy_10 + inserted_10 + 1

    trigger_cell_2 = {
        "cell_type": "code",
        "source": [
            "%python\n",
            "# AUTO-INSERTED: Run pipeline second time (incremental processing of new files)\n",
        ] + [line + "\n" for line in PIPELINE_TRIGGER_CODE_10.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }

    cells_10.insert(adjusted_idx, trigger_cell_2)
    inserted_10 += 1

    print(
        f"Inserted second pipeline trigger after copy command (index {adjusted_idx})"
    )
else:
    print("⚠️ Could not find copy_workspace_files_to_volume() cell.")

# ------------------------------------------------------------------------------
# Import modified notebook
# ------------------------------------------------------------------------------
if inserted_10 > 0:
    nb_json_10["cells"] = cells_10

    modified_content_10 = base64.b64encode(
        json.dumps(nb_json_10).encode()
    ).decode()

    w.workspace.import_(
        path=demo_10_path,
        content=modified_content_10,
        format=ImportFormat.JUPYTER,
        language=Language.SQL,
        overwrite=True,
    )

    print(
        f"\n✅ Successfully patched 10 Demo notebook with {inserted_10} auto-inserted cell(s)."
    )
else:
    print("\n⚠️ No cells were inserted. Please verify the notebook markers.")

# COMMAND ----------

# ── Patch 12 Demo Notebook: Insert Pipeline Trigger Cells ───────────────────
# The 12 Demo notebook creates a pipeline via create_declarative_pipeline() in
# cmd 15 and has UI instructions to run the pipeline manually at two points:
#   1. After cmd 24 (D5. Explore the Pipeline Graph) → trigger first pipeline run
#   2. After cmd 42 (E1. Run the SDP with the New File) → trigger second pipeline run (incremental CDC)
# ─────────────────────────────────────────────────────────────────────────────────

import os
import base64
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()

PIPELINE_NAME_12 = f"12 - Change Data Capture with AUTO CDC - {user_catalog}"

# Pipeline trigger code for 12 Demo
PIPELINE_TRIGGER_CODE_12 = f'''
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PIPELINE_NAME = "{PIPELINE_NAME_12}"
pipelines_list = list(w.pipelines.list_pipelines(filter=f"name LIKE '{{PIPELINE_NAME}}'"))
pipeline_id = next(p.pipeline_id for p in pipelines_list if p.name == PIPELINE_NAME)

print(f"Triggering pipeline update for: {{PIPELINE_NAME}} ({{pipeline_id}})")
update_response = w.pipelines.start_update(pipeline_id=pipeline_id)
update_id = update_response.update_id
print(f"Update ID: {{update_id}} - waiting for completion...")

while True:
    status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    state = status.update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)

assert state == "COMPLETED", f"Pipeline update failed with state: {{state}}"
print(f"✅ Pipeline update {{update_id}} completed successfully")
'''

# ------------------------------------------------------------------------------
# Export notebook
# ------------------------------------------------------------------------------
demo_12_path = os.path.normpath(
    f"{course_root}/../../12 Demo - Change Data Capture with AUTO CDC with SCD TYPE 1"
)

print(f"Exporting: {demo_12_path}")

export_resp_12 = w.workspace.export(
    path=demo_12_path,
    format=ExportFormat.JUPYTER
)

nb_json_12 = json.loads(base64.b64decode(export_resp_12.content))
cells_12 = nb_json_12["cells"]

# ------------------------------------------------------------------------------
# Locate insertion points
# ------------------------------------------------------------------------------

# 1. After cmd 24: markdown containing "D5. STEP 4"
idx_d5_step4_12 = None
for i, cell in enumerate(cells_12):
    if cell.get("cell_type") == "markdown" and "D5. STEP 4" in "".join(cell.get("source", [])):
        idx_d5_step4_12 = i
        break
# ── Language-agnostic fallbacks ────────────────────────────────────────────
if idx_d5_step4_12 is None:
    for i, cell in enumerate(cells_12):
        if cell.get("cell_type") == "markdown" and "D5." in "".join(cell.get("source", [])):
            idx_d5_step4_12 = i
            print(f"  ℹ️ Fallback: found 'D5.' section at index {i}")
            break
if idx_d5_step4_12 is None:
    # Code-based: first SELECT query after create_declarative_pipeline
    past_create = False
    for i, cell in enumerate(cells_12):
        src = "".join(cell.get("source", []))
        if "create_declarative_pipeline(" in src:
            past_create = True
            continue
        if past_create and cell.get("cell_type") == "code" and "SELECT" in src.upper():
            idx_d5_step4_12 = i
            print(f"  ℹ️ Fallback: first SELECT after pipeline creation at index {i}")
            break

# 2. After cmd 42: markdown containing "E1. Run the SDP with the New File"
idx_e1_run_12 = None
for i, cell in enumerate(cells_12):
    if (
        cell.get("cell_type") == "markdown"
        and "E1. Run the SDP with the New File" in "".join(cell.get("source", []))
    ):
        idx_e1_run_12 = i
        break
# ── Language-agnostic fallbacks ────────────────────────────────────────────
if idx_e1_run_12 is None:
    for i, cell in enumerate(cells_12):
        if cell.get("cell_type") == "markdown" and "E1." in "".join(cell.get("source", [])):
            idx_e1_run_12 = i
            print(f"  ℹ️ Fallback: found 'E1.' section at index {i}")
            break
if idx_e1_run_12 is None:
    for i, cell in enumerate(cells_12):
        if cell.get("cell_type") == "code" and "copy_workspace_files_to_volume" in "".join(cell.get("source", [])):
            idx_e1_run_12 = i
            print(f"  ℹ️ Fallback: found 'copy_workspace_files_to_volume' at index {i}")
            break

inserted_12 = 0

# ------------------------------------------------------------------------------
# Insert first trigger cell (after cmd 24 - first pipeline run)
# ------------------------------------------------------------------------------
if idx_d5_step4_12 is not None:
    trigger_1_12 = {
        "cell_type": "code",
        "source": [
            "%python\n",
            "# AUTO-INSERTED: Run pipeline first time (processes initial 00.json files)\n",
        ] + [line + "\n" for line in PIPELINE_TRIGGER_CODE_12.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }
    cells_12.insert(idx_d5_step4_12 + 1, trigger_1_12)
    inserted_12 += 1
    print(f"  Inserted first pipeline trigger after cmd 24 / D5. STEP 4 (index {idx_d5_step4_12 + 1})")
else:
    print("  ⚠️ Could not find D5. STEP 4 marker")

# ------------------------------------------------------------------------------
# Insert second trigger cell (after cmd 42 - second pipeline run for CDC)
# ------------------------------------------------------------------------------
if idx_e1_run_12 is not None:
    adjusted_idx = idx_e1_run_12 + inserted_12 + 1
    trigger_2_12 = {
        "cell_type": "code",
        "source": [
            "%python\n",
            "# AUTO-INSERTED: Run pipeline second time (incremental CDC processing of 01.json)\n",
        ] + [line + "\n" for line in PIPELINE_TRIGGER_CODE_12.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }
    cells_12.insert(adjusted_idx, trigger_2_12)
    inserted_12 += 1
    print(f"  Inserted second pipeline trigger after cmd 42 / E1. Run the SDP (index {adjusted_idx})")
else:
    print("  ⚠️ Could not find 'E1. Run the SDP with the New File' cell")

# ------------------------------------------------------------------------------
# Import modified notebook
# ------------------------------------------------------------------------------
if inserted_12 > 0:
    nb_json_12["cells"] = cells_12
    modified_content_12 = base64.b64encode(
        json.dumps(nb_json_12).encode()
    ).decode()

    w.workspace.import_(
        path=demo_12_path,
        content=modified_content_12,
        format=ImportFormat.JUPYTER,
        language=Language.SQL,
        overwrite=True,
    )
    print(f"\n✅ Successfully patched 12 Demo notebook with {inserted_12} auto-inserted pipeline trigger cell(s).")
else:
    print("\n⚠️ No cells were inserted. Please verify the notebook markers.")

# COMMAND ----------

# ── Patch 14 Bonus Lab: Write Solution SQL + Insert Pipeline Trigger Cells ───
# The 14 Bonus Lab notebook creates a pipeline via create_declarative_pipeline()
# in cmd 18 and has UI instructions to:
#   - Complete cdc_employees.sql with AUTO CDC INTO solution code (cmd 22)
#   - Run the pipeline manually after completing the code
# This patch:
#   1. Writes the solution code from cmd 22 to cdc_employees.sql in the pipeline project
#   2. After cmd 23 (E. Explore CDC SCD Type 1) → trigger first pipeline run
#   3. After cmd 31 (Go back to pipeline and Run) → trigger second pipeline run (challenge)
# ─────────────────────────────────────────────────────────────────────────────────

import os
import base64
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()

PIPELINE_NAME_14 = f"14 - CDC Lab Starter Project - {user_catalog}"

# ------------------------------------------------------------------------------
# 1. Write solution code to cdc_employees.sql
# ------------------------------------------------------------------------------
# The cdc_employees.sql file already has bronze and silver code.
# We need to append the AUTO CDC INTO solution from cmd 22.

cdc_project_path = os.path.normpath(
    f"{course_root}/../../14 - CDC Lab Starter Project"
)
cdc_sql_file_path = f"{cdc_project_path}/cdc_type_1_pipeline/cdc_employees.sql"

print(f"Reading existing SQL file: {cdc_sql_file_path}")

# Export the existing file
try:
    existing_export = w.workspace.export(path=cdc_sql_file_path, format=ExportFormat.AUTO)
    existing_content = base64.b64decode(existing_export.content).decode()
    print(f"  Existing file length: {len(existing_content)} chars")
except Exception as e:
    existing_content = ""
    print(f"  File not found or empty, will write fresh: {e}")

# Solution code from cmd 22
solution_sql = '''
-- Create the empty streaming table
CREATE OR REFRESH STREAMING TABLE sdp_lab_2_silver.current_employees_silver_demo14;

-- Perform CDC SCD Type 1
CREATE FLOW scd_type_1_flow AS
AUTO CDC INTO sdp_lab_2_silver.current_employees_silver_demo14
FROM STREAM sdp_lab_1_bronze.employees_bronze_clean_demo14
KEYS (EmployeeID)
APPLY AS DELETE WHEN Operation = 'delete'
SEQUENCE BY ProcessDate
COLUMNS * EXCEPT (Operation)
STORED AS SCD TYPE 1;
'''

# Append solution code if not already present
# NOTE: The file contains commented-out placeholder "-- AUTO CDC INTO<FILL-IN>",
# so we check for the actual uncommented solution line instead.
if "STORED AS SCD TYPE 1;" not in existing_content or "<FILL-IN>" in existing_content:
    updated_content = existing_content.rstrip() + "\n" + solution_sql
    w.workspace.import_(
        path=cdc_sql_file_path,
        content=base64.b64encode(updated_content.encode()).decode(),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    print(f"  ✅ Appended AUTO CDC INTO solution to: {cdc_sql_file_path}")
else:
    print(f"  ℹ️ Solution code already present in cdc_employees.sql — skipping write")

# ------------------------------------------------------------------------------
# 2. Pipeline trigger code for 14 Bonus Lab
# ------------------------------------------------------------------------------
PIPELINE_TRIGGER_CODE_14 = f'''
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PIPELINE_NAME = "{PIPELINE_NAME_14}"
pipelines_list = list(w.pipelines.list_pipelines(filter=f"name LIKE '{{PIPELINE_NAME}}'"))
pipeline_id = next(p.pipeline_id for p in pipelines_list if p.name == PIPELINE_NAME)

print(f"Triggering pipeline update for: {{PIPELINE_NAME}} ({{pipeline_id}})")
update_response = w.pipelines.start_update(pipeline_id=pipeline_id)
update_id = update_response.update_id
print(f"Update ID: {{update_id}} - waiting for completion...")

while True:
    status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    state = status.update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)

assert state == "COMPLETED", f"Pipeline update failed with state: {{state}}"
print(f"✅ Pipeline update {{update_id}} completed successfully")
'''

# ------------------------------------------------------------------------------
# 3. Export 14 Bonus Lab notebook
# ------------------------------------------------------------------------------
demo_14_path = os.path.normpath(
    f"{course_root}/../../14 Bonus Lab - AUTO CDC INTO with SCD Type 1"
)

print(f"\nExporting: {demo_14_path}")

export_resp_14 = w.workspace.export(
    path=demo_14_path,
    format=ExportFormat.JUPYTER
)

nb_json_14 = json.loads(base64.b64decode(export_resp_14.content))
cells_14 = nb_json_14["cells"]

# ------------------------------------------------------------------------------
# 4. Locate insertion points
# ------------------------------------------------------------------------------

# After cmd 23: markdown containing "E. Explore Your CDC SCD Type 1 Streaming Table"
idx_section_e_14 = None
for i, cell in enumerate(cells_14):
    if (
        cell.get("cell_type") == "markdown"
        and "E. Explore Your CDC SCD Type 1 Streaming Table" in "".join(cell.get("source", []))
    ):
        idx_section_e_14 = i
        break
# ── Language-agnostic fallbacks ────────────────────────────────────────────
if idx_section_e_14 is None:
    for i, cell in enumerate(cells_14):
        if cell.get("cell_type") == "markdown" and re.search(r'##\s+E\.', "".join(cell.get("source", []))):
            idx_section_e_14 = i
            print(f"  ℹ️ Fallback: found '## E.' section at index {i}")
            break
if idx_section_e_14 is None:
    for i, cell in enumerate(cells_14):
        if cell.get("cell_type") == "code" and "current_employees_silver_demo14" in "".join(cell.get("source", [])):
            idx_section_e_14 = i
            print(f"  ℹ️ Fallback: found 'current_employees_silver_demo14' query at index {i}")
            break

# After cmd 31: markdown containing "Go back to your pipeline and select"
idx_run_pipeline_14 = None
for i, cell in enumerate(cells_14):
    if (
        cell.get("cell_type") == "markdown"
        and "Go back to your pipeline and select" in "".join(cell.get("source", []))
    ):
        idx_run_pipeline_14 = i
        break
# ── Language-agnostic fallbacks ────────────────────────────────────────────
if idx_run_pipeline_14 is None:
    # Search for any markdown cell about pipeline/run after Section E
    search_start = (idx_section_e_14 or 0) + 2
    for i in range(search_start, len(cells_14)):
        cell = cells_14[i]
        if cell.get("cell_type") == "markdown":
            src = "".join(cell.get("source", []))
            if "pipeline" in src.lower() and re.search(r'\b(run|Run|select|Select)\b', src):
                idx_run_pipeline_14 = i
                print(f"  ℹ️ Fallback: found pipeline run instruction at index {i}")
                break
if idx_run_pipeline_14 is None:
    for i, cell in enumerate(cells_14):
        if cell.get("cell_type") == "code" and "copy_workspace_files_to_volume" in "".join(cell.get("source", [])):
            idx_run_pipeline_14 = i
            print(f"  ℹ️ Fallback: found 'copy_workspace_files_to_volume' at index {i}")
            break

inserted_14 = 0

# ------------------------------------------------------------------------------
# 5. Insert first trigger cell (after cmd 23 - first pipeline run)
# ------------------------------------------------------------------------------
if idx_section_e_14 is not None:
    trigger_1_14 = {
        "cell_type": "code",
        "source": [
            "%python\n",
            "# AUTO-INSERTED: Run pipeline first time (processes employees_1.csv and employees_2.csv)\n",
        ] + [line + "\n" for line in PIPELINE_TRIGGER_CODE_14.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }
    cells_14.insert(idx_section_e_14 + 1, trigger_1_14)
    inserted_14 += 1
    print(f"  Inserted first pipeline trigger after cmd 23 / Section E (index {idx_section_e_14 + 1})")
else:
    print("  ⚠️ Could not find Section E marker")

# ------------------------------------------------------------------------------
# 6. Insert second trigger cell (after cmd 31 - second pipeline run for challenge)
# ------------------------------------------------------------------------------
if idx_run_pipeline_14 is not None:
    adjusted_idx = idx_run_pipeline_14 + inserted_14 + 1
    trigger_2_14 = {
        "cell_type": "code",
        "source": [
            "%python\n",
            "# AUTO-INSERTED: Run pipeline second time (challenge - processes employees_3.csv)\n",
        ] + [line + "\n" for line in PIPELINE_TRIGGER_CODE_14.strip().split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }
    cells_14.insert(adjusted_idx, trigger_2_14)
    inserted_14 += 1
    print(f"  Inserted second pipeline trigger after cmd 31 / Run pipeline (index {adjusted_idx})")
else:
    print("  ⚠️ Could not find 'Go back to your pipeline and select Run pipeline' cell")

# ------------------------------------------------------------------------------
# 7. Import modified notebook
# ------------------------------------------------------------------------------
if inserted_14 > 0:
    nb_json_14["cells"] = cells_14
    modified_content_14 = base64.b64encode(
        json.dumps(nb_json_14).encode()
    ).decode()

    w.workspace.import_(
        path=demo_14_path,
        content=modified_content_14,
        format=ImportFormat.JUPYTER,
        language=Language.SQL,
        overwrite=True,
    )
    print(f"\n✅ Successfully patched 14 Bonus Lab notebook with {inserted_14} auto-inserted pipeline trigger cell(s).")
    print(f"   Solution SQL written to: {cdc_sql_file_path}")
else:
    print("\n⚠️ No cells were inserted. Please verify the notebook markers.")

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
# The QA checker may write to the 'default' schema — check both locations
if not spark.catalog.tableExists(qa_findings_fqn):
    qa_findings_alt = f"{RESULTS_CATALOG}.default.{QA_FINDINGS_TABLE}"
    if spark.catalog.tableExists(qa_findings_alt):
        qa_findings_fqn = qa_findings_alt
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
                        "version": 3,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "status", "scale": {"type": "categorical"}},
                            "y": {"fieldName": "task_count", "scale": {"type": "quantitative"}}
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
                                {"fieldName": "run_group"},
                                {"fieldName": "task_key"},
                                {"fieldName": "demo_name"},
                                {"fieldName": "status"},
                                {"fieldName": "duration_seconds"},
                                {"fieldName": "run_page_url"},
                                {"fieldName": "error_message"}
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
                        "version": 3,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "severity", "scale": {"type": "categorical"}},
                            "y": {"fieldName": "issue_count", "scale": {"type": "quantitative"}}
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
                                {"fieldName": "notebook_name"},
                                {"fieldName": "cell_index"},
                                {"fieldName": "issue_type"},
                                {"fieldName": "severity"},
                                {"fieldName": "offending_text"},
                                {"fieldName": "suggested_fix"},
                                {"fieldName": "check_source"}
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

# ── Auto-discover a SQL warehouse for the dashboard ───────────────────────────
from databricks.sdk.service.sql import State as WarehouseState

warehouse_id = None
try:
    for wh in w.warehouses.list():
        if wh.state in (WarehouseState.RUNNING, WarehouseState.STOPPED):
            warehouse_id = wh.id
            if wh.state == WarehouseState.RUNNING:
                break  # Prefer a running warehouse
except Exception as e:
    print(f"⚠️  Could not list warehouses: {e}")

if warehouse_id:
    print(f"Using SQL warehouse: {warehouse_id}")
else:
    print("⚠️  No SQL warehouse found — dashboard will have no data until one is assigned.")

# ── Create / re-create the Lakeview Dashboard ─────────────────────────────────
try:
    for d in w.lakeview.list():
        if d.display_name == DASHBOARD_NAME:
            w.lakeview.trash(dashboard_id=d.dashboard_id)
            print(f"Replaced existing dashboard: {d.dashboard_id}")
            break
except Exception:
    pass  # No existing dashboard — proceed to create

# ── Create / re-create the Lakeview Dashboard ─────────────────────────────────
# Clean up: remove any existing dashboard (API-created or file-based)
try:
    for d in w.lakeview.list():
        if d.display_name == DASHBOARD_NAME:
            state = str(d.lifecycle_state).upper() if d.lifecycle_state else ""
            if "TRASH" not in state:
                w.lakeview.trash(dashboard_id=d.dashboard_id)
                print(f"Replaced existing dashboard: {d.dashboard_id}")
except Exception:
    pass

# Also remove any .lvdash.json file with the same name
try:
    w.workspace.delete(path=f"{DASHBOARD_FOLDER}/{DASHBOARD_NAME}.lvdash.json")
except Exception:
    pass

dashboard = w.lakeview.create(Dashboard(
    display_name=DASHBOARD_NAME,
    serialized_dashboard=json.dumps(spec),
    parent_path=DASHBOARD_FOLDER,
    warehouse_id=warehouse_id,
))
dashboard_id = dashboard.dashboard_id
w.lakeview.publish(dashboard_id=dashboard_id, warehouse_id=warehouse_id, embed_credentials=True)

workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
dashboard_url  = f"https://{workspace_host}/dashboardsv3/{dashboard_id}"

print(f"✅  Lakeview Dashboard created & published!")
print(f"    Name : {DASHBOARD_NAME}")
print(f"    ID   : {dashboard_id}")
print(f"    URL  : {dashboard_url}")
print(f"\n    ⚠️  NOTE: Lakeview API limitation — widget visualizations require one-time")
print(f"    activation via the dashboard editor. Open the dashboard and re-save widgets.")

# ── Inline Visualization (always works, regardless of dashboard rendering) ────
print("\n" + "═" * 70)
print("INLINE RESULTS VISUALIZATION")
print("═" * 70)

# Status summary chart
status_df = spark.sql(status_summary_sql)
display(status_df)

# QA severity chart (if table exists)
if spark.catalog.tableExists(qa_findings_fqn):
    qa_df = spark.sql(qa_sev_sql)
    display(qa_df)

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