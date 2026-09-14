# Databricks notebook source
# MAGIC %md
# MAGIC # QA Content Checker — Grammar, Spelling, Formatting & Deprecated Features
# MAGIC
# MAGIC Automated quality checks on all content notebooks using:
# MAGIC - **Regex rules** for deterministic formatting, casing, and structure checks
# MAGIC - **LLM (`ai_query`)** for grammar, spelling, deprecated features, and contextual analysis
# MAGIC - **UI step detection** to flag manual actions that need human execution
# MAGIC
# MAGIC **Output:** All findings written to a Delta table for tracking.
# MAGIC
# MAGIC **Independence:** This task runs with `depends_on: []` — it does not depend on any other task in the job.

# COMMAND ----------

# ── Configuration ─────────────────────────────────────────────────────────────
import os

# Results table
RESULTS_CATALOG = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
RESULTS_SCHEMA  = "default"
QA_FINDINGS_TABLE = "qa_content_findings"

# LLM model for grammar/spelling/deprecated checks
LLM_MODEL = "databricks-claude-sonnet-4-5"

# Derive course root from this notebook's path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
course_root = "/".join(_nb_path.split("/")[:-2])  # Go up from CourseRunner to course root

# Notebooks to SKIP (setup, solutions, agenda, course runner)
SKIP_PATTERNS = ["SETUP", "Solution", "SOLUTION", "Agenda", "AGENDA", "CourseRunner", "run_all"]

# Content notebooks to CHECK (Demo + Lab)
INCLUDE_PATTERNS = ["Demo", "Lab"]

print(f"Course root: {course_root}")
print(f"QA findings table: {RESULTS_CATALOG}.{RESULTS_SCHEMA}.{QA_FINDINGS_TABLE}")
print(f"LLM model: {LLM_MODEL}")

# COMMAND ----------

import base64
import json
import re
import html as html_lib
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectType
from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, TimestampType
)

w = WorkspaceClient()
run_timestamp = datetime.now(timezone.utc)

# COMMAND ----------

# ── Discover all content notebooks ────────────────────────────────────────────
def list_notebooks_recursive(path):
    """Recursively list all notebooks under a path."""
    notebooks = []
    try:
        objects = w.workspace.list(path=path)
        for obj in objects:
            if obj.object_type == ObjectType.NOTEBOOK:
                notebooks.append(obj.path)
            elif obj.object_type == ObjectType.DIRECTORY:
                notebooks.extend(list_notebooks_recursive(obj.path))
    except Exception as e:
        print(f"  Warning: Could not list {path}: {e}")
    return notebooks


def should_check(notebook_path):
    """Return True if this notebook should be checked (Demo or Lab, not skipped)."""
    name = notebook_path.split("/")[-1]
    # Skip patterns
    for skip in SKIP_PATTERNS:
        if skip.lower() in name.lower():
            return False
    # Must match at least one include pattern
    for inc in INCLUDE_PATTERNS:
        if inc.lower() in name.lower():
            return True
    return False


all_notebooks = list_notebooks_recursive(course_root)
content_notebooks = [nb for nb in sorted(all_notebooks) if should_check(nb)]

print(f"Total notebooks found: {len(all_notebooks)}")
print(f"Content notebooks to check: {len(content_notebooks)}")
print()
for nb in content_notebooks:
    print(f"  ✓ {nb.split('/')[-1]}")

# COMMAND ----------

# ── Deterministic Regex Rules ─────────────────────────────────────────────────

# Product name casing rules (wrong → correct)
CASING_RULES = {
    r'\bdatabricks\b': 'Databricks',
    r'\bunity catalog\b': 'Unity Catalog',
    r'\bdelta lake\b': 'Delta Lake',
    r'\bPyspark\b': 'PySpark',
    r'\bpySpark\b': 'PySpark',
    r'\bSQl\b': 'SQL',
    r'\bMlflow\b': 'MLflow',
    r'\bmlFlow\b': 'MLflow',
    r'\bAutoml\b': 'AutoML',
    r'\bphoton\b(?!\s*=)': 'Photon',  # Not in code assignments
    r'\blakehouse\b(?!\.)': 'Lakehouse',  # Not in paths
}

# Known deprecated terms (regex pattern → suggested replacement)
DEPRECATED_TERMS = {
    r'\bDelta Live Tables\b': 'Lakeflow Spark Declarative Pipelines',
    r'\bDLT\b(?! pipeline)': 'SDP (Spark Declarative Pipelines)',
    r'\bWorkflows\b(?! tab)': 'Lakeflow Jobs',
    r'\bDatabricks Asset Bundles\b': 'Declarative Automation Bundles',
    r'\bDABs\b': 'Declarative Automation Bundles',
    r'\bRepos\b(?! folder)': 'Git Folders',
    r'dbutils\.fs\.mount': 'Unity Catalog Volumes / External Locations',
    r'dbutils\.library\.install': '%pip install',
    r'dbutils\.credentials': 'Unity Catalog storage credentials',
    r'\bdbfs:/': '/Volumes/catalog/schema/volume/',
    r'\bhive_metastore\.': 'Unity Catalog (3-level namespace)',
    r'\bimport koalas\b': 'import pyspark.pandas',
    r'\bimport databricks\.koalas\b': 'import pyspark.pandas',
    r'\bcredential.passthrough\b': 'Unity Catalog',
    r'\bMLflow Model Registry \(Workspace\)\b': 'Unity Catalog Model Registry',
}

# UI step indicators (patterns suggesting manual action)
UI_STEP_PATTERNS = [
    r'(?i)\bclick\s+(on\s+)?(the\s+)?["\']?\w+',
    r'(?i)\bnavigate\s+to\b',
    r'(?i)\bgo\s+to\s+the\s+\w+\s+(page|tab|panel|sidebar|menu)\b',
    r'(?i)\bselect\s+from\s+(the\s+)?(dropdown|menu|sidebar)\b',
    r'(?i)\bin\s+the\s+(left|right)\s+sidebar\b',
    r'(?i)\bopen\s+the\s+\w+\s+(UI|interface|portal|console)\b',
    r'(?i)\busing\s+the\s+UI\b',
    r'(?i)\bmanually\s+(run|execute|create|configure|set)\b',
    r'(?i)\bdrag\s+and\s+drop\b',
    r'(?i)\bright[- ]click\b',
    r'(?i)\bcheck\s+the\s+box\b',
    r'(?i)\btoggle\s+(the\s+)?\w+\s+(on|off)\b',
]

print(f"Casing rules: {len(CASING_RULES)}")
print(f"Deprecated term rules: {len(DEPRECATED_TERMS)}")
print(f"UI step patterns: {len(UI_STEP_PATTERNS)}")

# COMMAND ----------

# ── Check functions ───────────────────────────────────────────────────────────

def check_casing(text, notebook_path, cell_idx):
    """Check for product name casing issues."""
    findings = []
    for pattern, correct in CASING_RULES.items():
        # Case-sensitive search (we want to find the wrong casing)
        matches = re.finditer(pattern, text)
        for m in matches:
            # Skip if inside code blocks or backticks
            if _in_code_block(text, m.start()):
                continue
            findings.append({
                "notebook_path": notebook_path,
                "notebook_name": notebook_path.split('/')[-1],
                "cell_index": cell_idx,
                "issue_type": "CASING",
                "severity": "WARNING",
                "offending_text": m.group(),
                "suggested_fix": f"Change to: {correct}",
                "check_source": "REGEX",
            })
    return findings


def check_deprecated_terms(text, notebook_path, cell_idx):
    """Check for deprecated Databricks features/naming."""
    findings = []
    for pattern, replacement in DEPRECATED_TERMS.items():
        matches = re.finditer(pattern, text)
        for m in matches:
            findings.append({
                "notebook_path": notebook_path,
                "notebook_name": notebook_path.split('/')[-1],
                "cell_index": cell_idx,
                "issue_type": "DEPRECATED",
                "severity": "ERROR",
                "offending_text": m.group(),
                "suggested_fix": f"Replace with: {replacement}",
                "check_source": "REGEX",
            })
    return findings


def check_ui_steps(text, notebook_path, cell_idx):
    """Detect UI/manual steps that require human interaction."""
    findings = []
    for pattern in UI_STEP_PATTERNS:
        matches = re.finditer(pattern, text)
        for m in matches:
            # Get surrounding context (up to 100 chars)
            start = max(0, m.start() - 30)
            end = min(len(text), m.end() + 70)
            context = text[start:end].replace('\n', ' ').strip()
            findings.append({
                "notebook_path": notebook_path,
                "notebook_name": notebook_path.split('/')[-1],
                "cell_index": cell_idx,
                "issue_type": "UI_STEP",
                "severity": "INFO",
                "offending_text": context,
                "suggested_fix": "Manual UI action required — needs human execution",
                "check_source": "REGEX",
            })
    return findings


def check_copyright_footer(cells, notebook_path):
    """Check that the last cell contains the correct copyright footer."""
    if not cells:
        return []
    last_cell = cells[-1]
    source = ''.join(last_cell.get("source", []))
    expected_year = "2026"
    expected_text = "Databricks, Inc. All rights reserved"
    
    findings = []
    if expected_text not in source:
        findings.append({
            "notebook_path": notebook_path,
            "notebook_name": notebook_path.split('/')[-1],
            "cell_index": len(cells) - 1,
            "issue_type": "FORMATTING",
            "severity": "ERROR",
            "offending_text": "Missing or incomplete copyright footer",
            "suggested_fix": f"Add standard copyright footer with year {expected_year}",
            "check_source": "REGEX",
        })
    elif expected_year not in source:
        findings.append({
            "notebook_path": notebook_path,
            "notebook_name": notebook_path.split('/')[-1],
            "cell_index": len(cells) - 1,
            "issue_type": "FORMATTING",
            "severity": "WARNING",
            "offending_text": "Copyright year may be outdated",
            "suggested_fix": f"Update copyright year to {expected_year}",
            "check_source": "REGEX",
        })
    return findings


def check_header_image(cells, notebook_path):
    """Check that the first cell contains the correct header image."""
    if not cells:
        return []
    first_cell = cells[0]
    source = ''.join(first_cell.get("source", []))
    expected_img = "db-academy-rgb-1200px.png"
    
    if expected_img not in source:
        return [{
            "notebook_path": notebook_path,
            "notebook_name": notebook_path.split('/')[-1],
            "cell_index": 0,
            "issue_type": "FORMATTING",
            "severity": "ERROR",
            "offending_text": "Missing or broken header image",
            "suggested_fix": "First cell must contain the db-academy header image",
            "check_source": "REGEX",
        }]
    return []


def _in_code_block(text, pos):
    """Check if position is inside a code block (``` or backtick)."""
    # Simple heuristic: count backtick fences before pos
    before = text[:pos]
    triple_count = before.count('```')
    return triple_count % 2 == 1  # Odd means we're inside a fence

# COMMAND ----------

# ── LLM-powered checks ────────────────────────────────────────────────────────

def llm_check_cell(markdown_text, notebook_path, cell_idx):
    """
    Use ai_query to check a markdown cell for:
    - Grammar and spelling mistakes
    - Deprecated Databricks features (using model's inherent knowledge)
    - Outdated naming conventions
    
    Returns a list of finding dicts.
    """
    if not markdown_text.strip() or len(markdown_text.strip()) < 20:
        return []  # Skip very short cells (headers, etc.)
    
    prompt = f"""You are a Databricks curriculum QA reviewer. Analyze the following markdown text from a Databricks training notebook.

Report issues in these categories:
1. GRAMMAR - spelling mistakes, typos, subject-verb disagreement, doubled words, missing punctuation, run-on sentences
2. DEPRECATED - references to deprecated Databricks features, outdated APIs, or old product names that have been renamed. Key renames:
   - "Delta Live Tables"/"DLT" → "Lakeflow Spark Declarative Pipelines"/"SDP"
   - "Workflows" (for jobs) → "Lakeflow Jobs" 
   - "Databricks Asset Bundles"/"DABs" → "Declarative Automation Bundles"
   - "Repos" → "Git Folders"
   - dbutils.fs.mount → Unity Catalog Volumes
   - dbfs:/ paths → /Volumes/ paths
   - hive_metastore → Unity Catalog
   - koalas → pyspark.pandas
   - MLflow Model Registry (Workspace) → Unity Catalog Model Registry
   - Any reference to deprecated APIs, old cluster configs, or outdated Databricks Runtime versions

For each issue found, respond with a JSON array. Each element:
{{
  "issue_type": "GRAMMAR" or "DEPRECATED",
  "severity": "ERROR" or "WARNING",
  "offending_text": "exact text with the issue",
  "suggested_fix": "corrected text or replacement"
}}

If NO issues found, respond with: []

IMPORTANT: Only return the JSON array, no other text. Be precise — only flag genuine issues, not style preferences.

Markdown text to analyze:
---
{markdown_text}
---"""
    
    try:
        # Use parameterized SQL to avoid escaping issues with complex markdown/HTML/JS content
        result_df = spark.sql(
            "SELECT ai_query(:model, :prompt) as response",
            args={"model": LLM_MODEL, "prompt": prompt}
        )
        response = result_df.collect()[0]["response"]
        
        # Parse JSON response
        # Strip markdown code fences if present
        response = response.strip()
        if response.startswith('```'):
            response = re.sub(r'^```[\w]*\n?', '', response)
            response = re.sub(r'\n?```', '', response)
        
        # Handle cases where LLM returns extra text or multiple JSON values
        # Use raw_decode to parse exactly one complete JSON array, ignoring trailing content
        start_idx = response.find('[')
        if start_idx != -1:
            decoder = json.JSONDecoder()
            issues, _ = decoder.raw_decode(response, start_idx)
        else:
            issues = json.loads(response)
        
        findings = []
        for issue in issues:
            findings.append({
                "notebook_path": notebook_path,
                "notebook_name": notebook_path.split('/')[-1],
                "cell_index": cell_idx,
                "issue_type": issue.get("issue_type", "GRAMMAR"),
                "severity": issue.get("severity", "WARNING"),
                "offending_text": issue.get("offending_text", ""),
                "suggested_fix": issue.get("suggested_fix", ""),
                "check_source": "LLM",
            })
        return findings
        
    except Exception as e:
        print(f"    \u26a0\ufe0f LLM check failed for cell {cell_idx}: {e}")
        return [{
            "notebook_path": notebook_path,
            "notebook_name": notebook_path.split('/')[-1],
            "cell_index": cell_idx,
            "issue_type": "LLM_ERROR",
            "severity": "WARNING",
            "offending_text": f"LLM check failed: {str(e)[:200]}",
            "suggested_fix": "Re-run or check manually",
            "check_source": "LLM",
        }]

# COMMAND ----------

# ── Process all content notebooks ─────────────────────────────────────────────
all_findings = []
total_cells_checked = 0

print("=" * 70)
print("QA CONTENT CHECKER — Starting")
print("=" * 70)

for nb_path in content_notebooks:
    nb_name = nb_path.split('/')[-1]
    print(f"\n▶ Checking: {nb_name}")
    
    try:
        # Export notebook as ipynb
        export_resp = w.workspace.export(path=nb_path, format=ExportFormat.JUPYTER)
        nb_json = json.loads(base64.b64decode(export_resp.content))
        cells = nb_json.get("cells", [])
        
        # Check copyright footer and header image
        all_findings.extend(check_copyright_footer(cells, nb_path))
        all_findings.extend(check_header_image(cells, nb_path))
        
        # Process each cell
        for idx, cell in enumerate(cells):
            source = ''.join(cell.get("source", []))
            cell_type = cell.get("cell_type", "")
            
            # Regex checks on ALL cells (markdown + code)
            all_findings.extend(check_deprecated_terms(source, nb_path, idx))
            all_findings.extend(check_ui_steps(source, nb_path, idx))
            
            # Casing and grammar checks only on markdown cells
            if cell_type == "markdown":
                all_findings.extend(check_casing(source, nb_path, idx))
                
                # LLM check for grammar/spelling/deprecated (contextual)
                llm_findings = llm_check_cell(source, nb_path, idx)
                all_findings.extend(llm_findings)
                total_cells_checked += 1
        
        md_count = sum(1 for c in cells if c.get("cell_type") == "markdown")
        print(f"   Cells: {len(cells)} total, {md_count} markdown checked")
        
    except Exception as e:
        print(f"   ❌ ERROR processing notebook: {e}")
        all_findings.append({
            "notebook_path": nb_path,
            "notebook_name": nb_name,
            "cell_index": -1,
            "issue_type": "PROCESSING_ERROR",
            "severity": "ERROR",
            "offending_text": str(e)[:300],
            "suggested_fix": "Check notebook accessibility",
            "check_source": "SYSTEM",
        })

print(f"\n{'=' * 70}")
print(f"Processing complete. Total findings: {len(all_findings)}")
print(f"Markdown cells analyzed by LLM: {total_cells_checked}")
print("=" * 70)

# COMMAND ----------

# ── Write findings to Delta table ─────────────────────────────────────────────
qa_findings_fqn = f"{RESULTS_CATALOG}.{RESULTS_SCHEMA}.{QA_FINDINGS_TABLE}"

qa_schema = StructType([
    StructField("run_timestamp",  TimestampType(), nullable=False),
    StructField("notebook_path",  StringType(),    nullable=False),
    StructField("notebook_name",  StringType(),    nullable=False),
    StructField("cell_index",     IntegerType(),   nullable=False),
    StructField("issue_type",     StringType(),    nullable=False),
    StructField("severity",       StringType(),    nullable=False),
    StructField("offending_text", StringType(),    nullable=True),
    StructField("suggested_fix",  StringType(),    nullable=True),
    StructField("check_source",   StringType(),    nullable=False),
])

# Create table if not exists
if not spark.catalog.tableExists(qa_findings_fqn):
    spark.createDataFrame([], qa_schema).write.format("delta").saveAsTable(qa_findings_fqn)
    print(f"Created QA findings table: {qa_findings_fqn}")

# Write findings
if all_findings:
    rows = [Row(run_timestamp=run_timestamp, **f) for f in all_findings]
    findings_df = spark.createDataFrame(rows, schema=qa_schema)
    findings_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(qa_findings_fqn)
    print(f"\nAppended {len(all_findings)} findings to {qa_findings_fqn}")
else:
    print("\n✅ No issues found! All content notebooks passed QA checks.")
    findings_df = spark.createDataFrame([], qa_schema)

# COMMAND ----------

# ── Summary Display ───────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("QA FINDINGS SUMMARY")
print("=" * 70)

# Summary by issue type
type_counts = {}
for f in all_findings:
    t = f["issue_type"]
    type_counts[t] = type_counts.get(t, 0) + 1

print(f"\n{'Issue Type':<20} {'Count':>6}")
print("-" * 28)
for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
    print(f"{t:<20} {c:>6}")

# Summary by severity
sev_counts = {}
for f in all_findings:
    s = f["severity"]
    sev_counts[s] = sev_counts.get(s, 0) + 1

print(f"\n{'Severity':<20} {'Count':>6}")
print("-" * 28)
for s in ["ERROR", "WARNING", "INFO"]:
    print(f"{s:<20} {sev_counts.get(s, 0):>6}")

# UI Steps requiring manual action
ui_steps = [f for f in all_findings if f["issue_type"] == "UI_STEP"]
if ui_steps:
    print(f"\n\n{'=' * 70}")
    print("UI STEPS REQUIRING MANUAL EXECUTION")
    print("=" * 70)
    for step in ui_steps:
        print(f"\n  Notebook: {step['notebook_name']}")
        print(f"  Cell:     {step['cell_index']}")
        print(f"  Action:   {step['offending_text']}")

print(f"\n\nTotal notebooks checked: {len(content_notebooks)}")
print(f"Total findings: {len(all_findings)}")

# Display full findings as DataFrame
if all_findings:
    display(findings_df)

# COMMAND ----------

# ── Fail if critical issues found ─────────────────────────────────────────────
error_count = sev_counts.get("ERROR", 0)
warning_count = sev_counts.get("WARNING", 0)

status = "PASS"
if error_count > 0:
    status = "FAIL"
elif warning_count > 0:
    status = "PASS WITH WARNINGS"

print(f"\nQA Status: {status}")
print(f"  Errors: {error_count}  |  Warnings: {warning_count}  |  Info: {sev_counts.get('INFO', 0)}")

# Exit with structured result (does NOT raise — independent task should not block others)
dbutils.notebook.exit(json.dumps({
    "status": status,
    "total_findings": len(all_findings),
    "errors": error_count,
    "warnings": warning_count,
    "notebooks_checked": len(content_notebooks),
    "ui_steps_found": len(ui_steps),
}))