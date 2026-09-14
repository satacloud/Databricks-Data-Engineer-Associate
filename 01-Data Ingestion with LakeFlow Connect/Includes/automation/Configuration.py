# Databricks notebook source
# Change the course name as per your course name
COURSE_NAME = "Data Ingestion with Lakeflow Connect"
RUN_QA_CHECKER = True
QA_TASK_RELATIVE_PATH = "./qa_content_checker"
QA_TASK_NAME = "QA Content Checker — Grammar, Deprecated, UI Steps"

# Tester emails — notified on job failure and success. Add as many as you like with comma separated emails
TESTER_EMAILS = [
    "nitesh.ojha@databricks.com",
    "shivam.pandey@databricks.com",
    "s.kumar@databricks.com"
]

# Add your lab notebook paths relative from the CourseRunner folder as per your course
LAB_NOTEBOOKS = [
    "../../10 Lab - Creating Bronze Tables from CSV Files",
    "../../13 Lab - Creating Bronze Tables from JSON Files",
]

# Define the course notebook DAG here so this can be used for job scheduling as per your course
COURSE_TASKS = [
    {
        "task_key": "02 Demo - Exploring the Lab Environment",
        "name": "02 Demo - Exploring the Lab Environment",
        "relative_path": "../../02 Demo - Exploring the Lab Environment",
        "depends_on": [],
        "env_key": "course_02",
        "use_classic": True,
    },
    {
        "task_key": "04 Demo - Data Ingestion with CREATE TABLE AS and COPY INTO",
        "name": "04 Demo - Data Ingestion with CREATE TABLE AS and COPY INTO",
        "relative_path": "../../04 Demo - Data Ingestion with CREATE TABLE AS and COPY INTO",
        "depends_on": [],
        "env_key": "course_04",
        "use_classic": True,
    },
    {
        "task_key": "05 Demo - Create Streaming Tables with SQL using Auto Loader",
        "name": "05 Demo -  Create Streaming Tables with SQL using Auto Loader",
        "relative_path": "../../05 Demo -  Create Streaming Tables with SQL using Auto Loader",
        "depends_on": [],
        "env_key": "course_05",
        "use_classic": True,
        # NOTE: Streaming table DDL patched to regular CTAS for automated testing
    },
    {
        "task_key": "07 Demo - Adding Metadata Columns During Ingestion",
        "name": "07 Demo - Adding Metadata Columns During Ingestion",
        "relative_path": "../../07 Demo - Adding Metadata Columns During Ingestion",
        "depends_on": [],
        "env_key": "course_07",
        "use_classic": True,
    },
    {
        "task_key": "09 Demo - Handling CSV Ingestion with the Rescued Data Column",
        "name": "09 Demo - Handling CSV Ingestion with the Rescued Data Column",
        "relative_path": "../../09 Demo - Handling CSV Ingestion with the Rescued Data Column",
        "depends_on": [],
        "env_key": "course_09",
        "use_classic": True,
    },
    {
        "task_key": "10 Lab - Creating Bronze Tables from CSV Files",
        "name": "10 Lab - Creating Bronze Tables from CSV Files",
        "relative_path": "../../10 Lab - Creating Bronze Tables from CSV Files",
        "depends_on": [],
        "env_key": "course_10",
        "use_classic": True,
    },
    {
        "task_key": "12 Demo - Ingesting JSON files with Databricks",
        "name": "12 Demo - Ingesting JSON files with Databricks",
        "relative_path": "../../12 Demo - Ingesting JSON files with Databricks",
        "depends_on": [],
        "env_key": "course_12",
        "use_classic": True,
    },
    {
        "task_key": "13 Lab - Creating Bronze Tables from JSON Files",
        "name": "13 Lab - Creating Bronze Tables from JSON Files",
        "relative_path": "../../13 Lab - Creating Bronze Tables from JSON Files",
        "depends_on": [],
        "env_key": "course_13",
        "use_classic": True,
    },
    {
        "task_key": "15 Demo - Enterprise Data Ingestion with Lakeflow Connect",
        "name": "15 Demo - Enterprise Data Ingestion with Lakeflow Connect",
        "relative_path": "../../15 Demo - Enterprise Data Ingestion with Lakeflow Connect",
        "depends_on": [],
        "env_key": "course_15",
    },
    {
        "task_key": "17 Demo - BONUS - Data Ingestion with MERGE INTO",
        "name": "17 Demo - BONUS - Data Ingestion with MERGE INTO",
        "relative_path": "../../17 Demo - BONUS - Data Ingestion with MERGE INTO",
        "depends_on": [],
        "env_key": "course_17",
        "use_classic": True,
    },
]

# COMMAND ----------

# ── Auto-Fill Lab Notebooks ──────────────────────────────────────────────────
# This script finds unresolved lab placeholders in lab notebooks and replaces
# them with solution code extracted from the nearby solution blocks.
#
# HOW IT WORKS:
# 1. Exports each lab notebook as .ipynb JSON
# 2. Scans code cells for unresolved markers such as <FILL_IN>, <FILL-IN>,
#    <ToDo>, and TODO-only scaffolds
# 3. Looks ahead in subsequent markdown cells for <code>...</code> blocks
# 4. Extracts the solution and replaces the original cell source
# 5. Re-imports the patched notebook
#
# Safe to re-run: it only patches cells that still contain unresolved markers.
# ──────────────────────────────────────────────────────────────────────────────

import base64
import json
import os
import re
import html as html_lib
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()

# Derive course_root from this notebook's own path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
course_root = "/".join(_nb_path.split("/")[:-1])

# LAB_NOTEBOOKS is configured in the course-specific inputs cell above.
# Resolve to absolute workspace paths
lab_abs_paths = [os.path.normpath(f"{course_root}/{p}") for p in LAB_NOTEBOOKS]

PLACEHOLDER_PATTERNS = [
    re.compile(r'<\s*fill[-_ ]?in\s*>', re.IGNORECASE),
    re.compile(r'--\s*fill[-_ ]?in\s*--', re.IGNORECASE),
    re.compile(r'<\s*todo\s*>', re.IGNORECASE),
    re.compile(r'(?mi)^\s*#+\s*todo\b'),
    re.compile(r'(?mi)^\s*#+.*\bTODO\b'),
    # SQL lab pattern: -- TODO comment at the start of a cell (used in Databricks Academy SQL labs)
    re.compile(r'(?mi)^\s*--\s*TODO\b'),
]


def has_unresolved_placeholder(source: str) -> bool:
    """Return True when a code cell still contains a fill-in or TODO scaffold."""
    return any(pattern.search(source) for pattern in PLACEHOLDER_PATTERNS)


def extract_solution_from_source(source: str) -> str | None:
    """Extract code from a Databricks solution cell.

    Handles:
    - Fenced code blocks: ```python\\n...\\n``` (used in current labs)
    - HTML <code> tags: <code>...</code> (legacy fallback)
    - Strips %md / %md-sandbox magic prefixes before parsing.
    """
    # Strip Databricks magic prefix (%md, %md-sandbox, etc.)
    clean = re.sub(r'^%md[a-zA-Z-]*[ \t]*\n', '', source.lstrip(), count=1)

    # 1. Fenced code block (most common in current labs)
    match = re.search(r'```(?:python|sql|scala|r|sh)?\s*\n(.*?)\n[ \t]*```', clean, re.DOTALL)
    if match:
        code = match.group(1)
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    # 2. HTML <code> tags (fallback for older lab format)
    match = re.search(r'<code[^>]*>\s*\n?(.*?)\n?\s*</code>', clean, re.DOTALL)
    if match:
        code = html_lib.unescape(match.group(1))
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    return None


def extract_answer_cell(source: str) -> str | None:
    """Extract solution from a SQL '-- ANSWER' code cell.

    Strips the leading '-- ANSWER' comment line and returns the remaining code.
    Used in Databricks Academy SQL lab notebooks where answers follow TODO cells
    directly as sibling code cells (rather than being embedded in markdown hint blocks).
    """
    lines = source.split('\n')
    result_lines = []
    skipping_answer_header = True
    for line in lines:
        if skipping_answer_header and re.match(r'\s*--\s*ANSWER\b', line, re.IGNORECASE):
            continue
        skipping_answer_header = False
        result_lines.append(line)
    # Strip leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()
    return '\n'.join(result_lines) if result_lines else None


def extract_skip_cell(source: str) -> str | None:
    """Extract solution from a '%skip' code cell.

    Strips the leading '%skip' line and returns the remaining code.
    Used in Databricks Academy labs where solution cells are marked with %skip magic.
    """
    lines = source.split('\n')
    result_lines = []
    skipping_header = True
    for line in lines:
        if skipping_header and re.match(r'\s*%skip\b', line, re.IGNORECASE):
            continue
        skipping_header = False
        result_lines.append(line)
    # Strip leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()
    return '\n'.join(result_lines) if result_lines else None


def is_solution_candidate(cell: dict) -> bool:
    """Return True if a cell is a potential answer/solution block.

    In Databricks ipynb exports, %md-sandbox cells are stored as code cells
    (not markdown), so we must check both cell_type=='markdown' and code cells
    that start with a %md magic prefix.
    """
    source = ''.join(cell.get("source", []))
    has_code_block = '```' in source or '<code>' in source
    is_markdown_type = cell.get("cell_type") == "markdown"
    is_md_magic = cell.get("cell_type") == "code" and bool(re.match(r'\s*%md', source))
    return (is_markdown_type or is_md_magic) and has_code_block


def fill_notebook(notebook_path: str) -> dict:
    """Fill all unresolved placeholder cells in a single notebook. Returns stats."""
    stats = {"path": notebook_path, "blanks_found": 0, "filled": 0, "no_answer": 0}

    # Export notebook as ipynb
    export_resp = w.workspace.export(path=notebook_path, format=ExportFormat.JUPYTER)
    nb_json = json.loads(base64.b64decode(export_resp.content))
    cells = nb_json.get("cells", [])

    for i, cell in enumerate(cells):
        source = ''.join(cell.get("source", []))

        # Check if this cell still has unresolved fill-in / TODO markers
        if not has_unresolved_placeholder(source):
            continue

        # Skip markdown cells (we only fill code cells)
        if cell.get("cell_type") != "code":
            continue

        stats["blanks_found"] += 1

        # Look ahead up to 8 cells for a solution block.
        # %md-sandbox solution cells may appear as 'code' type in ipynb exports,
        # so is_solution_candidate() checks both cell_type and %md magic prefix.
        solution = None
        for j in range(i + 1, min(i + 9, len(cells))):
            next_cell = cells[j]
            next_source = ''.join(next_cell.get("source", []))
            # SQL lab pattern: -- ANSWER code cell immediately follows -- TODO cell
            if (next_cell.get("cell_type") == "code"
                    and re.match(r'\s*--\s*ANSWER\b', next_source, re.IGNORECASE)):
                solution = extract_answer_cell(next_source)
                if solution:
                    break
            # %skip pattern: solution cell with %skip magic prefix
            elif (next_cell.get("cell_type") == "code"
                    and re.match(r'\s*%skip\b', next_source, re.IGNORECASE)):
                solution = extract_skip_cell(next_source)
                if solution:
                    break
            elif is_solution_candidate(next_cell):
                solution = extract_solution_from_source(next_source)
                if solution:
                    break
            # Stop looking once we hit a real (non-magic, non-ANSWER) code cell
            elif (next_cell.get("cell_type") == "code"
                  and not re.match(r'\s*%', next_source)
                  and not re.match(r'\s*--\s*ANSWER\b', next_source, re.IGNORECASE)):
                break

        if solution:
            # Check if solution has %sql magic — if so, handle cell language
            if solution.strip().startswith('%sql'):
                # Remove %sql prefix, keep the SQL code
                solution_lines = solution.strip().split('\n')
                # Keep %sql as first line for the cell magic
                cell["source"] = [solution + '\n']
            else:
                cell["source"] = [solution + '\n']
            stats["filled"] += 1
        else:
            stats["no_answer"] += 1
            print(f"  ⚠️  NO ANSWER FOUND for cell {i} in {notebook_path}")

    # Re-import the patched notebook
    if stats["filled"] > 0:
        patched_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
        w.workspace.import_(
            path=notebook_path,
            content=patched_content,
            format=ImportFormat.JUPYTER,
            overwrite=True,
            language=Language.PYTHON,
        )

    return stats


# ── Run the filler ────────────────────────────────────────────────────────────
print("═" * 70)
print("AUTO-FILL LAB NOTEBOOKS")
print("═" * 70)

all_stats = []
for path in lab_abs_paths:
    print(f"\n▶ Processing: {path.split('/')[-1]}")
    try:
        s = fill_notebook(path)
        all_stats.append(s)
        print(f"   Blanks found: {s['blanks_found']}  |  Filled: {s['filled']}  |  No answer: {s['no_answer']}")
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        all_stats.append({"path": path, "blanks_found": 0, "filled": 0, "no_answer": 0, "error": str(e)})

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "═" * 70)
total_blanks = sum(s["blanks_found"] for s in all_stats)
total_filled = sum(s["filled"] for s in all_stats)
total_no_ans = sum(s["no_answer"] for s in all_stats)
print(f"TOTAL:  {total_blanks} blanks found  |  {total_filled} filled  |  {total_no_ans} blockers")
print("═" * 70)