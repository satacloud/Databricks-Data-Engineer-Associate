# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

DA.display_config_values([('Course Catalog',DA.catalog_name),('Your Schema',DA.schema_name)])

# COMMAND ----------

import os
current_dir = os.getcwd()

# COMMAND ----------

@DBAcademyHelper.add_method
def lesson_15_starter_job(self):
    from databricks.sdk.service.jobs import JobSettings as Job
    Lab_15 = Job.from_dict(
    {
        "name": f"Lab_15_Bank_Job_{DA.schema_name}",
        "tasks": [
            {
                "task_key": "ingesting_master_data",
                "notebook_task": {
                    "notebook_path":  os.path.join(current_dir, "Task Files/Lesson 05 Files/5.1 - Ingesting Banking Data"),
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "creating_borrower_details_table",
                "depends_on": [
                    {
                        "task_key": "ingesting_master_data",
                    },
                ],
                "notebook_task": {
                    "notebook_path":  os.path.join(current_dir, "Task Files/Lesson 05 Files/5.2 - Creating Borrower Details Table"),
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "creating_loan_details_table",
                "depends_on": [
                    {
                        "task_key": "ingesting_master_data",
                    },
                ],
                "notebook_task": {
                    "notebook_path":  os.path.join(current_dir, "Task Files/Lesson 05 Files/5.3 - Creating Loan Details Table"),
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "checking_for_risky_borrowers",
                "depends_on": [
                    {
                        "task_key": "creating_loan_details_table",
                    },
                ],
                "condition_task": {
                    "op": "EQUAL_TO",
                    "left": "{{tasks.creating_loan_details_table.values.risk_flag}}",
                    "right": "true",
                },
            },
            {
                "task_key": "processing_high_risk_borrowers",
                "depends_on": [
                    {
                        "task_key": "checking_for_risky_borrowers",
                        "outcome": "true",
                    },
                ],
                "notebook_task": {
                    "notebook_path":  os.path.join(current_dir, "Task Files/Lesson 10 Files/10.1 - Processing high risk borrowers"),
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "processing_low_risk_borrowers",
                "depends_on": [
                    {
                        "task_key": "checking_for_risky_borrowers",
                        "outcome": "false",
                    },
                ],
                "notebook_task": {
                    "notebook_path":  os.path.join(current_dir, "Task Files/Lesson 10 Files/10.2 - Processing low risk borrowers"),
                    "source": "WORKSPACE",
                },
            },
        ],
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    w.jobs.create(**Lab_15.as_shallow_dict())
    print(f"Created the Job Lab_15_Bank_Job_{DA.schema_name}")
