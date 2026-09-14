# Databricks notebook source
# MAGIC %run ./Classroom-Setup-07

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating Volume 
# MAGIC CREATE VOLUME IF NOT EXISTS trigger_storage_location

# COMMAND ----------

DA.copy_data()

# COMMAND ----------

import os
current_dir = os.getcwd()

# COMMAND ----------

# MAGIC %skip
# MAGIC # # Upgrade Databricks SDK to the latest version and restart Python to see updated packages
# MAGIC # @DBAcademyHelper.add_method
# MAGIC # def Demo_6_starter_job(self):
# MAGIC     
# MAGIC #     import os
# MAGIC #     from databricks.sdk.service.jobs import JobSettings as Job
# MAGIC #     current_dir = os.getcwd()
# MAGIC #     Demo_6 = Job.from_dict(
# MAGIC #         {
# MAGIC #         "name": f"Demo_06_Retail_Job_{DA.schema_name}",
# MAGIC #         "tasks": [
# MAGIC #             {
# MAGIC #                 "task_key": "ingesting_customers",
# MAGIC #                 "notebook_task": {
# MAGIC #                     "notebook_path": os.path.join(current_dir, "Task Files/Lesson 3 Files/3.1 - Creating customers table"),
# MAGIC #                     "source": "WORKSPACE",
# MAGIC #                 },
# MAGIC #             },
# MAGIC #             {
# MAGIC #                 "task_key": "ingesting_orders",
# MAGIC #                 "notebook_task": {
# MAGIC #                     "notebook_path": os.path.join(current_dir,"Task Files/Lesson 1 Files/1.1 - Creating orders table"),
# MAGIC #                     "source": "WORKSPACE",
# MAGIC #                 },
# MAGIC #             },
# MAGIC #             {
# MAGIC #                 "task_key": "customers_orders_report",
# MAGIC #                 "depends_on": [
# MAGIC #                     {
# MAGIC #                         "task_key": "ingesting_orders",
# MAGIC #                     },
# MAGIC #                     {
# MAGIC #                         "task_key": "ingesting_customers",
# MAGIC #                     },
# MAGIC #                 ],
# MAGIC #                 "notebook_task": {
# MAGIC #                     "notebook_path": os.path.join(current_dir,"Task Files/Lesson 4 Files/4.2 - Joining Customers and Orders Table"),
# MAGIC #                     "source": "WORKSPACE",
# MAGIC #                 },
# MAGIC #             },
# MAGIC #             {
# MAGIC #                 "task_key": "customers_orders_state_wise_report_iterator",
# MAGIC #                 "depends_on": [
# MAGIC #                     {
# MAGIC #                         "task_key": "customers_orders_report",
# MAGIC #                     },
# MAGIC #                 ],
# MAGIC #                 "for_each_task": {
# MAGIC #                     "inputs": "[\"CA\", \"NY\", \"VA\"]",
# MAGIC #                     "task": {
# MAGIC #                         "task_key": "customers_orders_state_wise_report",
# MAGIC #                         "notebook_task": {
# MAGIC #                             "notebook_path": os.path.join(current_dir,"Task Files/Lesson 4 Files/4.5 - For Each: Customer orders State"),
# MAGIC #                             "base_parameters": {
# MAGIC #                                 "state": "{{input}}",
# MAGIC #                             },
# MAGIC #                             "source": "WORKSPACE",
# MAGIC #                         },
# MAGIC #                     },
# MAGIC #                 },
# MAGIC #             },
# MAGIC #             {
# MAGIC #                 "task_key": "ingesting_sales",
# MAGIC #                 "notebook_task": {
# MAGIC #                     "notebook_path": os.path.join(current_dir,"Task Files/Lesson 1 Files/1.2 - Creating sales table"),
# MAGIC #                     "source": "WORKSPACE",
# MAGIC #                 },
# MAGIC #             },
# MAGIC #             {
# MAGIC #                 "task_key": "customers_sales_summary",
# MAGIC #                 "depends_on": [
# MAGIC #                     {
# MAGIC #                         "task_key": "ingesting_customers",
# MAGIC #                     },
# MAGIC #                     {
# MAGIC #                         "task_key": "ingesting_sales",
# MAGIC #                     },
# MAGIC #                 ],
# MAGIC #                 "notebook_task": {
# MAGIC #                     "notebook_path": os.path.join(current_dir,"Task Files/Lesson 4 Files/4.1 - Joining Customers and Sales Table"),
# MAGIC #                     "source": "WORKSPACE",
# MAGIC #                 },
# MAGIC #             },
# MAGIC #             {
# MAGIC #                 "task_key": "checking_for_duplicates",
# MAGIC #                 "depends_on": [
# MAGIC #                     {
# MAGIC #                         "task_key": "customers_sales_summary",
# MAGIC #                     },
# MAGIC #                 ],
# MAGIC #                 "condition_task": {
# MAGIC #                     "op": "EQUAL_TO",
# MAGIC #                     "left": "{{tasks.customers_sales_summary.values.has_duplicates}}",
# MAGIC #                     "right": "true",
# MAGIC #                 },
# MAGIC #             },
# MAGIC #             {
# MAGIC #                 "task_key": "dropping_duplicate_records",
# MAGIC #                 "depends_on": [
# MAGIC #                     {
# MAGIC #                         "task_key": "checking_for_duplicates",
# MAGIC #                         "outcome": "true",
# MAGIC #                     },
# MAGIC #                 ],
# MAGIC #                 "notebook_task": {
# MAGIC #                     "notebook_path": os.path.join(current_dir,"Task Files/Lesson 4 Files/4.3 - If Condition: Dropping Duplicates"),
# MAGIC #                     "source": "WORKSPACE",
# MAGIC #                 },
# MAGIC #             },
# MAGIC #             {
# MAGIC #                 "task_key": "transforming_customers_sales_table",
# MAGIC #                 "depends_on": [
# MAGIC #                     {
# MAGIC #                         "task_key": "checking_for_duplicates",
# MAGIC #                         "outcome": "false",
# MAGIC #                     },
# MAGIC #                     {
# MAGIC #                         "task_key": "dropping_duplicate_records",
# MAGIC #                     },
# MAGIC #                 ],
# MAGIC #                 "run_if": "NONE_FAILED",
# MAGIC #                 "notebook_task": {
# MAGIC #                     "notebook_path": os.path.join(current_dir,"Task Files/Lesson 4 Files/4.4 - Else Condition: Cleaning and Transforming Customers Sales Table"),
# MAGIC #                     "source": "WORKSPACE",
# MAGIC #                 },
# MAGIC #             },
# MAGIC #         ],
# MAGIC #         "parameters": [
# MAGIC #             {
# MAGIC #                 "name": "catalog",
# MAGIC #                 "default": "dbacademy",
# MAGIC #             },
# MAGIC #             {
# MAGIC #                 "name": "schema",
# MAGIC #                 "default": f"{DA.schema_name}",
# MAGIC #             },
# MAGIC #         ],
# MAGIC #         "performance_target": "PERFORMANCE_OPTIMIZED",
# MAGIC #     }
# MAGIC # )
# MAGIC #     from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC #     w = WorkspaceClient()
# MAGIC #     # create a new job using: 
# MAGIC #     w.jobs.create(**Demo_6.as_shallow_dict())
# MAGIC #     print(f"Created the Job Demo_06_Retail_Job_{DA.schema_name}")

# COMMAND ----------

@DBAcademyHelper.add_method
def Demo_12_starter_job(self):
    
    import os
    from databricks.sdk.service.jobs import JobSettings as Job

    current_dir = os.getcwd()

    Demo_12 = Job.from_dict(
        {
            "name": f"Demo_12_Retail_Job_{DA.schema_name}",
            "tasks": [
                {
                    "task_key": "ingesting_customers",
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 07 Files/7.1 - Creating customers table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "ingesting_orders",
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 04 Files/4.1 - Creating orders table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "customers_orders_report",
                    "depends_on": [
                        {"task_key": "ingesting_orders"},
                        {"task_key": "ingesting_customers"},
                    ],
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.2 - Joining Customers and Orders Table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "ingesting_sales",
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 04 Files/4.2 - Creating sales table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "customers_sales_summary",
                    "depends_on": [
                        {"task_key": "ingesting_customers"},
                        {"task_key": "ingesting_sales"},
                    ],
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.1 - Joining Customers and Sales Table"),
                        "source": "WORKSPACE",
                    },
                },
                {
                    "task_key": "checking_for_duplicates",
                    "depends_on": [
                        {"task_key": "customers_sales_summary"},
                    ],
                    "condition_task": {
                        "op": "EQUAL_TO",
                        "left": "{{tasks.customers_sales_summary.values.has_duplicates}}",
                        "right": "true",
                    },
                },
                {
                    "task_key": "dropping_duplicate_records",
                    "depends_on": [
                        {"task_key": "checking_for_duplicates", "outcome": "true"},
                    ],
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.3 - If Condition: Dropping Duplicates"),
                        "source": "WORKSPACE",
                    },
                    "environment_key": "Default",
                },
                {
                    "task_key": "transforming_customers_sales_table",
                    "depends_on": [
                        {"task_key": "checking_for_duplicates", "outcome": "false"},
                        {"task_key": "dropping_duplicate_records"},
                    ],
                    "run_if": "NONE_FAILED",
                    "notebook_task": {
                        "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.4 - Else Condition: Cleaning and Transforming Customers Sales Table"),
                        "source": "WORKSPACE",
                    },
                    "environment_key": "Default",
                },
                {
                    "task_key": "customers_orders_state_wise_report_iterator",
                    "depends_on": [
                        {"task_key": "customers_orders_report"},
                    ],
                    "for_each_task": {
                        "inputs": "[\"CA\",\"NY\",\"VA\"]",
                        "concurrency": 3,
                        "task": {
                            "task_key": "customers_orders_state_wise_report",
                            "notebook_task": {
                                "notebook_path": os.path.join(current_dir, "Task Files/Lesson 09 Files/9.5 - For Each: Customer orders State"),
                                "base_parameters": {
                                    "state": "{{input}}",
                                },
                                "source": "WORKSPACE",
                            },
                            "environment_key": "Default",
                        },
                    },
                },
            ],
            "queue": {
                "enabled": True,
            },
            "parameters": [
                {
                    "name": "catalog",
                    "default": "dbacademy",
                },
                {
                    "name": "schema",
                    "default": f"{DA.schema_name}",
                },
            ],
            "environments": [
                {
                    "environment_key": "Default",
                    "spec": {
                        "environment_version": "5",
                    },
                },
            ],
            "performance_target": "PERFORMANCE_OPTIMIZED",
        }
    )
    
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    # create a new job using: 
    w.jobs.create(**Demo_12.as_shallow_dict())
    print(f"Created the Job Demo_12_Retail_Job_{DA.schema_name}")


# COMMAND ----------

@DBAcademyHelper.add_method
def exporting_dashboard_from_input(self):
    import json
    import re

    # Path to your downloaded dashboard file
    input_file = os.path.join(current_dir,'Task Files/Lesson 12 Files/Input_file.json')
    output_file = os.path.join(current_dir,f'Task Files/Lesson 12 Files/{DA.schema_name}_Retail_Dashboard.json')


    # Old and new schema names
    old_schema = 'myschema'
    new_schema = f'{DA.schema_name}'

    # Regex pattern to match the schema name in SQL queries
    # This matches .myschema. in SQL statements like "dbacademy.myschema.table"
    schema_pattern = re.compile(rf'(\bdbacademy\.){old_schema}(\.[a-zA-Z0-9_]+)')

    def replace_schema(obj, pat, repl):
        if isinstance(obj, dict):
            return {k: replace_schema(v, pat, repl) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [replace_schema(item, pat, repl) for item in obj]
        elif isinstance(obj, str):
            # Replace in SQL query strings
            return pat.sub(rf'\1{repl}\2', obj)
        else:
            return obj

    # Load the dashboard JSON
    with open(input_file, 'r', encoding='utf-8') as f:
        dashboard_data = json.load(f)

    # Replace schema everywhere
    dashboard_data = replace_schema(dashboard_data, schema_pattern, new_schema)

    # Save the modified dashboard file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, indent=2)

    print(f'Updated dashboard exported to {current_dir}/')

# COMMAND ----------

@DBAcademyHelper.add_method
def dashboard_creation_from_json(self):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat
    from databricks.sdk.service.workspace import ObjectType
    import base64

    w = WorkspaceClient()

    # Read your JSON dashboard file
    with open(os.path.join(current_dir,f'Task Files/Lesson 12 Files/{DA.schema_name}_Retail_Dashboard.json'), 'r') as f:
        dashboard_content = f.read()

    # Base64 encode the content
    encoded_content = base64.b64encode(dashboard_content.encode()).decode()

    # Import the dashboard - path MUST end with .lvdash.json
    w.workspace.import_(
        path = os.path.join(current_dir,f'{DA.schema_name}_Retail_Dashboard.lvdash.json'),  # Critical: .lvdash.json extension
        format=ImportFormat.AUTO,  # Use ImportFormat.AUTO for automatic detection
        content=encoded_content,
        overwrite=True
    )
    dash_path = os.path.join(current_dir, f'{DA.schema_name}_Retail_Dashboard.lvdash.json')
    # Get workspace object status to read its object_id (resource_id)
    obj = w.workspace.get_status(path=dash_path) 
    dashboard_id = obj.object_id  

    # Publish the dashboard; set embed_credentials and optionally override warehouse
    w.lakeview.publish(
        dashboard_id=dashboard_id,
        embed_credentials=True,          
    )

    print("Retail Dashboard have been created successfully!\n")
    print(f"Your Dashboard name is {DA.schema_name}_Retail_Dashboard")

# COMMAND ----------

@DBAcademyHelper.add_method
def dashboard_creation_from_input(self):
    self.exporting_dashboard_from_input()
    self.dashboard_creation_from_json()
