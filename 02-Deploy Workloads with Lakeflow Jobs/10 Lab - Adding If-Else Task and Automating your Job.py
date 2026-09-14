# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 10 Lab - Adding If-Else Task and Automating Your Job
# MAGIC
# MAGIC #### Duration: ~15 minutes
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC * Add conditional logic to a Databricks job
# MAGIC * Schedule your job for automated execution
# MAGIC
# MAGIC ## Lab Scenario
# MAGIC You have already created two tables: **borrower_details_silver** and **loan_details_silver** from the **bank_master_data_bronze** dataset. In this lab, you will further transform these silver tables, focusing on the loan details table, and implement conditional logic to handle high-risk borrowers.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select Serverless Compute</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC Before starting this notebook, select the required compute environment listed below.
# MAGIC
# MAGIC - **Serverless Compute, Version 5**  
# MAGIC ![Serverless Select](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/common/select-serverless.png)
# MAGIC <br></br>
# MAGIC   - How to select an environment version:
# MAGIC [AWS](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/dependencies#-select-an-environment-version) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/compute/serverless/dependencies#-select-an-environment-version)
# MAGIC
# MAGIC **NOTE:**  This notebook was **developed and tested using Serverless V5**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to **dbacademy** and the schema to your specific schema name shown below using the `USE` statements.
# MAGIC <br></br>
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.
# MAGIC
# MAGIC **NOTE:** If you use Serverless V1 a warning will be returned. You can ignore the warning.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-10L

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Creating and Exploring the Starter Job
# MAGIC
# MAGIC Run the cell below to automatically create the starter job for this lab. This starter job includes all the tasks completed in **05 Lab - Create your First Job**:
# MAGIC
# MAGIC - ./Task Files/Lesson 05 Files/5.1 - Ingesting Banking Data
# MAGIC - ./Task Files/Lesson 05 Files/5.2 - Creating Borrower Details Table
# MAGIC - ./Task Files/Lesson 05 Files/5.3 - Creating Loan Details Table
# MAGIC

# COMMAND ----------

job_tasks = [
        {
            'task_name': 'ingesting_master_data',
            'file_path': '/Task Files/Lesson 05 Files/5.1 - Ingesting Banking Data',
            'depends_on': None
        },
        {
            'task_name': 'creating_borrower_details_table',
            'file_path': '/Task Files/Lesson 05 Files/5.2 - Creating Borrower Details Table',
            'depends_on': [{'task_key':'ingesting_master_data'}]
        },
        {
            'task_name': 'creating_loan_details_table',
            'file_path': '/Task Files/Lesson 05 Files/5.3 - Creating Loan Details Table',
            'depends_on': [{'task_key':'ingesting_master_data'}]
        }
    ]

myjob = DAJobConfig(job_name=f"Lab_10_Bank_Job_{DA.schema_name}",
                        job_tasks=job_tasks,
                        job_parameters=[])

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Explore the New Task Files
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Let's build upon the last lab by exploring the notebooks we want to add to the job. The notebooks can be found in **Task Files** > **Lesson 10 Files**.
# MAGIC
# MAGIC Use the links below to view and explore the code for each task:
# MAGIC
# MAGIC - [Task Files/Lesson 10 Files/10.1 - Processing high risk borrowers]($./Task Files/Lesson 10 Files/10.1 - Processing high risk borrowers)
# MAGIC   - This notebook identifies high-risk borrowers, processes them, and stores their data in the **high_risk_borrowers_silver** table.
# MAGIC
# MAGIC - [Task Files/Lesson 10 Files/10.2 - Processing low risk borrowers]($./Task Files/Lesson 10 Files/10.2 - Processing low risk borrowers)
# MAGIC   - This notebook identifies low-risk borrowers, processes them, and stores their data in the **low_risk_borrowers_silver** table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Adding an If/Else Conditional Task
# MAGIC
# MAGIC In this section, you will add a conditional task to your job that checks for high-risk borrowers in the **loan_details_silver** table. The relevant code can be found in [Task Files/Lesson 05 Files/5.3 - Creating Loan Details Table]($./Task Files/Lesson 05 Files/5.3 - Creating Loan Details Table).
# MAGIC
# MAGIC Pay close attention to the final commands, as they set the output task value. Whenever the number of borrowers with both an active loan and a credit card exceeds 100, the `risk_flag` is set to **true**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Review the Notebook: Check for High-Risk Borrowers
# MAGIC
# MAGIC 1. Refer to the notebook [Task Files/Lesson 05 Files/5.3 - Creating Loan Details Table]($./Task Files/Lesson 05 Files/5.3 - Creating Loan Details Table) to review the `risk_flag` logic for identifying high-risk borrowers in the **loan_details_silver** table.
# MAGIC
# MAGIC   - The **creating_loan_details_table** task creates the **loan_details_silver** table.
# MAGIC
# MAGIC   - Your goal is to check whether this table contains more than the allowed number of high-risk borrowers.
# MAGIC
# MAGIC   - Stores the result of this check (a boolean value) as `risk_flag` in the task output.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Create an If/Else Conditional Task
# MAGIC
# MAGIC 1. Navigate to the job that was created, go to the Tasks tab, and add a new task named **checking_for_risky_borrowers**.
# MAGIC
# MAGIC 2. Set the task type to **If/Else condition**.
# MAGIC
# MAGIC 3. Set this task to depend on the **creating_loan_details_table** task.
# MAGIC
# MAGIC 4. Select the condition by clicking on the `{}` button, then choose tasks.`creating_loan_details_table.values.my_value` and replace `my_value` with `risk_flag`.
# MAGIC
# MAGIC <!-- 5. For the condition, use the output value from the **creating_loan_details_table** task: `tasks.creating_loan_details_table.values.risk_flag` (Include the condition in double quotes) -->
# MAGIC
# MAGIC 5. Set the condition to check if this value is `== true`: `tasks.creating_loan_details_table.values.risk_flag == true`
# MAGIC
# MAGIC 6. Click on **Save Task**.
# MAGIC
# MAGIC ![Lesson10_ifelse](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lab_elif/Lesson10_ifelse.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Handle the True Condition
# MAGIC
# MAGIC 1. If high-risk borrowers are found (`risk_flag == true`), add a notebook task named **processing_high_risk_borrowers**.
# MAGIC
# MAGIC 2. This task should depend on the **True** branch of the **checking_for_risky_borrowers** task.
# MAGIC
# MAGIC 3. Use the notebook [Task Files/Lesson 10 Files/10.1 - Processing high risk borrowers]($./Task Files/Lesson 10 Files/10.1 - Processing high risk borrowers) for this condition.
# MAGIC
# MAGIC
# MAGIC ![Lesson10_if_task.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lab_elif/Lesson10_if_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Handle the False Condition
# MAGIC
# MAGIC 1. If high-risk borrowers are less than threshold (`risk_flag == false`), add a notebook task named **processing_low_risk_borrowers**.
# MAGIC
# MAGIC 2. This task should depend on the **False** branch of the **checking_for_risky_borrowers** task.
# MAGIC
# MAGIC 3. Use the notebook [Task Files/Lesson 10 Files/10.2 - Processing low risk borrowers]($./Task Files/Lesson 10 Files/10.2 - Processing low risk borrowers) for this condition.
# MAGIC
# MAGIC 4. This setup ensures your job processes high-risk and low-risk borrowers separately.
# MAGIC ![Lesson10_else_task](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lab_elif/Lesson10_else_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Scheduling Your Job Using the Jobs UI
# MAGIC
# MAGIC Follow these steps to schedule your Databricks job:
# MAGIC
# MAGIC 1. **Open Your Job:** Go to the job you just created.
# MAGIC
# MAGIC 2. **Go to the Tasks Tab:** Make sure you are viewing the **Tasks** tab within your job.
# MAGIC
# MAGIC 3. **Expand Job Details:**  On the right side of the Jobs UI, find the **Job Details** panel.  
# MAGIC    - **NOTE:** If the panel is collapsed, click the arrow icon to expand it.
# MAGIC
# MAGIC 4. **Add a Schedule:**  In the **Schedules & Triggers** section, click **Add trigger**. You will see five scheduling options:  
# MAGIC    - **Scheduled** (run at specific times)
# MAGIC
# MAGIC    - **Continuous** (run as soon as previous run finishes)
# MAGIC
# MAGIC    - **Table Update** (run automatically whenever one or more specified tables are updated)
# MAGIC
# MAGIC    - **File arrival** (run when files arrive in a location)
# MAGIC
# MAGIC    - **Model Update** (run a job when a model change)
# MAGIC
# MAGIC 5. **Set Up a Scheduled Run:**  
# MAGIC    - Choose **Scheduled**.
# MAGIC
# MAGIC    - Click on the **Advanced** section to see more options.
# MAGIC
# MAGIC 6. **Configure the Schedule:**  
# MAGIC    - Set the job to run **every day** at a time of your choice.
# MAGIC
# MAGIC    - Make sure to select your specific time zone.
# MAGIC
# MAGIC    - **Tip:** Set the schedule to start two minutes from your current time so you don’t have to wait long for the job to run.
# MAGIC
# MAGIC **NOTE:** Scheduling your job ensures it runs automatically at the times you specify. You can also start your job run by clicking **Run Now** at the top of your job.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## F. Validate your Job
# MAGIC
# MAGIC Check if the table **high_risk_borrowers_silver** exist under your schema.
# MAGIC
# MAGIC Also, run the below command to verify data of **high_risk_borrowers_silver**
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM high_risk_borrowers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Please ensure that the number of high-risk borrowers is **143**, which should match the total row count in the **high_risk_borrowers_silver** table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Cancel Trigger
# MAGIC
# MAGIC Once your job has executed, make sure to **delete** the trigger under **Schedules & Triggers** on the right side of the Tasks tab. Otherwise, it will continue to run indefinitely until cancelled.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>