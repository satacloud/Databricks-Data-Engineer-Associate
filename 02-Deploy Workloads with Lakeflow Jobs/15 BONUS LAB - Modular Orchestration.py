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
# MAGIC # 15 BONUS LAB - Modular Orchestration
# MAGIC
# MAGIC #### Duration: ~20 minutes
# MAGIC
# MAGIC **This is an optional lab that can be completed after class if you're interested in practicing modular design. You will most likely not have time to complete this bonus lab during a live class.**
# MAGIC
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Add a Run Job Task to your Job
# MAGIC * Repairing Failure Task
# MAGIC
# MAGIC ## Lab Overview
# MAGIC Now, Once we done with processing high-risk and low-risk borrowers separately, Its time to add personal details and create gold tables

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
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** The **DA** object is only used in Databricks Academy courses and is not available outside of these courses. 

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-15L

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Creating Starter Job
# MAGIC
# MAGIC In this section, you'll programmatically create a Databricks job using the Databricks SDK.
# MAGIC
# MAGIC > **Note:** The method for creating the job is defined in the [Classroom-Setup-Common]($./Includes/Classroom-Setup-Common) notebook. While the [Databricks SDK](https://databricks-sdk-py.readthedocs.io/en/latest/) is used here, a deep dive into the SDK is beyond the scope of this course.
# MAGIC
# MAGIC **Instructions:**
# MAGIC - Run the command below to automatically create a job that includes all tasks completed up to Previous Lab.

# COMMAND ----------

DA.lesson_15_starter_job()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Explore the Task Files
# MAGIC
# MAGIC So far, You have Ingested, transformed and added conditional Tasks into bank loan data. Now we are going to add more transformation and add personal details into **high_risk_borrowers_silver** and **low_risk_borrowers_silver** table using **borrower_details_bronze** table.
# MAGIC
# MAGIC
# MAGIC Please find the notebooks for this lab in **Task Files** > **Lesson 15 Files**. Use the links below to view and explore the code for each task:
# MAGIC
# MAGIC - [Task Files/Lesson 15 Files/15.1 - Adding Personal Details on high risk borrower table]($./Task Files/Lesson 15 Files/15.1 - Adding Personal Details on high risk borrower table)
# MAGIC
# MAGIC - [Task Files/Lesson 15 Files/15.2 - Adding Personal Details on low risk borrower table]($./Task Files/Lesson 15 Files/15.2 - Adding Personal Details on low risk borrower table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Creating a Run Job
# MAGIC
# MAGIC You will create a separate run job by following these steps:
# MAGIC
# MAGIC 1. Right-click on **Jobs and Pipelines** in the left navigation bar and open the link in a new tab.
# MAGIC 2. Click **Create** and select **Job** from the drop-down menu.
# MAGIC 3. Name your job as **Lab_15_Run_Job**
# MAGIC 3. Click on **Notebook** task from recommended task and configure the notebook task as shown below:
# MAGIC
# MAGIC | Setting      | Instructions |
# MAGIC |--------------|--------------|
# MAGIC | Task name    | Enter **run_job_notebook_task** |
# MAGIC | Type         | Select **Notebook** |
# MAGIC | Source       | Choose **Workspace** |
# MAGIC | Path         | Use the navigator to specify the path [Task Files/Lesson 15 Files/15.2 - Adding Personal Details on low risk borrower table]($./Task Files/Lesson 15 Files/15.2 - Adding Personal Details on low risk borrower table) |
# MAGIC | Compute      | Choose **Serverless** |
# MAGIC
# MAGIC ![Lesson15_run_job](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lab_mo/Lesson15_run_job.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Adding New Tasks to the Master Job
# MAGIC
# MAGIC Next, you will add both a notebook task and a run job task to your master job.
# MAGIC
# MAGIC #### E1. Adding a Notebook Task
# MAGIC
# MAGIC 1. Go to all jobs from **Jobs & Pipelines** and select your master job named **Lab_15<-your schema name->**.
# MAGIC
# MAGIC 2. Click **Add Task**, select **Notebook**, and configure the task as follows:
# MAGIC
# MAGIC | Setting      | Instructions |
# MAGIC |--------------|--------------|
# MAGIC | Task name    | Enter **creating_high_risk_borrower_gold_table** |
# MAGIC | Type         | Select **Notebook** |
# MAGIC | Source       | Select **Workspace** |
# MAGIC | Path         | Use the navigator to specify the path [Task Files/Lesson 15 Files/15.1 - Adding Personal Details on high risk borrower table]($./Task Files/Lesson 15 Files/15.1 - Adding Personal Details on high risk borrower table) under **Lesson 15 Files** |
# MAGIC | Compute      | Choose **Serverless** |
# MAGIC | Depends on   | Select **processing_high_risk_borrowers** and **creating_borrower_details_table** |
# MAGIC | Run if dependencies | Select **All Succeeded** |
# MAGIC | Parameters   | Click **Add**. For key, enter **should_fail**; for value, enter **"true"** |
# MAGIC
# MAGIC 3. Click **Create task**.
# MAGIC
# MAGIC ![Lesson15_notebook_task](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lab_mo/Lesson15_notebook_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC #### E2. Adding a Run Job Task
# MAGIC
# MAGIC 1. Right-click on **Jobs and Pipelines** in the left navigation bar and open the link in a new tab.
# MAGIC
# MAGIC 2. Go to the master job named **Lab_15<-your schema name->**.
# MAGIC
# MAGIC 3. Click on **Add Task** and Select **Run Job** and configure the task as follows:
# MAGIC
# MAGIC | Setting      | Instructions |
# MAGIC |--------------|--------------|
# MAGIC | Task name    | Enter **creating_low_risk_borrower_gold_table** |
# MAGIC | Type         | Ensure **Run Job** is selected |
# MAGIC | Job       | Select **Lab_15_Run_Job** |
# MAGIC | Depends      | Select **processing_low_risk_borrowers** and **creating_borrower_details_table** |
# MAGIC | Run if dependencies | Choose **All Succeeded** |
# MAGIC
# MAGIC
# MAGIC 4. Click **Create task**.
# MAGIC
# MAGIC
# MAGIC ![Lesson15_full_job](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lab_mo/Lesson15_full_job.png)
# MAGIC
# MAGIC
# MAGIC 5. Click **Run Now** to execute the entire job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Repairing a Run
# MAGIC
# MAGIC If your master job fails at the last task, follow these steps to repair the run:
# MAGIC
# MAGIC 1. Go to your job **Lab_15_<-your schema name->**.
# MAGIC 2. In the **Runs** section, locate the failed run.
# MAGIC 3. In the graph section, click on the failed task.
# MAGIC ![Lesson15_failed_task](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lab_mo/Lesson15_failed_task.png)
# MAGIC
# MAGIC 4. This will open the script snapshot page. Click **Repair run**.
# MAGIC 5. On the right, find the **Job parameters** section and update the parameter:
# MAGIC    * Key: **should_fail**
# MAGIC    * Value: **false**
# MAGIC 6. Click **Repair Run** to retry the run.
# MAGIC
# MAGIC
# MAGIC ![Lesson15_corrected_task](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lab_mo/Lesson15_corrected_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##G. Viewing Your Job
# MAGIC
# MAGIC Once your run is successful, your final master run should look like below
# MAGIC ![Lesson15_master_job](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lab_mo/Lesson15_master_job.png)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>