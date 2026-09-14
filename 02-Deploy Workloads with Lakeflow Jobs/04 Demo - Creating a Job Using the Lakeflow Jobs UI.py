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
# MAGIC # 04 - Creating a Job Using the Lakeflow Jobs UI
# MAGIC
# MAGIC In this lesson, we will start by creating a job using a single notebook and SQL Query and exploring the Lakeflow Jobs UI.
# MAGIC
# MAGIC In this demonstration, we will walk through the process of creating and running a Lakeflow Job in Databricks. 
# MAGIC
# MAGIC The demo will include:
# MAGIC
# MAGIC - Creating a new job with two tasks: one using a notebook and the other using a SQL query.
# MAGIC - Modifying task configurations.
# MAGIC - Exploring the Lakeflow Jobs UI to understand how to modify, monitor, and manage job runs.
# MAGIC
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Schedule a notebook task and Sql task in a Databricks Lakeflow Job
# MAGIC - Running a Job which have multiple task
# MAGIC
# MAGIC ## Data Overview 
# MAGIC We are going to use a retail dataset for this course across all demos. We have three different dimensions/data available: **customers data, sales data, and orders data** for our retail dataset.
# MAGIC

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
# MAGIC **NOTE:** If you want to use **Serverless** compute, make sure you are on the **latest version (version > 1)**. Otherwise, the setup will not work correctly.
# MAGIC
# MAGIC - [Select an environment version](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version). 

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore Your Environment

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Explore your Class Schema
# MAGIC
# MAGIC Complete the following to explore your **dbacademy.labuser** schema:
# MAGIC
# MAGIC 1. In the left navigation bar, select the catalog icon:  ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/catalog_icon.png)
# MAGIC
# MAGIC 2. Locate the catalog called **dbacademy** and expand the catalog.
# MAGIC
# MAGIC 3. Expand your **labuser** schema. 
# MAGIC
# MAGIC 4. Notice that within your schema no tables exist.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Explore your Source Catalogs
# MAGIC
# MAGIC #### dbacademy_bank Catalog
# MAGIC
# MAGIC Complete the following to explore your **dbacademy_bank** and **dbacademy_retail** catalogs. We will be ingesting tables and files from these locations during the demos and labs:
# MAGIC
# MAGIC 1. In the left navigation bar, select the catalog icon:  ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/catalog_icon.png)
# MAGIC
# MAGIC 2. Locate the catalog called **dbacademy_bank** and expand the catalog.
# MAGIC
# MAGIC 3. Expand your **v01** schema. 
# MAGIC
# MAGIC 4. Notice that within your schema a single volume named **banking** exists with a CSV file.
# MAGIC
# MAGIC #### dbacademy_retail Catalog
# MAGIC
# MAGIC 1. In the left navigation bar, select the catalog icon:  ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/catalog_icon.png)
# MAGIC
# MAGIC 2. Locate the catalog called **dbacademy_retail** and expand the catalog.
# MAGIC
# MAGIC 3. Expand your **v01** schema. 
# MAGIC
# MAGIC 4. Notice that within your schema:
# MAGIC   - Multiple tables exist
# MAGIC   - In **Volumes** two volumes exist: **retail-pipeline** and **source_files**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Viewing Your Files 
# MAGIC Complete the following steps to review the notebook and SQL file you will use in this job. All files are located in the **Task Files** folder within the directory for the corresponding lesson number.
# MAGIC
# MAGIC ### C1. Viewing Notebook File
# MAGIC 1. Navigate to (or click the link for) the notebook: [Task Files/Lesson 04 Files/4.1 - Creating orders table]($./Task Files/Lesson 04 Files/4.1 - Creating orders table).  
# MAGIC   - Review the notebook and note that it reads data from **dbacademy_retail.v01.sales_orders** and creates a simple table named **orders_bronze** in your designated **dbacademy.labuser** schema.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Create the Job
# MAGIC
# MAGIC Complete the steps below to create a Lakeflow Job with two tasks:
# MAGIC
# MAGIC - A notebook task  
# MAGIC - A SQL file task
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Generate your Job Configuration
# MAGIC
# MAGIC 1. Run the cell below to print out values you'll use to configure your job in subsequent steps. Make sure to specify the correct job name and Files.
# MAGIC
# MAGIC     **NOTE:** The `DA.print_job_config` object is specific to the Databricks Academy course. It will output the necessary information to help you create the job.

# COMMAND ----------

DA.print_job_config(job_name_extension='Demo_04_Retail_Job', 
                    file_paths='/Task Files/Lesson 04 Files',
                    Files=[
                        '4.1 - Creating orders table'
                    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Create and Name the Job
# MAGIC
# MAGIC Complete the following steps to create and name your job.
# MAGIC
# MAGIC 1. Right-click the **Jobs and Pipelines** button in the sidebar and select *Open Link in New Tab*.
# MAGIC
# MAGIC 2. In the new tab, confirm that you are in the **Jobs & Pipelines** tab.
# MAGIC
# MAGIC 3. Click the **Create** button and select **Job** from the dropdown.
# MAGIC
# MAGIC 4. In the top-left corner of the screen, you’ll see a default job name based on the current date and time (for example, *New Job Jul 29, 2025, 11:46 AM*).
# MAGIC
# MAGIC 5. Change the **Job Name** to the one provided in the previous cell (for example: **Demo_04_Retail_Job_labuser123**).
# MAGIC
# MAGIC 6. Leave the job open and proceed to the next steps.
# MAGIC
# MAGIC **NOTE:** If you click on a recommended task (like **Notebook**), you will be redirected to a different page than shown in the screenshot below.
# MAGIC
# MAGIC ![Lesson04_Jobs_UI.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_creating_a_job/Lesson04_Jobs_UI.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Create the Notebook Task
# MAGIC
# MAGIC Complete the following steps to add a notebook task.
# MAGIC
# MAGIC 1. In the Lakeflow Jobs UI, You may see some task suggestion. For Eg., **Notebook** or **SQL File**
# MAGIC
# MAGIC 2. Select the **Notebook** task type.
# MAGIC
# MAGIC 3. Configure the task using the settings below:
# MAGIC
# MAGIC | Setting         | Instructions |
# MAGIC |-----------------|--------------|
# MAGIC | **Task name**   | Enter **ingesting_orders** |
# MAGIC | **Type**        | Select **Notebook** |
# MAGIC | **Source**      | Choose **Workspace** |
# MAGIC | **Path**        | Use the file navigator to locate and select **Notebook #1**:<br>**./Task Files/Lesson 04 Files/4.1 - Creating orders table** |
# MAGIC | **Compute**     | Select a **Serverless** cluster from the dropdown menu.<br>(We will use Serverless clusters for all jobs in this course. You may specify a different cluster outside of this course, if needed.) <br></br>**NOTE**: If you selected your all-purpose cluster, you may get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.
# MAGIC  |
# MAGIC | **Create task** | Click **Create task** |
# MAGIC
# MAGIC 4. Keep the Lakeflow Jobs UI open, you’ll be adding another task in the next step.
# MAGIC ##### For better performance, please enable Performance Optimized Mode in Job Details. Otherwise, it might take 6 to 8 minutes to initiate execution.
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC #### Notebook Task Setup
# MAGIC
# MAGIC ![Lesson04_Notebook_task.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_creating_a_job/Lesson04_Notebook_task.png)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Create the SQL Query Task
# MAGIC
# MAGIC Follow these steps to add a SQL file as a task:
# MAGIC
# MAGIC 1. In the Lakeflow Jobs UI, click **Add task**.
# MAGIC
# MAGIC 2. Select the **SQL query** task type.
# MAGIC
# MAGIC 3. Configure the task using the settings below:
# MAGIC
# MAGIC | Setting           | Instructions |
# MAGIC |-------------------|--------------|
# MAGIC | **Task name**     | Enter **ingesting_sales** |
# MAGIC | **Type**          | Select **SQL** |
# MAGIC | **SQL task**      | Select **Query** |
# MAGIC | **SQL query**     | From the dropdown, choose the SQL file:<br>**4.2 - Creating sales table - SQL Query** |
# MAGIC | **SQL warehouse** | From the dropdown, select your SQL warehouse from drop-down menu |
# MAGIC | **Depends on**    | No task should be selected here.<br>(Unselect **ingesting_orders** if it is selected.) |
# MAGIC | **Create task**   | Click **Create task** |
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC #### SQL Task Setup
# MAGIC
# MAGIC ![Lesson04_task1_sql.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_creating_a_job/Lesson04_task1_sql.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D5. Explore and Modify the Job Details
# MAGIC
# MAGIC 1. Navigate to the Job Details page. In the right pane, you will find the following job-level details:
# MAGIC
# MAGIC - **Job Details:** Information such as Job ID, creator, and more.
# MAGIC - **Schedulers and Triggers:** View and configure various scheduling options and triggers for the job.
# MAGIC - **Job Parameters:** Options to declare parameters that apply to the entire job.
# MAGIC
# MAGIC  
# MAGIC #### For better performance, please turn on Performance Optimized Mode in Job Details.
# MAGIC
# MAGIC ##### Performance Optimized Mode
# MAGIC - Enables fast compute startup and improved execution speed.
# MAGIC
# MAGIC ##### Standard Mode
# MAGIC - Disabling performance optimization results in startup times similar to Classic infrastructure and may reduce your costs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Run the Job
# MAGIC
# MAGIC 1. In the upper-right corner, find the kebab menu (three dots) next to the **Run now** button. You will see options such as **Edit as YAML**, **Clone job**, **View as code**, and **Delete job**.
# MAGIC
# MAGIC 2. Click **View as code** to see your job represented in three formats: YAML, Python (SDK and DABS), and JSON.
# MAGIC
# MAGIC 3. Return to the main job page and click the **Run now** button in the top right to start the job.
# MAGIC
# MAGIC     **NOTE:** After starting the job, you can click the link to view the run in progress. In the next section, you will learn another way to view past and current job runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Review the Job Run
# MAGIC
# MAGIC 1. On the Job Details page, click the **Runs** tab in the top-left corner of the screen (you should currently be on the **Tasks** tab).
# MAGIC
# MAGIC 2. In the Runs tab of your job, you can see detailed information about each run.
# MAGIC    At the top, there is a time-based bar chart where:
# MAGIC
# MAGIC    - The X-axis represents each run.
# MAGIC    - The Y-axis shows the time taken by each task within that run.
# MAGIC 3. Color Coding
# MAGIC    -    key: green = success
# MAGIC    -    red = failed
# MAGIC    -    yellow = waiting/retry, 
# MAGIC    -    pink = skipped,
# MAGIC    -    grey = pending/canceled/timeout.
# MAGIC
# MAGIC    
# MAGIC Below the chart, you will find a tabular matrix view that provides the same information in detail. This table starts with the timestamp and includes fields such as run_id, run status, duration, and other relevant details for each run.
# MAGIC    
# MAGIC ![Lesson04_view_runs.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_creating_a_job/Lesson04_view_runs.png)
# MAGIC
# MAGIC 4. Open the output details by clicking the timestamp under the **Start time** column:
# MAGIC
# MAGIC    - If **the job is still running**, you will see the active state with a **Status** of **Pending** or **Running** in the right-side panel.
# MAGIC    
# MAGIC    - If **the job has completed**, you will see the full execution results with a **Status** of **Succeeded** or **Failed** in the right-side panel.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. View Your New Tables
# MAGIC 1. From left-hand pane, select **Catalog**. Then drill down from **dbacademy** catalog.
# MAGIC
# MAGIC 2. Expand your unique schema name.
# MAGIC
# MAGIC 3. Notice that within your schema a table named **sales_bronze** and **orders_bronze**

# COMMAND ----------

# MAGIC %md
# MAGIC ##H. Query Your New Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying sales_bronze table
# MAGIC SELECT * 
# MAGIC FROM sales_bronze
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying orders_bronze table
# MAGIC SELECT * 
# MAGIC FROM orders_bronze
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Lakeflow Jobs Documentation](https://docs.databricks.com/aws/en/jobs/)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>