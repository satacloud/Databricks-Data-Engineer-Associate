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
# MAGIC # 07 - Automating Workloads with Scheduling and Triggers
# MAGIC
# MAGIC In this lesson, we will explore the different scheduling options available for your jobs. We will also learn how to add a file arrival trigger to a task.
# MAGIC
# MAGIC This demo will cover:
# MAGIC - Exploring the various scheduling options under Schedules & Triggers
# MAGIC - Adding parameters to your job
# MAGIC - Configuring a file arrival trigger for your job
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Automate your job
# MAGIC - Use parameters

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

# MAGIC %run ./Includes/Classroom-Setup-07

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore Your Schema
# MAGIC Complete the following to explore your **dbacademy.labuser** schema:
# MAGIC
# MAGIC 1. In the left navigation bar, select the catalog icon:  ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/catalog_icon.png)
# MAGIC
# MAGIC 2. Locate the catalog called **dbacademy** and expand the catalog.
# MAGIC
# MAGIC 3. Expand your **labuser** schema. 
# MAGIC
# MAGIC 4. Notice that within your schema you will find two tables named as **sales_bronze** and **orders_bronze**.
# MAGIC
# MAGIC **Note:** If you have completed the 05Lab Exercise, you may find additional tables under your schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. View Your Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Complete the following steps to view the notebook file you will use in this job. All files are located in the **Task Files** folder within the directory for the corresponding lesson number.
# MAGIC
# MAGIC 1. Navigate to (or click the link for) the notebook: [Task Files/Lesson 07 Files/7.1 - Creating customers table]($./Task Files/Lesson 07 Files/7.1 - Creating customers table).  
# MAGIC   - Review the notebook. Note that it stores multiple widget values (passed as parameters) in a variable and creates a new table called **customers_bronze**.
# MAGIC
# MAGIC 2. After reviewing the notebook file, close it and return to this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Create the Job
# MAGIC
# MAGIC Complete the steps below to add new task into your Retail Job
# MAGIC <!-- Run the cell below to automatically create the starter job for this demonstration (The starter job includes all tasks completed in **04 Demo - Creating a Job Using Lakeflow Jobs UI).**.
# MAGIC
# MAGIC
# MAGIC     **NOTE:** The code below creates the required Databricks job for this demonstration using the [Databricks SDK](https://docs.databricks.com/aws/en/dev-tools/sdk-python). The method for creating the job is defined in the [Classroom-Setup-Common]($./Includes/Classroom-Setup-Common) notebook. While the [Databricks SDK](https://databricks-sdk-py.readthedocs.io/en/latest/) is used here, the SDK is beyond the scope of this course. Feel free to explore on your own. -->
# MAGIC
# MAGIC ###D1. Creating the starter Job

# COMMAND ----------

## Creates the starter job from the end of '04 Demo - Creating a Job Using Lakeflow Jobs UI'

job_tasks = [
    {
        'task_name': 'ingesting_orders',
        'file_path': '/Task Files/Lesson 04 Files/4.1 - Creating orders table',
        'depends_on': None
    },
    {
        'task_name': 'ingesting_sales',
        'file_path': '/Task Files/Lesson 04 Files/4.2 - Creating sales table',
        'depends_on': None
    }
]
 
myjob = DAJobConfig(job_name=f"Demo_07_Retail_Job_{DA.schema_name}",
                        job_tasks=job_tasks,
                        job_parameters=[])

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Confirming your Job Details
# MAGIC Complete the following steps to confirm that the new starter job, which begins with **Demo_07**, was created successfully and matches the job from **04 Demo - Creating a Job Using Lakeflow Jobs UI**:
# MAGIC
# MAGIC    a. Right-click on **Jobs and Pipelines** in the left navigation bar and select *Open Link in New Tab*.
# MAGIC
# MAGIC    b. Confirm that you see the job **Demo_07_Retail_Job_your-labuser-name**. Click the job to open it.
# MAGIC
# MAGIC    c. Select **Tasks** in the top navigation bar. The job should contain two tasks named **ingesting_orders** and **ingesting_sales**.
# MAGIC
# MAGIC    d. View the **Job details** section of the job. Confirm that **Performance optimized** mode enabled.
# MAGIC
# MAGIC    e. Note that this is the same job created in **04 Demo - Creating a Job Using Lakeflow Jobs UI**.
# MAGIC
# MAGIC    f. Leave the job page open and return to the instructions below.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Add a New Task to the Starter Job
# MAGIC
# MAGIC So far in our job we have ingested two tables into our job. 
# MAGIC
# MAGIC Next, we will add a new table via notebook Task
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Explore the notebook you will add to your job.
# MAGIC
# MAGIC    You can find the notebook in **Task Files** > **Lesson 07 Files** > **7.1 - Creating customers table**. 
# MAGIC
# MAGIC    **NOTE:** Direct link to the notebook: [Task Files/Lesson 07 Files/7.1 - Creating customers table]($./Task Files/Lesson 07 Files/7.1 - Creating customers table)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 2. Complete the following steps to add the **7.1 - Creating customers table** notebook as a task in your job.
# MAGIC
# MAGIC    a. If you are not in your **Demo_07_Retail_Job_your-labuser-name** job, right-click the **Jobs and Pipelines** button in the sidebar and select *Open Link in New Tab*.
# MAGIC    
# MAGIC    b. Find the **Demo_07_Retail_Job_your-labuser-name** job and go to the **Tasks** tab. 
# MAGIC
# MAGIC    c. Click **Add task**, then select **Notebook**.
# MAGIC
# MAGIC    d. Configure the task as specified below and Click **Create task** to save the task:
# MAGIC
# MAGIC | Setting   | Instructions |
# MAGIC |-----------|--------------|
# MAGIC | **Task name** | Enter **ingesting_customers** |
# MAGIC | **Type**      | Ensure **Notebook** is selected |
# MAGIC | **Source**    | Ensure **Workspace** is selected |
# MAGIC | **Path**      | Use the navigator to specify the path to [Task Files/Lesson 07 Files/7.1 - Creating customers table]($./Task Files/Lesson 07 Files/7.1 - Creating customers table)|
# MAGIC | **Compute**   | From the dropdown menu, select a **Serverless** cluster. (We will be using Serverless clusters for jobs in this course. You can specify a different cluster if required outside of this course.) |
# MAGIC | **Depends On**| None |
# MAGIC
# MAGIC
# MAGIC ![Lesson03_Notebook_task.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_scheduler/Lesson07_Notebook_task.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Explore Scheduling Options
# MAGIC
# MAGIC Complete the following steps to explore scheduling options and triggers within Lakeflow Jobs.
# MAGIC
# MAGIC 1. Return to your job.
# MAGIC
# MAGIC 2. Make sure you are in the **Tasks** tab of your job.
# MAGIC
# MAGIC 3. On the right-hand side of the Jobs UI, locate the **Job Details** section.  
# MAGIC    - **NOTE:** If the side panel is collapsed, click the left-pointing arrow icon to expand it.
# MAGIC
# MAGIC 4. Under the **Schedules & Triggers** section, click the **Add trigger** button to explore the options. There are five options (in addition to manual):
# MAGIC
# MAGIC    - **Scheduled** — you will see two types of schedule type **Simple** and **Advanced** 
# MAGIC       - Simple: This provides options for scheduling periodic job runs on a daily, hourly, or weekly basis.
# MAGIC       - Advanced: This option allows you to schedule jobs using CRON syntax for precise timing.
# MAGIC
# MAGIC    - **Table Update** - A trigger can be set up in platforms like Azure Databricks to run automatically whenever one or more specified tables are updated.
# MAGIC
# MAGIC    - **Continuous** — runs repeatedly with a short interval between runs.
# MAGIC
# MAGIC    - **File arrival** — monitors an external location or volume for new files. Note the **Advanced** settings, where you can adjust the time between checks and the delay after a new file arrives before starting a run.
# MAGIC
# MAGIC    - **Model Update** — automatically run a job when a model change occurs in Unity Catalog.
# MAGIC
# MAGIC 5. Leave the **Schedules & Triggers** panel open and return to the instructions below.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Configure the File Arrival Trigger for the Job
# MAGIC
# MAGIC In this step, we will set up a file arrival trigger to monitor a designated volume for new data files. The objective is to automatically start the job whenever a new file is detected in the specified location, enabling seamless and timely data processing.
# MAGIC
# MAGIC
# MAGIC   **NOTE:**  Databricks volumes are Unity Catalog objects representing a logical volume of storage in a cloud object storage location. Volumes provide capabilities for accessing, storing, governing, and organizing files. You can use volumes to store and access files in any format, including structured, semi-structured, and unstructured data.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to create a volume named **trigger_storage_location**. This volume will serve as the storage location to monitor for new files.  
# MAGIC    
# MAGIC    It will be created within the **dbacademy** catalog, inside your unique **labuser** schema.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS trigger_storage_location

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete the following steps to view your new volume **trigger_storage_location** in your **dbacademy.labuser** schema:
# MAGIC
# MAGIC    a. In the left navigation bar, click the **Catalog** ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/catalog_icon.png) icon.
# MAGIC
# MAGIC    b. Locate and expand the catalog named **dbacademy**.
# MAGIC
# MAGIC    c. Expand your **labuser** schema.
# MAGIC
# MAGIC    d. Expand **Volumes** and confirm that the **trigger_storage_location** volume appears.
# MAGIC
# MAGIC    e. Expand the **trigger_storage_location** volume and verify that it does **not** contain any files.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3. You can also use the `SHOW VOLUMES` statement to view available volumes in your schema (database).

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Run the following cell to get the path to this volume using the custom `DA` object created for this course.
# MAGIC
# MAGIC     **NOTE:** You can also select your volume under the catalog, click the three ellipses, and then select *Copy volume path* to get the volume path.

# COMMAND ----------

your_volume_path = (f"/Volumes/{DA.catalog_name}/{DA.schema_name}/trigger_storage_location/")
print(your_volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Complete the following to configure the **File Arrival** trigger on your job:
# MAGIC
# MAGIC    a. Navigate back to the browser tab with your job.
# MAGIC
# MAGIC    b. In your job, **click Add Trigger** in the Job details pane, and under Trigger type, select File Arrival for the trigger type.
# MAGIC
# MAGIC    c. Paste the path above into the **Storage location** field
# MAGIC
# MAGIC    d. Click **Test Trigger** to verify the correct path
# MAGIC
# MAGIC     - **NOTE:** You should see **Success**. If not, verify that you have run the cell above and copied all of the cell output into **Storage location**
# MAGIC
# MAGIC    e. Expand the **Advanced** options. Notice that you can set different trigger options.
# MAGIC
# MAGIC    f. Click **Save**
# MAGIC
# MAGIC **NOTE:**  There is a limit of 1000 files that can be triggered using file arrival trigger. 
# MAGIC
# MAGIC For Reference: https://docs.databricks.com/aws/en/jobs/file-arrival-triggers#limitations

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Setting Task Parameters
# MAGIC The Task notebook for this demo will needs to know the name of the catalog and schema we are working with. We can configure this using **Task parameters** (you can also use **Job Parameters** and it will get pushed down to all tasks). 
# MAGIC
# MAGIC   This provides flexibility and the ability to reuse code.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to view your **catalog** and **schema** names. We will need this when setting our parameters.

# COMMAND ----------

print(f"catalog : {DA.catalog_name}")
print(f"schema : {DA.schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete the following steps to **set the task parameters**:
# MAGIC
# MAGIC    a. Go back to your Task **ingesting_customers**. In **Task details** pane, under **Parameters**, click **Add**.
# MAGIC
# MAGIC    b. Set the following key-value pairs:
# MAGIC    - **catalog** (key) =  **dbacademy** (value) 
# MAGIC    - **schema** (key) = your **labuser** schema name from the above cell output (value) 
# MAGIC
# MAGIC    c. Click **Save task**.
# MAGIC
# MAGIC 3.  Click to open the [Task Files/Lesson 07 Files/7.1 - Creating customers table]($./Task Files/Lesson 07 Files/7.1 - Creating customers table) notebook. This notebook is used in the **ingesting_customers** task. Notice the following in the notebook:
# MAGIC
# MAGIC       - The `my_catalog` variable is obtaining the value from the `catalog` parameter we set in the task using the following:
# MAGIC          - `my_catalog = dbutils.widgets.get('catalog')`
# MAGIC
# MAGIC       - The `my_schema` variable is obtaining the value from the `schema` parameter we set in the task using the following: 
# MAGIC          - `my_schema = dbutils.widgets.get('schema')`
# MAGIC
# MAGIC       - The `my_volume_path` variable uses the parameters we set to point to your **trigger_storage_location** volume using:
# MAGIC          - `f"/Volumes/{my_catalog}/{my_schema}/trigger_storage_location/"`
# MAGIC
# MAGIC 4.  Close the **Creating customer table** notebook
# MAGIC
# MAGIC
# MAGIC ![Lesson03_TriggerJob.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_scheduler/Lesson07_TriggerJob.png)
# MAGIC
# MAGIC
# MAGIC    Your File Arrival Trigger and ingesting_customers Task should look like the above screenshot.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##H. Adding a File into Volume
# MAGIC Run the cell below to land a new file in your volume **trigger_storage_location**. 
# MAGIC
# MAGIC For the File Arrival Trigger, only new files are ingested and processed. Modified files will not trigger the run again. To re-trigger, you need to manually update the file name.
# MAGIC
# MAGIC **NOTE:** This is a custom course function we created for adding data to your volume to mimic data being loaded to cloud storage.

# COMMAND ----------

DA.copy_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Monitoring the Run
# MAGIC
# MAGIC Once the trigger is configured, Databricks will automatically monitor the storage location for new files (checked every minute by default). Follow these steps to monitor your job runs:
# MAGIC
# MAGIC 1. **Open the Runs Tab**
# MAGIC    - In the upper-left corner, click the **Runs** tab.
# MAGIC    - Look for the **Trigger status**. If you don't see it, wait a minute and verify your **File arrival** trigger setup if needed.
# MAGIC
# MAGIC 2. **Check Trigger Evaluation**
# MAGIC    - The trigger will show as evaluated. If no new files are found, the job will not run.
# MAGIC
# MAGIC 3. **View Job Run Details**
# MAGIC    - After the job runs (usually within 1-2 minutes), click the **Start time** to see run details.
# MAGIC
# MAGIC > **Tip:**  
# MAGIC > To manually trigger a run with different parameters, go to the job configuration page, click **Edit task** from the **Run output** page, then click the down arrow next to **Run now** and select **Run now with different settings**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Viewing Data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC [Parameterize jobs](https://docs.databricks.com/aws/en/jobs/parameters) documentation
# MAGIC
# MAGIC [Automating jobs with schedules and triggers](https://docs.databricks.com/aws/en/jobs/triggers) documentation

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>