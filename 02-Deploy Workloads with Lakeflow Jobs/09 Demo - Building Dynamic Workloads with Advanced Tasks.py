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
# MAGIC # 09 - Building Dynamic Workloads with Advanced Tasks
# MAGIC
# MAGIC In this demo, we will show how to build dynamic Lakeflow jobs using conditional logic (`if-else`) and iterative tasks (`for each` loop).
# MAGIC
# MAGIC This demo will cover:
# MAGIC - Defining dependencies between tasks
# MAGIC - Adding a conditional `if-else` task
# MAGIC - Adding an iterative `for each` task
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC Create and visualize a dynamic Lakeflow job with multiple tasks and dependencies.
# MAGIC
# MAGIC ![Lesson04_final_job](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_advanced_tasks/Lesson09_final_job.png)
# MAGIC
# MAGIC After completing this demo, your job will look like above.

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

# MAGIC %run ./Includes/Classroom-Setup-09

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
# MAGIC 4. Notice that within your schema you will find three tables named as **sales_bronze**, **customers_bronze** and **orders_bronze**.
# MAGIC
# MAGIC **Note:** If you have completed the 05 Lab Exercise, you may find additional tables under your schema.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## C. View Your Notebook
# MAGIC
# MAGIC Follow these steps to view the notebook files used in this job. All files are located in the **Task Files** folder within the directory for the corresponding lesson number.
# MAGIC
# MAGIC 1. Navigate to (or click the link for) the notebook: [Task Files/Lesson 09 Files/9.1 - Joining Customers and Sales Table]($./Task Files/Lesson 09 Files/9.1 - Joining Customers and Sales Table)
# MAGIC    - Review the notebook. It creates a new table by joining the **customers_bronze** and **sales_bronze** tables. Pay attention to the code that sets the task value for the key **has_duplicates**.
# MAGIC
# MAGIC 2. Navigate to (or click the link for) the notebook: [Task Files/Lesson 09 Files/9.2 - Joining Customers and Orders Table]($./Task Files/Lesson 09 Files/9.2 - Joining Customers and Orders Table)
# MAGIC    - Review the notebook. It creates a new table by joining the **customers_bronze** and **orders_bronze** tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Adding Task in Job
# MAGIC Complete the steps below to add new task into your Retail Job
# MAGIC
# MAGIC ###D1. Creating the Starter Job
# MAGIC 1. Next, we will add the notebooks listed below as tasks to our job using the Databricks SDK. This approach avoids manually adding notebook tasks, as we've already done it in previous demonstrations and labs:
# MAGIC
# MAGIC
# MAGIC    - **9.1 - Joining Customers and Sales Table**  
# MAGIC
# MAGIC    - **9.2 - Joining Customers and Orders Table**  
# MAGIC    
# MAGIC    Run the cell below to build the starter job that we have been continually building in this course. These commands will set up your job with all work completed so far and add the required tasks for this demonstration.
# MAGIC
# MAGIC

# COMMAND ----------

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
        },
        {
            'task_name': 'ingesting_customers',
            'file_path': '/Task Files/Lesson 07 Files/7.1 - Creating customers table',
            'depends_on': None
        }
        ,{
            'task_name': 'customers_sales_summary',
            'file_path': '/Task Files/Lesson 09 Files/9.1 - Joining Customers and Sales Table',
            'depends_on': [
                        {'task_key':'ingesting_customers'},
                        {'task_key': 'ingesting_sales'}
                        ]
        }
        ,{
            'task_name' : 'customers_orders_report',
            'file_path': '/Task Files/Lesson 09 Files/9.2 - Joining Customers and Orders Table',
            'depends_on': None
        }
    ]

myjob = DAJobConfig(job_name=f"Demo_09_Retail_Job_{DA.schema_name}",
                    job_tasks=job_tasks,
                    job_parameters=[
                        {'name':'catalog', 'default':'dbacademy'},
                        {'name':'schema', 'default':f'{DA.schema_name}'}
                    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Set Dependencies on the Tasks
# MAGIC
# MAGIC In this step, we will modify the existing job to define task dependencies. Specifically, we'll configure the main task to run only after all preceding tasks have completed successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Complete the following to review the job and set the following dependencies to the **customers_orders_report** task:
# MAGIC    - **ingesting_orders**
# MAGIC    - **ingesting_customers**
# MAGIC
# MAGIC 1. Navigate to **Jobs and Pipelines** and open it in a new tab.
# MAGIC
# MAGIC 2. Select your new job that starts with **Demo_09_Retail_Job_labuser**.
# MAGIC
# MAGIC 3. Click on **Tasks** in the top navigation bar.
# MAGIC
# MAGIC 4. Review your job. You should see five tasks: 
# MAGIC    - **customers_orders_report**.
# MAGIC    - **ingesting_customers**, 
# MAGIC    - **ingesting_orders**, 
# MAGIC    - **ingesting_sales**, 
# MAGIC    - **customers_sales_summary**,
# MAGIC
# MAGIC 5. Select the **customers_sales_summary** task. 
# MAGIC    - Notice that it depends on two tasks: **ingesting_customers** and **ingesting_sales**, with the dependency set to **All Succeeded**.
# MAGIC
# MAGIC 6. Next, select the **customers_orders_report** task and set the following task options: 
# MAGIC
# MAGIC    - In the **Depends on**, add **ingesting_orders** and **ingesting_customers**
# MAGIC
# MAGIC    - In **Run if dependencies**, set the dependency to **All Succeeded**.
# MAGIC
# MAGIC    - Select **Save task**.
# MAGIC
# MAGIC 7. Click on **Run_now** to run the job.
# MAGIC
# MAGIC <br></br>
# MAGIC #### Final Dependencies
# MAGIC ![Lesson04_dependencies](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_advanced_tasks/Lesson09_dependencies.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Add an If/Else Conditional Task
# MAGIC
# MAGIC In this section, you will add a conditional task to your job that checks for duplicate records in the **customers_sales_silver** table (Task **customers_sales_summary**). 
# MAGIC
# MAGIC Based on the result, the workflow will branch to handle duplicates appropriately.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Checking for Duplication Logic
# MAGIC
# MAGIC 1. Recall the logic used to detect duplicates in the **customers_sales_silver** table. (The **customers_sales_summary** task creates the **customers_sales_silver** table.)
# MAGIC
# MAGIC 2. In that notebook, we check whether the **customers_sales_silver** table contains any duplicate records. If duplicates are found, the result of this check (a boolean value) is stored as `has_duplicates` in the task output.
# MAGIC
# MAGIC **Code Reference:**
# MAGIC
# MAGIC         df = spark.sql("""
# MAGIC             SELECT * FROM customers_sales_silver
# MAGIC         """)
# MAGIC
# MAGIC         duplicate_exists = df.count() > df.dropDuplicates().count()
# MAGIC
# MAGIC         dbutils.jobs.taskValues.set(key="has_duplicates", value=duplicate_exists)
# MAGIC
# MAGIC **Notebook for Reference:**  
# MAGIC [Task Files/Lesson 09 Files/9.1 - Joining Customers and Sales Table]($./Task Files/Lesson 09 Files/9.1 - Joining Customers and Sales Table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Create an If/Else Conditional Task
# MAGIC
# MAGIC Create an **If/else conditional** task to determine what to execute based on whether duplicate records are found.
# MAGIC
# MAGIC 1. In your **Demo_09_Retail_Job_labuser** job, select **Add task**.
# MAGIC
# MAGIC 2. In the dialog box, scroll down to the **Advanced** section and select the task type **If/else condition**.
# MAGIC
# MAGIC 3. Name the new task **checking_for_duplicates**.
# MAGIC
# MAGIC 4. Set the **Depend on**  value to the **customers_sales_summary** task.
# MAGIC
# MAGIC 5. For the **Condition** field, use the parameter value created in the `customers_sales_summary` task:
# MAGIC
# MAGIC    **Dynamic Value References:**
# MAGIC    This syntax leverages dynamic value references to access output variables from earlier tasks in your job. When a task runs (like `customers_sales_summary`), its results—including variables registered or output by the task (such as `has_duplicates`)—become available for downstream tasks.
# MAGIC
# MAGIC    **By referencing** `tasks.customers_sales_summary.values.has_duplicates`, you dynamically pass the value (whether duplicates exist) to the If/Else condition. This enables conditional branching based on run-time data rather than static configuration, making your workflow adaptable and responsive to actual results.
# MAGIC
# MAGIC     **Adding Condition Field:** 
# MAGIC      - To manually add the parameter value, select the `{}` in the **Condition** field. 
# MAGIC      - Find and click on `tasks.customers_sales_summary.values`, it will automatically add a suffix of `my_value` to it.
# MAGIC      - Replace `my_value` with the task parameter created in the notebook: `has_duplicates`.
# MAGIC
# MAGIC 6. Then set the condition to check if this value  `== true`
# MAGIC
# MAGIC 7. Select **Save task** to create the conditional task.
# MAGIC      
# MAGIC
# MAGIC <br></br>
# MAGIC ##### IF/ELSE CONDITION TASK
# MAGIC
# MAGIC ![Lesson04_conditional_task.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_advanced_tasks/Lesson09_conditional_task.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Set the True Condition (Duplicates Exists) Task
# MAGIC
# MAGIC Complete the following steps to add a task that runs **only if duplicates are found** (`tasks.customers_sales_summary.values.has_duplicates == true`).
# MAGIC
# MAGIC 1. Select the **checking_for_duplicates** task.
# MAGIC
# MAGIC 2. Click **Add task**, and choose **Notebook**.
# MAGIC
# MAGIC 3. Name the new task **dropping_duplicate_records**.
# MAGIC
# MAGIC 4. Use the notebook [9.3 - If Condition: Dropping Duplicates]($./Task Files/Lesson 09 Files/9.3 - If Condition: Dropping Duplicates) as the task source.  
# MAGIC    - This notebook includes logic to remove duplicate records from the **customers_sales_silver** table.
# MAGIC
# MAGIC 5. In the **Depends on** field, set this task to depend on the **True** branch of the **checking_for_duplicates** task (`checking_for_duplicates (true)`).
# MAGIC
# MAGIC 6. Click on **Create Task** 
# MAGIC <br></br>
# MAGIC ##### TRUE DEPENDENCY TASK
# MAGIC
# MAGIC ![Lesson04_true_task](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_advanced_tasks/Lesson09_true_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### E4. Set the False Condition (No Duplicates) Task
# MAGIC
# MAGIC Complete the following steps to add a task that runs only if duplicates are not found (`tasks.customers_sales_summary.values.has_duplicates == false`).
# MAGIC
# MAGIC This setup ensures your job automatically handles duplicates if they exist, or proceeds to data transformation if no duplicates are found.
# MAGIC
# MAGIC 1. Select the **checking_for_duplicates** task.
# MAGIC
# MAGIC 2. Click **Add task**, and choose **Notebook**.
# MAGIC
# MAGIC 3. Name the new task **transforming_customers_sales_table**.
# MAGIC
# MAGIC 4. Use the notebook [Task Files/Lesson 09 Files/9.4 - Else Condition: Cleaning and Transforming Customers Sales Table]($./Task Files/Lesson 09 Files/9.4 - Else Condition: Cleaning and Transforming Customers Sales Table) as the task source.  
# MAGIC    - This notebook includes logic to clean and transform the **customers_sales_silver** table.
# MAGIC
# MAGIC 5. In the **Depends on** field, set this task to depend on the following:
# MAGIC    - The **False** branch of the **checking_for_duplicates** task (`checking_for_duplicates (false)`).
# MAGIC    - The **dropping_duplicate_records** task.
# MAGIC
# MAGIC 6. In the **Run if dependencies** field, select **None Failed**.  
# MAGIC    - This ensures:
# MAGIC      - If there are no duplicates, the transformation runs immediately.
# MAGIC      - If duplicates exist, the job runs the task **dropping_duplicate_records**, then proceeds with the transformation task **transforming_customers_sales_table**.
# MAGIC
# MAGIC 7. Click on **Create Task**.
# MAGIC
# MAGIC 8. Click on **Run now** button to run the job
# MAGIC
# MAGIC
# MAGIC <br></br>
# MAGIC ##### FALSE DEPENDENCY TASK
# MAGIC
# MAGIC ![Lesson04_false_task.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_advanced_tasks/Lesson09_false_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### E5. Job Confirmation  
# MAGIC Confirm your job looks like the following after adding the **If/else condition** and associated tasks:
# MAGIC
# MAGIC
# MAGIC ![Lesson04_IfElse](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_advanced_tasks/Lesson09_IfElse.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Add a For Each Loop Task
# MAGIC
# MAGIC In this section, you will add a downstream task to **customers_orders_report** that uses a **For Each** loop. This loop allows the job to execute the same task multiple times, once for each item in a specified list or collection. Execution may happen sequentially or concurrently, depending on job configuration. 
# MAGIC
# MAGIC In our case, from the customers_orders_silver table, we want to generate orders reports specifically for the states of **California, New York, and Virginia**. We will create three different tables to store state-specific data. We will use the same code script and dynamically pass the state name with the help of the **For Each** task.

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Explore the Notebooks
# MAGIC
# MAGIC 1. Review the notebook [Task Files/Lesson 09 Files/9.2 - Joining Customers and Orders Table]($./Task Files/Lesson 09 Files/9.2 - Joining Customers and Orders Table), which creates the **customers_orders_silver** table.
# MAGIC
# MAGIC 2. The [Task Files/Lesson 09 Files/9.5 - For Each: Customer orders State]($./Task Files/Lesson 09 Files/9.5 - For Each: Customer orders State) notebook will be executed in a loop for each state mentioned above. This script dynamically takes the state value and runs it for each state, creating a state-specific table with customers_order_silver data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Creating a For Each Iterator Task (Part 1 of 2)
# MAGIC
# MAGIC The "For Each" task involves two steps: first, define the iterator, and then specify the script to be iterated. Now, complete the following to add a **For Each** iterator task to loop over a series of **state** values.
# MAGIC
# MAGIC 1. In the same job, select the **customers_orders_report** task.
# MAGIC
# MAGIC 2. Select **Add task** and select the task type **For each**.
# MAGIC
# MAGIC 3. Name the task **customers_orders_state_wise_report_iterator**. 
# MAGIC
# MAGIC 4. Set the iterator **Inputs** field to `["CA", "NY", "VA"]`.  
# MAGIC   — These are the states with the highest number of customers.
# MAGIC
# MAGIC 5. Leave the **Concurrency** setting blank (recommended for single-node runs to avoid slowing down the process).
# MAGIC
# MAGIC 6. Set the **Depends on** field for this task to **customers_orders_report**.
# MAGIC
# MAGIC 7. Ensure the **Run if dependencies** is set to **All succeeded**.
# MAGIC
# MAGIC 8. Click on **Add a task to loop over**.
# MAGIC
# MAGIC #### For Each Iterator
# MAGIC
# MAGIC ![Lesson04_For_Each_Task_Iterator.png](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_advanced_tasks/Lesson09_For_Each_Task_Iterator.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### F3. Add a Task to Loop Over (Part 2 of 2)
# MAGIC
# MAGIC Now that the **For Each** task iterator is set, we need to specify a task to loop over. Complete the following to add the task to loop over.
# MAGIC
# MAGIC 1. Now that the iterator is set. Select **Add a task to loop over**. 
# MAGIC
# MAGIC 2. Name the task to iterate over **customers_orders_state_wise_report**.
# MAGIC
# MAGIC 3. Confirm the task **Type** is **Notebook** and the **Source** is **Workspace**.
# MAGIC
# MAGIC 4. Set the notebook path to [Task Files/Lesson 09 Files/9.5 - For Each: Customer orders State]($./Task Files/Lesson 09 Files/9.5 - For Each: Customer orders State), which is under the **Task Files** folder.
# MAGIC
# MAGIC 5. Set **Compute** to **serverless**.
# MAGIC
# MAGIC 6. Add a key-value parameter:
# MAGIC    - For key, add **state**. 
# MAGIC    - For the value, click on the **{}** symbol and select **input**.  
# MAGIC    - This will automatically pass each state code from the iterator loop to the notebook.
# MAGIC
# MAGIC 7. Click **Create task**.
# MAGIC
# MAGIC <br></br>
# MAGIC #### Iterator Task
# MAGIC ![Lesson04_iterator_task.pngg](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/demo_advanced_tasks/Lesson09_iterator_task.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Run the Entire Job
# MAGIC
# MAGIC To execute your job:
# MAGIC
# MAGIC 1. Click on **Run Now** to start the job.
# MAGIC
# MAGIC 2. Go to the **Runs** tab to monitor the progress and view the results of each task.
# MAGIC
# MAGIC This will run all tasks in your job according to the dependencies and logic you have set up.
# MAGIC
# MAGIC **NOTE:** This job will take about 5 minutes to complete.

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Conclusion and Results
# MAGIC
# MAGIC When your job run is successful, Click on catalog icon, go to your schema under dbacademy catalog. Look out for new tables **customers_sales_gold** , **customers_orders_ca_silver**, **customers_orders_ny_silver** and **customers_orders_va_silver**.

# COMMAND ----------

# MAGIC %md
# MAGIC The `customers_sales_gold` table does not require any transformation. It is our gold-tier table containing sales metrics such as **units_purchased, avg_price_per_unit, total_price**, customer details like **customer_id, customer_name, loyalty_segment**, and supporting order details.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM customers_sales_gold

# COMMAND ----------

# MAGIC %md
# MAGIC The tables **customers_orders_ca_silver**, **customers_orders_ny_silver**, and **customers_orders_va_silver** are state-specific and contain relevant data for each state. These tables will be further transformed to add business columns, which we will do in a future demo to create gold-tier tables. Now, query them to see the type of data they contain.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM customers_orders_ca_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM customers_orders_ny_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM customers_orders_va_silver

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>