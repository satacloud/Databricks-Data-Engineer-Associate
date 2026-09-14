# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 08 Lab - Create a Pipeline  
# MAGIC ### Estimated Duration: ~15-20 minutes
# MAGIC
# MAGIC In this lab, you'll migrate a traditional ETL workflow to a pipeline for incremental data processing. You'll practice building streaming tables and materialized views using Apache Spark™ Declarative Pipelines syntax.
# MAGIC
# MAGIC #### Your Tasks:
# MAGIC - Create a new pipeline  
# MAGIC - Convert traditional SQL ETL to declarative syntax for incremental processing 
# MAGIC - Configure pipeline settings  
# MAGIC - Define data quality expectations  
# MAGIC - Validate and run the pipeline
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC - Create a pipeline and execute it successfully using the Lakeflow Pipeline Editor.
# MAGIC - Modify and configure pipeline settings to align with specific data processing requirements.
# MAGIC - Integrate data quality expectations into a pipeline and evaluate their effectiveness.

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
# MAGIC ![Serverless Select](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/select-serverless.png)
# MAGIC <br></br>
# MAGIC   - How to select an environment version:
# MAGIC [AWS](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/dependencies#-select-an-environment-version) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/compute/serverless/dependencies#-select-an-environment-version)
# MAGIC
# MAGIC **NOTE:** This notebook was **developed and tested using Serverless V5**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this lab.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-Lab-basic

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Scenario
# MAGIC
# MAGIC Your data engineering team has identified an opportunity to modernize an existing ETL pipeline that was originally developed in a Databricks notebook. While the current pipeline gets the job done, it lacks the scalability, observability, efficiency, and automated data quality features required as your data volume and complexity grow.
# MAGIC
# MAGIC To address this, you've been asked to migrate the existing pipeline to a Apache Spark™ Declarative Pipeline. Spark Declarative Pipelines will enable your team to define data transformations more declaratively, apply data quality rules, and benefit from built-in optimization, lineage tracking, and monitoring.
# MAGIC
# MAGIC Your goal is to refactor the original notebook-based logic (shown in the cells below) into a Spark Declarative Pipeline.
# MAGIC
# MAGIC ### Requirements:
# MAGIC   - Migrate the ETL code below to a Spark Declarative Pipeline.
# MAGIC   - Add the required data quality expectations to the bronze table and silver table.
# MAGIC   - Create materialized views for the most up-to-date aggregated information.
# MAGIC
# MAGIC Follow the steps below to complete your task.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 22px 26px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC
# MAGIC   <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/genie-code.png" style="height: 44px; margin-bottom: 10px;">
# MAGIC
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 12px;">
# MAGIC     Need Help? Use Genie Code
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.6; margin-bottom: 16px;">
# MAGIC     Genie is an AI-powered assistant that can help you as you work through this lab. 
# MAGIC     Use it if you get stuck or want a little extra guidance.
# MAGIC   </div>
# MAGIC
# MAGIC   <a href="https://docs.databricks.com/aws/en/genie-code/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 22px; border-radius: 8px; text-decoration: none;">AWS</a> 
# MAGIC   <a href="https://learn.microsoft.com/en-us/azure/databricks/genie-code/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 22px; border-radius: 8px; text-decoration: none;">Azure</a> 
# MAGIC   <a href="https://docs.databricks.com/gcp/en/genie-code/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 22px; border-radius: 8px; text-decoration: none;">GCP</a>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Explore the Raw Data
# MAGIC
# MAGIC 1. Complete the following steps to view where the lab's streaming raw source files are coming from:
# MAGIC
# MAGIC    a. Select the **Catalog** icon ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/catalog_icon.png) in the left navigation bar.  
# MAGIC
# MAGIC    b. Expand your **labuser** catalog.  
# MAGIC
# MAGIC    c. Expand the **sdp_lab_1_bronze** schema.  
# MAGIC
# MAGIC    d. Expand the **lab_files** volume.  
# MAGIC
# MAGIC    e. You should see a single CSV file named **employees_1.csv**. If not, refresh the catalog.  
# MAGIC
# MAGIC    f. The files in the **lab_files** volume will be the data source files you will be ingesting.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell below to view the raw CSV file in your **lab_files** volume. Notice the following:
# MAGIC
# MAGIC    - It is a simple CSV file separated by commas.  
# MAGIC    - It contains headers.  
# MAGIC    - It has 7 rows in total (6 data records and 1 header row).  
# MAGIC    - The first record (row 2) is a test record and should not be included in the pipeline. It will be dropped by a data quality expectation later.

# COMMAND ----------

spark.sql(f'''
        SELECT *
        FROM csv.`/Volumes/{my_catalog}/sdp_lab_1_bronze/lab_files/`
        ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Current ETL Code
# MAGIC
# MAGIC Run each cell below to view the results of the current ETL pipeline. This will give you an idea of the expected output. Don't worry too much about the data transformations within the SQL queries.
# MAGIC
# MAGIC The focus of this lab is on using **declarative SQL** and creating a **Spark Declarative Pipeline**. You will not need to modify the transformation logic. 
# MAGIC
# MAGIC You will only need to modify the `CREATE` statements and `FROM` clauses to ensure data is read and processed incrementally in your pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.1 - CSV to Bronze
# MAGIC
# MAGIC Explore the code and run the cell. Observe the results. Notice that:
# MAGIC
# MAGIC - The CSV file in the volume is read in as a table named **employees_bronze_lab08** in the **labuser.sdp_lab_1_bronze** schema.  
# MAGIC - The table contains 6 rows with the correct column names.
# MAGIC
# MAGIC Think about what you will need to change when migrating this to a Spark Declarative Pipeline. Hints are added as comments in the code below.
# MAGIC
# MAGIC **NOTE:** In your Spark Declarative Pipeline, you will want to add data quality expectations to document any bad data coming into the pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Specify to use your labuser catalog
# MAGIC USE CATALOG IDENTIFIER(my_catalog);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sdp_lab_1_bronze.employees_bronze_lab08
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() AS ingestion_time,
# MAGIC   _metadata.file_name AS raw_file_name
# MAGIC FROM read_files(
# MAGIC   '/Volumes/' || my_catalog || '/sdp_lab_1_bronze/lab_files',
# MAGIC   format => 'CSV',
# MAGIC   -- Explicit schema
# MAGIC   schema => '
# MAGIC     EmployeeID STRING,
# MAGIC     FirstName STRING,
# MAGIC     Country STRING,
# MAGIC     Department STRING,
# MAGIC     Salary DOUBLE,
# MAGIC     HireDate DATE,
# MAGIC     Operation STRING,
# MAGIC     ProcessDate DATE
# MAGIC   ',
# MAGIC   -- CSV parsing options
# MAGIC   header => 'true',
# MAGIC   inferSchema => 'false'
# MAGIC );
# MAGIC
# MAGIC -- Display table
# MAGIC SELECT *
# MAGIC FROM sdp_lab_1_bronze.employees_bronze_lab08;

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.2 - Bronze to Silver
# MAGIC
# MAGIC 1. Run the cell below to create the table **labuser.sdp_lab_1_bronze.employees_silver_lab08** and explore the results. 
# MAGIC
# MAGIC     Notice that a few simple data transformations were applied to the bronze table and metadata columns were removed.
# MAGIC
# MAGIC     Think about what you will need to change when migrating this to a Spark Declarative Pipeline. Hints are added as comments in the code below.
# MAGIC
# MAGIC     **NOTE:** For simplicity, we are leaving the **test** row in place, and you will remove it using data quality expectations. Typically, we could have just filtered out the null value(s).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sdp_lab_2_silver.employees_silver_lab08 -- You will have to modify this to create a streaming table in the pipeline
# MAGIC AS
# MAGIC SELECT
# MAGIC   EmployeeID,
# MAGIC   FirstName,
# MAGIC   upper(Country) AS Country,
# MAGIC   Department,
# MAGIC   Salary,
# MAGIC   HireDate,
# MAGIC   date_format(HireDate, 'MMMM') AS HireMonthName,
# MAGIC   year(HireDate) AS HireYear, 
# MAGIC   Operation
# MAGIC FROM sdp_lab_1_bronze.employees_bronze_lab08;  -- You will have to modify FROM clause to incrementally read in data
# MAGIC
# MAGIC
# MAGIC -- Display table
# MAGIC SELECT *
# MAGIC FROM sdp_lab_2_silver.employees_silver_lab08;

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.3 - Silver to Gold
# MAGIC The code below creates two traditional views to aggregate the silver table.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell to create a view that calculates the **total number of employees and total salary by country**.
# MAGIC
# MAGIC     Think about what you will need to change when migrating this to a Spark Declarative Pipeline. A hint is added as a comment in the code below.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sdp_lab_3_gold.employees_by_country_gold_lab08 -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT 
# MAGIC   Country,
# MAGIC   count(*) AS TotalEmployees,
# MAGIC   sum(Salary) AS TotalSalary
# MAGIC FROM sdp_lab_2_silver.employees_silver_lab08 -- You will have to modify FROM clause to incrementally read in data
# MAGIC GROUP BY Country;
# MAGIC
# MAGIC
# MAGIC -- Display view
# MAGIC SELECT *
# MAGIC FROM sdp_lab_3_gold.employees_by_country_gold_lab08;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell to create a view that calculates the **total salary by department**.
# MAGIC
# MAGIC     Think about what you will need to change when migrating this to a Spark Declarative Pipeline. A hint is added as a comment in the code below.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sdp_lab_3_gold.salary_by_department_gold_lab08  -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT
# MAGIC   Department,
# MAGIC   sum(Salary) AS TotalSalary
# MAGIC FROM sdp_lab_2_silver.employees_silver_lab08
# MAGIC GROUP BY Department;
# MAGIC
# MAGIC
# MAGIC -- Display view
# MAGIC SELECT *
# MAGIC FROM sdp_lab_3_gold.salary_by_department_gold_lab08;

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.4 - Delete the Tables
# MAGIC
# MAGIC Run the cell below to delete all the tables you created above. You will recreate them as streaming tables and materialized views in the Spark Declarative Pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sdp_lab_1_bronze.employees_bronze_lab08;
# MAGIC DROP TABLE IF EXISTS sdp_lab_2_silver.employees_silver_lab08;
# MAGIC DROP VIEW IF EXISTS sdp_lab_3_gold.employees_by_country_gold_lab08;
# MAGIC DROP VIEW IF EXISTS sdp_lab_3_gold.salary_by_department_gold_lab08;

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to view and copy the path to your **lab_files** volume. You will need this path when building your pipeline to reference your data source files.
# MAGIC
# MAGIC **NOTE:** You can also navigate to the volume and copy the path using the UI.

# COMMAND ----------

print(f'/Volumes/{my_catalog}/sdp_lab_1_bronze/lab_files')

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. TO DO: Create the Apache Spark™ Declarative Pipeline (Steps)
# MAGIC
# MAGIC Now that you have explored the traditional ETL code used to create the tables and views, it's time to modify that syntax to declarative SQL for your new pipeline.
# MAGIC
# MAGIC You will need to complete the following:

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Create the Spark Declarative Pipeline
# MAGIC
# MAGIC 1. To create the pipeline and add existing assets to associate it with code files already available in your Workspace (including Git folders) complete the following:
# MAGIC
# MAGIC    a. For ease of use, open **Jobs & Pipelines** in a separate tab:
# MAGIC
# MAGIC     - On the main navigation bar, right-click on **Jobs & Pipelines** and select **Open in a New Tab**.
# MAGIC
# MAGIC    b. In **Jobs & Pipelines** select **Create** → **ETL Pipeline**.
# MAGIC
# MAGIC    c. Select **Settings (or the gear icon)** and complete the following:
# MAGIC
# MAGIC     | Section | Field | Value |
# MAGIC     |---------|-------|---------------|
# MAGIC     | **Pipeline settings** |  **Name** | `lab08 - firstname pipeline project` |
# MAGIC     | **Default location for data assets** | **Default catalog** | Your **labuser** catalog |
# MAGIC     | **Default location for data assets** | **Default schema** | Your **sdp_lab_1_bronze** schema (database) |
# MAGIC
# MAGIC     **NOTE:** In the pipeline, navigate to the transformations folder and locate the my_transformation.py file. Open the kebab menu (three dots) next to the file, select Rename, and change the file name from `my_transformation.py` to `my_transformation.sql`.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Create the Bronze Table
# MAGIC
# MAGIC 1. Migrate the ETL code (shown below for each step as a reference) into one or more files and folders to organize your pipeline (you can also put everything in a single file if you prefer).
# MAGIC
# MAGIC <br></br>
# MAGIC Modify the code shown below to create the **bronze** streaming table in your pipeline by completing the following:
# MAGIC
# MAGIC - Modify the `CREATE OR REPLACE TABLE` statement to create a streaming table.  
# MAGIC
# MAGIC - Add the keyword `STREAM` in the `FROM` clause to incrementally ingest data from the volume.
# MAGIC
# MAGIC - Update the path in the `read_files()` function to point to your **labuser.sdp_lab_1_bronze.lab_files** volume path (example: `/Volumes/labuser1234/sdp_lab_1_bronze/lab_files`). 
# MAGIC     - You can statically add the path in the `read_files` function, or use a configuration parameter.
# MAGIC
# MAGIC <br></br>
# MAGIC ```SQL
# MAGIC CREATE OR REPLACE TABLE sdp_lab_1_bronze.employees_bronze_lab08
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() AS ingestion_time,
# MAGIC   _metadata.file_name AS raw_file_name
# MAGIC FROM read_files(
# MAGIC   'YOUR_PATH_TO_YOUR_lab_files_VOLUME', --Add the path to your lab_files volume
# MAGIC   format => 'CSV',
# MAGIC   -- Explicit schema
# MAGIC   schema => '
# MAGIC     EmployeeID STRING,
# MAGIC     FirstName STRING,
# MAGIC     Country STRING,
# MAGIC     Department STRING,
# MAGIC     Salary DOUBLE,
# MAGIC     HireDate DATE,
# MAGIC     Operation STRING,
# MAGIC     ProcessDate DATE
# MAGIC   ',
# MAGIC   -- CSV parsing options
# MAGIC   header => 'true',
# MAGIC   inferSchema => 'false'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC 2. Try a **Dry Run** to confirm the syntax is correct.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ANSWER
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC CREATE OR REFRESH STREAMING TABLE sdp_lab_1_bronze.employees_bronze_lab08
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() AS ingestion_time,
# MAGIC   _metadata.file_name AS raw_file_name
# MAGIC FROM STREAM read_files(
# MAGIC   '/Volumes/ADD_YOUR_CATALOG_NAME/sdp_lab_1_bronze/lab_files', --Add the path to your lab_files volume
# MAGIC   format => 'CSV',
# MAGIC   -- Explicit schema
# MAGIC   schema => '
# MAGIC     EmployeeID STRING,
# MAGIC     FirstName STRING,
# MAGIC     Country STRING,
# MAGIC     Department STRING,
# MAGIC     Salary DOUBLE,
# MAGIC     HireDate DATE,
# MAGIC     Operation STRING,
# MAGIC     ProcessDate DATE
# MAGIC   ',
# MAGIC   -- CSV parsing options
# MAGIC   header => 'true',
# MAGIC   inferSchema => 'false'
# MAGIC );
# MAGIC
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Create the Silver Table
# MAGIC
# MAGIC 1. Modify the code shown below to create the **silver** streaming table by completing the following in your pipeline project:
# MAGIC
# MAGIC - Modify the `CREATE OR REPLACE TABLE` statement to create a streaming table.  
# MAGIC
# MAGIC - Add the keyword `STREAM` in the `FROM` clause to incrementally ingest data.
# MAGIC
# MAGIC - Add the following data quality expectations:      
# MAGIC     ```
# MAGIC     CONSTRAINT check_country EXPECT (Country IN ('US','GR')),
# MAGIC     CONSTRAINT check_salary EXPECT (Salary > 0),
# MAGIC     CONSTRAINT check_null_id EXPECT (EmployeeID IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC
# MAGIC     ```
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC CREATE OR REPLACE TABLE sdp_lab_2_silver.employees_silver_lab08 -- You will have to modify this to create a streaming table in the pipeline
# MAGIC AS
# MAGIC SELECT
# MAGIC   EmployeeID,
# MAGIC   FirstName,
# MAGIC   upper(Country) AS Country,
# MAGIC   Department,
# MAGIC   Salary,
# MAGIC   HireDate,
# MAGIC   date_format(HireDate, 'MMMM') AS HireMonthName,
# MAGIC   year(HireDate) AS HireYear, 
# MAGIC   Operation
# MAGIC FROM sdp_lab_1_bronze.employees_bronze_lab08;  -- You will have to modify FROM clause to incrementally read in data
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC 2. Try a **Dry Run** to confirm the syntax is correct.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ANSWER
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC CREATE OR REFRESH STREAMING TABLE sdp_lab_2_silver.employees_silver_lab08 -- You will have to modify this to create a streaming table in the pipeline
# MAGIC (
# MAGIC     CONSTRAINT check_country EXPECT (Country IN ('US','GR')),
# MAGIC     CONSTRAINT check_salary EXPECT (Salary > 0),
# MAGIC     CONSTRAINT check_null_id EXPECT (EmployeeID IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   EmployeeID,
# MAGIC   FirstName,
# MAGIC   upper(Country) AS Country,
# MAGIC   Department,
# MAGIC   Salary,
# MAGIC   HireDate,
# MAGIC   date_format(HireDate, 'MMMM') AS HireMonthName,
# MAGIC   year(HireDate) AS HireYear, 
# MAGIC   Operation
# MAGIC FROM STREAM sdp_lab_1_bronze.employees_bronze_lab08;  -- You will have to modify FROM clause to incrementally read in data
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4. Create the Gold Materialized Views
# MAGIC
# MAGIC 1. Replace the `CREATE OR REPLACE VIEW` statement in both views to **create materialized views** instead of traditional views in your Spark Declarative Pipeline.
# MAGIC
# MAGIC ```
# MAGIC -- Employee by Country
# MAGIC CREATE OR REPLACE VIEW sdp_lab_3_gold.employees_by_country_gold_lab08 -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT 
# MAGIC   Country,
# MAGIC   count(*) AS TotalEmployees,
# MAGIC   sum(Salary) AS TotalSalary
# MAGIC FROM sdp_lab_2_silver.employees_silver_lab08
# MAGIC GROUP BY Country;
# MAGIC
# MAGIC -- Salary by Department
# MAGIC CREATE OR REPLACE VIEW sdp_lab_3_gold.salary_by_department_gold_lab08  -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT
# MAGIC   Department,
# MAGIC   sum(Salary) AS TotalSalary
# MAGIC FROM sdp_lab_2_silver.employees_silver_lab08
# MAGIC GROUP BY Department;
# MAGIC ```
# MAGIC
# MAGIC 2. Try a **Dry Run** to confirm the syntax is correct.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ANSWER
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC -- Employee by Country
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW sdp_lab_3_gold.employees_by_country_gold_lab08 -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT 
# MAGIC   Country,
# MAGIC   count(*) AS TotalEmployees,
# MAGIC   sum(Salary) AS TotalSalary
# MAGIC FROM sdp_lab_2_silver.employees_silver_lab08
# MAGIC GROUP BY Country;
# MAGIC
# MAGIC -- Salary by Department
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW sdp_lab_3_gold.salary_by_department_gold_lab08  -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT
# MAGIC   Department,
# MAGIC   sum(Salary) AS TotalSalary
# MAGIC FROM sdp_lab_2_silver.employees_silver_lab08
# MAGIC GROUP BY Department;
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C5. Modify the Pipeline Settings
# MAGIC
# MAGIC 1. Pipeline configuration requirements:
# MAGIC
# MAGIC - Your Spark Declarative Pipeline should use **Serverless** compute.  
# MAGIC
# MAGIC - Your pipeline should use your **labuser** catalog by default.  
# MAGIC
# MAGIC - Your pipeline should use your **sdp_lab_1_bronze** schema by default.
# MAGIC
# MAGIC - Make sure your pipeline is including your files.
# MAGIC
# MAGIC - **(OPTIONAL)** If using a configuration variable to reference your volume path, make sure it is defined and applied in the `read_files()` function.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C6. Run the Pipeline
# MAGIC 1. When complete, run the pipeline. Troubleshoot any errors.
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC ##### Final Spark Declarative Pipeline Image  
# MAGIC Below is what your final pipeline should look like after the first run with a single CSV file.
# MAGIC
# MAGIC ![Final lab08 Pipeline](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lab-1/lab-1-run-2-one-file.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Explore the Streaming Tables and Materialized Views
# MAGIC
# MAGIC After you have created and run your Spark Declarative Pipeline, complete the following tasks to explore your new streaming tables and materialized views.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. In the Catalog Explorer on the left, expand your **labuser** catalog and expand the following schemas:
# MAGIC
# MAGIC    - **sdp_lab_1_bronze** schema
# MAGIC       - **employees_bronze_lab08** streaming table
# MAGIC
# MAGIC    - **sdp_lab_2_silver** schema
# MAGIC       - **employees_silver_lab08** streaming table
# MAGIC       - **country_lookup** UC table (not used in this lab)
# MAGIC
# MAGIC    - **sdp_lab_3_gold** schema
# MAGIC       - **employees_by_country_gold_lab08** materialized view
# MAGIC       - **salary_by_department_gold_lab08** materialized view

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell below to view the data in your **labuser.sdp_lab_1_bronze.employees_bronze_lab08** streaming table. 
# MAGIC
# MAGIC     Notice that the:
# MAGIC     - first row contains a `null` **EmployeeID**.
# MAGIC     - table contains a total of 6 rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_lab_1_bronze.employees_bronze_lab08;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the cell below to view the data in your **labuser.sdp_lab_2_silver.employees_silver_lab08** streaming table. 
# MAGIC
# MAGIC     Notice that the silver table:
# MAGIC     - removed the row where **EmployeeID** was `null` using a data quality expectation.
# MAGIC     - contains a total of 5 rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_lab_2_silver.employees_silver_lab08;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Run the cell below to view the data in your **labuser.sdp_lab_3_gold.employees_by_country_gold_lab08** materialized view. 
# MAGIC
# MAGIC     **Final Results**
# MAGIC     | Country | TotalCount | TotalSalary |
# MAGIC     |---------|------------|-------------|
# MAGIC     | GR      | 2          | 108000      |
# MAGIC     | US      | 3          | 201000      |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_lab_3_gold.employees_by_country_gold_lab08
# MAGIC ORDER BY TotalSalary;

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run the cell below to view the data in your **labuser.sdp_lab_3_gold.salary_by_department_gold_lab08** materialized view. 
# MAGIC
# MAGIC     **Final Results**
# MAGIC     | Department  | TotalSalary |
# MAGIC     |-------------|-------------|
# MAGIC     | Sales       | 141000      |
# MAGIC     | IT          | 168000      |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_lab_3_gold.salary_by_department_gold_lab08
# MAGIC ORDER BY TotalSalary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Challenge Scenario (Optional in Live Class)
# MAGIC ### Duration: ~10 minutes
# MAGIC
# MAGIC **NOTE:** *If you finish early in a live class, feel free to complete the challenge below. The challenge is optional and most likely will not be completed during the live class. Only continue if your Spark Declarative Pipeline was set up correctly in the previous section by comparing your pipeline to the solution image.*
# MAGIC
# MAGIC **SCENARIO:** In this challenge, you will land a new CSV file in your **lab_files** cloud storage volume and rerun the pipeline to observe that the Spark Declarative Pipeline only ingests the new data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Land Another CSV File in Cloud Storage and Preview

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to copy another file to your **labuser.sdp_lab_1_bronze.lab_files** volume.
# MAGIC

# COMMAND ----------

## Find data in workspace data folder
data_path = "/Volumes/dbacademy/default/data"

## Land another CSV file to your lab_files volume
copy_workspace_files_to_volume(
    src_workspace_folder=f'{data_path}/lab_files',
    target_volume_path=f'/Volumes/{my_catalog}/sdp_lab_1_bronze/lab_files',
    n=2
)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. In the left navigation area, navigate to your **labuser.sdp_lab_1_bronze.lab_files** volume and expand it. 
# MAGIC
# MAGIC     Confirm it contains two CSV files: 
# MAGIC     - **employees_1.csv** 
# MAGIC     - **employees_2.csv**
# MAGIC
# MAGIC **NOTE:** You may need to refresh your catalog if the file is not shown.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the cell below to preview only the new CSV file (`employees_2.csv`) and view the results. Notice that the new CSV file contains employee information:
# MAGIC
# MAGIC     - Contains 4 rows.  
# MAGIC     - The **Operation** column specifies an action for each employee (e.g., update the record, delete the record, or add a new employee).
# MAGIC
# MAGIC **Output**
# MAGIC | EmployeeID | FirstName | Country | Department | Salary | HireDate   | Operation | ProcessDate |
# MAGIC |------------|----------|---------|------------|--------|------------|-----------|-------------|
# MAGIC | 6          | Emily    | us      | Enablement | 80000  | 2025-06-09 | new       | 2025-06-22  |
# MAGIC | 7          | Yannis   | gR      | HR         | 70000  | 2025-06-20 | new       | 2025-06-22  |
# MAGIC | 3          | Liam     | US      | Sales      | 100000 | 2025-05-03 | update    | 2025-06-22  |
# MAGIC | 1          | null     | null    | null       | null   | -          | delete    | 2025-06-22  |
# MAGIC
# MAGIC **NOTE:** Don't worry about the **Operation** column yet. We'll cover how to capture these specific changes in your data (Change Data Capture) in a later demonstration.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   '/Volumes/' || my_catalog || '/sdp_lab_1_bronze/lab_files/employees_2.csv',
# MAGIC   format => 'CSV',
# MAGIC   schema => '
# MAGIC     EmployeeID STRING,
# MAGIC     FirstName STRING,
# MAGIC     Country STRING,
# MAGIC     Department STRING,
# MAGIC     Salary DOUBLE,
# MAGIC     HireDate DATE,
# MAGIC     Operation STRING,
# MAGIC     ProcessDate DATE
# MAGIC   ',
# MAGIC   -- CSV parsing options
# MAGIC   header => 'true',
# MAGIC   inferSchema => 'false'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Run the Pipeline with the New CSV File

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Now that you have explored the new CSV file in cloud storage, go back to your Spark Declarative Pipeline and select **Run pipeline**. 
# MAGIC
# MAGIC     Notice that the pipeline incrementally processes the new file from cloud storage.
# MAGIC
# MAGIC
# MAGIC ##### Final Spark Declarative Pipeline Image
# MAGIC Below is what your final pipeline should look like after the second run with two CSV files.
# MAGIC
# MAGIC ![Final Challenge lab08 DLT Pipeline](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lab-1/lab-1-run-2-challenge.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. View the Streaming Tables and Materialized Views

# COMMAND ----------

# MAGIC %md
# MAGIC 1. View your **bronze** streaming table. It should have a total of **10** rows.
# MAGIC
# MAGIC **Output**
# MAGIC | EmployeeID | FirstName | Country | Department | Salary | HireDate   | Operation | ProcessDate | ingestion_time               | raw_file_name   |
# MAGIC |------------|----------|---------|------------|--------|------------|-----------|-------------|------------------------------|-----------------|
# MAGIC | null       | test     | test    | test       | 9999   | 2025-01-01 | new       | 2025-06-05  | 2026-04-08T15:56:55.097+00:00 | employees_1.csv |
# MAGIC | 1          | Sophia   | US      | Sales      | 72000  | 2025-04-01 | new       | 2025-06-05  | 2026-04-08T15:56:55.097+00:00 | employees_1.csv |
# MAGIC | 1          | null     | null    | null       | null   | -          | delete    | 2025-06-22  | 2026-04-08T16:02:47.814+00:00 | employees_2.csv |
# MAGIC | 2          | Nikos    | Gr      | IT         | 55000  | 2025-04-10 | new       | 2025-06-05  | 2026-04-08T15:56:55.097+00:00 | employees_1.csv |
# MAGIC | 3          | Liam     | US      | Sales      | 100000 | 2025-05-03 | update    | 2025-06-22  | 2026-04-08T16:02:47.814+00:00 | employees_2.csv |
# MAGIC | 3          | Liam     | US      | Sales      | 69000  | 2025-05-03 | new       | 2025-06-05  | 2026-04-08T15:56:55.097+00:00 | employees_1.csv |
# MAGIC | 4          | Elena    | GR      | IT         | 53000  | 2025-06-04 | new       | 2025-06-05  | 2026-04-08T15:56:55.097+00:00 | employees_1.csv |
# MAGIC | 5          | James    | Us      | IT         | 60000  | 2025-06-05 | new       | 2025-06-05  | 2026-04-08T15:56:55.097+00:00 | employees_1.csv |
# MAGIC | 6          | Emily    | us      | Enablement | 80000  | 2025-06-09 | new       | 2025-06-22  | 2026-04-08T16:02:47.814+00:00 | employees_2.csv |
# MAGIC | 7          | Yannis   | gR      | HR         | 70000  | 2025-06-20 | new       | 2025-06-22  | 2026-04-08T16:02:47.814+00:00 | employees_2.csv |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_lab_1_bronze.employees_bronze_lab08
# MAGIC ORDER BY EmployeeID;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. View your **silver** streaming table. It should have a total of **9 rows**.
# MAGIC
# MAGIC **Output**
# MAGIC | EmployeeID | FirstName | Country | Department | Salary | HireDate   | HireMonthName | HireYear | Operation |
# MAGIC |------------|----------|---------|------------|--------|------------|----------------|----------|-----------|
# MAGIC | 1          | Sophia   | US      | Sales      | 72000  | 2025-04-01 | April          | 2025     | new       |
# MAGIC | 1          | null     | null    | null       | null   | -          | null           | null     | delete    |
# MAGIC | 2          | Nikos    | GR      | IT         | 55000  | 2025-04-10 | April          | 2025     | new       |
# MAGIC | 3          | Liam     | US      | Sales      | 100000 | 2025-05-03 | May            | 2025     | update    |
# MAGIC | 3          | Liam     | US      | Sales      | 69000  | 2025-05-03 | May            | 2025     | new       |
# MAGIC | 4          | Elena    | GR      | IT         | 53000  | 2025-06-04 | June           | 2025     | new       |
# MAGIC | 5          | James    | US      | IT         | 60000  | 2025-06-05 | June           | 2025     | new       |
# MAGIC | 6          | Emily    | US      | Enablement | 80000  | 2025-06-09 | June           | 2025     | new       |
# MAGIC | 7          | Yannis   | GR      | HR         | 70000  | 2025-06-20 | June           | 2025     | new       |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_lab_2_silver.employees_silver_lab08
# MAGIC ORDER BY EmployeeID;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Explore the history of your streaming tables using the Catalog Explorer. 
# MAGIC
# MAGIC     Notice that there are two **STREAMING UPDATES** to both the **bronze** and **silver** tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sdp_lab_1_bronze.employees_bronze_lab08;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sdp_lab_2_silver.employees_silver_lab08;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
# MAGIC