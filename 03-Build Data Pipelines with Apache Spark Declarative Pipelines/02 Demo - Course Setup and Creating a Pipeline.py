# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # 02 - REQUIRED - Course Setup and Creating a Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC In this demo, we'll set up the course environment, explore its components, build a traditional ETL pipeline using JSON files as the data source, and then learn how to create a sample Apache Spark™ Declarative Pipeline (SDP).
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lesson, you will be able to:
# MAGIC
# MAGIC 1. **Navigate the Workspace** to locate course catalogs, schemas, and source files.
# MAGIC 2. **Create an Apache Spark™ Declarative Pipeline** using the Workspace and the Pipeline UI.

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
# MAGIC **NOTE:**  This notebook was **developed and tested using Serverless V5**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC 1. Run the cell below to initialize your environment.
# MAGIC
# MAGIC     This setup step does the following:
# MAGIC
# MAGIC     - **Assumes you have permission to create a catalog** when running outside of a Databricks-provided Vocareum workspace.
# MAGIC     - Creates three schemas in your specified catalog:
# MAGIC         - **sdp_1_bronze**
# MAGIC         - **sdp_2_silver**
# MAGIC         - **sdp_3_gold**
# MAGIC     - Creates three volumes in your **YOUR_LABUSER_CATALOG.sdp_1_bronze.source** volume and adds a single JSON file in each volume.
# MAGIC     - Checks your specified Serverless compute version.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-REQUIRED

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Lab Environment
# MAGIC
# MAGIC Explore the raw data source files, catalogs, and schemas in the course lab environment.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### B1. View Your Catalog and Schemas
# MAGIC
# MAGIC 1. Complete these steps to explore your user catalog and schemas:
# MAGIC
# MAGIC    a. Select the **Catalog** icon ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/catalog_icon.png) in the left navigation bar.
# MAGIC
# MAGIC    b. You should see your unique catalog, named something like **labuser_USERNAME**. You will use this catalog throughout the course.
# MAGIC
# MAGIC    c. Expand your **labuser** catalog. It should contain the following schemas:
# MAGIC       - **sdp_1_bronze**
# MAGIC       - **sdp_2_silver**
# MAGIC       - **sdp_3_gold**
# MAGIC       - **default**

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. View the Streaming Source Files
# MAGIC
# MAGIC 1. Complete the following steps to view where your streaming raw source files are located:
# MAGIC
# MAGIC    a. Select the **Catalog** icon ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/catalog_icon.png) in the left navigation bar.
# MAGIC
# MAGIC    b. Expand your **labuser_USERNAME** catalog.
# MAGIC
# MAGIC    c. Expand the **sdp_1_bronze** schema
# MAGIC
# MAGIC    d. Expand your **source** volume. You should see three folders:
# MAGIC       - **customers**
# MAGIC       - **orders**
# MAGIC       - **status**
# MAGIC
# MAGIC    e. Expand each folder and notice that each location contains a single JSON file to start with.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Confirm the Source Volume Path
# MAGIC
# MAGIC To easily reference the volume path (`/Volumes/LABUSER_YOURUSERNAME/sdp_1_bronze/source`) throughout the course, use the following variables:
# MAGIC
# MAGIC - **Python:** `source_volume_path`
# MAGIC - **SQL:** `source_volume_path`
# MAGIC
# MAGIC Run the cells below and confirm the path points to your volume.
# MAGIC
# MAGIC **Example:** `/Volumes/LABUSER_YOURUSERNAME/sdp_1_bronze/source`

# COMMAND ----------

## With Python
print(source_volume_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- With SQL
# MAGIC values(source_volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Build a Traditional ETL Pipeline
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Preview the Raw Orders Data
# MAGIC
# MAGIC 1. Query the raw JSON file(s) in your `/Volumes/labuser/sdp_1_bronze/source/orders` volume to preview the data.
# MAGIC
# MAGIC    The JSON file is displayed in tabular form using the `read_files` function. 
# MAGIC
# MAGIC    Notice the following:
# MAGIC
# MAGIC    a. The **orders** JSON file contains order data for a company.
# MAGIC
# MAGIC    b. The one JSON file in your **source/orders** volume (**00.json**) contains 174 rows. Remember that number for later.

# COMMAND ----------

spark.sql(f'''
          SELECT * 
          FROM json.`{source_volume_path}/orders`
          ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Write a Traditional ETL Pipeline
# MAGIC
# MAGIC 1. Traditionally, you would build an ETL pipeline by reading all files within a cloud storage location each time the pipeline runs. As data scales, this method becomes inefficient, more expensive, and time-consuming.
# MAGIC
# MAGIC    **NOTES:**
# MAGIC    - The tables and views will be written to your **labuser.sdp_1_bronze** schema (database).
# MAGIC    - Knowledge of the Databricks `read_files` function is a prerequisite for this course.
# MAGIC       - `read_files` table-valued function: [AWS](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) |[Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/read_files) |
# MAGIC       [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/functions/read_files)

# COMMAND ----------

# MAGIC %sql
# MAGIC --------------------------------------------
# MAGIC -- JSON -> Bronze
# MAGIC --------------------------------------------
# MAGIC -- Read ALL files from your working directory each time the query is executed
# MAGIC CREATE OR REPLACE TABLE sdp_1_bronze.orders_bronze
# MAGIC AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   current_timestamp() AS processing_time,
# MAGIC   _metadata.file_name AS source_file
# MAGIC FROM read_files(
# MAGIC     source_volume_path || "/orders",
# MAGIC     format =>"json");
# MAGIC
# MAGIC --------------------------------------------
# MAGIC -- Bronze -> Silver
# MAGIC --------------------------------------------
# MAGIC -- Read the entire bronze table each time the query is executed
# MAGIC CREATE OR REPLACE TABLE sdp_1_bronze.orders_silver
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   timestamp(order_timestamp) AS order_timestamp,
# MAGIC   customer_id,
# MAGIC   notifications
# MAGIC FROM sdp_1_bronze.orders_bronze;
# MAGIC
# MAGIC --------------------------------------------
# MAGIC -- Silver -> Gold View
# MAGIC --------------------------------------------
# MAGIC -- Aggregate the silver each time the query is executed.
# MAGIC CREATE OR REPLACE VIEW sdp_1_bronze.orders_by_date_vw
# MAGIC AS
# MAGIC SELECT
# MAGIC   date(order_timestamp) AS order_date,
# MAGIC   count(*) AS total_daily_orders
# MAGIC FROM sdp_1_bronze.orders_silver
# MAGIC GROUP BY date(order_timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Preview the Pipeline Results
# MAGIC
# MAGIC 1. Run the cells below to preview the:
# MAGIC     - **orders_bronze** table
# MAGIC     - **orders_silver** table
# MAGIC     - **orders_by_date_vw** view. 
# MAGIC
# MAGIC     Explore the results.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_1_bronze.orders_bronze
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_1_bronze.orders_silver
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM sdp_1_bronze.orders_by_date_vw
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4. Traditional Batch Pipeline Limitations

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <style>
# MAGIC .info-wrapper {
# MAGIC   background: #F9F7F4;
# MAGIC   border-radius: 10px;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC   padding: 32px;
# MAGIC   position: relative;
# MAGIC }
# MAGIC .info-wrapper::before {
# MAGIC   content: "";
# MAGIC   position: absolute;
# MAGIC   top: 0;
# MAGIC   left: 0;
# MAGIC   width: 100%;
# MAGIC   height: 6px;
# MAGIC   background: #FF5F46;
# MAGIC   border-radius: 10px 10px 0 0;
# MAGIC }
# MAGIC .info-header {
# MAGIC   font-size: 20pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #0b2026;
# MAGIC   margin-bottom: 8px;
# MAGIC }
# MAGIC .info-subtitle {
# MAGIC   font-size: 14pt;
# MAGIC   color: #5A6F77;
# MAGIC   margin-bottom: 28px;
# MAGIC }
# MAGIC .info-row {
# MAGIC   display: flex;
# MAGIC   align-items: flex-start;
# MAGIC   gap: 20px;
# MAGIC   padding: 20px 0;
# MAGIC   border-bottom: 1px solid #EEEDE9;
# MAGIC }
# MAGIC .info-row:last-child {
# MAGIC   border-bottom: none;
# MAGIC }
# MAGIC .info-accent {
# MAGIC   font-size: 24pt;
# MAGIC   font-weight: 800;
# MAGIC   line-height: 1;
# MAGIC   flex-shrink: 0;
# MAGIC   width: 44px;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .info-content {
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .info-title {
# MAGIC   font-size: 16pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #0b2026;
# MAGIC   margin-bottom: 4px;
# MAGIC }
# MAGIC .info-desc {
# MAGIC   font-size: 14pt;
# MAGIC   color: #5A6F77;
# MAGIC   line-height: 1.5;
# MAGIC }
# MAGIC .info-tag {
# MAGIC   font-size: 14pt;
# MAGIC   color: #618794;
# MAGIC   font-weight: 700;
# MAGIC   flex-shrink: 0;
# MAGIC   white-space: nowrap;
# MAGIC   padding: 6px 10px;
# MAGIC   background: #EEF4F7;
# MAGIC   border-radius: 999px;
# MAGIC   margin-top: 2px;
# MAGIC }
# MAGIC </style>
# MAGIC <div class="info-wrapper">
# MAGIC   <div class="info-header">Traditional Batch Pipeline Limitations</div>
# MAGIC   <div class="info-subtitle">Why this current ETL approach struggles as data grows</div>
# MAGIC   <div class="info-row">
# MAGIC     <div class="info-accent" style="color: #FF5F46;">&#9679;</div>
# MAGIC     <div class="info-content">
# MAGIC       <div class="info-title">Bronze: Full Re-Read Every Run</div>
# MAGIC       <div class="info-desc">Every execution reads <strong>all files</strong> from cloud storage, not just new ones. Cost and runtime grow with every file added.</div>
# MAGIC     </div>
# MAGIC     <div class="info-tag">Efficiency</div>
# MAGIC   </div>
# MAGIC   <div class="info-row">
# MAGIC     <div class="info-accent" style="color: #FF5F46;">&#9679;</div>
# MAGIC     <div class="info-content">
# MAGIC       <div class="info-title">Silver: Always Reprocesses All Rows</div>
# MAGIC       <div class="info-desc">The silver layer reads the entire bronze table on every run. No incremental processing. A full scan every time.</div>
# MAGIC     </div>
# MAGIC     <div class="info-tag">Efficiency</div>
# MAGIC   </div>
# MAGIC   <div class="info-row">
# MAGIC     <div class="info-accent" style="color: #FF5F46;">&#9679;</div>
# MAGIC     <div class="info-content">
# MAGIC       <div class="info-title">Views Re-Execute on Every Call</div>
# MAGIC       <div class="info-desc"><strong><code>orders_by_date_vw</code></strong> runs the underlying query each time it is referenced. No caching, no optimization as data scales.</div>
# MAGIC     </div>
# MAGIC     <div class="info-tag">Performance</div>
# MAGIC   </div>
# MAGIC   <div class="info-row">
# MAGIC     <div class="info-accent" style="color: #FF5F46;">&#9679;</div>
# MAGIC     <div class="info-content">
# MAGIC       <div class="info-title">Data Quality Requires Extra Code</div>
# MAGIC       <div class="info-desc">Checking for invalid or missing values means writing and maintaining additional validation logic outside the pipeline.</div>
# MAGIC     </div>
# MAGIC     <div class="info-tag">Quality</div>
# MAGIC   </div>
# MAGIC   <div class="info-row">
# MAGIC     <div class="info-accent" style="color: #FF5F46;">&#9679;</div>
# MAGIC     <div class="info-content">
# MAGIC       <div class="info-title">Run Monitoring Is a Challenge</div>
# MAGIC       <div class="info-desc">Tracking success, failure, and row counts across runs requires custom logging. There is no built-in visibility.</div>
# MAGIC     </div>
# MAGIC     <div class="info-tag">Observability</div>
# MAGIC   </div>
# MAGIC   <div class="info-row">
# MAGIC     <div class="info-accent" style="color: #FF5F46;">&#9679;</div>
# MAGIC     <div class="info-content">
# MAGIC       <div class="info-title">No UI to Explore or Fix Issues</div>
# MAGIC       <div class="info-desc">When something goes wrong, there is no simple interface to inspect, debug, or remediate problems from run to run.</div>
# MAGIC     </div>
# MAGIC     <div class="info-tag">Observability</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to expand</summary>
# MAGIC
# MAGIC <h3>Bronze: Full Re-Read Every Run</h3>
# MAGIC <ul>
# MAGIC <li>The batch code uses <code>spark.read</code>, a static, full read. It does not use <code>readStream</code> (streaming/incremental).</li>
# MAGIC <li>Every execution scans every file in the volume, even files already processed in prior runs.</li>
# MAGIC <li>As months of data accumulate, the same old files get re-read on every trigger.</li>
# MAGIC <li>Cost scales with total file count, not with the volume of new data.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>Silver: Always Reprocesses All Rows</h3>
# MAGIC <ul>
# MAGIC <li>The silver transformation reads all rows from bronze, not just rows added since the last run.</li>
# MAGIC <li>Even a simple filter or join runs against the full dataset every time.</li>
# MAGIC <li>This compounds with bronze ingestion — you are already paying to re-read files, then re-transforming all of the results.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>Views Re-Execute on Every Call</h3>
# MAGIC <ul>
# MAGIC <li>Traditional views are just saved SQL. They do not actually store results.</li>
# MAGIC <li><code>orders_by_date_vw</code> queries the silver table fresh each time it is referenced.</li>
# MAGIC <li>In a dashboard or downstream query context, this means redundant full scans with no caching benefit.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>Data Quality Requires Extra Code</h3>
# MAGIC <ul>
# MAGIC <li>Nothing in the traditional pipeline enforces constraints on incoming data.</li>
# MAGIC <li>You would need to write separate validation logic: <code>WHERE</code> clauses, <code>CASE</code> statements, or custom assertions.</li>
# MAGIC <li>This logic lives outside the pipeline and is easy to forget, skip, or break during refactoring.</li>
# MAGIC <li>There is no standard pattern for what to do when bad data arrives — drop it, halt the run, or log it somewhere?</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>Run Monitoring Is a Challenge</h3>
# MAGIC <ul>
# MAGIC <li>Batch jobs expose success/failure at the job level, but not at the data level.</li>
# MAGIC <li>How many rows were written? Were there duplicates? Did any files fail silently?</li>
# MAGIC <li>You often will not know there is a problem until a downstream user reports something wrong.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>No UI to Explore or Fix Issues</h3>
# MAGIC <ul>
# MAGIC <li>When a run fails or produces bad data, debugging means going back to code and logs.</li>
# MAGIC <li>There is no lineage view, no data preview per stage, and no way to see what changed between runs.</li>
# MAGIC <li>Fixing one issue often requires re-running the entire pipeline from scratch.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <p><strong>Transition to SDP:</strong> Each of these problems has a direct solution in Spark Declarative Pipelines — incremental processing, built-in data quality constraints, a pipeline UI with lineage, and event-driven monitoring.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C5. Traditional Structured Streaming Pipeline Limitations

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <style>
# MAGIC .ss-wrapper {
# MAGIC   background: #F9F7F4;
# MAGIC   border-radius: 10px;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC   padding: 32px;
# MAGIC   position: relative;
# MAGIC   margin-top: 24px;
# MAGIC }
# MAGIC .ss-wrapper::before {
# MAGIC   content: "";
# MAGIC   position: absolute;
# MAGIC   top: 0;
# MAGIC   left: 0;
# MAGIC   width: 100%;
# MAGIC   height: 6px;
# MAGIC   background: #FF3621;
# MAGIC   border-radius: 10px 10px 0 0;
# MAGIC }
# MAGIC .ss-header {
# MAGIC   font-size: 20pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #0b2026;
# MAGIC   margin-bottom: 8px;
# MAGIC }
# MAGIC .ss-subtitle {
# MAGIC   font-size: 14pt;
# MAGIC   color: #5A6F77;
# MAGIC   margin-bottom: 24px;
# MAGIC }
# MAGIC .ss-body {
# MAGIC   font-size: 14pt;
# MAGIC   color: #0b2026;
# MAGIC   line-height: 1.6;
# MAGIC   margin-bottom: 20px;
# MAGIC }
# MAGIC .ss-row {
# MAGIC   display: flex;
# MAGIC   align-items: flex-start;
# MAGIC   gap: 20px;
# MAGIC   padding: 20px 0;
# MAGIC   border-bottom: 1px solid #EEEDE9;
# MAGIC }
# MAGIC .ss-row:last-child {
# MAGIC   border-bottom: none;
# MAGIC }
# MAGIC .ss-accent {
# MAGIC   font-size: 24pt;
# MAGIC   font-weight: 800;
# MAGIC   line-height: 1;
# MAGIC   flex-shrink: 0;
# MAGIC   width: 44px;
# MAGIC   text-align: center;
# MAGIC   color: #FFAB00;
# MAGIC }
# MAGIC .ss-content {
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .ss-title {
# MAGIC   font-size: 16pt;
# MAGIC   font-weight: 700;
# MAGIC   color: #0b2026;
# MAGIC   margin-bottom: 4px;
# MAGIC }
# MAGIC .ss-desc {
# MAGIC   font-size: 14pt;
# MAGIC   color: #5A6F77;
# MAGIC   line-height: 1.5;
# MAGIC }
# MAGIC .ss-tag {
# MAGIC   font-size: 14pt;
# MAGIC   color: #618794;
# MAGIC   font-weight: 700;
# MAGIC   flex-shrink: 0;
# MAGIC   white-space: nowrap;
# MAGIC   padding: 6px 10px;
# MAGIC   background: #EEF4F7;
# MAGIC   border-radius: 999px;
# MAGIC   margin-top: 2px;
# MAGIC }
# MAGIC .ss-takeaway {
# MAGIC   background: #EEEDE9;
# MAGIC   border-radius: 8px;
# MAGIC   padding: 16px 20px;
# MAGIC   margin-top: 24px;
# MAGIC }
# MAGIC .ss-takeaway-text {
# MAGIC   font-size: 14pt;
# MAGIC   color: #0b2026;
# MAGIC   line-height: 1.6;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ss-wrapper">
# MAGIC   <div class="ss-header">"But Can't We Just Use Structured Streaming?"</div>
# MAGIC   <div class="ss-subtitle">Incremental processing is possible, but it comes at a cost of complexity</div>
# MAGIC
# MAGIC   <div class="ss-body">
# MAGIC     You <em>could</em> replace <code style="background: #EEEDE9; padding: 2px 6px; border-radius: 4px;">the pipeline</code> with <code style="background: #EEEDE9; padding: 2px 6px; border-radius: 4px;">spark.readStream</code> to get incremental ingestion. But that means rewriting your pipeline with significantly more code and operational overhead.
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="ss-row">
# MAGIC     <div class="ss-accent">&#9679;</div>
# MAGIC     <div class="ss-content">
# MAGIC       <div class="ss-title">Checkpoint Management</div>
# MAGIC       <div class="ss-desc">Every stream requires its own checkpoint location. You must configure and maintain these paths for each layer of the pipeline.</div>
# MAGIC     </div>
# MAGIC     <div class="ss-tag">Complexity</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="ss-row">
# MAGIC     <div class="ss-accent">&#9679;</div>
# MAGIC     <div class="ss-content">
# MAGIC       <div class="ss-title">foreachBatch + MERGE for Upserts</div>
# MAGIC       <div class="ss-desc">Simple overwrites become multi-step logic: define a function, create a temp view, write a MERGE statement, and wire it into <code style="background: #EEEDE9; padding: 2px 6px; border-radius: 4px;">foreachBatch</code>.</div>
# MAGIC     </div>
# MAGIC     <div class="ss-tag">Complexity</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="ss-row">
# MAGIC     <div class="ss-accent">&#9679;</div>
# MAGIC     <div class="ss-content">
# MAGIC       <div class="ss-title">Trigger and Schema Configuration</div>
# MAGIC       <div class="ss-desc">You must choose trigger modes, define schema locations for Auto Loader, and manage stream lifecycle (start, stop, await termination).</div>
# MAGIC     </div>
# MAGIC     <div class="ss-tag">Complexity</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="ss-row">
# MAGIC     <div class="ss-accent">&#9679;</div>
# MAGIC     <div class="ss-content">
# MAGIC       <div class="ss-title">Still No Built-In Quality or Observability</div>
# MAGIC       <div class="ss-desc">Structured Streaming solves incremental processing, but you still have no data quality constraints, no pipeline UI, and no lineage view.</div>
# MAGIC     </div>
# MAGIC     <div class="ss-tag">Gap</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="ss-takeaway">
# MAGIC     <div class="ss-takeaway-text">
# MAGIC       <strong>The takeaway:</strong> Structured Streaming solves the "re-read everything" problem, but it doesn't solve data quality, observability, or operational simplicity. You get incremental processing at the cost of writing and maintaining significantly more plumbing code. Spark Declarative Pipelines give you incremental processing <em>and</em> everything else, declaratively.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to expand</summary>
# MAGIC
# MAGIC <h3>What Structured Streaming Actually Requires</h3>
# MAGIC
# MAGIC To convert the batch pipeline above into an incremental streaming pipeline, here's what the bronze and silver layers would look like:
# MAGIC
# MAGIC ```python
# MAGIC # Checkpoint locations for every stream
# MAGIC checkpoint_base = "/Volumes/.../checkpoints"
# MAGIC
# MAGIC # Bronze: readStream + writeStream + trigger + checkpoint
# MAGIC (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze_schema")
# MAGIC     .load(source_path)
# MAGIC     .writeStream
# MAGIC     .option("checkpointLocation", f"{checkpoint_base}/bronze")
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable("bronze_orders")
# MAGIC )
# MAGIC
# MAGIC # Silver: readStream from bronze + foreachBatch for merge logic
# MAGIC def upsert_to_silver(batch_df, batch_id):
# MAGIC     batch_df.createOrReplaceTempView("updates")
# MAGIC     batch_df.sparkSession.sql("""
# MAGIC         MERGE INTO silver_orders AS target
# MAGIC         USING updates AS source
# MAGIC         ON target.order_id = source.order_id
# MAGIC         WHEN MATCHED THEN UPDATE SET *
# MAGIC         WHEN NOT MATCHED THEN INSERT *
# MAGIC     """)
# MAGIC
# MAGIC (spark.readStream
# MAGIC     .table("bronze_orders")
# MAGIC     .writeStream
# MAGIC     .foreachBatch(upsert_to_silver)
# MAGIC     .option("checkpointLocation", f"{checkpoint_base}/silver")
# MAGIC     .trigger(availableNow=True)
# MAGIC     .start()
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC <h3>Checkpoint Management</h3>
# MAGIC <ul>
# MAGIC <li>Every streaming query needs a unique, persistent checkpoint location.</li>
# MAGIC <li>Checkpoints track which data has been processed so far, so the stream can resume where it left off.</li>
# MAGIC <li>If the checkpoint gets deleted or corrupted, the stream either reprocesses everything or fails.</li>
# MAGIC <li>In a multi-layer pipeline (bronze, silver, gold), you manage a separate checkpoint path for each layer.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>foreachBatch + MERGE for Upserts</h3>
# MAGIC <ul>
# MAGIC <li>Structured Streaming's default output modes (append, complete, update) don't support MERGE/upsert logic directly.</li>
# MAGIC <li>To do incremental upserts, you need <code>foreachBatch</code>: a callback function that receives each micro-batch as a DataFrame.</li>
# MAGIC <li>Inside that function, you register a temp view and run a SQL MERGE statement manually.</li>
# MAGIC <li>This is boilerplate that every streaming silver/gold layer needs. In Spark Declarative Pipelines, you just write the query and the framework handles incremental updates.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>Trigger and Schema Configuration</h3>
# MAGIC <ul>
# MAGIC <li><code>trigger(availableNow=True)</code> processes all available data then stops. Other options include <code>processingTime</code> for interval-based triggers and <code>continuous</code> for low-latency.</li>
# MAGIC <li>Auto Loader (<code>cloudFiles</code>) requires a <code>schemaLocation</code> so it can track and evolve the inferred schema over time.</li>
# MAGIC <li>You need to call <code>.start()</code> or <code>.toTable()</code> correctly, and optionally <code>.awaitTermination()</code> to block until the stream finishes.</li>
# MAGIC <li>Getting any of these wrong results in silent failures or streams that never terminate.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>What Streaming Still Doesn't Solve</h3>
# MAGIC <ul>
# MAGIC <li><strong>Data quality:</strong> No built-in way to define expectations or constraints. You still write custom validation logic.</li>
# MAGIC <li><strong>Observability:</strong> No pipeline UI, no lineage graph, no per-table row counts or data quality metrics out of the box.</li>
# MAGIC <li><strong>Error handling:</strong> If bad data arrives, you need custom logic to quarantine or skip it. There is no "expect" or "drop" pattern.</li>
# MAGIC <li><strong>Operational overhead:</strong> You manage stream state, checkpoint cleanup, schema evolution, and failure recovery yourself.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <p><strong>Transition to SDP:</strong> Spark Declarative Pipelines handles all of this for you. You declare your tables, write your transformation logic, and the framework manages incremental processing, checkpoints, data quality expectations, and the pipeline UI automatically. No <code>foreachBatch</code>, no checkpoint paths, no trigger configuration.</p>
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Introduction to Apache Spark™ Declarative Pipelines
# MAGIC <br></br>
# MAGIC <style>
# MAGIC .genie-wrapper {
# MAGIC     display: flex;
# MAGIC     justify-content: center;
# MAGIC     align-items: center;
# MAGIC }
# MAGIC .box.genie-box {
# MAGIC     width: min(1100px, 100%);
# MAGIC     min-height: 300px;
# MAGIC }
# MAGIC .box {
# MAGIC     background: #F8F9FC;
# MAGIC     border-radius: 8px;
# MAGIC     box-shadow: 0 2px 8px rgba(28,48,55,0.06);
# MAGIC     display: flex;
# MAGIC     flex-direction: column;
# MAGIC     align-items: center;
# MAGIC     justify-content: center;
# MAGIC     gap: 18px;
# MAGIC     padding: 24px;
# MAGIC     text-align: center;
# MAGIC     position: relative;
# MAGIC     box-sizing: border-box;
# MAGIC }
# MAGIC .box::before {
# MAGIC     content: "";
# MAGIC     position: absolute;
# MAGIC     top: 0;
# MAGIC     left: 0;
# MAGIC     width: 100%;
# MAGIC     height: 8px;
# MAGIC     background: #00A972;
# MAGIC }
# MAGIC .genie-box::before {
# MAGIC     background: #FF5F46;
# MAGIC }
# MAGIC .genie-box .icon-img {
# MAGIC     max-width: 120px;
# MAGIC }
# MAGIC .genie-box .hero-img {
# MAGIC     width: 100%;
# MAGIC     max-width: 100%;
# MAGIC     height: auto;
# MAGIC }
# MAGIC .box-title {
# MAGIC     font-size: 26pt;
# MAGIC     font-weight: 700;
# MAGIC     color: #0b2026;
# MAGIC }
# MAGIC .three-sides {
# MAGIC     display: flex;
# MAGIC     justify-content: center;
# MAGIC     align-items: stretch;
# MAGIC     gap: 30px;
# MAGIC     flex-wrap: wrap;
# MAGIC     margin: 40px 0;
# MAGIC }
# MAGIC .small-box {
# MAGIC     flex: 0 0 340px;
# MAGIC     min-height: 260px;
# MAGIC     background: #F9F7F4;
# MAGIC     border-radius: 8px;
# MAGIC     box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC     display: flex;
# MAGIC     flex-direction: column;
# MAGIC     justify-content: flex-start;
# MAGIC     gap: 8px;
# MAGIC     padding: 24px 24px 20px 24px;
# MAGIC     text-align: left;
# MAGIC     position: relative;
# MAGIC     box-sizing: border-box;
# MAGIC }
# MAGIC .small-box::before {
# MAGIC     content: "";
# MAGIC     position: absolute;
# MAGIC     top: 0;
# MAGIC     left: 0;
# MAGIC     width: 100%;
# MAGIC     height: 8px;
# MAGIC     background: #4299E0;
# MAGIC }
# MAGIC .box-text {
# MAGIC     font-size: 18pt;
# MAGIC     font-weight: 700;
# MAGIC     color: #0b2026;
# MAGIC     line-height: 1.3;
# MAGIC     text-align: center;
# MAGIC     margin-top: 6px;
# MAGIC }
# MAGIC .box-subtext {
# MAGIC     font-size: 13.5pt;
# MAGIC     color: #5A6F77;
# MAGIC     line-height: 1.45;
# MAGIC }
# MAGIC .box-subtext ul {
# MAGIC     margin: 0;
# MAGIC     padding-left: 22px;
# MAGIC }
# MAGIC .box-subtext li {
# MAGIC     margin-bottom: 8px;
# MAGIC }
# MAGIC .bottom-note {
# MAGIC     max-width: 1100px;
# MAGIC     margin: 0 auto 20px auto;
# MAGIC     padding: 18px 26px;
# MAGIC     background: #F8F9FC;
# MAGIC     border: 3px solid #FF5F46;
# MAGIC     border-radius: 8px;
# MAGIC     text-align: center;
# MAGIC }
# MAGIC .bottom-note-text {
# MAGIC     font-size: 1.15em;
# MAGIC     color: #0b2026;
# MAGIC     line-height: 1.5;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="genie-wrapper">
# MAGIC   <div class="box genie-box">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/sdp-icon.png" class="icon-img" alt="SDP Icon">
# MAGIC     <div class="box-title">Apache Spark™ Declarative Pipelines</div>
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/lakeflow-declarative-pipelines-hero.png" class="hero-img" alt="Lakeflow Hero">
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="three-sides">
# MAGIC   <div class="small-box">
# MAGIC     <div class="box-text">Efficient Ingestion</div>
# MAGIC     <div class="box-subtext">
# MAGIC       <ul>
# MAGIC         <li><strong>Load data from any Apache Spark-supported source</strong> on Databricks</li>
# MAGIC         <li>Support for <strong>batch, streaming, and CDC</strong> ingestion patterns</li>
# MAGIC         <li>Built for <strong>data engineers, Python developers, data scientists, and SQL analysts</strong></li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="small-box">
# MAGIC     <div class="box-text">Intelligent Transformation</div>
# MAGIC     <div class="box-subtext">
# MAGIC       <ul>
# MAGIC         <li>From just a <strong>few lines of code</strong>, pipelines are planned and executed efficiently</li>
# MAGIC         <li>Automatically optimizes for <strong>cost or performance</strong></li>
# MAGIC         <li>Reduces engineering effort by <strong>minimizing pipeline complexity</strong></li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="small-box">
# MAGIC     <div class="box-text">Automated Operations</div>
# MAGIC     <div class="box-subtext">
# MAGIC       <ul>
# MAGIC         <li>Codifies <strong>pipeline best practices</strong> out of the box</li>
# MAGIC         <li>Automates <strong>dependency management, scaling, recovery, and data quality rules</strong></li>
# MAGIC         <li>Lets engineers focus on <strong>delivering high-quality data</strong>, not managing infrastructure</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="bottom-note">
# MAGIC   <div class="bottom-note-text">
# MAGIC     Convert your traditional batch pipeline to <strong>Spark Declarative Pipelines (SDP)</strong> and get incremental processing, data quality enforcement, infrastructure management, and full pipeline visibility.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC [Apache Spark™ DECLARATIVE PIPELINES](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines)
# MAGIC
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to expand</summary>
# MAGIC
# MAGIC <h3>Efficient Ingestion</h3>
# MAGIC <ul>
# MAGIC <li>SDP supports <strong>ingestion from any Apache Spark-supported source</strong> on Databricks.</li>
# MAGIC <li>Works across <strong>batch, streaming, and CDC pipelines</strong> with a unified approach.</li>
# MAGIC <li>Designed for multiple personas, including <strong>data engineers, Python developers, data scientists, and SQL analysts</strong>.</li>
# MAGIC <li>Eliminates the need to build separate ingestion frameworks for different data patterns.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>Intelligent Transformation</h3>
# MAGIC <ul>
# MAGIC <li>With just a <strong>few lines of code</strong>, SDP determines how to build and execute the pipeline.</li>
# MAGIC <li>Automatically selects the most efficient execution strategy for <strong>batch or streaming workloads</strong>.</li>
# MAGIC <li>Optimizes for <strong>cost or performance</strong> without manual tuning.</li>
# MAGIC <li>Reduces engineering effort by <strong>abstracting pipeline complexity</strong>.</li>
# MAGIC </ul>
# MAGIC
# MAGIC <h3>Automated Operations</h3>
# MAGIC <ul>
# MAGIC <li>SDP <strong>codifies best practices by default</strong>, reducing the need for custom engineering.</li>
# MAGIC <li>Automates <strong>dependency management, scaling, recovery, and data quality enforcement</strong>.</li>
# MAGIC <li>No infrastructure management required — pipelines are fully managed.</li>
# MAGIC <li>Enables teams to focus on <strong>delivering high-quality data</strong>, not operating pipelines.</li>
# MAGIC </ul>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Enable the Lakeflow Pipelines Editor
# MAGIC
# MAGIC In this section, you will create a Spark Declarative Pipeline using the new Lakeflow Pipelines Editor.
# MAGIC
# MAGIC **NOTE: Do not run or modify the pipeline yet.**
# MAGIC
# MAGIC 1. Complete the following steps to enable the **Lakeflow Pipelines Editor**:
# MAGIC       - **NOTE:** This may already be enabled in your workspace.
# MAGIC
# MAGIC    a. In the top-right corner, select your user icon ![User Lab Icon](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/user_lab_circle_icon.png).
# MAGIC
# MAGIC    b. Right-click on **Settings** and select **Open in New Tab**.
# MAGIC
# MAGIC    c. Select **Developer**.
# MAGIC
# MAGIC    d. Scroll to the bottom, enable **Lakeflow Pipelines Editor** if it is not already enabled, and select **Enable tabs for notebooks and files**.
# MAGIC
# MAGIC    ![Lakeflow Pipeline Editor](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/creating-a-pipeline/lakeflow-pipeline-editor.png)
# MAGIC
# MAGIC    e. Refresh your browser to apply the changes.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D2. Create a Pipeline Using the File Explorer
# MAGIC
# MAGIC 1. Complete the following steps to create a Spark Declarative Pipeline from the left navigation pane:
# MAGIC
# MAGIC    a. In the left navigation bar, select the **Folder** ![Folder Icon](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/folder_icon.png) icon to open the Workspace navigation.
# MAGIC
# MAGIC    b. Navigate to the **Build Data Pipelines with Apache Spark™ Declarative Pipelines** folder.
# MAGIC
# MAGIC    c. **NOTE:** To follow along with these instructions more easily, open this notebook in a second browser tab. 
# MAGIC       - Right-click the notebook **02 Demo - Course Setup and Creating a Pipeline** and select **Open in new browser tab**.
# MAGIC
# MAGIC    d. In the other tab, select the three-ellipsis icon ![Ellipsis Icon](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/ellipsis_icon.png) in the folder navigation bar.
# MAGIC
# MAGIC    e. Select **Create** -> **ETL Pipeline**. This will bring you to the **Lakeflow Pipeline Editor**.
# MAGIC
# MAGIC    <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC    <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC    <div>
# MAGIC    <strong style="color: #0d47a1; font-size: 1.1em;">Note</strong>
# MAGIC    <p style="margin: 8px 0 0 0; color: #333;">If you have not enabled the <strong>Lakeflow Pipelines Editor</strong>, a pop-up may appear asking you to enable it. Select <strong>Enable</strong>, or complete the previous step first.</p>
# MAGIC    </div>
# MAGIC    </div>
# MAGIC    </div>
# MAGIC
# MAGIC    f. Select **Settings** and use the following:
# MAGIC
# MAGIC    | Setting | Value / Action |
# MAGIC    |---|---|
# MAGIC    | **Name** | `Test - yourfirstname-my-pipeline-project` |
# MAGIC    | **Default catalog** | Select your **labuser** catalog |
# MAGIC    | **Default schema** | Select your **sdp_1_bronze** schema (database) |

# COMMAND ----------

# MAGIC %md
# MAGIC 2. The project will open in the **Lakeflow Pipelines Editor** and look like the following:
# MAGIC
# MAGIC    ![Pipeline Editor](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/creating-a-pipeline/lakeflow-editor-overview.png)
# MAGIC
# MAGIC    *Overview of the Lakeflow Pipelines Editor UI:* [AWS](https://docs.databricks.com/aws/en/ldp/multi-file-editor#overview-of-the-lakeflow-pipelines-editor-ui) |
# MAGIC    [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/multi-file-editor#overview-of-the-lakeflow-pipelines-editor-ui) |
# MAGIC    [GCP](https://docs.databricks.com/gcp/en/ldp/multi-file-editor#overview-of-the-lakeflow-pipelines-editor-ui)
# MAGIC
# MAGIC    a. Your Spark Declarative Pipeline will open within the **Lakeflow Pipelines Editor**.
# MAGIC
# MAGIC    b. By default, a folder is created with a `py` file then we need to change to `sql` file.
# MAGIC
# MAGIC    c. Explore the **Lakeflow Pipelines Editor** and notice the following:
# MAGIC       - The Spark Declarative Pipeline is located within the **Pipeline** tab.
# MAGIC
# MAGIC       - To navigate back to all your files and folders, select **All Files**.
# MAGIC
# MAGIC       - Write your code in the file editor.
# MAGIC
# MAGIC       - View the **Pipeline graph** panel by selecting the ![Graph](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/common/pipeline-graph-icon.png) icon.
# MAGIC
# MAGIC       - The bottom window displays pipeline run information, data previews, and performance details.
# MAGIC
# MAGIC    d. Close the tab.
# MAGIC
# MAGIC    **NOTE:** You will explore the pipeline editor in depth and run a pipeline in the next demonstration.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D3. Create a Pipeline Using the Pipeline UI
# MAGIC
# MAGIC 1. You can also create a Spark Declarative Pipeline from the main left navigation bar in **Jobs & Pipelines**. 
# MAGIC
# MAGIC    a. On the far-left navigation bar, right-click **Jobs and Pipelines** and select **Open Link in New Tab**.
# MAGIC
# MAGIC    b. Select the blue **Create** button.
# MAGIC
# MAGIC    c. Here you can select **ETL pipeline**. **No need to create another pipeline for this demonstration.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Conclusion
# MAGIC
# MAGIC In this demonstration, you:
# MAGIC
# MAGIC 1. Initialized the course environment and confirmed your catalog, schemas, and source volume.
# MAGIC 2. Previewed raw JSON order data using the `read_files` function.
# MAGIC 3. Built a traditional ETL pipeline across bronze, silver, and gold layers and identified its key limitations at scale.
# MAGIC 4. Explored how Apache Spark™ Declarative Pipelines address those limitations through efficient ingestion, intelligent transformation, and automated operations.
# MAGIC 5. Created a Spark Declarative Pipeline using both the File Explorer and the Pipeline UI.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next demonstration, you will open the Lakeflow Pipelines Editor, write your first declarative pipeline code, and run the pipeline to see incremental processing and the pipeline graph in action.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
# MAGIC