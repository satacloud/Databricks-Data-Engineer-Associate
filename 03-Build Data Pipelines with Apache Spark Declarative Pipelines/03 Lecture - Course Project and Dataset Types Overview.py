# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Course Project and Dataset Types Overview
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will first review the course project and then learn how streaming tables, materialized views, and views are used in Apache Spark™ Declarative Pipelines.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Build** a hands-on course project using Apache Spark™ Declarative Pipelines
# MAGIC 2. **Understand** the core concepts and components of Apache Spark™ Declarative Pipelines pipelines, including how streaming tables, materialized views, and temporary views function and differ

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Course Project Overview
# MAGIC
# MAGIC Throughout this course, we’ll build a hands-on project together using Apache Spark™ Declarative Pipelines.
# MAGIC The project begins with files stored in cloud storage. For this course, all files will be in JSON format, but keep in mind that Lakeflow supports many file types in real-world pipelines.
# MAGIC
# MAGIC We’ll build three flows within a single pipeline.
# MAGIC
# MAGIC **Select each tab below to learn more about the three flows.**
# MAGIC
# MAGIC
# MAGIC <div style="width: 100%; margin: auto; font-family: sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC .flow-grid {
# MAGIC   display: flex; flex-direction: column; gap: 40px;
# MAGIC   justify-content: center; align-items: center;
# MAGIC }
# MAGIC .flow-box {
# MAGIC   width: 80%; min-height: 380px; background: #F9F7F4;
# MAGIC   border: none; border-radius: 8px;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC   overflow: hidden; display: flex; flex-direction: column;
# MAGIC   gap: 12px; padding: 20px; text-align: center;
# MAGIC   position: relative; box-sizing: border-box;
# MAGIC }
# MAGIC .flow-box::before {
# MAGIC   content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 8px;
# MAGIC }
# MAGIC .flow-box.flow1::before { background: #FF7A59; }
# MAGIC .flow-box.flow2::before { background: #4299E0; }
# MAGIC .flow-box.flow3::before { background: #00A972; }
# MAGIC
# MAGIC .flow-box-title {
# MAGIC   font-size: 16pt; font-weight: bold; text-align: center;
# MAGIC }
# MAGIC
# MAGIC .flow-box-content {
# MAGIC   display: flex; align-items: flex-start; justify-content: center;
# MAGIC   gap: 24px; width: 100%; flex-wrap: wrap;
# MAGIC }
# MAGIC
# MAGIC .flow-box-img {
# MAGIC   flex: 1 1 55%; min-width: 260px; text-align: center;
# MAGIC }
# MAGIC .flow-box-img img {
# MAGIC   width: 800px; max-width: 100%; height: auto; border-radius: 6px;
# MAGIC   box-shadow: 0 2px 10px rgba(0,0,0,0.10);
# MAGIC }
# MAGIC
# MAGIC .flow-box-text {
# MAGIC   flex: 1 1 45%; min-width: 260px;
# MAGIC   font-size: 13.5pt; text-align: left; line-height: 1.7;
# MAGIC }
# MAGIC .flow-box-text ul {
# MAGIC   padding-left: 18px; margin: 10px 0 0 0;
# MAGIC }
# MAGIC .flow-box-text li { margin-bottom: 8px; }
# MAGIC .flow-box-text li:last-child { margin-bottom: 0; }
# MAGIC
# MAGIC .flow-pill {
# MAGIC   background: transparent;
# MAGIC   border-radius: 18px;
# MAGIC   padding: 4px 12px;
# MAGIC   font-size: 11pt;
# MAGIC   border: 1px solid;
# MAGIC   display: inline-block;
# MAGIC }
# MAGIC
# MAGIC .flow-pill.flow2 { border-color:#4299E0; color:#4299E0; }
# MAGIC .flow-pill.flow3 { border-color:#00A972; color:#00A972; }
# MAGIC
# MAGIC /* Tabs */
# MAGIC .flow-tabbar {
# MAGIC   display: flex; border-bottom: 2px solid #EEEDE9; margin-bottom: 0;
# MAGIC }
# MAGIC .flow-tab {
# MAGIC   padding: 10px 18px; border: none; border-bottom: 3px solid transparent;
# MAGIC   background: none; font-size: 14pt; font-weight: bold;
# MAGIC   color: #888; cursor: pointer; margin-bottom: -2px;
# MAGIC }
# MAGIC .flow-tab-active-1 { color:#FF7A59; border-bottom-color:#FF7A59; }
# MAGIC .flow-tab-active-2 { color:#4299E0; border-bottom-color:#4299E0; }
# MAGIC .flow-tab-active-3 { color:#00A972; border-bottom-color:#00A972; }
# MAGIC
# MAGIC .flow-panel { display: none; }
# MAGIC .flow-panel-active { display: block; }
# MAGIC </style>
# MAGIC
# MAGIC <!-- Tabs -->
# MAGIC <div class="flow-tabbar">
# MAGIC   <button class="flow-tab" onclick="showFlowTab(1)">Step-1</button>
# MAGIC   <button class="flow-tab" onclick="showFlowTab(2)">Step-2</button>
# MAGIC   <button class="flow-tab" onclick="showFlowTab(3)">Step-3</button>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <!-- FLOW 1 PANEL -->
# MAGIC <div class="flow-panel">
# MAGIC   <div class="flow-grid">
# MAGIC     <div class="flow-box flow1">
# MAGIC       <div class="flow-box-title">Orders Flow</div>
# MAGIC       <div class="flow-box-content">
# MAGIC         <div class="flow-box-img">
# MAGIC           <img
# MAGIC             src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_course_project/flow1_orders.png"
# MAGIC             alt="Orders Flow">
# MAGIC         </div>
# MAGIC         <div class="flow-box-text">
# MAGIC           <p><strong>What this flow does</strong></p>
# MAGIC           <ul>
# MAGIC             <li>Ingests orders JSON files into an <b>orders_bronze</b> streaming table.</li>
# MAGIC             <li>Transforms the data into an <b>orders_silver</b> streaming table.</li>
# MAGIC             <li>Creates the materialized view <b>gold_orders_by_date</b> summarizing the number of orders by date.</li>
# MAGIC           </ul>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- FLOW 2 PANEL -->
# MAGIC <div class="flow-panel">
# MAGIC   <div class="flow-grid">
# MAGIC     <div class="flow-box flow2">
# MAGIC       <div class="flow-box-title">Status Flow</div>
# MAGIC       <div class="flow-box-content">
# MAGIC         <div class="flow-box-img">
# MAGIC           <img
# MAGIC             src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_course_project/flow2_status.png"
# MAGIC             alt="Status Flow">
# MAGIC         </div>
# MAGIC         <div class="flow-box-text">
# MAGIC           <p><strong>What this flow does</strong></p>
# MAGIC           <ul>
# MAGIC             <li>Ingests status JSON files into a <b>status_bronze</b> streaming table.</li>
# MAGIC             <li>Transforms the data into a <b>status_silver</b> table.</li>
# MAGIC             <li>Joins the orders and status tables to create the materialized view <b>full_order_info_gold</b>.</li>
# MAGIC           </ul>
# MAGIC           <p style="margin-top:12px;"><strong>Produces two materialized views</strong></p>
# MAGIC           <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:6px;">
# MAGIC             <span class="flow-pill flow2">cancelled_orders</span>
# MAGIC             <span class="flow-pill flow2">delivered_orders</span>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- FLOW 3 PANEL -->
# MAGIC <div class="flow-panel">
# MAGIC   <div class="flow-grid">
# MAGIC     <div class="flow-box flow3">
# MAGIC       <div class="flow-box-title">Customers Flow</div>
# MAGIC       <div class="flow-box-content">
# MAGIC         <div class="flow-box-img">
# MAGIC           <img
# MAGIC             src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_course_project/flow3_customers.png"
# MAGIC             alt="Customers Flow">
# MAGIC         </div>
# MAGIC         <div class="flow-box-text">
# MAGIC           <p><strong>What this flow does</strong></p>
# MAGIC           <ul>
# MAGIC             <li>Ingests customer JSON files into a <b>customers_bronze</b> table.</li>
# MAGIC             <li>Cleans the data into a refined bronze table named <b>customers_bronze_clean</b>.</li>
# MAGIC             <li>Performs CDC to track data update changes in the <b>type1_customers_silver</b> table.</li>
# MAGIC           </ul>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function showFlowTab(n) {
# MAGIC   var tabs = document.getElementsByClassName("flow-tab");
# MAGIC   var panels = document.getElementsByClassName("flow-panel");
# MAGIC   // reset all
# MAGIC   for (var i = 0; i < tabs.length; i++) {
# MAGIC     tabs[i].className = "flow-tab";
# MAGIC     panels[i].className = "flow-panel";
# MAGIC   }
# MAGIC   // activate selected
# MAGIC   panels[n-1].className = "flow-panel flow-panel-active";
# MAGIC   tabs[n-1].className = "flow-tab flow-tab-active-" + n;
# MAGIC }
# MAGIC // default to Flow 1
# MAGIC window.onload = function() { showFlowTab(1); };
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Dataset Types
# MAGIC
# MAGIC Spark Declarative Pipelines support three main dataset types, each designed for a different type of data processing.
# MAGIC
# MAGIC
# MAGIC <div style="max-width: 1100px; margin: 20px auto 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <div style="display: flex; gap: 16px; align-items: stretch;">
# MAGIC
# MAGIC   <div style="flex: 1; background: #F9F7F4; border: 2px solid #FF5F46; border-radius: 10px; padding: 20px;">
# MAGIC     <div style="text-align: center; margin-bottom: 12px;">
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png" alt="Streaming Tables icon" style="max-height: 80px; max-width: 100px; width: auto; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     </div>
# MAGIC     <div style="font-size: 16pt; font-weight: 700; margin-bottom: 12px;">Streaming Tables (ST)</div>
# MAGIC     <ul style="font-size: 14pt; line-height: 1.7; padding-left: 18px; margin: 0;">
# MAGIC       <li>A table with support for streaming or incremental data processing, <b>only processing new data</b></li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex: 1; background: #F9F7F4; border: 2px solid #FF5F46; border-radius: 10px; padding: 20px;">
# MAGIC     <div style="text-align: center; margin-bottom: 12px;">
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/materialized.png" alt="Materialized Views icon" style="max-height: 80px; max-width: 100px; width: auto; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     </div>
# MAGIC     <div style="font-size: 16pt; font-weight: 700; margin-bottom: 12px;">Materialized Views (MV)</div>
# MAGIC     <ul style="font-size: 14pt; line-height: 1.7; padding-left: 18px; margin: 0;">
# MAGIC       <li>Records are processed as required to return accurate results for the <b>current data state</b></li>
# MAGIC       <li>Used for data processing tasks such as:
# MAGIC         <ul>
# MAGIC           <li>Transformations</li>
# MAGIC           <li>Aggregations</li>
# MAGIC           <li>Pre-computing slow queries</li>
# MAGIC           <li>Frequently used computations</li>
# MAGIC         </ul>
# MAGIC       </li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex: 1; background: #F9F7F4; border: 2px solid #FF5F46; border-radius: 10px; padding: 20px;">
# MAGIC     <div style="text-align: center; margin-bottom: 12px;">
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/views.png" alt="Views icon" style="max-height: 80px; max-width: 100px; width: auto; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     </div>
# MAGIC     <div style="font-size: 16pt; font-weight: 700; margin-bottom: 12px;">Views</div>
# MAGIC     <ul style="font-size: 14pt; line-height: 1.7; padding-left: 18px; margin: 0;">
# MAGIC       <li>Constructs a virtual table with no physical data based on the query in your Declarative Pipelines</li>
# MAGIC       <li>View types:
# MAGIC         <ul>
# MAGIC           <li>Temporary View</li>
# MAGIC           <li>View</li>
# MAGIC         </ul>
# MAGIC       </li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Streaming Table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Streaming Table Overview
# MAGIC
# MAGIC Streaming Tables are designed specifically for streaming or incremental data processing. This means they only process new data as it arrives, rather than reprocessing everything each time the pipeline runs. This approach significantly increases efficiency and decreases cost, especially when working with large or frequently updated data.
# MAGIC
# MAGIC <br>
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="display: flex; flex-direction: column; gap: 16px;">
# MAGIC
# MAGIC <!-- Header -->
# MAGIC <div style="background: #FFCC66; color: black; border-radius: 8px 8px 4px 4px; padding: 20px 24px;">
# MAGIC   <div style="display: flex; align-items: center; justify-content: center; gap: 16px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png"
# MAGIC     alt="Streaming Table Icon"
# MAGIC     style="width: 60px; height: 60px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <div style="text-align: left;">
# MAGIC       <div style="font-size: 22pt; font-weight: 700;">Streaming Table Overview</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Row 1 -->
# MAGIC <div style="display: flex; gap: 12px;">
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #98182A; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">1. Incremental Processing Modes</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">
# MAGIC     Supports batch or streaming for incremental data processing (exactly-once processing of files)
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #4299E0; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">2. Efficient Data Updates</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">
# MAGIC     Each time a streaming table is refreshed, data added to the source tables is appended to the streaming table
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Row 2 -->
# MAGIC <div style="display: flex; gap: 12px;">
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #02A36F; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">3. SQL Command Usage</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">
# MAGIC     SQL syntax to create streaming tables: <code>CREATE OR REFRESH STREAMING TABLE</code>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #FFAB00; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">4. Streaming Read Syntax</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">
# MAGIC     Must use <code>FROM STREAM read_files()</code> to enable incremental streaming reads with checkpointing
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Row 3 -->
# MAGIC <div style="display: flex; gap: 12px;">
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #618794; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">5. Auto Loader Integration</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">
# MAGIC     This syntax leverages Databricks Auto Loader, which automatically tracks new files and ensures reliable, incremental ingestion.
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #FF5F46; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">6. No Duplicate Reads</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">
# MAGIC     File names are guaranteed to be read only once, with the goal of not having duplicate reads within your incremental ingestion.
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %md
# MAGIC #####Documentation
# MAGIC
# MAGIC See <a href="https://docs.databricks.com/aws/en/dlt/load" target="_blank" style="color: #1976d2; text-decoration: underline;">Load data with Apache Spark™ Declarative Pipelines</a> for full reference on streaming ingestion patterns.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. Create a Streaming Table with SQL — Bronze Layer
# MAGIC
# MAGIC Let’s walk through a basic example of how to create a STREAMING TABLE named <b>orders_bronze</b> from a set of JSON files in cloud storage.
# MAGIC
# MAGIC
# MAGIC #####Click each step below to explore how the `orders_bronze` streaming table is constructed.
# MAGIC
# MAGIC <br>
# MAGIC <div style="width:100%;font-family:'Segoe UI',sans-serif;max-width:900px;margin:0 auto;">
# MAGIC
# MAGIC <style>
# MAGIC .b2-journey-wrap { display: flex; flex-direction: column; gap: 0; position: relative; }
# MAGIC .b2-journey-wrap::before { content: ""; position: absolute; left: 27px; top: 44px; bottom: 44px; width: 2px; background: linear-gradient(to bottom, #2574B5, #02A36F, #1C3037); z-index: 0; }
# MAGIC .b2-step-row { display: flex; align-items: flex-start; gap: 18px; cursor: pointer; position: relative; z-index: 1; margin-bottom: 6px; }
# MAGIC .b2-step-badge { width: 56px; height: 56px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 20px; font-weight: 800; color: white; flex-shrink: 0; border: 3px solid white; box-shadow: 0 2px 10px rgba(0,0,0,0.15); transition: transform 0.2s, box-shadow 0.2s; z-index: 2; position: relative; }
# MAGIC .b2-step-row:hover .b2-step-badge { transform: scale(1.1); box-shadow: 0 6px 18px rgba(0,0,0,0.2); }
# MAGIC .b2-step-card { flex: 1; background: #F9F7F4; border-radius: 10px; border: 1.5px solid #e8e5e0; overflow: hidden; transition: box-shadow 0.25s, border-color 0.25s; }
# MAGIC .b2-step-card:hover { box-shadow: 0 4px 18px rgba(0,0,0,0.10); }
# MAGIC .b2-step-card.active { border-color: var(--step-color); box-shadow: 0 4px 18px rgba(0,0,0,0.12); }
# MAGIC .b2-step-header { display: flex; align-items: center; justify-content: space-between; padding: 14px 18px; background: #F9F7F4; }
# MAGIC .b2-step-header-left { display: flex; align-items: center; gap: 10px; }
# MAGIC .b2-step-dot { width: 10px; height: 10px; border-radius: 50%; background: var(--step-color); flex-shrink: 0; }
# MAGIC .b2-step-title { font-size: 13pt; font-weight: 700; color: #1b3139; }
# MAGIC .b2-step-subtitle { font-size: 11pt; color: #7a7974; margin-top: 1px; }
# MAGIC .b2-step-chevron { font-size: 14px; color: #aaa; transition: transform 0.25s; user-select: none; }
# MAGIC .b2-step-card.active .b2-step-chevron { transform: rotate(180deg); color: var(--step-color); }
# MAGIC .b2-step-body { display: none; padding: 0 18px 18px 18px; border-top: 1.5px solid #eeede9; animation: b2ExpandIn 0.25s ease; }
# MAGIC @keyframes b2ExpandIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
# MAGIC .b2-step-card.active .b2-step-body { display: block; }
# MAGIC .b2-diag-row { display: flex; align-items: center; gap: 18px; margin-top: 14px; flex-wrap: wrap; }
# MAGIC .b2-diag-box { background: white; border-radius: 8px; border: 1.5px solid #e0ddd8; padding: 14px 16px; min-width: 160px; flex: 1; }
# MAGIC .b2-step-desc { font-size: 11pt; color: #3a3a3a; line-height: 1.7; margin-top: 12px; }
# MAGIC .b2-step-desc strong { color: #1b3139; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="b2-journey-wrap">
# MAGIC
# MAGIC   <!-- STEP 1 -->
# MAGIC   <div class="b2-step-row" onclick="toggleB2Step('b2-s1')">
# MAGIC     <div class="b2-step-badge" style="background:#2574B5;">1</div>
# MAGIC     <div class="b2-step-card" id="b2-s1" style="--step-color:#2574B5;">
# MAGIC       <div class="b2-step-header">
# MAGIC         <div class="b2-step-header-left">
# MAGIC           <div class="b2-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="b2-step-title">Syntax to Create a Streaming Table</div>
# MAGIC             <div class="b2-step-subtitle">CREATE OR REFRESH STREAMING TABLE</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="b2-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="b2-step-body">
# MAGIC         <p class="b2-step-desc">
# MAGIC           Use the <code>CREATE OR REFRESH STREAMING TABLE</code> statement to create a streaming table called <code>orders_bronze</code> in the <code>1_bronze_db</code> schema.
# MAGIC         </p>
# MAGIC         <div class="b2-diag-row">
# MAGIC           <div class="b2-diag-box">
# MAGIC             <code>CREATE OR REFRESH STREAMING TABLE 1_bronze_db.orders_bronze AS</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 2 -->
# MAGIC   <div class="b2-step-row" onclick="toggleB2Step('b2-s2')">
# MAGIC     <div class="b2-step-badge" style="background:#1b7fb0;">2</div>
# MAGIC     <div class="b2-step-card" id="b2-s2" style="--step-color:#1b7fb0;">
# MAGIC       <div class="b2-step-header">
# MAGIC         <div class="b2-step-header-left">
# MAGIC           <div class="b2-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="b2-step-title">Column Selection Definition</div>
# MAGIC             <div class="b2-step-subtitle">SELECT clause</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="b2-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="b2-step-body">
# MAGIC         <p class="b2-step-desc">
# MAGIC           The <code>SELECT</code> clause defines which columns to pull from the source. Here, all source columns are selected with <code>*</code>, and two metadata columns are added: <code>processing_time</code> captures when the record was ingested, and <code>source_file</code> records which file it came from.
# MAGIC         </p>
# MAGIC         <div class="b2-diag-row">
# MAGIC           <div class="b2-diag-box">
# MAGIC             <code>SELECT<br>&nbsp;&nbsp;*,<br>&nbsp;&nbsp;current_timestamp() AS processing_time,<br>&nbsp;&nbsp;_metadata.file_name AS source_file</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 3 -->
# MAGIC   <div class="b2-step-row" onclick="toggleB2Step('b2-s3')">
# MAGIC     <div class="b2-step-badge" style="background:#02A36F;">3</div>
# MAGIC     <div class="b2-step-card" id="b2-s3" style="--step-color:#02A36F;">
# MAGIC       <div class="b2-step-header">
# MAGIC         <div class="b2-step-header-left">
# MAGIC           <div class="b2-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="b2-step-title">Streaming Source Configuration</div>
# MAGIC             <div class="b2-step-subtitle">FROM clause</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="b2-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="b2-step-body">
# MAGIC         <p class="b2-step-desc">
# MAGIC           <ul>
# MAGIC             <li>The <code>STREAM</code> keyword tells the pipeline to use streaming semantics, meaning it will process new files incrementally as they arrive.</li>
# MAGIC             <li>The <code>read_files()</code> function points to the location of your source files and returns their contents in tabular format.</li>
# MAGIC           </ul>
# MAGIC         </p>
# MAGIC         <div class="b2-diag-row">
# MAGIC           <div class="b2-diag-box">
# MAGIC             <code>FROM <strong>STREAM</strong> read_files(<br>&nbsp;&nbsp;"{{source_path}}/orders",<br>&nbsp;&nbsp;format => 'JSON');</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 20px; padding: 18px 24px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 16pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px;"><strong>RESULT</strong>: Streaming table <code>orders_bronze</code> reading from JSON files</div>
# MAGIC     <pre style="background:#f8f8f8; border-radius:8px; padding:16px; overflow-x:auto; border:1px solid #e0e0e0; font-family:Consolas,Monaco,monospace; font-size:13px; color:#1b3139;">CREATE OR REFRESH STREAMING TABLE 1_bronze_db.orders_bronze AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   current_timestamp() AS processing_time,
# MAGIC   _metadata.file_name AS source_file
# MAGIC FROM STREAM read_files(
# MAGIC   "{{source_path}}/orders",
# MAGIC   format => 'JSON');</pre>
# MAGIC   </div>
# MAGIC   <br>
# MAGIC <b>Note</b>: <i>The query is using the default catalog</i>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function toggleB2Step(id) {
# MAGIC   var cards = document.querySelectorAll(".b2-step-card");
# MAGIC   var card = document.getElementById(id);
# MAGIC   var isActive = card.classList.contains("active");
# MAGIC   cards.forEach(function(c){ c.classList.remove("active"); });
# MAGIC   if (!isActive) { card.classList.add("active"); }
# MAGIC }
# MAGIC window.addEventListener("DOMContentLoaded", function(){ toggleB2Step("b2-s1"); });
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC In this example, we use the <code>CREATE OR REFRESH STREAMING TABLE</code> statement to create a streaming table called <code>orders_bronze</code> in the <code>1_bronze_db</code> schema.
# MAGIC
# MAGIC The <code>SELECT</code> clause defines which columns you want to pull into the streaming table from your source data.
# MAGIC
# MAGIC In the <code>FROM</code> clause:
# MAGIC - The <code>STREAM</code> keyword tells the pipeline to use streaming semantics, meaning it will process new files incrementally as they arrive.
# MAGIC - The <code>read_files()</code> function points to the location of your source files. It reads the files and returns the contents in a tabular format.
# MAGIC
# MAGIC This setup ensures that only new data is processed each time the pipeline runs, enabling efficient, scalable ingestion directly from cloud storage.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C3. Create a Streaming Table with SQL — Silver Layer
# MAGIC
# MAGIC Now let’s take it a step further and create a silver streaming table named <b>orders_silver</b>, which reads from the <b>orders_bronze</b> streaming table.
# MAGIC
# MAGIC #####Click each step below to explore how the `orders_silver` streaming table is constructed.
# MAGIC
# MAGIC <br>
# MAGIC <div style="width:100%;font-family:'Segoe UI',sans-serif;max-width:900px;margin:0 auto;">
# MAGIC
# MAGIC <style>
# MAGIC .b3-journey-wrap { display: flex; flex-direction: column; gap: 0; position: relative; }
# MAGIC .b3-journey-wrap::before { content: ""; position: absolute; left: 27px; top: 44px; bottom: 44px; width: 2px; background: linear-gradient(to bottom, #2574B5, #02A36F, #1C3037); z-index: 0; }
# MAGIC .b3-step-row { display: flex; align-items: flex-start; gap: 18px; cursor: pointer; position: relative; z-index: 1; margin-bottom: 6px; }
# MAGIC .b3-step-badge { width: 56px; height: 56px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 20px; font-weight: 800; color: white; flex-shrink: 0; border: 3px solid white; box-shadow: 0 2px 10px rgba(0,0,0,0.15); transition: transform 0.2s, box-shadow 0.2s; z-index: 2; position: relative; }
# MAGIC .b3-step-row:hover .b3-step-badge { transform: scale(1.1); box-shadow: 0 6px 18px rgba(0,0,0,0.2); }
# MAGIC .b3-step-card { flex: 1; background: #F9F7F4; border-radius: 10px; border: 1.5px solid #e8e5e0; overflow: hidden; transition: box-shadow 0.25s, border-color 0.25s; }
# MAGIC .b3-step-card:hover { box-shadow: 0 4px 18px rgba(0,0,0,0.10); }
# MAGIC .b3-step-card.active { border-color: var(--step-color); box-shadow: 0 4px 18px rgba(0,0,0,0.12); }
# MAGIC .b3-step-header { display: flex; align-items: center; justify-content: space-between; padding: 14px 18px; background: #F9F7F4; }
# MAGIC .b3-step-header-left { display: flex; align-items: center; gap: 10px; }
# MAGIC .b3-step-dot { width: 10px; height: 10px; border-radius: 50%; background: var(--step-color); flex-shrink: 0; }
# MAGIC .b3-step-title { font-size: 13pt; font-weight: 700; color: #1b3139; }
# MAGIC .b3-step-subtitle { font-size: 11pt; color: #7a7974; margin-top: 1px; }
# MAGIC .b3-step-chevron { font-size: 14px; color: #aaa; transition: transform 0.25s; user-select: none; }
# MAGIC .b3-step-card.active .b3-step-chevron { transform: rotate(180deg); color: var(--step-color); }
# MAGIC .b3-step-body { display: none; padding: 0 18px 18px 18px; border-top: 1.5px solid #eeede9; animation: b3ExpandIn 0.25s ease; }
# MAGIC @keyframes b3ExpandIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
# MAGIC .b3-step-card.active .b3-step-body { display: block; }
# MAGIC .b3-diag-row { display: flex; align-items: center; gap: 18px; margin-top: 14px; flex-wrap: wrap; }
# MAGIC .b3-diag-box { background: white; border-radius: 8px; border: 1.5px solid #e0ddd8; padding: 14px 16px; min-width: 160px; flex: 1; }
# MAGIC .b3-step-desc { font-size: 11pt; color: #3a3a3a; line-height: 1.7; margin-top: 12px; }
# MAGIC .b3-step-desc strong { color: #1b3139; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="b3-journey-wrap">
# MAGIC
# MAGIC   <!-- STEP 1 -->
# MAGIC   <div class="b3-step-row" onclick="toggleB3Step('b3-s1')">
# MAGIC     <div class="b3-step-badge" style="background:#2574B5;">1</div>
# MAGIC     <div class="b3-step-card" id="b3-s1" style="--step-color:#2574B5;">
# MAGIC       <div class="b3-step-header">
# MAGIC         <div class="b3-step-header-left">
# MAGIC           <div class="b3-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="b3-step-title">Syntax to Create a Streaming Table</div>
# MAGIC             <div class="b3-step-subtitle">CREATE OR REFRESH STREAMING TABLE</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="b3-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="b3-step-body">
# MAGIC         <p class="b3-step-desc">
# MAGIC           Use the <code>CREATE OR REFRESH STREAMING TABLE</code> statement to create a streaming table called <code>orders_silver</code> in the <code>2_silver_db</code> schema.
# MAGIC         </p>
# MAGIC         <div class="b3-diag-row">
# MAGIC           <div class="b3-diag-box">
# MAGIC             <code>CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver AS</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 2 -->
# MAGIC   <div class="b3-step-row" onclick="toggleB3Step('b3-s2')">
# MAGIC     <div class="b3-step-badge" style="background:#1b7fb0;">2</div>
# MAGIC     <div class="b3-step-card" id="b3-s2" style="--step-color:#1b7fb0;">
# MAGIC       <div class="b3-step-header">
# MAGIC         <div class="b3-step-header-left">
# MAGIC           <div class="b3-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="b3-step-title">SQL Transformation Definition</div>
# MAGIC             <div class="b3-step-subtitle">SELECT clause</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="b3-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="b3-step-body">
# MAGIC         <p class="b3-step-desc">
# MAGIC           The <code>SELECT</code> clause is where you apply SQL transformation logic to the streaming data. Here, specific columns are selected and <code>order_timestamp</code> is cast to a proper <code>timestamp</code> type. In practice, these transformations can include complex joins, filters, column derivations, and more.
# MAGIC         </p>
# MAGIC         <div class="b3-diag-row">
# MAGIC           <div class="b3-diag-box">
# MAGIC             <code>SELECT<br>&nbsp;&nbsp;order_id,<br>&nbsp;&nbsp;timestamp(order_timestamp) AS order_timestamp,<br>&nbsp;&nbsp;customer_id,<br>&nbsp;&nbsp;notifications</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 3 -->
# MAGIC   <div class="b3-step-row" onclick="toggleB3Step('b3-s3')">
# MAGIC     <div class="b3-step-badge" style="background:#02A36F;">3</div>
# MAGIC     <div class="b3-step-card" id="b3-s3" style="--step-color:#02A36F;">
# MAGIC       <div class="b3-step-header">
# MAGIC         <div class="b3-step-header-left">
# MAGIC           <div class="b3-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="b3-step-title">Streaming Source Configuration</div>
# MAGIC             <div class="b3-step-subtitle">FROM STREAM clause</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="b3-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="b3-step-body">
# MAGIC         <p class="b3-step-desc">
# MAGIC           <ul>
# MAGIC             <li>The <code>STREAM</code> keyword tells the pipeline to use streaming semantics — it will incrementally transform only the <strong>new rows</strong> that have landed in the bronze table since the last run.</li>
# MAGIC             <li>Instead of pointing to a file path, the source here is the <code>orders_bronze</code> streaming table, chaining your bronze-to-silver transformation within the same pipeline.</li>
# MAGIC           </ul>
# MAGIC         </p>
# MAGIC         <div class="b3-diag-row">
# MAGIC           <div class="b3-diag-box">
# MAGIC             <code>FROM <strong>STREAM</strong> 1_bronze_db.orders_bronze;</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 20px; padding: 18px 24px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 16pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px;"><strong>RESULT</strong>: Streaming table <code>orders_silver</code> reading from <code>orders_bronze</code></div>
# MAGIC     <pre style="background:#f8f8f8; border-radius:8px; padding:16px; overflow-x:auto; border:1px solid #e0e0e0; font-family:Consolas,Monaco,monospace; font-size:13px; color:#1b3139;">CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   timestamp(order_timestamp) AS order_timestamp,
# MAGIC   customer_id,
# MAGIC   notifications
# MAGIC FROM STREAM 1_bronze_db.orders_bronze;</pre>
# MAGIC   </div>
# MAGIC   <br>
# MAGIC <b>Note</b>: <i>The query is using the default catalog</i>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function toggleB3Step(id) {
# MAGIC   var cards = document.querySelectorAll(".b3-step-card");
# MAGIC   var card = document.getElementById(id);
# MAGIC   var isActive = card.classList.contains("active");
# MAGIC   cards.forEach(function(c){ c.classList.remove("active"); });
# MAGIC   if (!isActive) { card.classList.add("active"); }
# MAGIC }
# MAGIC window.addEventListener("DOMContentLoaded", function(){ toggleB3Step("b3-s1"); });
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC The <code>SELECT</code> clause is where you apply your SQL transformation logic to the streaming data. In this example, we’re keeping it simple, but in practice these transformations can include complex joins, filters, column derivations, and more.
# MAGIC
# MAGIC The most important part is in the <code>FROM</code> clause. Here, you use the <code>STREAM</code> keyword with the name of the source streaming table, in this case, <code>orders_bronze</code>. This tells the pipeline to incrementally transform only the new rows that have landed in the bronze table since the last run.
# MAGIC
# MAGIC This setup helps ensure that your silver table always stays up to date with the latest processed data, without reprocessing anything that’s already been handled.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Materialized Views

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. Materialized Views Overview
# MAGIC
# MAGIC Materialized Views process records as needed to deliver accurate results based on the current state of your streaming tables. They are designed to automatically keep results up to date as new data flows through upstream tables.
# MAGIC
# MAGIC <br>
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="display: flex; flex-direction: column; gap: 16px;">
# MAGIC
# MAGIC <!-- Header -->
# MAGIC <div style="background: #FF8774; color: black; border-radius: 8px 8px 4px 4px; padding: 20px 24px;">
# MAGIC   <div style="display: flex; align-items: center; justify-content: center; gap: 16px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/materialized.png"
# MAGIC     alt="Materialized Views icon"
# MAGIC     style="width: 60px; height: 60px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <div style="text-align: left;">
# MAGIC       <div style="font-size: 22pt; font-weight: 700;">Materialized Views Overview</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Row 1 -->
# MAGIC <div style="display: flex; gap: 12px;">
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #98182A; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">1. Dynamic Query Recalculation</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Each time a materialized view is updated, query results are recalculated to reflect changes in upstream datasets</div>
# MAGIC </div>
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #4299E0; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">2. Pipeline-Driven Maintenance</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Automatically created and updated by the pipeline</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Row 2 -->
# MAGIC <div style="display: flex; gap: 12px;">
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #02A36F; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">3. SQL Command Usage</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Use the <code>CREATE OR REFRESH MATERIALIZED VIEW</code> syntax</div>
# MAGIC </div>
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #FFAB00; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">4. Flexible Pipeline Placement</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Can be used anywhere in your pipeline, not just in the gold layer</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Row 3 -->
# MAGIC <div style="display: flex; gap: 12px;">
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #618794; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">5. Incremental Refresh Capability</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Where applicable, results are incrementally refreshed, avoiding a full rebuild when new data arrives. Supported on Serverless compute.</div>
# MAGIC </div>
# MAGIC <div style="flex: 1; background: #F9F7F4; border-radius: 8px; padding: 18px; position: relative; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="position: absolute; top: 0; left: 0; width: 100%; height: 6px; background: #FF5F46; border-radius: 8px 8px 0 0;"></div>
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">6. Cost-Based Optimization</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Incremental refresh is driven by a cost-based optimizer to power fast and efficient transformations on Serverless compute</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####Documentation
# MAGIC
# MAGIC Behind the scenes, a cost-based optimizer determines whether to incrementally refresh or fully recompute the view. See <a href="https://docs.databricks.com/aws/en/optimizations/incremental-refresh" style="color: #1976d2; text-decoration: underline;">Incremental refresh for materialized views</a> for details.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D2. Create a Materialized View with SQL — Gold Layer
# MAGIC
# MAGIC Let’s take a look at an example of how to create a Materialized View called `gold_orders_by_date`, which summarizes data from the `orders_silver` streaming table.
# MAGIC
# MAGIC #####Click each step below to explore how `gold_orders_by_date` is constructed.
# MAGIC
# MAGIC <br>
# MAGIC <div style="width:100%;font-family:'Segoe UI',sans-serif;max-width:900px;margin:0 auto;">
# MAGIC
# MAGIC <style>
# MAGIC .c2-journey-wrap { display: flex; flex-direction: column; gap: 0; position: relative; }
# MAGIC .c2-journey-wrap::before { content: ""; position: absolute; left: 27px; top: 44px; bottom: 44px; width: 2px; background: linear-gradient(to bottom, #2574B5, #02A36F, #FF5F46, #1C3037); z-index: 0; }
# MAGIC .c2-step-row { display: flex; align-items: flex-start; gap: 18px; cursor: pointer; position: relative; z-index: 1; margin-bottom: 6px; }
# MAGIC .c2-step-badge { width: 56px; height: 56px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 20px; font-weight: 800; color: white; flex-shrink: 0; border: 3px solid white; box-shadow: 0 2px 10px rgba(0,0,0,0.15); transition: transform 0.2s, box-shadow 0.2s; z-index: 2; position: relative; }
# MAGIC .c2-step-row:hover .c2-step-badge { transform: scale(1.1); box-shadow: 0 6px 18px rgba(0,0,0,0.2); }
# MAGIC .c2-step-card { flex: 1; background: #F9F7F4; border-radius: 10px; border: 1.5px solid #e8e5e0; overflow: hidden; transition: box-shadow 0.25s, border-color 0.25s; }
# MAGIC .c2-step-card:hover { box-shadow: 0 4px 18px rgba(0,0,0,0.10); }
# MAGIC .c2-step-card.active { border-color: var(--step-color); box-shadow: 0 4px 18px rgba(0,0,0,0.12); }
# MAGIC .c2-step-header { display: flex; align-items: center; justify-content: space-between; padding: 14px 18px; background: #F9F7F4; }
# MAGIC .c2-step-header-left { display: flex; align-items: center; gap: 10px; }
# MAGIC .c2-step-dot { width: 10px; height: 10px; border-radius: 50%; background: var(--step-color); flex-shrink: 0; }
# MAGIC .c2-step-title { font-size: 13pt; font-weight: 700; color: #1b3139; }
# MAGIC .c2-step-subtitle { font-size: 11pt; color: #7a7974; margin-top: 1px; }
# MAGIC .c2-step-chevron { font-size: 14px; color: #aaa; transition: transform 0.25s; user-select: none; }
# MAGIC .c2-step-card.active .c2-step-chevron { transform: rotate(180deg); color: var(--step-color); }
# MAGIC .c2-step-body { display: none; padding: 0 18px 18px 18px; border-top: 1.5px solid #eeede9; animation: c2ExpandIn 0.25s ease; }
# MAGIC @keyframes c2ExpandIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: translateY(0); } }
# MAGIC .c2-step-card.active .c2-step-body { display: block; }
# MAGIC .c2-diag-row { display: flex; align-items: center; gap: 18px; margin-top: 14px; flex-wrap: wrap; }
# MAGIC .c2-diag-box { background: white; border-radius: 8px; border: 1.5px solid #e0ddd8; padding: 14px 16px; min-width: 160px; flex: 1; }
# MAGIC .c2-step-desc { font-size: 11pt; color: #3a3a3a; line-height: 1.7; margin-top: 12px; }
# MAGIC .c2-step-desc strong { color: #1b3139; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="c2-journey-wrap">
# MAGIC
# MAGIC   <!-- STEP 1 -->
# MAGIC   <div class="c2-step-row" onclick="toggleC2Step('c2-s1')">
# MAGIC     <div class="c2-step-badge" style="background:#2574B5;">1</div>
# MAGIC     <div class="c2-step-card" id="c2-s1" style="--step-color:#2574B5;">
# MAGIC       <div class="c2-step-header">
# MAGIC         <div class="c2-step-header-left">
# MAGIC           <div class="c2-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="c2-step-title">Syntax to Create a Materialized View</div>
# MAGIC             <div class="c2-step-subtitle">CREATE OR REFRESH MATERIALIZED VIEW</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="c2-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="c2-step-body">
# MAGIC         <p class="c2-step-desc">
# MAGIC           Use the <code>CREATE OR REFRESH MATERIALIZED VIEW</code> statement to define this view inside the <code>3_gold_db</code> schema. Unlike streaming tables, materialized views automatically track changes and manage their own refreshes based on the upstream source.
# MAGIC         </p>
# MAGIC         <div class="c2-diag-row">
# MAGIC           <div class="c2-diag-box">
# MAGIC             <code>CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date AS</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 2 -->
# MAGIC   <div class="c2-step-row" onclick="toggleC2Step('c2-s2')">
# MAGIC     <div class="c2-step-badge" style="background:#1b7fb0;">2</div>
# MAGIC     <div class="c2-step-card" id="c2-s2" style="--step-color:#1b7fb0;">
# MAGIC       <div class="c2-step-header">
# MAGIC         <div class="c2-step-header-left">
# MAGIC           <div class="c2-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="c2-step-title">Aggregation and Column Definition</div>
# MAGIC             <div class="c2-step-subtitle">SELECT clause</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="c2-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="c2-step-body">
# MAGIC         <p class="c2-step-desc">
# MAGIC           The <code>SELECT</code> clause defines the gold-layer aggregation logic. Here, <code>order_timestamp</code> is truncated to a date using <code>date()</code>, and <code>count(*)</code> computes the total number of orders per day. This is where raw streaming data is transformed into business-ready metrics.
# MAGIC         </p>
# MAGIC         <div class="c2-diag-row">
# MAGIC           <div class="c2-diag-box">
# MAGIC             <code>SELECT<br>&nbsp;&nbsp;date(order_timestamp) AS order_date,<br>&nbsp;&nbsp;count(*) AS total_daily_orders</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 3 -->
# MAGIC   <div class="c2-step-row" onclick="toggleC2Step('c2-s3')">
# MAGIC     <div class="c2-step-badge" style="background:#02A36F;">3</div>
# MAGIC     <div class="c2-step-card" id="c2-s3" style="--step-color:#02A36F;">
# MAGIC       <div class="c2-step-header">
# MAGIC         <div class="c2-step-header-left">
# MAGIC           <div class="c2-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="c2-step-title">Source Table Reference — No STREAM Keyword</div>
# MAGIC             <div class="c2-step-subtitle">FROM clause</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="c2-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="c2-step-body">
# MAGIC         <p class="c2-step-desc">
# MAGIC           In the <code>FROM</code> clause, the source table <code>orders_silver</code> is referenced <strong>without the <code>STREAM</code> keyword</strong>. Materialized views automatically handle incremental refresh logic — you do not need to declare streaming semantics explicitly.
# MAGIC         </p>
# MAGIC         <div class="c2-diag-row">
# MAGIC           <div class="c2-diag-box">
# MAGIC             <code>FROM 2_silver_db.orders_silver</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 4 -->
# MAGIC   <div class="c2-step-row" onclick="toggleC2Step('c2-s4')">
# MAGIC     <div class="c2-step-badge" style="background:#FF5F46;">4</div>
# MAGIC     <div class="c2-step-card" id="c2-s4" style="--step-color:#FF5F46;">
# MAGIC       <div class="c2-step-header">
# MAGIC         <div class="c2-step-header-left">
# MAGIC           <div class="c2-step-dot"></div>
# MAGIC           <div>
# MAGIC             <div class="c2-step-title">Grouping for Daily Aggregation</div>
# MAGIC             <div class="c2-step-subtitle">GROUP BY clause</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="c2-step-chevron">▼</div>
# MAGIC       </div>
# MAGIC       <div class="c2-step-body">
# MAGIC         <p class="c2-step-desc">
# MAGIC           The <code>GROUP BY</code> clause groups all order records by their date so that <code>count(*)</code> returns the total number of orders placed on each individual day. This produces a clean, day-level summary ideal for reporting and dashboards.
# MAGIC         </p>
# MAGIC         <div class="c2-diag-row">
# MAGIC           <div class="c2-diag-box">
# MAGIC             <code>GROUP BY date(order_timestamp);</code>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 20px; padding: 18px 24px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 16pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px;"><strong>RESULT</strong>: Materialized view <code>gold_orders_by_date</code> summarizing <code>orders_silver</code></div>
# MAGIC     <pre style="background:#f8f8f8; border-radius:8px; padding:16px; overflow-x:auto; border:1px solid #e0e0e0; font-family:Consolas,Monaco,monospace; font-size:13px; color:#1b3139;">CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date AS
# MAGIC SELECT
# MAGIC   date(order_timestamp) AS order_date,
# MAGIC   count(*) AS total_daily_orders
# MAGIC FROM 2_silver_db.orders_silver
# MAGIC GROUP BY date(order_timestamp);</pre>
# MAGIC   </div>
# MAGIC   <br>
# MAGIC   <b>Note</b>: <i>The query is using the default catalog</i>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function toggleC2Step(id) {
# MAGIC   var cards = document.querySelectorAll(".c2-step-card");
# MAGIC   var card = document.getElementById(id);
# MAGIC   var isActive = card.classList.contains("active");
# MAGIC   cards.forEach(function(c){ c.classList.remove("active"); });
# MAGIC   if (!isActive) { card.classList.add("active"); }
# MAGIC }
# MAGIC window.addEventListener("DOMContentLoaded", function(){ toggleC2Step("c2-s1"); });
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC We use the CREATE OR REFRESH MATERIALIZED VIEW statement to define this view inside the 3_gold_db schema.
# MAGIC
# MAGIC In the FROM clause, we reference the source table, orders_silver, without the STREAM keyword. That’s because Materialized Views automatically track changes and manage refreshes based on the upstream streaming table.
# MAGIC
# MAGIC <b>Important note</b>: Where possible, the system will use incremental refreshes to update the view efficiently, rather than rebuilding it from scratch. This is supported in Serverless compute and driven by a cost-based optimizer for performance.
# MAGIC
# MAGIC This approach is ideal for creating gold-layer aggregations or business-ready outputs with minimal overhead and high performance.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Temporary Views and Views

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. Views Overview and Limitations
# MAGIC
# MAGIC Views create virtual tables that do not store any physical data. Instead, they are simply logical representations based on the SQL query defined in your pipeline. 
# MAGIC
# MAGIC There are two types: **Temporary Views**, which exist only for the duration of a pipeline run, and **Views**, which are registered as objects in Unity Catalog and persist beyond the run.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="display: flex; flex-direction: column; gap: 16px;">
# MAGIC
# MAGIC <!-- Header -->
# MAGIC <div style="background: #FF8774; color: black; border-radius: 8px 8px 4px 4px; padding: 20px 24px;">
# MAGIC   <div style="display: flex; align-items: center; justify-content: center; gap: 16px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/views.png"
# MAGIC     alt="Views Icon"
# MAGIC     style="width: 60px; height: 60px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <div style="text-align: left;">
# MAGIC       <div style="font-size: 22pt; font-weight: 700;">Views Overview</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Column Headers -->
# MAGIC <div style="display: flex; gap: 12px;">
# MAGIC <div style="flex: 1; background: #F9F7F4; border-left: 4px solid #98182A; border-right: 4px solid #98182A; border-top: 4px solid #98182A; border-radius: 8px; padding: 22px 20px; box-shadow: 0 2px 8px rgba(11,32,38,0.07);">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px; text-align: center;">Temporary View</div>
# MAGIC </div>
# MAGIC <div style="flex: 1; background: #F9F7F4; border-left: 4px solid #98182A; border-right: 4px solid #98182A; border-top: 4px solid #98182A; border-radius: 8px; padding: 22px 20px; box-shadow: 0 2px 8px rgba(11,32,38,0.07);">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px; text-align: center;">View</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Row 1 -->
# MAGIC <div style="display: flex; gap: 12px;">
# MAGIC <div style="flex: 1; background: #F9F7F4; border-left: 4px solid #4299E0; border-right: 4px solid #4299E0; border-radius: 8px; padding: 22px 20px; box-shadow: 0 2px 8px rgba(11,32,38,0.07);">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">1. Temporary and Pipeline-Scoped</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Exists only during the pipeline run and is not registered in Unity Catalog</div>
# MAGIC </div>
# MAGIC <div style="flex: 1; background: #F9F7F4; border-right: 4px solid #FFAB00; border-left: 4px solid #FFAB00; border-radius: 8px; padding: 22px 20px; box-shadow: 0 2px 8px rgba(11,32,38,0.07);">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">1. Virtual Tables with No Storage</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Created from SQL query results without storing physical data</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Row 2 -->
# MAGIC <div style="display: flex; gap: 12px;">
# MAGIC <div style="flex: 1; background: #F9F7F4; border-left: 4px solid #618794; border-right: 4px solid #618794; border-radius: 8px; padding: 22px 20px; box-shadow: 0 2px 8px rgba(11,32,38,0.07);">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">2. SQL-Based Intermediate Layer</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Created using <code>CREATE TEMPORARY VIEW</code> and used for internal, non-user-facing queries</div>
# MAGIC </div>
# MAGIC <div style="flex: 1; background: #F9F7F4; border-right: 4px solid #FF5F46; border-left: 4px solid #FF5F46; border-radius: 8px; padding: 22px 20px; box-shadow: 0 2px 8px rgba(11,32,38,0.07);">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #0b2026; margin-bottom: 8px;">2. Catalog-Registered Views</div>
# MAGIC   <div style="font-size: 14pt; color: #333; line-height: 1.5;">Stored in Unity Catalog and created using <code>CREATE VIEW</code></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #c62828; font-size: 1.1em;">View Limitations</strong>
# MAGIC <ul style="margin: 12px 0 0 16px; color: #333;">
# MAGIC   <li>The pipeline must be a <strong>Unity Catalog pipeline</strong>.</li>
# MAGIC   <li>Views <strong>cannot have streaming queries</strong> and cannot be used as a streaming source for a pipeline.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E2. Create Views with SQL
# MAGIC
# MAGIC Both view types use standard SQL `CREATE` syntax without the `OR REFRESH` clause. The key difference is scope: a temporary view is discarded after the pipeline run, while a standard view is persisted in Unity Catalog.
# MAGIC <div style="display:flex; gap:24px; align-items:flex-start; justify-content:flex-start; flex-wrap:wrap; margin-top:12px;">
# MAGIC   <!-- Temporary View -->
# MAGIC   <div style="flex:1 1 320px; min-width:0;">
# MAGIC     <h4 style="margin-top:0;">Temporary View</h4>
# MAGIC     <p style="margin-top:4px; font-size:13px; color:#475569;">
# MAGIC       Pipeline-scoped, not registered in Unity Catalog:
# MAGIC     </p>
# MAGIC     <pre style="background:#f8f8f8; border-radius:8px; padding:16px; overflow-x:auto; border:1px solid #e0e0e0; font-family:Consolas,Monaco,monospace; font-size:13px; color:#1b3139;">CREATE TEMPORARY VIEW orders_active AS
# MAGIC SELECT *
# MAGIC FROM 2_silver_db.orders_silver
# MAGIC WHERE notifications = 'Y';</pre>
# MAGIC   </div>
# MAGIC   <!-- Standard View -->
# MAGIC   <div style="flex:1 1 320px; min-width:0;">
# MAGIC     <h4 style="margin-top:0;">View</h4>
# MAGIC     <p style="margin-top:4px; font-size:13px; color:#475569;">
# MAGIC       Registered in Unity Catalog, available after the pipeline run:
# MAGIC     </p>
# MAGIC     <pre style="background:#f8f8f8; border-radius:8px; padding:16px; overflow-x:auto; border:1px solid #e0e0e0; font-family:Consolas,Monaco,monospace; font-size:13px; color:#1b3139;">CREATE VIEW 3_gold_db.orders_active_view AS
# MAGIC SELECT *
# MAGIC FROM 2_silver_db.orders_silver
# MAGIC WHERE notifications = 'Y';</pre>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####Documentation
# MAGIC
# MAGIC <ul style="margin: 12px 0 0 16px; color: #333;">
# MAGIC   <li><a href="https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-create-temporary-view" style="color: #1976d2; text-decoration: underline;">CREATE TEMPORARY VIEW (Apache Spark™ Declarative Pipelines)</a></li>
# MAGIC   <li><a href="https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-create-view" style="color: #1976d2; text-decoration: underline;">CREATE VIEW (Apache Spark™ Declarative Pipelines)</a></li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. What Changed from DLT to SDP

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### F1. Syntax Migration Reference
# MAGIC
# MAGIC Existing users of **Delta Live Tables (DLT)** will notice that the syntax has evolved under **Spark Declarative Pipelines (SDP)**. The old keywords remain supported for backward compatibility, but all new pipelines should use the updated syntax.
# MAGIC
# MAGIC <div style="max-width:1000px; margin: 20px auto;">
# MAGIC
# MAGIC <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; margin-bottom: 16px;">
# MAGIC
# MAGIC   <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px;">
# MAGIC     <div style="display: flex; flex-direction: column; align-items: center; gap: 10px;">
# MAGIC       <div style="display: inline-flex; align-items: center; gap: 10px; background: #ffebee; border: 2px solid #f44336; border-radius: 8px; padding: 10px 18px; font-family: 'Segoe UI', sans-serif; font-size: 13pt; font-weight: 700; color: #c62828;">
# MAGIC         <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#c62828" stroke-width="3" stroke-linecap="round" stroke-linejoin="round">
# MAGIC           <line x1="18" y1="6" x2="6" y2="18"/>
# MAGIC           <line x1="6" y1="6" x2="18" y2="18"/>
# MAGIC         </svg>
# MAGIC         Deprecated (DLT)
# MAGIC       </div>
# MAGIC       <div style="display: flex; flex-direction: column; gap: 8px; width: 100%;">
# MAGIC         <code style="background:#fff; padding:6px 10px; border-radius:4px; display:block; color:#c62828; border: 1px solid #ef9a9a; text-align:left;">CREATE OR REFRESH STREAMING LIVE TABLE</code>
# MAGIC         <code style="background:#fff; padding:6px 10px; border-radius:4px; display:block; color:#c62828; border: 1px solid #ef9a9a; text-align:left;">CREATE OR REFRESH LIVE TABLE</code>
# MAGIC         <code style="background:#fff; padding:6px 10px; border-radius:4px; display:block; color:#c62828; border: 1px solid #ef9a9a; text-align:left;">CREATE LIVE VIEW / CREATE TEMPORARY LIVE VIEW</code>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="border-left: 4px solid #4caf50; background:#e8f5e9; padding: 16px 20px; border-radius: 4px;">
# MAGIC     <div style="display: flex; flex-direction: column; align-items: center; gap: 10px;">
# MAGIC       <div style="display: inline-flex; align-items: center; gap: 10px; background: #E8F5E9; border: 2px solid #02A36F; border-radius: 8px; padding: 10px 18px; font-family: 'Segoe UI', sans-serif; font-size: 13pt; font-weight: 700; color: #02A36F;">
# MAGIC         <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#02A36F" stroke-width="3" stroke-linecap="round" stroke-linejoin="round">
# MAGIC           <polyline points="20 6 9 17 4 12"/>
# MAGIC         </svg>
# MAGIC         Modern (SDP)
# MAGIC       </div>
# MAGIC       <div style="display: flex; flex-direction: column; gap: 8px; width: 100%;">
# MAGIC         <code style="background:#fff; padding:6px 10px; border-radius:4px; display:block; color:#2e7d32; border: 1px solid #a5d6a7; text-align:left;">CREATE OR REFRESH STREAMING TABLE</code>
# MAGIC         <code style="background:#fff; padding:6px 10px; border-radius:4px; display:block; color:#2e7d32; border: 1px solid #a5d6a7; text-align:left;">CREATE OR REFRESH MATERIALIZED VIEW</code>
# MAGIC         <code style="background:#fff; padding:6px 10px; border-radius:4px; display:block; color:#2e7d32; border: 1px solid #a5d6a7; text-align:left;">CREATE VIEW / CREATE TEMPORARY VIEW</code>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC For existing users of DLT, you’ll notice that we’ve updated some names in Apache Spark™ Declarative Pipelines.
# MAGIC
# MAGIC The core semantics and functionality remain the same, so your current workflows will continue to work seamlessly.
# MAGIC
# MAGIC We also support the old syntax to maintain backward compatibility during the transition.
# MAGIC
# MAGIC Our goal with these changes is to simplify the syntax and better align with industry standards, making it easier to learn and use across different systems.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. The Declarative Pipeline Graph

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G1. Automatic Dependency Resolution
# MAGIC
# MAGIC One of the powerful features of Declarative Pipelines is that pipeline dependencies are automatically parsed. You don’t need to worry about the order of your code.
# MAGIC
# MAGIC All dependencies are connected behind the scenes in the Declarative Pipeline Graph, which ensures data flows correctly from one step to the next.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_dataset_types/declarative_pipeline_graph.png"
# MAGIC     alt="Traditional Data Ingestion Challenges"
# MAGIC     style="width: 1100px; max-width: 100%; height: auto; border: 1px solid #000000; border-radius: 4px;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC For example:
# MAGIC
# MAGIC - The <code>orders_bronze</code> table ingests the raw JSON data.
# MAGIC - The <code>orders_silver</code> streaming table is linked to <code>orders_bronze</code>, transforming the ingested data incrementally.
# MAGIC - The <code>gold_orders_by_date</code> materialized view depends on <code>orders_silver</code> streaming table, aggregating the transformed data for downstream business use.
# MAGIC
# MAGIC This automatic linking simplifies pipeline development and reduces errors by removing the need to manually manage execution order. It also makes it easy to view and monitor your pipeline and its dependencies, giving you clear visibility into how data flows through each stage.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Conclusion
# MAGIC
# MAGIC In this lecture, you reviewed the course project and learned how dataset types support Apache Spark™ Declarative Pipelines:
# MAGIC
# MAGIC 1. Course project overview introduced a complete, modular pipeline architecture for ingesting, transforming, joining, and aggregating retail data.
# MAGIC 2. Streaming tables, materialized views, and views each serve distinct roles in data processing, from continuous ingestion to incremental aggregation and on-demand query execution.
# MAGIC 3. SQL-based creation using `CREATE OR REFRESH STREAMING TABLE`, `CREATE OR REFRESH MATERIALIZED VIEW`, and `CREATE VIEW` provides a consistent declarative syntax.
# MAGIC 4. Pipeline dependencies are automatically managed, helping ensure data flows correctly from one stage to the next without manually controlling execution order.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next lecture, you will explore simplified pipeline development.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/>
# MAGIC <a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> |
# MAGIC <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> |
# MAGIC <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
# MAGIC