# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Data Ingestion from Cloud Storage
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how raw files from cloud storage can be efficiently converted into UC tables using Databricks tools, unlocking advanced management and analytics capabilities within the Lakehouse.
# MAGIC
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Demonstrate how to ingest data from cloud object storage into UC tables** using CREATE TABLE AS, COPY INTO, and Auto Loader, including **capturing input file metadata in Bronze layer tables**
# MAGIC 2. **Explain how rescued columns are used during ingestion** to manage malformed records

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Data Ingestion Patterns From Cloud Object Storage
# MAGIC
# MAGIC Data ingestion is a critical component of modern Lakehouse architecture, enabling organizations to take advantage of large volumes of data stored in cloud object storage systems.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 800px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <div style="display: flex; gap: 16px; align-items: center;">
# MAGIC
# MAGIC   <!-- Cloud Storage -->
# MAGIC   <div style="flex: 0 0 170px; background: #F9F7F4; border-radius: 10px; padding: 18px; box-shadow: 0 2px 8px rgba(27,49,57,0.08); text-align: center;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; margin-top: 10px;">Cloud Storage</div>
# MAGIC     <ul style="font-size: 14pt; padding-left: 18px; margin: 10px 0 0; text-align: left;">
# MAGIC       <li>CSV</li>
# MAGIC       <li>JSON</li>
# MAGIC       <li>Parquet</li>
# MAGIC       <li>etc.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Arrow -->
# MAGIC   <div style="font-size: 28pt; color: #618794;"> > </div>
# MAGIC
# MAGIC   <!-- Data Ingestion Methods -->
# MAGIC   <div style="flex: 0 0 240px; background: #4299E0; border-radius: 10px; padding: 18px; text-align: center; color: white;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; margin-bottom: 14px;">Data Ingestion</div>
# MAGIC     <div style="display: flex; flex-direction: column; gap: 8px;">
# MAGIC       <div style="background: white; color: #1B5162; border-radius: 6px; padding: 8px; font-size: 14pt; font-weight: 500;">CREATE TABLE AS</div>
# MAGIC       <div style="background: white; color: #1B5162; border-radius: 6px; padding: 8px; font-size: 14pt; font-weight: 500;">COPY INTO</div>
# MAGIC       <div style="background: white; color: #1B5162; border-radius: 6px; padding: 8px; font-size: 14pt; font-weight: 500;">AUTO LOADER</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Arrow -->
# MAGIC   <div style="font-size: 28pt; color: #618794;"> > </div>
# MAGIC
# MAGIC   <!-- UC table -->
# MAGIC   <div style="flex: 0 0 170px; background: #F9F7F4; border-radius: 10px; padding: 18px; box-shadow: 0 2px 8px rgba(27,49,57,0.08); text-align: center;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; margin-top: 10px;">UC table</div>
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/table.png" style="height: 150px;">
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- Bottom callout -->
# MAGIC <div style="
# MAGIC   margin: 18px auto 0;
# MAGIC   padding: 12px 20px;
# MAGIC   background: #F9F7F4;
# MAGIC   border: 2px solid #4299E0;
# MAGIC   border-radius: 8px;
# MAGIC   text-align: center;
# MAGIC   font-size: 14pt;
# MAGIC   max-width: 900px;
# MAGIC ">
# MAGIC   Convert <strong>raw file formats</strong> to <strong>UC tables</strong>
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <ul>
# MAGIC <li>Common file formats like <strong>CSV, JSON, and Parquet</strong> are frequently used due to their flexibility and ease of use.</li>
# MAGIC <li>Our goal is to convert these <strong>raw files into UC tables</strong>, unlocking advanced functionality such as ACID transactions, time travel, and schema enforcement.</li>
# MAGIC <li>We'll explore three <strong>primary methods for ingesting files</strong> from cloud object storage into UC tables:
# MAGIC <ul>
# MAGIC <li>CREATE TABLE AS (CTAS)</li>
# MAGIC <li>COPY INTO</li>
# MAGIC <li>Auto Loader</li>
# MAGIC </ul>
# MAGIC </li>
# MAGIC <li>Ingestion from cloud object storage is performed using <strong>Lakeflow Connect Standard Connectors</strong>.</li>
# MAGIC </ul>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Data Ingestion Methods
# MAGIC
# MAGIC When ingesting data into Databricks using Lakeflow Connect Standard Connectors, you can choose from several ingestion methods.
# MAGIC
# MAGIC ##### Click on the tabs to switch between the ingestion methods.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="width: 100%; margin: auto; font-family: sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC .four-grid {
# MAGIC     display: flex;
# MAGIC     flex-direction: column;
# MAGIC     gap: 60px;
# MAGIC     justify-content: center;
# MAGIC     align-items: center;
# MAGIC }
# MAGIC .ing-box {
# MAGIC     width: 80%;
# MAGIC     min-height: 400px;
# MAGIC     background: #F9F7F4;
# MAGIC     border: none;
# MAGIC     border-radius: 8px;
# MAGIC     box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC     overflow: hidden;
# MAGIC     display: flex;
# MAGIC     flex-direction: column;
# MAGIC     gap: 12px;
# MAGIC     padding: 20px;
# MAGIC     text-align: center;
# MAGIC     position: relative;
# MAGIC     box-sizing: border-box;
# MAGIC }
# MAGIC .ing-box::before {
# MAGIC     content: "";
# MAGIC     position: absolute;
# MAGIC     top: 0; left: 0;
# MAGIC     width: 100%; height: 8px;
# MAGIC }
# MAGIC .ing-box.batch::before       { background: #2574B5; }
# MAGIC .ing-box.incremental::before { background: #02A36F; }
# MAGIC .ing-box.streaming::before   { background: #FE3722; }
# MAGIC
# MAGIC .ing-box-title { font-size: 16pt; font-weight: bold; text-align: center; }
# MAGIC .ing-box-icon  { display: inline-flex; align-items: center; justify-content: center; gap: 8px; }
# MAGIC .ing-box-icon img {
# MAGIC     width: 50px; height: auto;
# MAGIC     background: transparent;
# MAGIC     mix-blend-mode: multiply;
# MAGIC     filter: contrast(1.15) brightness(1);
# MAGIC     border-radius: 4px;
# MAGIC }
# MAGIC
# MAGIC .ing-box-content {
# MAGIC     display: flex;
# MAGIC     align-items: flex-start;
# MAGIC     justify-content: center;
# MAGIC     gap: 24px;
# MAGIC     width: 100%;
# MAGIC }
# MAGIC .ing-box-text {
# MAGIC     font-size: 14pt;
# MAGIC     max-width: 500px;
# MAGIC     text-align: left;
# MAGIC     line-height: 1.6;
# MAGIC }
# MAGIC .ing-box-text ul { text-align: left; padding-left: 18px; margin: 0 0 14px 0; }
# MAGIC .ing-box-text li { margin-bottom: 10px; }
# MAGIC .ing-box-text li:last-child { margin-bottom: 0; }
# MAGIC .ing-example {
# MAGIC     padding: 12px 14px;
# MAGIC     border-radius: 8px;
# MAGIC     font-size: 14pt;
# MAGIC     line-height: 1.6;
# MAGIC     font-weight: 400;
# MAGIC }
# MAGIC .batch .ing-example       { background: rgba(37,116,181,0.10); border-left: 4px solid #2574B5; }
# MAGIC .incremental .ing-example { background: rgba(2,163,111,0.10);  border-left: 4px solid #02A36F; }
# MAGIC .streaming .ing-example   { background: rgba(254,55,34,0.10);  border-left: 4px solid #FE3722; }
# MAGIC
# MAGIC .code-block-wrapper {
# MAGIC     flex: 1;
# MAGIC     min-width: 100;
# MAGIC     min-height: 200px;
# MAGIC }
# MAGIC
# MAGIC .code-block-container {
# MAGIC     background: #f8f8f8;
# MAGIC     border-radius: 8px;
# MAGIC     padding: 16px;
# MAGIC     overflow-x: auto;
# MAGIC     border: 1px solid #e0e0e0;
# MAGIC     font-family: Consolas, Monaco, monospace;
# MAGIC     font-size: 10pt;
# MAGIC     line-height: 1.6;
# MAGIC     text-align: left;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <!-- Tabs -->
# MAGIC <div style="display: flex; border-bottom: 2px solid #EEEDE9; margin-bottom: 0;">
# MAGIC   <button class="dbtab" onclick="showTab(1)" style="padding: 10px 18px; border: none; border-bottom: 3px solid #2574B5; background: none; font-size: 14pt; font-weight: bold; color: #2574B5; cursor: pointer; margin-bottom: -2px;">CREATE TABLE AS</button>
# MAGIC   <button class="dbtab" onclick="showTab(2)" style="padding: 10px 18px; border: none; border-bottom: 3px solid transparent; background: none; font-size: 14pt; font-weight: bold; color: #888; cursor: pointer; margin-bottom: -2px;">COPY INTO</button>
# MAGIC   <button class="dbtab" onclick="showTab(3)" style="padding: 10px 18px; border: none; border-bottom: 3px solid transparent; background: none; font-size: 14pt; font-weight: bold; color: #888; cursor: pointer; margin-bottom: -2px;">AUTO LOADER</button>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <!-- TAB 1 -->
# MAGIC <div class="dbpanel" style="display: block;">
# MAGIC   <div class="four-grid">
# MAGIC     <div class="ing-box batch">
# MAGIC       <div class="ing-box-title">
# MAGIC         <div class="ing-box-icon">
# MAGIC           <img src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/batch.png" alt="Create Table As">
# MAGIC           <span>Method 1 - Batch - <br><code>CREATE TABLE AS (CTAS)</code></span>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="ing-box-content">
# MAGIC         <div class="code-block-wrapper">
# MAGIC         <div class="code-block" data-language="sql">
# MAGIC           CREATE TABLE new_table AS
# MAGIC           SELECT *
# MAGIC           FROM read_files(
# MAGIC             &lt;<i>path_to_file(s)</i>&gt;,
# MAGIC             format => '&lt;<i>file_type</i>&gt;',
# MAGIC             &lt;<i>other_format_specific_options</i>&gt;
# MAGIC           );
# MAGIC         </div>
# MAGIC         </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var code = block.textContent.trim();
# MAGIC         var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre>' +
# MAGIC             '</div>';
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>
# MAGIC <br>
# MAGIC <div class="ing-box-text">
# MAGIC   <div class="ing-example">
# MAGIC <code><strong>CREATE TABLE AS (CTAS)</strong></code> creates a UC table <strong>by default</strong> from files in cloud object storage.
# MAGIC </div>
# MAGIC <br>
# MAGIC   <div class="ing-example">
# MAGIC The <code><strong>read_files()</strong></code> function reads files under a provided location and returns the data in <strong>tabular form.</strong>
# MAGIC   </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC <!-- TAB 2 -->
# MAGIC <div class="dbpanel" style="display: none;">
# MAGIC   <div class="four-grid">
# MAGIC     <div class="ing-box incremental">
# MAGIC       <div class="ing-box-title">
# MAGIC         <div class="ing-box-icon">
# MAGIC           <img src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/incremental.png" alt="COPY INTO">
# MAGIC           <span>Method 2 - Incremental Batch - <br><code>COPY INTO</code></span>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="ing-box-content">
# MAGIC         <div class="code-block-wrapper">
# MAGIC         <div class="code-block" data-language="sql">
# MAGIC         CREATE TABLE new_table;
# MAGIC         <br>
# MAGIC         COPY INTO new_table
# MAGIC         FROM '&lt;<i>dir_path</i>&gt;'
# MAGIC         FILEFORMAT = &lt;<i>file_type</i>&gt;
# MAGIC         FORMAT_OPTIONS (&lt;<i>options</i>&gt;)
# MAGIC         COPY_OPTIONS (&lt;<i>options</i>&gt;)
# MAGIC         </div>
# MAGIC         </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC     document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC         if (block.getAttribute('data-processed')) return;
# MAGIC         block.setAttribute('data-processed', 'true');
# MAGIC         var lang = block.getAttribute('data-language') || 'sql';
# MAGIC         var code = block.textContent.trim();
# MAGIC         var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC         block.innerHTML =
# MAGIC             '<div style="position:relative;margin:16px 0;">' +
# MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC                 '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre>' +
# MAGIC             '</div>';
# MAGIC         var codeEl = document.getElementById(id);
# MAGIC         codeEl.textContent = code;
# MAGIC         Prism.highlightElement(codeEl);
# MAGIC         block.querySelector('.copy-btn').onclick = function() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = code;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             this.textContent = '✓ Copied!';
# MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC         };
# MAGIC     });
# MAGIC })();
# MAGIC </script>
# MAGIC <br>
# MAGIC <div class="ing-box-text">
# MAGIC <div class="ing-example">
# MAGIC Use the <code><strong>COPY INTO</strong></code> statement to copy files from cloud storage into the UC table; this performs a bulk load from files in cloud object storage into the table.
# MAGIC </div>
# MAGIC <br>
# MAGIC <div class="ing-example">
# MAGIC The <code><strong>COPY INTO</strong></code> will skip any files that have already been loaded into the table, and only new files will be ingested.
# MAGIC </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- TAB 3 -->
# MAGIC <div class="dbpanel" style="display: none;">
# MAGIC   <div class="four-grid">
# MAGIC     <div class="ing-box streaming">
# MAGIC       <div class="ing-box-title">
# MAGIC         <div class="ing-box-icon">
# MAGIC           <img src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/streaming.png" alt="AUTO LOADER">
# MAGIC           <span>Method 3 - Incremental Batch or Streaming - <br><code>AUTO LOADER</code></span>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="ing-box-content">
# MAGIC         <div style="max-width: 700px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC           <div style="display: flex; gap: 20px; align-items: flex-start;">
# MAGIC             <!-- Python -->
# MAGIC             <div style="flex: 1;">
# MAGIC               <div style="font-size: 12pt; font-weight: 400; margin-bottom: 10px;"><strong>Python Auto Loader</strong></div>
# MAGIC               <div class="code-block-container">
# MAGIC                 <div class="code-block-wrapper">
# MAGIC                   (spark<br>
# MAGIC                   &nbsp;&nbsp;.readStream<br>
# MAGIC                   &nbsp;&nbsp;&nbsp;&nbsp;.<span style="color: #4299E0;">format</span>(<span style="color: #00A972;">"cloudFiles"</span>)<br>
# MAGIC                   &nbsp;&nbsp;&nbsp;&nbsp;.<span style="color: #4299E0;">option</span>(<span style="color: #00A972;">"cloudFiles.format"</span>, <span style="color: #00A972;">"json"</span>)<br>
# MAGIC                   &nbsp;&nbsp;&nbsp;&nbsp;.<span style="color: #4299E0;">option</span>(<span style="color: #00A972;">"cloudFiles.schemaLocation"</span>, <span style="color: #00A972;">"&lt;<code>checkpoint_path</code>&gt;"</span>)<br>
# MAGIC                   &nbsp;&nbsp;&nbsp;&nbsp;.<span style="color: #4299E0;">load</span>(<span style="color: #00A972;">"/Volumes/catalog/schema/files"</span>)<br>
# MAGIC                   &nbsp;&nbsp;.writeStream<br>
# MAGIC                   &nbsp;&nbsp;&nbsp;&nbsp;.<span style="color: #4299E0;">option</span>(<span style="color: #00A972;">"checkpointLocation"</span>, <span style="color: #00A972;">"&lt;<code>checkpoint_path</code>&gt;"</span>)<br>
# MAGIC                   &nbsp;&nbsp;&nbsp;&nbsp;.<span style="color: #4299E0;">trigger</span>(processingTime=<span style="color: #00A972;">"5 seconds"</span>)<br>
# MAGIC                   &nbsp;&nbsp;&nbsp;&nbsp;.<span style="color: #4299E0;">toTable</span>(<span style="color: #00A972;">"catalog.database.table"</span>)<br>
# MAGIC                   )
# MAGIC                 </div>
# MAGIC               </div>
# MAGIC             </div>
# MAGIC             <!-- SQL -->
# MAGIC             <div style="flex: 1;">
# MAGIC               <div style="font-size: 12pt; font-weight: 400; margin-bottom: 10px;"><strong>Auto Loader with SQL (Declarative Pipelines)</strong></div>
# MAGIC               <div class="code-block-container">
# MAGIC                 <span style="color: #4299E0; font-weight: 400;">CREATE OR REFRESH STREAMING TABLE</span><br>
# MAGIC                 &nbsp;&nbsp;catalog.schema.table<br>
# MAGIC                 <span style="color: #4299E0; font-weight: 400;">SCHEDULE EVERY</span> 1 HOUR<br>
# MAGIC                 <span style="color: #4299E0; font-weight: 400;">AS</span><br>
# MAGIC                 <span style="color: #4299E0; font-weight: 400;">SELECT</span> *<br>
# MAGIC                 <span style="color: #4299E0; font-weight: 400;">FROM STREAM</span> read_files(<br>
# MAGIC                 &nbsp;&nbsp;'&lt;<code>dir_path</code>&gt;',<br>
# MAGIC                 &nbsp;&nbsp;format => '&lt;<code>file_type</code>&gt;'<br>
# MAGIC                 )
# MAGIC               </div>
# MAGIC             </div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function showTab(n) {
# MAGIC   var tabs   = document.getElementsByClassName("dbtab");
# MAGIC   var panels = document.getElementsByClassName("dbpanel");
# MAGIC   var colors = ["#2574B5", "#02A36F", "#FE3722"];
# MAGIC   for (var i = 0; i < tabs.length; i++) {
# MAGIC     tabs[i].style.color        = "#888";
# MAGIC     tabs[i].style.borderBottom = "3px solid transparent";
# MAGIC     panels[i].style.display    = "none";
# MAGIC   }
# MAGIC   tabs[n-1].style.color        = colors[n-1];
# MAGIC   tabs[n-1].style.borderBottom = "3px solid " + colors[n-1];
# MAGIC   panels[n-1].style.display    = "block";
# MAGIC }
# MAGIC window.onload = function() { showTab(1); };
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Documentation
# MAGIC
# MAGIC For more information on Auto Loader, see the official [Databricks documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader) and [tutorials](https://www.databricks.com/resources/demos/tutorials?itm_data=demo_center).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <ul>
# MAGIC <li><strong>CREATE TABLE AS (CTAS)</strong>
# MAGIC   <ol>
# MAGIC     <li>Supports reading <strong>file formats</strong> like:<br>
# MAGIC         | JSON | CSV | XML | TEXT | BINARYFILE | PARQUET | AVRO | ORC</li>
# MAGIC     <li>Can detect the file format automatically and <strong>infer a unified schema</strong> across all files.</li>
# MAGIC     <li>Specify <strong>specific file format options</strong> to read in the data based on the source file format.</li>
# MAGIC     <li>Can be used in <strong>streaming tables</strong> to <strong>incrementally</strong> ingest files into Delta Lake using Auto Loader.</li>
# MAGIC   </ol>
# MAGIC </li><br>
# MAGIC <li><strong>COPY INTO</strong><br>
# MAGIC Use the <code><strong>COPY INTO</strong></code> statement to copy files from cloud storage into the UC table. This command performs a bulk load from files in cloud object storage into the table, and in this example, it will load files into the empty table new_table. The <code><strong>FROM</strong></code> clause specifies the location of the CSV files.</li><br>
# MAGIC <code><strong>COPY INTO</strong></code> is ideal for situations where the cloud storage location is continuously adding files, since it is a retriable and idempotent operation designed for incremental batch ingestion.
# MAGIC
# MAGIC   <strong>Key aspects of <code>COPY INTO</code></strong>:
# MAGIC   <ul>
# MAGIC     <li><strong>Idempotent</strong>: Will skip any files that have already been loaded into the table; only new files will be ingested</li>
# MAGIC     <li><strong>File format support</strong>: Parquet, JSON, XML, and others</li>
# MAGIC     <li><strong><code>FROM clause</code></strong>: Specifies the path of the cloud storage location where new files are being continuously added</li>
# MAGIC     <li><strong><code>FORMAT_OPTIONS()</code></strong>: Controls how the source files are parsed and interpreted (options depend on file format)</li>
# MAGIC     <li><strong><code>COPY_OPTIONS()</code></strong>: Controls the behavior of the <code>COPY INTO</code> operation itself, such as:
# MAGIC       <ul>
# MAGIC         <li>Schema evolution using (<strong>mergeSchema</strong>)</li>
# MAGIC         <li>Idempotency using (<strong>force</strong>)</li>
# MAGIC       </ul>
# MAGIC     </li>
# MAGIC   </ul>
# MAGIC </li><br>
# MAGIC <li><strong>AUTO LOADER</strong>
# MAGIC   <ol>
# MAGIC     <li>Incremental batch or streaming ingestion using Auto Loader.
# MAGIC       <ul>
# MAGIC         <li>Process new data files <strong>incrementally</strong> as they arrive in cloud storage (batch or streaming)</li>
# MAGIC         <li>Ingest data <strong>without extra setup</strong> or complex configuration</li>
# MAGIC         <li><strong>Automatically</strong> detect and load new files into UC tables</li>
# MAGIC         <li>Simplify handling of incremental and streaming data</li>
# MAGIC         <li>Use with both <strong>Python</strong> and <strong>SQL</strong> (via Declarative Pipelines)</li>
# MAGIC         <li>Scale to <strong>process billions of files</strong></li>
# MAGIC         <li>Rely on <strong>Spark Structured Streaming</strong> for efficient and reliable ingestion</li>
# MAGIC       </ul>
# MAGIC     </li><br>
# MAGIC     <li>Auto Loader in Python to read streaming data from cloud storage:
# MAGIC       <ul>
# MAGIC         <li>We start with <strong><code>.readStream</code></strong> and set the <strong>format</strong> to "<code><strong>cloudFiles</strong></code>", which enables Auto Loader.</li>
# MAGIC         <li>Then, we specify the <strong>file format</strong> as <strong>JSON</strong>, and define the <strong>schema location</strong> using <code><strong>cloudFiles.schemaLocation</strong></code>, which is used to track schema inference and evolution.</li>
# MAGIC         <li>Next, we use <strong><code>.load()</code></strong> to point to the location of the files, in this case a path under /Volumes referencing Unity Catalog.</li>
# MAGIC         <li>On the write side, we configure <strong><code>.writeStream</code></strong> with a <strong>checkpoint location</strong> to maintain state and progress, and set a <strong>trigger</strong> interval of <strong>every 5 seconds</strong>.</li>
# MAGIC         <li>Finally, we use <strong><code>.toTable()</code></strong> to write the data into a UC table specified by catalog, database, and table name.</li>
# MAGIC       </ul>
# MAGIC     </li><br>
# MAGIC     <li>Auto Loader with Databricks SQL
# MAGIC       <ul>
# MAGIC         <li>Databricks recommends using streaming tables to ingest data with Databricks SQL (instead of COPY INTO). A streaming table is a table registered to <strong>Unity Catalog</strong> that includes additional support for streaming or incremental data processing.
# MAGIC           <ul>
# MAGIC             <li>When you create a streaming table, a <strong>pipeline</strong> is automatically generated for it.</li>
# MAGIC             <li>Streaming tables can be used for incremental data loading from both <strong>Kafka</strong> and <strong>cloud object storage</strong>.</li>
# MAGIC           </ul>
# MAGIC         </li>
# MAGIC         <li>To create a streaming table from files in a volume, you use Auto Loader. Databricks recommends using <strong>Auto Loader with Apache Spark™ Declarative Pipelines</strong> for most data ingestion tasks from cloud object storage. Together, Auto Loader and Declarative Pipelines are designed to incrementally and idempotently load continuously growing datasets as they arrive.</li>
# MAGIC         <li>Streaming tables in Databricks SQL are backed by <strong>serverless</strong> Spark Declarative Pipelines. Your workspace must support serverless pipelines to use this functionality. Alternatively, you can <strong>build your own</strong> Spark Declarative Pipelines for incremental processing, optimization, and monitoring. Declarative Pipelines offer a range of additional features, which you can learn more about <a href="https://docs.databricks.com/aws/en/dlt/" style="color:#1976D2;">here</a>.</li>
# MAGIC         <li>To use Auto Loader in Databricks SQL, use the <strong><code>read_files</code></strong> function with the <strong><code>STREAM</code></strong> keyword in the <strong><code>FROM</code></strong> clause.</li>
# MAGIC       </ul>
# MAGIC     </li>
# MAGIC   </ol>
# MAGIC </li>
# MAGIC </ul>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Ingestion Methods at a Glance
# MAGIC
# MAGIC Here is a quick summary of all three data ingestion methods.
# MAGIC
# MAGIC <div style="max-width: 1100px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <style>
# MAGIC table td, table th {
# MAGIC   font-size: 12pt !important;
# MAGIC }
# MAGIC table ul {
# MAGIC   font-size: 12pt !important;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <table style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9; width: 140px;">FEATURE</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">CREATE TABLE AS (CTAS) + spark.read</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">COPY INTO</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Auto Loader</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-weight: 700;">Ingestion Type</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Batch</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Incremental Batch</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Incremental (Batch or Streaming)</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-weight: 700;">Use Cases</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Best for smaller datasets</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Ideal for thousands of files</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Scale to millions+ of files per hour, backfills with billions of files</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-weight: 700;">Syntax/Interface</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">
# MAGIC         <ul style="margin: 0; padding-left: 16px;">
# MAGIC           <li>Python (spark.read)</li>
# MAGIC           <li>SQL (CTAS)</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">SQL</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">
# MAGIC         <ul style="margin: 0; padding-left: 16px;">
# MAGIC           <li>Python (spark.readStream)</li>
# MAGIC           <li>SQL with Declarative Pipelines (CREATE OR REFRESH STREAMING TABLES)</li>
# MAGIC           <li>Streaming tables in Databricks SQL</li>
# MAGIC         </ul>
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-weight: 700;">Idempotency</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">No</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Yes</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Yes</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-weight: 700;">Schema Evolution</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Manual or inferred during read</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Supported with options</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Auto Loader automatically detects and evolves schemas. Handles new columns as they appear.</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-weight: 700;">Latency</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">High</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Moderate (scheduled)</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Low or high depending on configuration</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-weight: 700;">Ease of Use</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Simple</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Simple and SQL-based</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Intermediate to advanced depending on the implementation</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-weight: 700;">Summary</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Best for one-time, ad hoc ingestion. Can be scheduled to always read and process all data.</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Simple and repeatable for incremental file ingestion. Great for scheduled jobs or pipelines.</td>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">Best for near real-time streaming or incremental ingestion, with high automation and scalability.</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Conclusion
# MAGIC
# MAGIC In this lecture, you learned the three primary methods for ingesting data from cloud object storage into UC tables:
# MAGIC
# MAGIC - **`CREATE TABLE AS (CTAS)`**: Batch ingestion using `read_files()` that creates UC tables from raw files. Best for smaller, ad hoc datasets.
# MAGIC - **`COPY INTO`**: Incremental batch ingestion that is idempotent and retriable. Skips already-loaded files and supports format and copy options for fine-grained control.
# MAGIC - **`AUTO LOADER`**: The most scalable method, built on Spark Structured Streaming. Supports both Python and SQL (via Declarative Pipelines), processes billions of files, and automatically handles schema evolution.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next section, you will work hands-on with these ingestion methods to load data from cloud storage into UC tables.

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