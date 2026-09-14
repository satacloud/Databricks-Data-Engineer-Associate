-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lab - Creating Bronze Tables from JSON Files
-- MAGIC ### Duration: ~ 15 minutes
-- MAGIC
-- MAGIC In this lab you will ingest a JSON file as UC table and then flatten the JSON formatted string column.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC   - Inspect a raw JSON file.
-- MAGIC   - Read in JSON files to a UC table and flatten the JSON formatted string column.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
-- MAGIC
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #f44336;
-- MAGIC   background: #ffebee;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select Serverless Compute</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC Before starting this notebook, select the required compute environment listed below.
-- MAGIC
-- MAGIC - **Serverless Compute, Version 5**  
-- MAGIC ![Serverless Select](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/common/select-serverless.png)
-- MAGIC <br></br>
-- MAGIC   - How to select an environment version:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/dependencies#-select-an-environment-version) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/compute/serverless/dependencies#-select-an-environment-version)
-- MAGIC
-- MAGIC **NOTE:**  This notebook was **developed and tested using Serverless V5**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this notebook.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-13L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to view your default catalog and schema. Notice that your default catalog is **labuser123_678** and your default schema is your unique **data_ingestion** schema.
-- MAGIC
-- MAGIC **NOTE:** The default catalog and schema are pre-configured for you to avoid the need to specify the three-level name when writing your tables (i.e., catalog.schema.table).

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Lab - JSON Ingestion
-- MAGIC **Scenario:** You are working with your data team on ingesting a JSON file into Databricks. Your job is to ingest the JSON file as is into a bronze table, then create a second bronze table that flattens the JSON formatted string column in the raw bronze table for downstream processing.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Inspect the Dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. In the cell below, view the value of the variable `my_catalog`. This variable is created within the classroom setup script to dynamically reference your unique volume. You can concatenate the `my_catalog` SQL variable with a string to specify a specific subdirectory in your specific volume.
-- MAGIC
-- MAGIC    Run the cell to view the data in the `/Volumes/your-labuser-name/data_ingestion/landing_folder/json_demo_files/lab_kafka_events.json` file in the location from above.
-- MAGIC
-- MAGIC **Note:** Instead of using the `my_catalog` variable, you could also specify the path name directly by right clicking on your volume and selecting **Copy volume path**.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'''
-- MAGIC           SELECT * 
-- MAGIC           FROM json.`/Volumes/{my_catalog}/data_ingestion/landing_folder/json_demo_files/lab_kafka_events.json`
-- MAGIC           ''').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Create the Raw Bronze Table
-- MAGIC
-- MAGIC Inspect and run the code below to ingest the raw JSON file  `/Volumes/your-labuser-name/data_ingestion/landing_folder/json_demo_files/lab_kafka_events.json` and create the **lab13_lab_kafka_events_raw** table.
-- MAGIC
-- MAGIC Notice the following:
-- MAGIC - The **value** column is decoded.
-- MAGIC - The **decoded_value** column was created and returns the decoded column as a JSON-formatted string.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE lab13_lab_kafka_events_raw
AS
SELECT 
  *,
  cast(unbase64(value) as STRING) as decoded_value
FROM read_files(
        '/Volumes/' || my_catalog || '/data_ingestion/landing_folder/json_demo_files/lab_kafka_events.json',
        format => "json", 
        schema => '''
          key STRING, 
          timestamp DOUBLE, 
          value STRING
        ''',
        rescueddatacolumn => '_rescued_data'
      );

-- View the table
SELECT *
FROM lab13_lab_kafka_events_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. Create the Flattened Bronze Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Your goal is to flatten the JSON formatted string column **decoded_value** from the table **lab13_lab_kafka_events_raw** to create a new table named **lab13_lab_kafka_events_flattened** for downstream processing. The table should contain the following columns:
-- MAGIC     - **key**
-- MAGIC     - **timestamp**
-- MAGIC     - **user_id**
-- MAGIC     - **event_type**
-- MAGIC     - **event_timestamp**
-- MAGIC     - **items**
-- MAGIC
-- MAGIC     You can use whichever technique you prefer:
-- MAGIC
-- MAGIC     - Parse the JSON formatted string (easiest) to flatten
-- MAGIC       - [Query JSON strings](https://docs.databricks.com/aws/en/semi-structured/json):
-- MAGIC
-- MAGIC     - Convert the JSON formatted string as a VARIANT and flatten
-- MAGIC       - [parse_json function](https://docs.databricks.com/gcp/en/sql/language-manual/functions/parse_json)
-- MAGIC
-- MAGIC     - Convert the JSON formatted string to a STRUCT and flatten
-- MAGIC       - [schema_of_json function](https://docs.databricks.com/aws/en/sql/language-manual/functions/schema_of_json)
-- MAGIC       - [from_json function](https://docs.databricks.com/gcp/en/sql/language-manual/functions/from_json)
-- MAGIC
-- MAGIC **NOTE:** View the lab solution notebook to view the solutions for each.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. To begin, run the code below to view the final solution table **lab13_lab_kafka_events_flattened_solution**. This will give you an idea of what your final table should look like.
-- MAGIC
-- MAGIC   **NOTE**: Depending on your solution, the data types of the columns may vary slightly.  
-- MAGIC
-- MAGIC
-- MAGIC ##### Optional Challenge
-- MAGIC
-- MAGIC   As a challenge, after flattening the table, try converting the data types accordingly. Depending on your skill set, you may not convert all columns to the correct data types within the allotted time.
-- MAGIC
-- MAGIC   - **key** STRING
-- MAGIC   - **timestamp** DOUBLE
-- MAGIC   - **user_id** STRING
-- MAGIC   - **event_type** STRING
-- MAGIC   - **event_timestamp** TIMESTAMP
-- MAGIC   - **items** (STRUCT or VARIANT) depending on the method you used.

-- COMMAND ----------

SELECT *
FROM lab13_lab_kafka_events_flattened_solution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Write the query in the cell below to read the **lab_kafka_events_raw** table and create the flattened table **lab13_lab_kafka_events_flattened** following the requirements from above.

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC <summary><strong>Click to reveal answer</strong></summary>
-- MAGIC
-- MAGIC <!-- Hidden source for the Python answer -->
-- MAGIC <textarea id="raw-answer-python-customer-id-analysis" style="display:none;">
-- MAGIC ---- Parse the JSON formatted STRING
-- MAGIC CREATE OR REPLACE TABLE lab13_lab_kafka_events_flattened_str
-- MAGIC AS
-- MAGIC SELECT 
-- MAGIC   key,
-- MAGIC   timestamp,
-- MAGIC   decoded_value:user_id,
-- MAGIC   decoded_value:event_type,
-- MAGIC   cast(decoded_value:event_timestamp AS TIMESTAMP),
-- MAGIC   from_json(decoded_value:items,'ARRAY<STRUCT<item_id: STRING, price_usd: DOUBLE, quantity: BIGINT>>') AS items
-- MAGIC FROM lab13_lab_kafka_events_raw;
-- MAGIC
-- MAGIC
-- MAGIC ---- Display the table
-- MAGIC SELECT *
-- MAGIC FROM lab13_lab_kafka_events_flattened_str;
-- MAGIC
-- MAGIC </textarea>
-- MAGIC
-- MAGIC <div class="code-block-dark" data-language="python" data-source="raw-answer-python-customer-id-analysis"></div>
-- MAGIC </details>
-- MAGIC
-- MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-okaidia.min.css" rel="stylesheet" />
-- MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
-- MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
-- MAGIC
-- MAGIC <script>
-- MAGIC (function() {
-- MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
-- MAGIC         if (block.getAttribute('data-processed')) return;
-- MAGIC         block.setAttribute('data-processed', 'true');
-- MAGIC
-- MAGIC         var lang = block.getAttribute('data-language') || 'python';
-- MAGIC         var sourceId = block.getAttribute('data-source');
-- MAGIC
-- MAGIC         // Use textarea as the single source of truth so multiline f-strings render correctly [web:137][web:143].
-- MAGIC         var raw;
-- MAGIC         if (sourceId) {
-- MAGIC             var rawEl = document.getElementById(sourceId);
-- MAGIC             if (!rawEl) return;
-- MAGIC             raw = rawEl.value;
-- MAGIC         } else {
-- MAGIC             raw = block.textContent;
-- MAGIC         }
-- MAGIC
-- MAGIC         var code = raw.trim(); // preserve original trimming behavior [web:4]
-- MAGIC
-- MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
-- MAGIC
-- MAGIC         block.innerHTML =
-- MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
-- MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
-- MAGIC                 '<pre style="background:#272822;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
-- MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
-- MAGIC                 '</pre>' +
-- MAGIC             '</div>';
-- MAGIC
-- MAGIC         var codeEl = document.getElementById(id);
-- MAGIC         codeEl.textContent = code; // Prism highlights content inside <pre><code> with language-* class [web:114][web:75]
-- MAGIC         Prism.highlightElement(codeEl);
-- MAGIC
-- MAGIC         block.querySelector('.copy-btn').onclick = function() {
-- MAGIC             var t = document.createElement('textarea');
-- MAGIC             t.value = code;
-- MAGIC             document.body.appendChild(t);
-- MAGIC             t.select();
-- MAGIC             document.execCommand('copy');
-- MAGIC             document.body.removeChild(t);
-- MAGIC             this.textContent = '✓ Copied!';
-- MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
-- MAGIC         };
-- MAGIC     });
-- MAGIC })();
-- MAGIC </script>

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC <summary><strong>Click to reveal answer</strong></summary>
-- MAGIC
-- MAGIC <!-- Hidden source for the Python answer -->
-- MAGIC <textarea id="raw-answer-python-customer-id-analysis" style="display:none;">
-- MAGIC ---- Convert the JSON formatted string as a VARIANT
-- MAGIC ---- NOTE: The VARIANT decoded_value_variant column is included in this solution to display the column
-- MAGIC ---- NOTE: Variant data type will not work on Serverless Version 1.
-- MAGIC CREATE OR REPLACE TABLE lab13_lab_kafka_events_flattened_variant
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   key,
-- MAGIC   timestamp,
-- MAGIC   parse_json(decoded_value) AS decoded_value_variant,
-- MAGIC   cast(decoded_value_variant:user_id AS STRING),
-- MAGIC   decoded_value_variant:event_type :: STRING,
-- MAGIC   decoded_value_variant:event_timestamp :: TIMESTAMP,
-- MAGIC   decoded_value_variant:items
-- MAGIC FROM lab13_lab_kafka_events_raw;
-- MAGIC
-- MAGIC
-- MAGIC ---- Display the table
-- MAGIC SELECT *
-- MAGIC FROM lab13_lab_kafka_events_flattened_variant;
-- MAGIC </textarea>
-- MAGIC
-- MAGIC <div class="code-block-dark" data-language="python" data-source="raw-answer-python-customer-id-analysis"></div>
-- MAGIC </details>
-- MAGIC
-- MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-okaidia.min.css" rel="stylesheet" />
-- MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
-- MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
-- MAGIC
-- MAGIC <script>
-- MAGIC (function() {
-- MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
-- MAGIC         if (block.getAttribute('data-processed')) return;
-- MAGIC         block.setAttribute('data-processed', 'true');
-- MAGIC
-- MAGIC         var lang = block.getAttribute('data-language') || 'python';
-- MAGIC         var sourceId = block.getAttribute('data-source');
-- MAGIC
-- MAGIC         // Use textarea as the single source of truth so multiline f-strings render correctly [web:137][web:143].
-- MAGIC         var raw;
-- MAGIC         if (sourceId) {
-- MAGIC             var rawEl = document.getElementById(sourceId);
-- MAGIC             if (!rawEl) return;
-- MAGIC             raw = rawEl.value;
-- MAGIC         } else {
-- MAGIC             raw = block.textContent;
-- MAGIC         }
-- MAGIC
-- MAGIC         var code = raw.trim(); // preserve original trimming behavior [web:4]
-- MAGIC
-- MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
-- MAGIC
-- MAGIC         block.innerHTML =
-- MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
-- MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
-- MAGIC                 '<pre style="background:#272822;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
-- MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
-- MAGIC                 '</pre>' +
-- MAGIC             '</div>';
-- MAGIC
-- MAGIC         var codeEl = document.getElementById(id);
-- MAGIC         codeEl.textContent = code; // Prism highlights content inside <pre><code> with language-* class [web:114][web:75]
-- MAGIC         Prism.highlightElement(codeEl);
-- MAGIC
-- MAGIC         block.querySelector('.copy-btn').onclick = function() {
-- MAGIC             var t = document.createElement('textarea');
-- MAGIC             t.value = code;
-- MAGIC             document.body.appendChild(t);
-- MAGIC             t.select();
-- MAGIC             document.execCommand('copy');
-- MAGIC             document.body.removeChild(t);
-- MAGIC             this.textContent = '✓ Copied!';
-- MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
-- MAGIC         };
-- MAGIC     });
-- MAGIC })();
-- MAGIC </script>

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC <summary><strong>Click to reveal answer</strong></summary>
-- MAGIC
-- MAGIC <!-- Hidden source for the Python answer -->
-- MAGIC <textarea id="raw-answer-python-customer-id-analysis" style="display:none;">
-- MAGIC ---- Convert the JSON formatted string as a STRUCT
-- MAGIC ---- Return the structure of the JSON formatted string
-- MAGIC SELECT schema_of_json(decoded_value)
-- MAGIC FROM lab13_lab_kafka_events_raw
-- MAGIC LIMIT 1;
-- MAGIC
-- MAGIC
-- MAGIC ---- Use the JSON structure from above within the from_json function to convert the JSON formatted string to a STRUCT
-- MAGIC ---- NOTE: The STRUCT decoded_value_struct column is included in this solution to display the column
-- MAGIC CREATE OR REPLACE TABLE lab13_lab_kafka_events_flattened_struct
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   key,
-- MAGIC   timestamp,
-- MAGIC   from_json(decoded_value, 'STRUCT<event_timestamp: STRING, event_type: STRING, items: ARRAY<STRUCT<item_id: STRING, price_usd: DOUBLE, quantity: BIGINT>>, user_id: STRING>') AS decoded_value_struct,
-- MAGIC   decoded_value_struct.user_id,
-- MAGIC   decoded_value_struct.event_type,
-- MAGIC   cast(decoded_value_struct.event_timestamp AS TIMESTAMP),
-- MAGIC   decoded_value_struct.items
-- MAGIC FROM lab13_lab_kafka_events_raw;
-- MAGIC
-- MAGIC
-- MAGIC ---- Display the table
-- MAGIC SELECT *
-- MAGIC FROM lab13_lab_kafka_events_flattened_struct;
-- MAGIC </textarea>
-- MAGIC
-- MAGIC <div class="code-block-dark" data-language="python" data-source="raw-answer-python-customer-id-analysis"></div>
-- MAGIC </details>
-- MAGIC
-- MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-okaidia.min.css" rel="stylesheet" />
-- MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
-- MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
-- MAGIC
-- MAGIC <script>
-- MAGIC (function() {
-- MAGIC     document.querySelectorAll('.code-block-dark').forEach(function(block) {
-- MAGIC         if (block.getAttribute('data-processed')) return;
-- MAGIC         block.setAttribute('data-processed', 'true');
-- MAGIC
-- MAGIC         var lang = block.getAttribute('data-language') || 'python';
-- MAGIC         var sourceId = block.getAttribute('data-source');
-- MAGIC
-- MAGIC         // Use textarea as the single source of truth so multiline f-strings render correctly [web:137][web:143].
-- MAGIC         var raw;
-- MAGIC         if (sourceId) {
-- MAGIC             var rawEl = document.getElementById(sourceId);
-- MAGIC             if (!rawEl) return;
-- MAGIC             raw = rawEl.value;
-- MAGIC         } else {
-- MAGIC             raw = block.textContent;
-- MAGIC         }
-- MAGIC
-- MAGIC         var code = raw.trim(); // preserve original trimming behavior [web:4]
-- MAGIC
-- MAGIC         var id = 'code-dark-' + Math.random().toString(36).substr(2, 9);
-- MAGIC
-- MAGIC         block.innerHTML =
-- MAGIC             '<div style="position:relative;margin:16px 0;max-width:100%;">' +
-- MAGIC                 '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#555;color:#fff;border:1px solid #666;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
-- MAGIC                 '<pre style="background:#272822;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #444;max-width:100%;box-sizing:border-box;">' +
-- MAGIC                     '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:13px;word-wrap:break-word;white-space:pre-wrap;"></code>' +
-- MAGIC                 '</pre>' +
-- MAGIC             '</div>';
-- MAGIC
-- MAGIC         var codeEl = document.getElementById(id);
-- MAGIC         codeEl.textContent = code; // Prism highlights content inside <pre><code> with language-* class [web:114][web:75]
-- MAGIC         Prism.highlightElement(codeEl);
-- MAGIC
-- MAGIC         block.querySelector('.copy-btn').onclick = function() {
-- MAGIC             var t = document.createElement('textarea');
-- MAGIC             t.value = code;
-- MAGIC             document.body.appendChild(t);
-- MAGIC             t.select();
-- MAGIC             document.execCommand('copy');
-- MAGIC             document.body.removeChild(t);
-- MAGIC             this.textContent = '✓ Copied!';
-- MAGIC             setTimeout(() => this.textContent = 'Copy', 2000);
-- MAGIC         };
-- MAGIC     });
-- MAGIC })();
-- MAGIC </script>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>