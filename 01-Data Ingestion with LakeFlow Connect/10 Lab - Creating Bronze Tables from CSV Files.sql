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
-- MAGIC # Lab - Creating Bronze Tables from CSV Files
-- MAGIC ### Duration: ~15-20 minutes
-- MAGIC
-- MAGIC This lab is divided into two sections: the **course lab** and an **optional challenge**. 
-- MAGIC
-- MAGIC In a live class, if you finish the main lab early, feel free to attempt the challenge. You can also complete the challenge after class. 
-- MAGIC
-- MAGIC **NOTE:** The challenge will require you to use resources outside the scope of this course.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC   - Inspect CSV files.
-- MAGIC   - Read in CSV files as a UC table and append metadata columns.
-- MAGIC   - Record malformed data types using the `_rescued_data` column.  
-- MAGIC
-- MAGIC ### Challenge Learning Objectives:
-- MAGIC   - Clean malformed data types during ingestion by leveraging the `_rescued_data` column.

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
-- MAGIC # Course Lab - Creating Bronze Tables from CSV Files
-- MAGIC
-- MAGIC ### Scenario
-- MAGIC
-- MAGIC You are working with your data team on ingesting a CSV file into Databricks. However, you notice there is a malformed row in your CSV file. Your job is to ingest the file and rescue the malformed data and write as a UC table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this notebook.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-10L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to view your default catalog and schema. Notice that your default catalog is **labuser123_678** and your default schema is your unique **data_ingestion** schema.
-- MAGIC
-- MAGIC **NOTE:** The default catalog and schema are pre-configured for you to avoid the need to specify the three-level name when writing your tables (i.e., catalog.schema.table).

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Lab - CSV File Ingestion
-- MAGIC Ingest the CSV file as a UC table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Inspect the Dataset
-- MAGIC
-- MAGIC 1. In the cell below, view the value of the variable `my_catalog`. This variable is created within the classroom setup script to dynamically reference your unique volume. You can concatenate the `my_catalog` variable with a string to specify a specific subdirectory in your specific volume.
-- MAGIC
-- MAGIC    Run the cell below and review the results. You’ll notice that it returns the path to your **malformed_example_1_data.csv** file. This method will be used when referencing your volume within the `read_files` function.
-- MAGIC
-- MAGIC **Note:** Instead of using the `my_catalog` variable, you could also specify the path name directly by right clicking on your volume and selecting **Copy volume path**.

-- COMMAND ----------

values('/Volumes/' || my_catalog || '/data_ingestion/landing_folder/csv_demo_files/lab_malformed_data.csv')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Next, let's take a look at our CSV file. 
-- MAGIC
-- MAGIC     Copy the path from the output below and paste it within the backticks in the query to reference the **lab_malformed_data.csv** file.
-- MAGIC
-- MAGIC     The query should use `text.<path_from_above>` to return the headers and rows from the CSV file. 
-- MAGIC
-- MAGIC     Run the cell and view row 4. Notice that the value for the price contains a `$`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'''
-- MAGIC SELECT *
-- MAGIC FROM text.`<FILL-IN>`
-- MAGIC '''
-- MAGIC ).display()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC <summary><strong>Click to reveal answer</strong></summary>
-- MAGIC
-- MAGIC <!-- Hidden source for the Python answer -->
-- MAGIC <textarea id="raw-answer-python-customer-id-analysis" style="display:none;">
-- MAGIC %python
-- MAGIC spark.sql(f'''
-- MAGIC SELECT *
-- MAGIC FROM text.`/Volumes/{my_catalog}/data_ingestion/landing_folder/csv_demo_files/lab_malformed_data.csv`
-- MAGIC '''
-- MAGIC ).display()
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
-- MAGIC ### B2. Ingesting and Rescuing Malformed Data
-- MAGIC
-- MAGIC Begin developing your query to ingest the CSV file in the specified path and view malformed records using the **_rescued_data** column.
-- MAGIC
-- MAGIC #### Requirements
-- MAGIC Your final SQL query should ingest the CSV file using CTAS and `read_files`. **In the cell below, do not create a table yet. Simply start developing your query to ingest and create the table**:
-- MAGIC
-- MAGIC 1. Select all columns from the raw CSV file.
-- MAGIC
-- MAGIC 2. Use the `read_files()` function with appropriate options to read the CSV file. 
-- MAGIC    - **HINT:** Note that the delimiter is a comma (`,`) not a pipe (`|`).
-- MAGIC
-- MAGIC 3. Explicitly define the schema for ingestion. The schema is defined as follows:  
-- MAGIC    - `item_id` (STRING)  
-- MAGIC    - `name` (STRING)  
-- MAGIC    - `price` (DOUBLE)
-- MAGIC
-- MAGIC 4. Use the correct option to include the rescued data column and name it **_rescued_data** to capture malformed rows.
-- MAGIC
-- MAGIC    - **HINT**: If you define a schema you must [use the rescuedDataColumn option](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files#csv-options) to add the **_rescued_data** column.
-- MAGIC
-- MAGIC **SOLUTION OUTPUT**
-- MAGIC
-- MAGIC Your output result should look like the following:
-- MAGIC | item_id (STRING)   | name (STRING)                   | price (DOUBLE)| _rescued_data (STRING)                                                                                         |
-- MAGIC |-----------|-------------------------|-------|-----------------------------------------------------------------------------------------------------------|
-- MAGIC | M_PREM_Q  | Premium Queen Mattress   | 1795  | null                                                                                                      |
-- MAGIC | M_STAN_F  | Standard Full Mattress   | 945   | null                                                                                                      |
-- MAGIC | M_PREM_A  | Premium Queen Mattress   | null  | {"price":"$100.00","_file_path":"dbfs:/Volumes/dbacademy/ops/peter_s@databricks_com/csv_demo_files/lab_malformed_data.csv"} |
-- MAGIC

-- COMMAND ----------

<FILL-IN>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC <summary><strong>Click to reveal answer</strong></summary>
-- MAGIC
-- MAGIC <!-- Hidden source for the Python answer -->
-- MAGIC <textarea id="raw-answer-python-customer-id-analysis" style="display:none;">
-- MAGIC SELECT * 
-- MAGIC FROM read_files(
-- MAGIC         '/Volumes/'|| my_catalog || '/data_ingestion/landing_folder/csv_demo_files/lab_malformed_data.csv',
-- MAGIC         format => "csv",
-- MAGIC         sep => ",",
-- MAGIC         header => true,
-- MAGIC         schema => '''
-- MAGIC               item_id STRING, 
-- MAGIC               name STRING, 
-- MAGIC               price DOUBLE
-- MAGIC         ''',
-- MAGIC         rescueddatacolumn => "_rescued_data"
-- MAGIC       )
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
-- MAGIC ### B3. Add Additional Metadata Columns During Ingestion
-- MAGIC
-- MAGIC Next, you can create the final bronze table named **10_lab_bronze** that contains the additional metadata columns. Use the query you created above as the starting point.
-- MAGIC
-- MAGIC ### Final Table Requirements
-- MAGIC
-- MAGIC Incorporate the SQL query you created in the previous section and complete the following:
-- MAGIC
-- MAGIC 1. Use a CTAS statement to create the final bronze UC table named **10_lab_bronze**. 
-- MAGIC
-- MAGIC 1. Ingest the same CSV file `/Volumes/dbacademy/ops/<username>/csv_demo_files/lab_malformed_data.csv`
-- MAGIC
-- MAGIC 1. Use the same defined schema:  
-- MAGIC    - `item_id` (STRING)  
-- MAGIC    - `name` (STRING)  
-- MAGIC    - `price` (DOUBLE)
-- MAGIC
-- MAGIC 1. Use the `_metadata` column to create two new columns named **file_modification_time** and **source_file**  within your SELECT statement.
-- MAGIC    - **HINT:** [_metadata](https://docs.databricks.com/en/ingestion/file-metadata-column.html)
-- MAGIC
-- MAGIC 1. Add a column named **ingestion_time** that provides a timestamp for ingestion. 
-- MAGIC    - **HINT:** Use the [current_timestamp()](https://docs.databricks.com/aws/en/sql/language-manual/functions/current_timestamp) to record the current timestamp at the start of the query evaluation. 
-- MAGIC
-- MAGIC
-- MAGIC **NOTE:** If you need to see how your final table should look, run the next cell to view the solution table **10_lab_solution**.

-- COMMAND ----------

-- Run if you want to see the final table
SELECT * 
FROM 10_lab_solution

-- COMMAND ----------

---- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS 10_lab_bronze;

---- Complete your CTAS statement below:
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC <summary><strong>Click to reveal answer</strong></summary>
-- MAGIC
-- MAGIC <!-- Hidden source for the Python answer -->
-- MAGIC <textarea id="raw-answer-python-customer-id-analysis" style="display:none;">
-- MAGIC ---- Drop the table if it exists for demonstration purposes
-- MAGIC DROP TABLE IF EXISTS 10_lab_bronze;
-- MAGIC
-- MAGIC ---- Create the UC table
-- MAGIC CREATE TABLE 10_lab_bronze 
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   *,
-- MAGIC   _metadata.file_modification_time AS file_modification_time,
-- MAGIC   _metadata.file_name AS source_file, 
-- MAGIC   current_timestamp() as ingestion_time
-- MAGIC FROM read_files(
-- MAGIC         '/Volumes/'|| my_catalog || '/data_ingestion/landing_folder/csv_demo_files/lab_malformed_data.csv',
-- MAGIC         format => "csv",
-- MAGIC         sep => ",",
-- MAGIC         header => true,
-- MAGIC         schema => 'item_id STRING, name STRING, price DOUBLE', 
-- MAGIC         rescueddatacolumn => "_rescued_data"
-- MAGIC       );
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
-- MAGIC Run the cell below to view your final table **10_lab_bronze** and compare it with the solution table.

-- COMMAND ----------

-- View the final table
SELECT * 
FROM 10_lab_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. (Optional) Challenge: Rescuing Data
-- MAGIC The challenge is optional during a live class if you have time. This challenge may require you to use documentation. If you run out of time during a live class, try and complete this after class.
-- MAGIC
-- MAGIC #### Optional Challenge Scenario
-- MAGIC You report back to your data team and everyone agrees to clean up any values in the rescued data column that contain a `$` when ingesting as a bronze table. To fix this issue, you agree to handle these edge case during ingestion by leveraging the `_rescued_data` column.
-- MAGIC
-- MAGIC Your team has decided to strip the `$` in the price and simply store the numeric value during ingestion.
-- MAGIC
-- MAGIC
-- MAGIC #### Requirements
-- MAGIC
-- MAGIC - Complete the SQL query below to modify the **_rescued_data** column and correctly fixes the malformed value.
-- MAGIC
-- MAGIC - Your final table should return the following columns:
-- MAGIC   - **item_id**
-- MAGIC   - **name**
-- MAGIC   - **price** (the original price column)
-- MAGIC   - **TO DO**: **price_fixed** (fix the malformed row in the rescued data column `$100` and return all prices a numeric values)
-- MAGIC   - **_rescued_data**
-- MAGIC   - **source_file**
-- MAGIC   - **file_modification_time**
-- MAGIC   - **ingestion_time**
-- MAGIC
-- MAGIC - Use `CREATE TABLE AS` with `read_files()` to read the CSV file and create a table named .
-- MAGIC
-- MAGIC **HINT:** One solution you can use is the [`COALESCE()`](https://docs.databricks.com/aws/en/sql/language-manual/functions/coalesce) function along with the [`REPLACE()`](https://docs.databricks.com/aws/en/sql/language-manual/functions/replace) function to replace the malformed string price (`$100`) with `100`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can view the final table by running the next cell if you would like a hint for building your query.

-- COMMAND ----------

-- Run if you want to see the final table
SELECT * 
FROM 10_lab_challenge_solution;

-- COMMAND ----------

---- COMPLETE THE QUERY BELOW TO CLEAN UP THE _RESCUED_DATA column to create the price_fixed column
CREATE TABLE 10_lab_challenge
SELECT
  item_id,
  name,
  price,
  <FILL-IN> AS price_fixed,   -- CLEAN the rescued data column and return it as a numeric value with the other prices
  _rescued_data,
  _metadata.file_modification_time AS file_modification_time,
  _metadata.file_name AS source_file, 
  current_timestamp() as ingestion_timestamp
FROM read_files(
        '/Volumes/'|| my_catalog || '/data_ingestion/landing_folder/csv_demo_files/lab_malformed_data.csv',  -- <FILL-IN>
        format => "csv",
        sep => ",",
        header => true,
        schema => 'item_id STRING, name STRING, price DOUBLE', 
        rescueddatacolumn => "_rescued_data"
      );

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <details>
-- MAGIC <summary><strong>Click to reveal answer</strong></summary>
-- MAGIC
-- MAGIC <!-- Hidden source for the Python answer -->
-- MAGIC <textarea id="raw-answer-python-customer-id-analysis" style="display:none;">
-- MAGIC CREATE OR REPLACE TABLE 10_lab_challenge
-- MAGIC SELECT
-- MAGIC   item_id,
-- MAGIC   name,
-- MAGIC   price,
-- MAGIC   coalesce(price, replace(_rescued_data:price,'$','')) AS price_fixed,
-- MAGIC   _rescued_data,
-- MAGIC   _metadata.file_modification_time AS file_modification_time,
-- MAGIC   _metadata.file_name AS source_file, 
-- MAGIC   current_timestamp() as ingestion_timestamp
-- MAGIC FROM read_files(
-- MAGIC         '/Volumes/'|| my_catalog || '/data_ingestion/landing_folder/csv_demo_files/lab_malformed_data.csv',
-- MAGIC         format => "csv",
-- MAGIC         sep => ",",
-- MAGIC         header => true,
-- MAGIC         schema => 'item_id STRING, name STRING, price DOUBLE', 
-- MAGIC         rescueddatacolumn => "_rescued_data"
-- MAGIC       );
-- MAGIC
-- MAGIC ---- Display the table
-- MAGIC SELECT *
-- MAGIC FROM 10_lab_challenge
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