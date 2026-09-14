# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Appending Metadata Columns on Ingest
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how metadata columns such as source file name and modification time can be appended during data ingestion from cloud storage using the <code>_metadata</code> column, enabling essential context to be captured for each row during table creation in the Lakehouse.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Explain the purpose of metadata columns** and why they are valuable during data ingestion
# MAGIC 2. **Use the `_metadata` column to append file-level metadata** such as file name and modification time during table creation
# MAGIC 3. **Identify common `_metadata` fields** including `_metadata.file_name` and `_metadata.file_modification_time`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Adding a Metadata Column
# MAGIC
# MAGIC You can append metadata column information from input data source files when creating a table.
# MAGIC
# MAGIC ##### Click each step below to explore how a metadata column is added in Bronze table.
# MAGIC
# MAGIC <div style="width:100%;font-family:'Segoe UI',sans-serif;max-width:1000px;margin:0 auto;">
# MAGIC
# MAGIC <style>
# MAGIC .meta-tabs{
# MAGIC  display:flex;
# MAGIC  align-items:center;
# MAGIC  justify-content:center;
# MAGIC  gap:6px;
# MAGIC  flex-wrap:nowrap;
# MAGIC  margin-bottom:8px
# MAGIC }
# MAGIC .meta-tab{
# MAGIC  width:170px;background:#F9F7F4;border:1.5px solid #e8e5e0;border-radius:8px;
# MAGIC  padding:8px 8px 7px;cursor:pointer;transition:.2s;box-sizing:border-box
# MAGIC }
# MAGIC .meta-tab:hover{box-shadow:0 3px 10px rgba(0,0,0,.08)}
# MAGIC .meta-num{
# MAGIC  width:24px;height:24px;border-radius:50%;display:flex;align-items:center;justify-content:center;
# MAGIC  color:#fff;font-weight:800;font-size:11px;margin:0 auto 5px
# MAGIC }
# MAGIC .meta-tab-title{font-size:10.5pt;font-weight:700;color:#1b3139;text-align:center;line-height:1.2}
# MAGIC .meta-tab-sub{font-size:8.5pt;color:#7a7974;text-align:center;margin-top:2px;line-height:1.25}
# MAGIC
# MAGIC .meta-arrow-connector{
# MAGIC  width:18px;
# MAGIC  height:0;
# MAGIC  border-top:2px solid #90A5B1;
# MAGIC  position:relative;
# MAGIC  flex-shrink:0;
# MAGIC }
# MAGIC .meta-arrow-connector::after{
# MAGIC  content:"";
# MAGIC  position:absolute;
# MAGIC  right:-1px;
# MAGIC  top:-5px;
# MAGIC  border-top:4px solid transparent;
# MAGIC  border-bottom:4px solid transparent;
# MAGIC  border-left:6px solid #90A5B1;
# MAGIC }
# MAGIC
# MAGIC .meta-panel{
# MAGIC  display:none;background:#F9F7F4;border:1.5px solid #e8e5e0;border-radius:10px;
# MAGIC  padding:12px 14px;animation:fadeIn .2s ease
# MAGIC }
# MAGIC @keyframes fadeIn{from{opacity:0;transform:translateY(-4px)}to{opacity:1;transform:translateY(0)}}
# MAGIC
# MAGIC input[name="meta-step"]{display:none}
# MAGIC #ms1:checked ~ .meta-tabs label[for="ms1"],
# MAGIC #ms2:checked ~ .meta-tabs label[for="ms2"],
# MAGIC #ms3:checked ~ .meta-tabs label[for="ms3"],
# MAGIC #ms4:checked ~ .meta-tabs label[for="ms4"]{
# MAGIC  border-color:var(--accent); box-shadow:0 3px 12px rgba(0,0,0,.10)
# MAGIC }
# MAGIC #ms1:checked ~ .meta-panels .p1,
# MAGIC #ms2:checked ~ .meta-panels .p2,
# MAGIC #ms3:checked ~ .meta-panels .p3,
# MAGIC #ms4:checked ~ .meta-panels .p4{display:block}
# MAGIC
# MAGIC .meta-desc{font-size:10pt;color:#3a3a3a;line-height:1.5;margin:0}
# MAGIC .meta-tags{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
# MAGIC .meta-tag{padding:2px 8px;border-radius:999px;font-size:8pt;font-weight:600;color:#fff}
# MAGIC .meta-diag{display:flex;align-items:center;gap:10px;margin-top:10px;flex-wrap:wrap}
# MAGIC .meta-box{
# MAGIC  background:#fff;border-radius:8px;border:1.5px solid #e0ddd8;padding:10px 12px;min-width:150px;flex:1
# MAGIC }
# MAGIC .meta-label{
# MAGIC  font-size:8pt;font-weight:700;color:#7a7974;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px
# MAGIC }
# MAGIC .meta-table{width:100%;border-collapse:collapse;font-size:9pt}
# MAGIC .meta-table th{padding:5px 8px;text-align:left;color:#fff;font-weight:700;font-size:8pt}
# MAGIC .meta-table td{padding:5px 8px;color:#1b3139;border-bottom:1px solid #f0edea;font-size:8pt}
# MAGIC .meta-table tr:last-child td{border-bottom:none;color:#aaa}
# MAGIC .meta-arrow{display:flex;flex-direction:column;align-items:center;gap:2px;color:#2574B5;font-size:8pt;font-weight:700}
# MAGIC .meta-arrow-svg{width:32px;height:16px}
# MAGIC .meta-green{background:#02A36F !important}
# MAGIC .meta-callouts{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
# MAGIC .meta-callout{
# MAGIC  flex:1;min-width:180px;padding:8px 10px;background:rgba(2,163,111,.08);
# MAGIC  border-left:4px solid #02A36F;border-radius:6px;font-size:8.5pt;line-height:1.35
# MAGIC }
# MAGIC
# MAGIC /* Image highlight area */
# MAGIC .meta-figure-wrap{
# MAGIC  position:relative;
# MAGIC  width:720px;
# MAGIC  max-width:100%;
# MAGIC  margin:10px auto 0 auto;
# MAGIC  overflow:hidden;
# MAGIC  border-radius:8px;
# MAGIC }
# MAGIC .meta-figure-wrap img{
# MAGIC  width:100%;
# MAGIC  height:auto;
# MAGIC  display:block;
# MAGIC  transition:transform .3s ease;
# MAGIC }
# MAGIC .meta-highlight{
# MAGIC  position:absolute;
# MAGIC  border:3px solid #FF3B30;
# MAGIC  border-radius:8px;
# MAGIC  box-shadow:0 0 0 9999px rgba(255,255,255,0.08);
# MAGIC  opacity:0;
# MAGIC  transition:opacity .25s ease;
# MAGIC  pointer-events:none;
# MAGIC }
# MAGIC .meta-h1{ left:2%; top:38%; width:23%; height:33%; }
# MAGIC .meta-h2{ left:3%; top:3%; width:75%; height:30%; }
# MAGIC .meta-h3{ left:70%; top:35%; width:28%; height:38%; }
# MAGIC .meta-h4{ left:35%; top:36%; width:63%; height:59%; }
# MAGIC
# MAGIC #ms1:checked ~ .meta-figure-wrap .meta-h1,
# MAGIC #ms2:checked ~ .meta-figure-wrap .meta-h2,
# MAGIC #ms3:checked ~ .meta-figure-wrap .meta-h3,
# MAGIC #ms4:checked ~ .meta-figure-wrap .meta-h4{
# MAGIC  opacity:1;
# MAGIC }
# MAGIC
# MAGIC #ms1:checked ~ .meta-figure-wrap img,
# MAGIC #ms2:checked ~ .meta-figure-wrap img,
# MAGIC #ms3:checked ~ .meta-figure-wrap img,
# MAGIC #ms4:checked ~ .meta-figure-wrap img{transform:scale(1.01);}
# MAGIC
# MAGIC @media (max-width:900px){
# MAGIC  .meta-arrow-connector{display:none}
# MAGIC  .meta-tabs{flex-wrap:wrap}
# MAGIC  .meta-tab{width:calc(50% - 6px)}
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <input type="radio" name="meta-step" id="ms1" checked>
# MAGIC <input type="radio" name="meta-step" id="ms2">
# MAGIC <input type="radio" name="meta-step" id="ms3">
# MAGIC <input type="radio" name="meta-step" id="ms4">
# MAGIC
# MAGIC <div class="meta-figure-wrap">
# MAGIC  <img
# MAGIC  src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/lecture_append_metadata_column/adding-metadata-column-example.png"
# MAGIC  alt="Adding Metadata Column">
# MAGIC  <div class="meta-highlight meta-h1"></div>
# MAGIC  <div class="meta-highlight meta-h2"></div>
# MAGIC  <div class="meta-highlight meta-h3"></div>
# MAGIC  <div class="meta-highlight meta-h4"></div>
# MAGIC </div>
# MAGIC <br>
# MAGIC
# MAGIC <div class="meta-tabs">
# MAGIC  <label class="meta-tab" for="ms1" style="--accent:#2574B5;">
# MAGIC   <div class="meta-num" style="background:#2574B5;">1</div>
# MAGIC   <div class="meta-tab-title">Raw Files</div>
# MAGIC   <div class="meta-tab-sub">CSV · TXT · JSON · other formats</div>
# MAGIC  </label>
# MAGIC  <div class="meta-arrow-connector"></div>
# MAGIC  <label class="meta-tab" for="ms2" style="--accent:#1b7fb0;">
# MAGIC   <div class="meta-num" style="background:#1b7fb0;">2</div>
# MAGIC   <div class="meta-tab-title">Create a UC Table</div>
# MAGIC   <div class="meta-tab-sub">Ingest raw files into Bronze</div>
# MAGIC  </label>
# MAGIC  <div class="meta-arrow-connector"></div>
# MAGIC  <label class="meta-tab" for="ms3" style="--accent:#02A36F;">
# MAGIC   <div class="meta-num" style="background:#02A36F;">3</div>
# MAGIC   <div class="meta-tab-title">Metadata Columns</div>
# MAGIC   <div class="meta-tab-sub">Added from the ingestion source</div>
# MAGIC  </label>
# MAGIC  <div class="meta-arrow-connector"></div>
# MAGIC  <label class="meta-tab" for="ms4" style="--accent:#1C3037;">
# MAGIC   <div class="meta-num" style="background:#1C3037;">4</div>
# MAGIC   <div class="meta-tab-title">Bronze Table</div>
# MAGIC   <div class="meta-tab-sub">Original columns plus metadata columns</div>
# MAGIC  </label>
# MAGIC </div>
# MAGIC <div class="meta-panels">
# MAGIC  <div class="meta-panel p1">
# MAGIC   <p class="meta-desc">
# MAGIC    Suppose your cloud storage location contains a set of raw files. These could be CSV, TXT, JSON, or other formats.
# MAGIC   </p>
# MAGIC   <div class="meta-tags">
# MAGIC    <span class="meta-tag" style="background:#2574B5;">CSV</span>
# MAGIC    <span class="meta-tag" style="background:#2574B5;">TXT</span>
# MAGIC    <span class="meta-tag" style="background:#2574B5;">JSON</span>
# MAGIC   </div>
# MAGIC   <div class="meta-diag">
# MAGIC    <div class="meta-box" style="max-width:320px">
# MAGIC     <div class="meta-label">raw_file</div>
# MAGIC     <table class="meta-table">
# MAGIC      <thead>
# MAGIC       <tr style="background:#2c5f7c;"><th>users</th><th>unix_ts</th></tr>
# MAGIC      </thead>
# MAGIC      <tbody>
# MAGIC       <tr><td>peter</td><td>1592187804331222</td></tr>
# MAGIC       <tr><td>zebi</td><td>1592200952155132</td></tr>
# MAGIC       <tr><td>…</td><td>…</td></tr>
# MAGIC      </tbody>
# MAGIC     </table>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <div class="meta-panel p2">
# MAGIC   <p class="meta-desc">
# MAGIC    You want to ingest those files into a bronze-level table.
# MAGIC   </p>
# MAGIC  </div>
# MAGIC  <div class="meta-panel p3">
# MAGIC   <p class="meta-desc">
# MAGIC    As part of that ingestion, you may want to add specific metadata columns to each row in the table.
# MAGIC    These columns can include information from the ingestion source, such as:
# MAGIC   </p>
# MAGIC   <div class="meta-tags">
# MAGIC    <span class="meta-tag" style="background:#02A36F;">The last modification time of the file the row originated from</span>
# MAGIC    <span class="meta-tag" style="background:#02A36F;">The source file name</span>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <div class="meta-panel p4">
# MAGIC   <div class="meta-diag">
# MAGIC    <div class="meta-box" style="min-width:140px;">
# MAGIC     <div class="meta-label">raw_file</div>
# MAGIC     <table class="meta-table">
# MAGIC      <thead>
# MAGIC       <tr style="background:#2c5f7c;"><th>users</th><th>unix_ts</th></tr>
# MAGIC      </thead>
# MAGIC      <tbody>
# MAGIC       <tr><td>peter</td><td>1592187804331222</td></tr>
# MAGIC       <tr><td>zebi</td><td>1592200952155132</td></tr>
# MAGIC       <tr><td>…</td><td>…</td></tr>
# MAGIC      </tbody>
# MAGIC     </table>
# MAGIC    </div>
# MAGIC    <div class="meta-arrow">
# MAGIC     <svg class="meta-arrow-svg" viewBox="0 0 50 20" fill="none">
# MAGIC      <path d="M2 10 H42 M36 4 L48 10 L36 16" stroke="#2574B5" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
# MAGIC     </svg>
# MAGIC     <span style="font-size:7.5pt;">Create a UC Table</span>
# MAGIC    </div>
# MAGIC    <div class="meta-box" style="flex:2;">
# MAGIC     <div class="meta-label">Bronze Table</div>
# MAGIC     <table class="meta-table">
# MAGIC      <thead>
# MAGIC       <tr>
# MAGIC        <th style="background:#2c5f7c;">users</th>
# MAGIC        <th style="background:#2c5f7c;">unix_ts</th>
# MAGIC        <th class="meta-green">last_mod_time</th>
# MAGIC        <th class="meta-green">source</th>
# MAGIC       </tr>
# MAGIC      </thead>
# MAGIC      <tbody>
# MAGIC       <tr>
# MAGIC        <td>peter</td>
# MAGIC        <td>1592187804331222</td>
# MAGIC        <td style="background:rgba(2,163,111,.08);">2024-10-01T18:04…</td>
# MAGIC        <td style="background:rgba(2,163,111,.08);">raw_file</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC        <td>zebi</td>
# MAGIC        <td>1592200952155132</td>
# MAGIC        <td style="background:rgba(2,163,111,.08);">2024-10-01T18:04…</td>
# MAGIC        <td style="background:rgba(2,163,111,.08);">raw_file</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC        <td>…</td>
# MAGIC        <td>…</td>
# MAGIC        <td style="background:rgba(2,163,111,.08);">…</td>
# MAGIC        <td style="background:rgba(2,163,111,.08);">…</td>
# MAGIC       </tr>
# MAGIC      </tbody>
# MAGIC     </table>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <div class="meta-callouts">
# MAGIC    <div class="meta-callout">
# MAGIC     <strong>last_mod_time</strong><br>
# MAGIC     Add the last file modification time
# MAGIC    </div>
# MAGIC    <div class="meta-callout">
# MAGIC     <strong>source</strong><br>
# MAGIC     Add the source file name
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <p class="meta-desc" style="margin-top:10px;">
# MAGIC    This helps preserve important context about the data’s origin, which can be valuable for auditing, lineage, and debugging purposes.
# MAGIC   </p>
# MAGIC  </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC For example, suppose your cloud storage location contains <strong>a set of raw files</strong> (CSV, TXT, JSON, or other formats) and you want to ingest those files into a <strong>bronze-level table</strong>.
# MAGIC
# MAGIC As part of that ingestion, you may want to add specific metadata columns to each row in the table. These columns can include information from the ingestion source, such as:
# MAGIC
# MAGIC - The **last modification time** of the file the row originated from
# MAGIC - The **source file name**
# MAGIC
# MAGIC This helps preserve important context about the data's origin, which can be valuable for auditing, lineage, and debugging purposes.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Common File Metadata Fields
# MAGIC
# MAGIC To add metadata columns during ingestion, you can use the special `_metadata` column. This is a hidden column that is available for all input file formats.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 700px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <div style="display: flex; flex-direction: column; gap: 24px;">
# MAGIC
# MAGIC   <!-- file_modification_time -->
# MAGIC   <div>
# MAGIC     <div style="font-size: 14pt; margin-bottom: 8px;"><strong>Add last file modification timestamp</strong></div>
# MAGIC     <div style="display: flex; align-items: center; gap: 14px;">
# MAGIC       <div style="display: flex; align-items: center; gap: 4px;">
# MAGIC         <div style="background: #FFAB00; color: white; border-radius: 4px; padding: 4px 10px; font-family: monospace; font-size: 13pt; font-weight: 600;">_metadata</div>
# MAGIC         <div style="font-family: monospace; font-size: 12pt;">.file_modification_time</div>
# MAGIC       </div>
# MAGIC       <div style="font-size: 24pt; color: #00A972;"> > </div>
# MAGIC       <div style="border: 2px solid #EEEDE9; border-radius: 6px; padding: 8px 14px; font-family: monospace; font-size: 12pt;">
# MAGIC         2024-10-07T18:04:42.885+00:00
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- file_name -->
# MAGIC   <div>
# MAGIC     <div style="font-size: 14pt; margin-bottom: 8px;"><strong>Add input file name</strong></div>
# MAGIC     <div style="display: flex; align-items: center; gap: 14px;">
# MAGIC       <div style="display: flex; align-items: center; gap: 4px;">
# MAGIC         <div style="background: #FFAB00; color: white; border-radius: 4px; padding: 4px 10px; font-family: monospace; font-size: 13pt; font-weight: 600;">_metadata</div>
# MAGIC         <div style="font-family: monospace; font-size: 12pt;">.file_name</div>
# MAGIC       </div>
# MAGIC       <div style="font-size: 24pt; color: #00A972;"> > </div>
# MAGIC       <div style="border: 2px solid #EEEDE9; border-radius: 6px; padding: 8px 14px; font-family: monospace; font-size: 12pt;">
# MAGIC         part-00002-7573-1-c000.file_name
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Documentation
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/aws/en/ingestion/file-metadata-column" style="color:#1976D2;">File Metadata Column</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC To include the <code>_metadata</code> column in the returned DataFrame, you must explicitly select it in your read query when specifying the source.
# MAGIC
# MAGIC The <code>_metadata</code> column contains a variety of useful fields. Two common fields include:
# MAGIC <ol>
# MAGIC <li><code>_metadata.file_modification_time</code> – provides the last modification timestamp of the input file</li>
# MAGIC <li><code>_metadata.file_name</code> – returns the name of the source file for each row</li>
# MAGIC </ol>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Conclusion
# MAGIC
# MAGIC In this lecture, you learned how to append metadata columns from input source files during data ingestion:
# MAGIC
# MAGIC - **Metadata columns** preserve context about data origin, which is valuable for auditing, lineage, and debugging.
# MAGIC - The special **`_metadata` column** is a hidden column available for all input file formats. You must explicitly select it in your read query.
# MAGIC - Two commonly used fields:
# MAGIC   - **`_metadata.file_modification_time`**: The last modification timestamp of the input file
# MAGIC   - **`_metadata.file_name`**: The name of the source file for each row
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next section, you will work hands-on with metadata columns to enrich your ingested data.

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