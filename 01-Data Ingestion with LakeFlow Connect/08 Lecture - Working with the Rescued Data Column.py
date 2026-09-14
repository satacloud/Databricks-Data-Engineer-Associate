# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Working with the Rescued Data Column
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how the rescued data column (`_rescued_data`) captures mismatched or unparseable fields as JSON during data ingestion, preserving non-conforming input values in your Lakehouse tables instead of dropping them.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Explain the purpose of the rescued data column** and how it preserves non-conforming data during ingestion
# MAGIC 2. **Describe how schema mismatches are handled** when using `read_files()`, `spark.read`, or Auto Loader
# MAGIC 3. **Interpret rescued data values** stored as JSON-formatted strings in the `_rescued_data` column

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Rescuing Malformed Rows on Ingestion
# MAGIC
# MAGIC During data ingestion there are times when the input data does not match the schema in your table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width:1100px; margin:0 auto; padding:14px 16px 6px; font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; color:#16313b; background:#fff;">
# MAGIC
# MAGIC   <style>
# MAGIC     .resc-wrap{position:relative;}
# MAGIC     .top-callout{
# MAGIC       border:3px solid #ff5b43;
# MAGIC       padding:12px 16px 8px;
# MAGIC       text-align:center;
# MAGIC       font-size:26px;
# MAGIC       line-height:1.2;
# MAGIC       font-weight:400;
# MAGIC       margin:0 110px 20px 0;
# MAGIC       color:#142b34;
# MAGIC     }
# MAGIC     .top-callout b{font-weight:800;}
# MAGIC
# MAGIC     /* simple elbow connector: right line + down line */
# MAGIC     .top-route{
# MAGIC       position:absolute;
# MAGIC       right:65px;
# MAGIC       top:44px;
# MAGIC       width:50px;
# MAGIC       height:150px;
# MAGIC       border-top:4px solid #ff5b43;
# MAGIC       border-right:4px solid #ff5b43;
# MAGIC       box-sizing:border-box;
# MAGIC     }
# MAGIC
# MAGIC     .flow{
# MAGIC       display:flex;
# MAGIC       align-items:flex-start;
# MAGIC       justify-content:space-between;
# MAGIC       gap:16px;
# MAGIC       margin-top:4px;
# MAGIC     }
# MAGIC
# MAGIC     .source-box{
# MAGIC       width:180px;
# MAGIC       height:320px;
# MAGIC       border:2px solid #1b5568;
# MAGIC       display:flex;
# MAGIC       flex-direction:column;
# MAGIC       align-items:center;
# MAGIC       padding-top:22px;
# MAGIC       box-sizing:border-box;
# MAGIC     }
# MAGIC     .source-box img{
# MAGIC       width:90px;
# MAGIC       height:70px;
# MAGIC       object-fit:contain;
# MAGIC       margin-bottom:12px;
# MAGIC       background:transparent;
# MAGIC       mix-blend-mode:multiply;
# MAGIC       filter:contrast(1.05) brightness(1.02);
# MAGIC     }
# MAGIC     .source-list{
# MAGIC       margin-top:4px;
# MAGIC       font-size:30px;
# MAGIC       line-height:1.7;
# MAGIC       text-align:center;
# MAGIC       font-weight:400;
# MAGIC     }
# MAGIC
# MAGIC     .big-arrow{
# MAGIC       width:82px;
# MAGIC       height:30px;
# MAGIC       background:#3e93d9;
# MAGIC       clip-path:polygon(0 0, 70% 0, 70% -5%, 100% 50%, 70% 105%, 70% 100%, 0 100%);
# MAGIC       border:1.5px solid #165b8e;
# MAGIC       box-sizing:border-box;
# MAGIC       flex-shrink:0;
# MAGIC       margin-top:150px;
# MAGIC     }
# MAGIC
# MAGIC     .ingestion{
# MAGIC       width:235px;
# MAGIC       height:300px;
# MAGIC       border:3px solid #4d97e3;
# MAGIC       border-radius:24px;
# MAGIC       padding:16px 18px;
# MAGIC       box-sizing:border-box;
# MAGIC     }
# MAGIC     .ingestion-title{
# MAGIC       font-size:28px;
# MAGIC       font-weight:700;
# MAGIC       color:#1d556a;
# MAGIC       text-align:center;
# MAGIC       margin-bottom:12px;
# MAGIC       line-height:1.1;
# MAGIC     }
# MAGIC     .ing-box{
# MAGIC       height:58px;
# MAGIC       display:flex;
# MAGIC       align-items:center;
# MAGIC       justify-content:center;
# MAGIC       font-size:20px;
# MAGIC       font-weight:700;
# MAGIC       color:#fff;
# MAGIC       margin-bottom:14px;
# MAGIC       box-sizing:border-box;
# MAGIC       border:1.5px solid rgba(0,0,0,.15);
# MAGIC     }
# MAGIC     .ing-blue{background:#1f78bf;}
# MAGIC     .ing-grey{background:#cfcfcf; color:#f7f7f7;}
# MAGIC
# MAGIC     .bronze-col{
# MAGIC       width:230px;
# MAGIC       text-align:center;
# MAGIC       position:relative;
# MAGIC       padding-top:6px;
# MAGIC     }
# MAGIC     .bronze-col img{
# MAGIC       width:108px;
# MAGIC       height:80px;
# MAGIC       object-fit:contain;
# MAGIC       margin:0 auto 8px;
# MAGIC       display:block;
# MAGIC       background:transparent;
# MAGIC       mix-blend-mode:multiply;
# MAGIC       filter:contrast(1.05) brightness(1.02);
# MAGIC     }
# MAGIC     .bronze-label{
# MAGIC       font-size:38px;
# MAGIC       font-weight:800;
# MAGIC       color:#132730;
# MAGIC       line-height:1.02;
# MAGIC     }
# MAGIC
# MAGIC     .rescue-col{width:300px; position:relative; margin-right:4px;}
# MAGIC     .rescue-table{
# MAGIC       border:1.5px solid #5d6f79;
# MAGIC       width:100%;
# MAGIC       background:#fff;
# MAGIC       box-sizing:border-box;
# MAGIC       margin-top:66px;
# MAGIC       position:relative;
# MAGIC     }
# MAGIC     .rescue-head{display:grid; grid-template-columns:30px 1fr;}
# MAGIC     .rescue-head .left{
# MAGIC       background:#1f78bf; color:#fff; padding:6px 6px; font-weight:700; font-size:20px; line-height:1;
# MAGIC       border-right:1.5px solid #5d6f79;
# MAGIC     }
# MAGIC     .rescue-head .right{
# MAGIC       background:#ff5b43; color:#fff; padding:5px 10px; font-weight:800; font-size:16px; line-height:1.15;
# MAGIC     }
# MAGIC     .rescue-row{
# MAGIC       display:grid;
# MAGIC       grid-template-columns:30px 1fr;
# MAGIC       border-top:1.5px solid #5d6f79;
# MAGIC       min-height:38px;
# MAGIC     }
# MAGIC     .rescue-row .left{
# MAGIC       border-right:1.5px solid #5d6f79;
# MAGIC       padding:8px 6px;
# MAGIC       font-size:18px;
# MAGIC       color:#3a4d57;
# MAGIC       line-height:1;
# MAGIC     }
# MAGIC     .rescue-row .right{
# MAGIC       padding:5px 10px;
# MAGIC       font-size:14px;
# MAGIC       line-height:1.2;
# MAGIC       font-weight:600;
# MAGIC       color:#20353d;
# MAGIC     }
# MAGIC     .red-x{position:absolute; right:86px; bottom:-22px; width:46px; height:46px;}
# MAGIC     .red-x:before,.red-x:after{
# MAGIC       content:""; position:absolute; left:20px; top:0; width:6px; height:46px; background:#ff3b30; border-radius:3px;
# MAGIC     }
# MAGIC     .red-x:before{transform:rotate(45deg);}
# MAGIC     .red-x:after{transform:rotate(-45deg);}
# MAGIC   </style>
# MAGIC
# MAGIC   <div class="resc-wrap">
# MAGIC     <div class="top-callout">
# MAGIC       <b>read_files(), spark.read or Auto Loader</b> provides a <b>rescued data column</b> if the raw data does not match the schema
# MAGIC     </div>
# MAGIC     <div class="top-route"></div>
# MAGIC     <div class="flow">
# MAGIC       <div class="source-box">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/cloud_icon.png" alt="Cloud icon">
# MAGIC         <div class="source-list">CSV<br>JSON<br>Parquet<br>etc.</div>
# MAGIC       </div>
# MAGIC       <div class="big-arrow"></div>
# MAGIC       <div class="ingestion">
# MAGIC         <div class="ingestion-title">Data Ingestion</div>
# MAGIC         <div class="ing-box ing-blue">CTAS</div>
# MAGIC         <div class="ing-box ing-grey">COPY INTO</div>
# MAGIC         <div class="ing-box ing-blue">AUTO LOADER</div>
# MAGIC       </div>
# MAGIC       <div class="big-arrow"></div>
# MAGIC       <div class="bronze-col">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/bronze_table_icon.png" alt="Bronze table icon">
# MAGIC         <div class="bronze-label">Bronze Table</div>
# MAGIC       </div>
# MAGIC       <div class="big-arrow"></div>
# MAGIC       <div class="rescue-col">
# MAGIC         <div class="rescue-table">
# MAGIC           <div class="rescue-head">
# MAGIC             <div class="left">…</div>
# MAGIC             <div class="right">_rescued_data</div>
# MAGIC           </div>
# MAGIC           <div class="rescue-row">
# MAGIC             <div class="left">…</div>
# MAGIC             <div class="right">{"column": "< data >", "_file_path": "< file_path >"}</div>
# MAGIC           </div>
# MAGIC           <div class="rescue-row">
# MAGIC             <div class="left">…</div>
# MAGIC             <div class="right">{"column": "< data >", "_file_path": "< file_path >"}</div>
# MAGIC           </div>
# MAGIC           <div class="rescue-row">
# MAGIC             <div class="left">…</div>
# MAGIC             <div class="right">null</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="red-x"></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Ingestion techniques like `read_files()`, `spark.read`, or Auto Loader provide a rescued data column during ingestion:
# MAGIC
# MAGIC - The rescued data column ensures that columns that do not match the schema are <strong>rescued instead of being dropped</strong>
# MAGIC - Mismatched values are stored as <strong>JSON-formatted strings</strong> in the <code>_rescued_data</code> column
# MAGIC - If a row has no schema mismatches, the <code>_rescued_data</code> column will be <code>null</code>
# MAGIC - This preserves all input data and prevents silent data loss
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Rescued Data Column Example
# MAGIC
# MAGIC ##### Click each step below to explore how to work with the Rescued Data Column.
# MAGIC
# MAGIC <div style="width:100%;font-family:'Segoe UI',sans-serif;max-width:1100px;margin:0 auto;">
# MAGIC
# MAGIC <style>
# MAGIC .rj-shell {
# MAGIC  display:flex;
# MAGIC  flex-direction:column;
# MAGIC  gap:16px;
# MAGIC }
# MAGIC
# MAGIC /* Image */
# MAGIC .rj-figure-wrap{
# MAGIC  position:relative;
# MAGIC  width:720px;
# MAGIC  max-width:100%;
# MAGIC  margin:10px auto 0 auto;
# MAGIC  overflow:hidden;
# MAGIC  border-radius:8px;
# MAGIC }
# MAGIC .rj-figure-wrap img{
# MAGIC  width:100%;
# MAGIC  height:auto;
# MAGIC  display:block;
# MAGIC }
# MAGIC .rj-highlight{
# MAGIC  position:absolute;
# MAGIC  border:3px solid #FF3B30;
# MAGIC  border-radius:8px;
# MAGIC  box-shadow:0 0 0 9999px rgba(255,255,255,0.08);
# MAGIC  opacity:0;
# MAGIC  transition:opacity .25s ease;
# MAGIC  pointer-events:none;
# MAGIC }
# MAGIC .rj-highlight.active{ opacity:1; }
# MAGIC
# MAGIC /* Highlight regions */
# MAGIC .rj-h1{ left:2%; top:3%; width:20%; height:31%; }     /* files */
# MAGIC .rj-h2{ left:1%; top:34%; width:23%; height:28%; }    /* users and cost */
# MAGIC .rj-h3{ left:39%; top:25%; width:20%; height:35%; }   /* STRING and BIGINT */
# MAGIC .rj-h4{ left:39%; top:40%; width:58%; height:7%; }    /* Peter + $100 main row */
# MAGIC .rj-h4b{ left:28%; top:77%; width:46%; height:17%; }  /* Peter + $100 extra box */
# MAGIC .rj-h5{ left:39%; top:46.5%; width:58%; height:7%; }  /* zebi + 300 main row */
# MAGIC .rj-h5b{ left:28%; top:61%; width:32%; height:15%; }  /* zebi + 300 extra box */
# MAGIC
# MAGIC /* Horizontal steps */
# MAGIC .rj-list {
# MAGIC  display:flex;
# MAGIC  align-items:stretch;
# MAGIC  justify-content:center;
# MAGIC  gap:10px;
# MAGIC  flex-wrap:wrap;
# MAGIC }
# MAGIC .rj-trigger {
# MAGIC  display:flex;
# MAGIC  flex-direction:column;
# MAGIC  align-items:center;
# MAGIC  gap:8px;
# MAGIC  cursor:pointer;
# MAGIC  width:200px;
# MAGIC }
# MAGIC .rj-badge {
# MAGIC  width:22px;
# MAGIC  height:22px;
# MAGIC  border-radius:50%;
# MAGIC  display:flex;
# MAGIC  align-items:center;
# MAGIC  justify-content:center;
# MAGIC  font-size:16px;
# MAGIC  font-weight:800;
# MAGIC  color:white;
# MAGIC  border:3px solid white;
# MAGIC  box-shadow:0 2px 10px rgba(0,0,0,0.15);
# MAGIC  transition:transform 0.2s, box-shadow 0.2s;
# MAGIC }
# MAGIC .rj-trigger:hover .rj-badge {
# MAGIC  transform:scale(1.08);
# MAGIC  box-shadow:0 6px 18px rgba(0,0,0,0.2);
# MAGIC }
# MAGIC .rj-label-box {
# MAGIC  width:100%;
# MAGIC  background:#F9F7F4;
# MAGIC  border-radius:8px;
# MAGIC  border:1.5px solid #e8e5e0;
# MAGIC  padding:10px 12px;
# MAGIC  transition:border-color 0.2s, box-shadow 0.2s;
# MAGIC  box-sizing:border-box;
# MAGIC  min-height:82px;
# MAGIC }
# MAGIC .rj-trigger.active .rj-label-box {
# MAGIC  border-color:var(--step-color);
# MAGIC  box-shadow:0 2px 10px rgba(0,0,0,0.10);
# MAGIC  background:#fff;
# MAGIC }
# MAGIC .rj-trigger:hover .rj-label-box {
# MAGIC  box-shadow:0 2px 8px rgba(0,0,0,0.08);
# MAGIC }
# MAGIC .rj-label-top {
# MAGIC  display:flex;
# MAGIC  align-items:center;
# MAGIC  gap:7px;
# MAGIC  justify-content:center;
# MAGIC }
# MAGIC .rj-dot {
# MAGIC  width:9px;
# MAGIC  height:9px;
# MAGIC  border-radius:50%;
# MAGIC  background:var(--step-color);
# MAGIC  flex-shrink:0;
# MAGIC }
# MAGIC .rj-title {
# MAGIC  font-size:12pt;
# MAGIC  font-weight:700;
# MAGIC  color:#1b3139;
# MAGIC  line-height:1.25;
# MAGIC  text-align:center;
# MAGIC }
# MAGIC .rj-sub {
# MAGIC  font-size:9pt;
# MAGIC  color:#7a7974;
# MAGIC  margin-top:4px;
# MAGIC  line-height:1.35;
# MAGIC  text-align:center;
# MAGIC }
# MAGIC .rj-chevron {
# MAGIC  font-size:11px;
# MAGIC  color:#ccc;
# MAGIC  transition:transform 0.25s, color 0.2s;
# MAGIC  flex-shrink:0;
# MAGIC }
# MAGIC .rj-trigger.active .rj-chevron {
# MAGIC  transform:rotate(90deg);
# MAGIC  color:var(--step-color);
# MAGIC }
# MAGIC
# MAGIC .rj-panels { width:100%; }
# MAGIC
# MAGIC .rj-panel {
# MAGIC  display:none;
# MAGIC  background:#F9F7F4;
# MAGIC  border-radius:12px;
# MAGIC  border:1.5px solid #e8e5e0;
# MAGIC  padding:20px 22px;
# MAGIC  animation:rjIn 0.28s ease;
# MAGIC }
# MAGIC .rj-panel.active {
# MAGIC  display:block;
# MAGIC  border-color:var(--panel-color);
# MAGIC  box-shadow:0 4px 20px rgba(0,0,0,0.10);
# MAGIC }
# MAGIC @keyframes rjIn {
# MAGIC  from { opacity:0; transform:translateY(-10px); }
# MAGIC  to { opacity:1; transform:translateY(0); }
# MAGIC }
# MAGIC
# MAGIC .rj-panel-title {
# MAGIC  display:flex;
# MAGIC  align-items:center;
# MAGIC  gap:10px;
# MAGIC  margin-bottom:14px;
# MAGIC  padding-bottom:10px;
# MAGIC  border-bottom:1.5px solid #eeede9;
# MAGIC }
# MAGIC .rj-panel-dot {
# MAGIC  width:11px;
# MAGIC  height:11px;
# MAGIC  border-radius:50%;
# MAGIC  background:var(--panel-color);
# MAGIC  flex-shrink:0;
# MAGIC }
# MAGIC .rj-panel-title h4 {
# MAGIC  font-size:13pt;
# MAGIC  font-weight:800;
# MAGIC  color:#1b3139;
# MAGIC  margin:0;
# MAGIC }
# MAGIC .rj-panel-title span {
# MAGIC  font-size:11pt;
# MAGIC  color:#7a7974;
# MAGIC  margin-left:4px;
# MAGIC }
# MAGIC .rj-desc {
# MAGIC  font-size:11pt;
# MAGIC  color:#3a3a3a;
# MAGIC  line-height:1.75;
# MAGIC  margin:0 0 12px 0;
# MAGIC }
# MAGIC .rj-desc strong { color:#1b3139; }
# MAGIC
# MAGIC .tbl-wrap { overflow-x:auto; margin-top:12px; }
# MAGIC .rj-table { width:100%; border-collapse:collapse; font-size:9.5pt; }
# MAGIC .rj-table th { padding:7px 11px; text-align:left; color:white; font-weight:700; font-size:9pt; }
# MAGIC .rj-table td { padding:7px 11px; color:#1b3139; border-bottom:1px solid #f0edea; font-size:9pt; }
# MAGIC .rj-table tr:last-child td { border-bottom:none; color:#aaa; }
# MAGIC
# MAGIC .th-blue { background:#2574B5; }
# MAGIC .th-orange { background:#E05A2B; }
# MAGIC .td-rescued { background:rgba(224,90,43,0.07); font-size:8.5pt; font-family:monospace; color:#c0400a; }
# MAGIC .td-null { color:#aaa !important; font-style:italic; }
# MAGIC .td-ok { background:rgba(2,163,111,0.06); }
# MAGIC
# MAGIC .callout {
# MAGIC  margin-top:12px;
# MAGIC  padding:11px 15px;
# MAGIC  border-radius:8px;
# MAGIC  font-size:10.5pt;
# MAGIC  line-height:1.65;
# MAGIC }
# MAGIC .callout-orange { background:rgba(224,90,43,0.09); border-left:4px solid #E05A2B; }
# MAGIC .callout-green { background:rgba(2,163,111,0.08); border-left:4px solid #02A36F; }
# MAGIC
# MAGIC .tag-row { display:flex; gap:8px; flex-wrap:wrap; margin-top:10px; }
# MAGIC .tag { padding:3px 11px; border-radius:999px; font-size:9pt; font-weight:600; color:white; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="rj-shell">
# MAGIC  <!-- Image -->
# MAGIC  <div class="rj-figure-wrap">
# MAGIC   <img
# MAGIC    src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/lecture_rescued_data/rescued_data_example.png"
# MAGIC    alt="Rescued Data Column">
# MAGIC   <div class="rj-highlight rj-h1" id="rh1"></div>
# MAGIC   <div class="rj-highlight rj-h2" id="rh2"></div>
# MAGIC   <div class="rj-highlight rj-h3" id="rh3"></div>
# MAGIC   <div class="rj-highlight rj-h4" id="rh4"></div>
# MAGIC   <div class="rj-highlight rj-h4b" id="rh4b"></div>
# MAGIC   <div class="rj-highlight rj-h5" id="rh5"></div>
# MAGIC   <div class="rj-highlight rj-h5b" id="rh5b"></div>
# MAGIC  </div>
# MAGIC  <!-- Horizontal steps -->
# MAGIC  <div class="rj-list">
# MAGIC   <div class="rj-trigger active" style="--step-color:#2574B5;" onclick="rjSwitch(this,'rp1','rh1')">
# MAGIC    <div class="rj-badge" style="background:#2574B5;">1</div>
# MAGIC    <div class="rj-label-box">
# MAGIC     <div class="rj-label-top">
# MAGIC      <div class="rj-dot"></div>
# MAGIC      <div class="rj-title">Files</div>
# MAGIC      <span class="rj-chevron">▶</span>
# MAGIC     </div>
# MAGIC     <div class="rj-sub">raw files in cloud storage</div>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <div class="rj-trigger" style="--step-color:#E05A2B;" onclick="rjSwitch(this,'rp2','rh2')">
# MAGIC    <div class="rj-badge" style="background:#E05A2B;">2</div>
# MAGIC    <div class="rj-label-box">
# MAGIC     <div class="rj-label-top">
# MAGIC      <div class="rj-dot"></div>
# MAGIC      <div class="rj-title">Users and Cost</div>
# MAGIC      <span class="rj-chevron">▶</span>
# MAGIC     </div>
# MAGIC     <div class="rj-sub">source columns in the raw files</div>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <div class="rj-trigger" style="--step-color:#02A36F;" onclick="rjSwitch(this,'rp3','rh3')">
# MAGIC    <div class="rj-badge" style="background:#02A36F;">3</div>
# MAGIC    <div class="rj-label-box">
# MAGIC     <div class="rj-label-top">
# MAGIC      <div class="rj-dot"></div>
# MAGIC      <div class="rj-title">STRING and BIGINT</div>
# MAGIC      <span class="rj-chevron">▶</span>
# MAGIC     </div>
# MAGIC     <div class="rj-sub">expected types during ingestion</div>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <div class="rj-trigger" style="--step-color:#1C3037;" onclick="rjSwitch(this,'rp4','rh4')">
# MAGIC    <div class="rj-badge" style="background:#1C3037;">4</div>
# MAGIC    <div class="rj-label-box">
# MAGIC     <div class="rj-label-top">
# MAGIC      <div class="rj-dot"></div>
# MAGIC      <div class="rj-title">"Peter" and "$100"</div>
# MAGIC      <span class="rj-chevron">▶</span>
# MAGIC     </div>
# MAGIC     <div class="rj-sub">captured in _rescued_data</div>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <div class="rj-trigger" style="--step-color:#618794;" onclick="rjSwitch(this,'rp5','rh5')">
# MAGIC    <div class="rj-badge" style="background:#618794;">5</div>
# MAGIC    <div class="rj-label-box">
# MAGIC     <div class="rj-label-top">
# MAGIC      <div class="rj-dot"></div>
# MAGIC      <div class="rj-title">"zebi" and "300"</div>
# MAGIC      <span class="rj-chevron">▶</span>
# MAGIC     </div>
# MAGIC     <div class="rj-sub">read without issues</div>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <!-- Panels -->
# MAGIC  <div class="rj-panels">
# MAGIC   <div class="rj-panel active" id="rp1" style="--panel-color:#2574B5;">
# MAGIC    <div class="rj-panel-title">
# MAGIC     <div class="rj-panel-dot"></div>
# MAGIC     <h4>files</h4>
# MAGIC     <span>raw files in cloud storage</span>
# MAGIC    </div>
# MAGIC    <p class="rj-desc">
# MAGIC     Suppose your cloud storage location contains a set of raw files. These could be CSV, TXT, JSON, or other formats.
# MAGIC    </p>
# MAGIC    <div class="tag-row">
# MAGIC     <span class="tag" style="background:#2574B5;">CSV</span>
# MAGIC     <span class="tag" style="background:#2574B5;">TXT</span>
# MAGIC     <span class="tag" style="background:#2574B5;">JSON</span>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <div class="rj-panel" id="rp2" style="--panel-color:#E05A2B;">
# MAGIC    <div class="rj-panel-title">
# MAGIC     <div class="rj-panel-dot"></div>
# MAGIC     <h4>users and cost</h4>
# MAGIC     <span>columns in the raw files</span>
# MAGIC    </div>
# MAGIC    <p class="rj-desc">
# MAGIC     The raw files contain a <strong>users</strong> column and a <strong>cost</strong> column.
# MAGIC    </p>
# MAGIC    <div class="tbl-wrap">
# MAGIC     <table class="rj-table" style="max-width:320px;">
# MAGIC      <thead>
# MAGIC       <tr><th class="th-blue">users</th><th class="th-blue">cost</th></tr>
# MAGIC      </thead>
# MAGIC      <tbody>
# MAGIC       <tr><td>Peter</td><td>$100</td></tr>
# MAGIC       <tr><td>zebi</td><td>300</td></tr>
# MAGIC      </tbody>
# MAGIC     </table>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <div class="rj-panel" id="rp3" style="--panel-color:#02A36F;">
# MAGIC    <div class="rj-panel-title">
# MAGIC     <div class="rj-panel-dot"></div>
# MAGIC     <h4>STRING and BIGINT</h4>
# MAGIC     <span>expected types during ingestion</span>
# MAGIC    </div>
# MAGIC    <p class="rj-desc">
# MAGIC     When ingesting this data, the <strong>users</strong> column must be read into the table as a <strong>STRING</strong>, and the <strong>cost</strong> column must be read as a <strong>BIGINT</strong>.
# MAGIC    </p>
# MAGIC    <div class="tbl-wrap">
# MAGIC     <table class="rj-table" style="max-width:360px;">
# MAGIC      <thead>
# MAGIC       <tr><th class="th-blue">Column</th><th class="th-blue">Expected Type</th></tr>
# MAGIC      </thead>
# MAGIC      <tbody>
# MAGIC       <tr><td>users</td><td>STRING</td></tr>
# MAGIC       <tr><td>cost</td><td>BIGINT</td></tr>
# MAGIC      </tbody>
# MAGIC     </table>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <div class="rj-panel" id="rp4" style="--panel-color:#1C3037;">
# MAGIC    <div class="rj-panel-title">
# MAGIC     <div class="rj-panel-dot"></div>
# MAGIC     <h4>First row: "Peter" and "$100"</h4>
# MAGIC     <span>_rescued_data is populated</span>
# MAGIC    </div>
# MAGIC    <p class="rj-desc">
# MAGIC     In the first row of data, the value <strong>"Peter"</strong> will be read into the <strong>users</strong> column of the bronze table correctly. However, since the <strong>cost</strong> column contains a string like <strong>$100</strong>, it does not match the expected <strong>BIGINT</strong> type.
# MAGIC    </p>
# MAGIC    <div class="callout callout-orange">
# MAGIC     As a result, this value will not be inserted into the cost column. Instead, it will be captured in the <strong>_rescued_data</strong> column, stored as a <strong>JSON-formatted string</strong>.
# MAGIC    </div>
# MAGIC    <div class="tbl-wrap">
# MAGIC     <table class="rj-table">
# MAGIC      <thead>
# MAGIC       <tr>
# MAGIC        <th class="th-blue">users</th>
# MAGIC        <th class="th-blue">cost</th>
# MAGIC        <th class="th-orange">_rescued_data</th>
# MAGIC       </tr>
# MAGIC      </thead>
# MAGIC      <tbody>
# MAGIC       <tr>
# MAGIC        <td>Peter</td>
# MAGIC        <td class="td-null">null</td>
# MAGIC        <td class="td-rescued">{"cost": "$100", "_file_path": "&lt;file_path&gt;"}</td>
# MAGIC       </tr>
# MAGIC      </tbody>
# MAGIC     </table>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <div class="rj-panel" id="rp5" style="--panel-color:#618794;">
# MAGIC    <div class="rj-panel-title">
# MAGIC     <div class="rj-panel-dot"></div>
# MAGIC     <h4>Second row: "zebi" and 300</h4>
# MAGIC     <span>_rescued_data remains empty</span>
# MAGIC    </div>
# MAGIC    <p class="rj-desc">
# MAGIC     In the second row, both values are valid: the value <strong>"zebi"</strong> is a <strong>STRING</strong>, and <strong>300</strong> is a <strong>BIGINT</strong>.
# MAGIC    </p>
# MAGIC    <div class="callout callout-green">
# MAGIC     Therefore, this row will be read into the bronze table without issues, and the <strong>_rescued_data</strong> column will remain empty for that row.
# MAGIC    </div>
# MAGIC    <div class="tbl-wrap">
# MAGIC     <table class="rj-table">
# MAGIC      <thead>
# MAGIC       <tr>
# MAGIC        <th class="th-blue">users</th>
# MAGIC        <th class="th-blue">cost</th>
# MAGIC        <th class="th-orange">_rescued_data</th>
# MAGIC       </tr>
# MAGIC      </thead>
# MAGIC      <tbody>
# MAGIC       <tr>
# MAGIC        <td>zebi</td>
# MAGIC        <td class="td-ok">300</td>
# MAGIC        <td class="td-null">null</td>
# MAGIC       </tr>
# MAGIC      </tbody>
# MAGIC     </table>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function rjSwitch(triggerEl, panelId, highlightId) {
# MAGIC   var allTriggers = document.querySelectorAll('.rj-trigger');
# MAGIC   var allPanels = document.querySelectorAll('.rj-panel');
# MAGIC   var allHighlights = document.querySelectorAll('.rj-highlight');
# MAGIC   var isAlreadyActive = triggerEl.classList.contains('active');
# MAGIC
# MAGIC   allTriggers.forEach(function(t){ t.classList.remove('active'); });
# MAGIC   allPanels.forEach(function(p){ p.classList.remove('active'); });
# MAGIC   allHighlights.forEach(function(h){ h.classList.remove('active'); });
# MAGIC
# MAGIC   if (!isAlreadyActive) {
# MAGIC     triggerEl.classList.add('active');
# MAGIC     document.getElementById(panelId).classList.add('active');
# MAGIC     document.getElementById(highlightId).classList.add('active');
# MAGIC
# MAGIC     if (highlightId === 'rh4') {
# MAGIC       document.getElementById('rh4b').classList.add('active');
# MAGIC     }
# MAGIC     if (highlightId === 'rh5') {
# MAGIC       document.getElementById('rh5b').classList.add('active');
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC document.getElementById('rh1').classList.add('active');
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC For example, suppose your cloud storage location contains a set of raw files (these could be CSV, TXT, JSON, or other formats) and it contains a users and cost column.
# MAGIC
# MAGIC When ingesting this data, the users column must be read into the table as a STRING, and the cost column must be read as a BIGINT.
# MAGIC
# MAGIC In the first row of data, the value "Peter" will be read into the users column of the bronze table correctly. However, since the cost column contains a string like $100, it does not match the expected BIGINT type. As a result, this value will not be inserted into the cost column. Instead, it will be captured in the <code>_rescued_data</code> column, stored as a JSON-formatted string.
# MAGIC
# MAGIC In the second row, both values are valid: the value "zebi" is a STRING, and 300 is a BIGINT. Therefore, this row will be read into the bronze table without issues, and the <code>_rescued_data</code> column will remain empty for that row.
# MAGIC
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Conclusion
# MAGIC
# MAGIC In this lecture, you learned how the rescued data column works during data ingestion:
# MAGIC
# MAGIC - When input data does not match the expected schema, **mismatched values are captured in the `_rescued_data` column** as JSON-formatted strings instead of being dropped.
# MAGIC - The rescued data column is available when using **`read_files()`**, **`spark.read`**, or **Auto Loader**.
# MAGIC - Values that match the schema are ingested normally, and `_rescued_data` is `null` for those rows.
# MAGIC - This feature prevents silent data loss and allows you to inspect and address schema mismatches after ingestion.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next section, you will work hands-on with the rescued data column to handle schema mismatches during ingestion.

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