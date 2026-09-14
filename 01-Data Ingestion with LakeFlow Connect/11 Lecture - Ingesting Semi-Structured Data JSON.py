# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Ingesting Semi-Structured Data: JSON
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how ingesting semi-structured data such as JSON enables efficient parsing and transformation of complex, nested input into structured UC table for advanced analytics in the Lakehouse.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Describe the structure of JSON data** including objects, keys, values, nested objects, and arrays
# MAGIC 2. **Explain three approaches for working with JSON columns**: STRING, STRUCT, and VARIANT data types
# MAGIC 3. **Map JSON types to Databricks SQL data types** and define STRUCT schemas for nested JSON
# MAGIC 4. **Use `schema_of_json` and `from_json` to derive and apply schemas** when converting JSON strings to STRUCT columns
# MAGIC 5. **Describe the VARIANT data type** and its benefits for semi-structured data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. JSON Overview
# MAGIC
# MAGIC Ingesting semi-structured data like JSON enables efficient parsing and transformation of complex, nested input into structured UC tables for advanced analytics in the Lakehouse.
# MAGIC
# MAGIC Understanding the format of JSON is important because it affects how we parse and transform the data during ingestion.
# MAGIC
# MAGIC ##### Click each term to review the basic structure of a JSON file.
# MAGIC
# MAGIC <br>
# MAGIC <div style="max-width: 1100px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC .acc-wrap {
# MAGIC   display: flex;
# MAGIC   gap: 24px;
# MAGIC   align-items: flex-start;
# MAGIC }
# MAGIC
# MAGIC .acc-list {
# MAGIC   flex: 0 0 240px;
# MAGIC }
# MAGIC
# MAGIC .acc-item {
# MAGIC   border: 2px solid #D6D6D6;
# MAGIC   border-radius: 8px;
# MAGIC   margin-bottom: 8px;
# MAGIC   overflow: hidden;
# MAGIC   cursor: pointer;
# MAGIC }
# MAGIC
# MAGIC .acc-item.open { border-color: #5A5A5A; }
# MAGIC
# MAGIC .acc-header {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   gap: 14px;
# MAGIC   padding: 14px 18px;
# MAGIC   background: #F4F4F4;
# MAGIC   user-select: none;
# MAGIC }
# MAGIC
# MAGIC .acc-item.open .acc-header { background: #E0E0E0; }
# MAGIC
# MAGIC .acc-num {
# MAGIC   width: 32px; height: 32px;
# MAGIC   border-radius: 50%;
# MAGIC   background: #CECECE;
# MAGIC   color: #2C2C2C;
# MAGIC   font-size: 13pt;
# MAGIC   font-weight: 700;
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   flex-shrink: 0;
# MAGIC }
# MAGIC
# MAGIC .acc-item.open .acc-num { background: #5A5A5A; color: #fff; }
# MAGIC
# MAGIC .acc-summary {
# MAGIC   flex: 1;
# MAGIC   font-size: 14pt;
# MAGIC   font-weight: 600;
# MAGIC   color: #2C2C2C;
# MAGIC }
# MAGIC
# MAGIC .acc-chevron {
# MAGIC   font-size: 12pt;
# MAGIC   color: #5A5A5A;
# MAGIC   transition: transform 0.25s;
# MAGIC }
# MAGIC
# MAGIC .acc-item.open .acc-chevron { transform: rotate(-90deg); }
# MAGIC
# MAGIC /* ── Right content panel ── */
# MAGIC .acc-panel {
# MAGIC   flex: 1;
# MAGIC   min-height: 200px;
# MAGIC   background: #F9F7F4;
# MAGIC   border: 2px solid #EEEDE9;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 24px;
# MAGIC   font-size: 13pt;
# MAGIC   color: #0B2026;
# MAGIC   line-height: 1.7;
# MAGIC }
# MAGIC
# MAGIC .acc-panel-detail {
# MAGIC   margin-bottom: 16px;
# MAGIC   font-size: 13pt;
# MAGIC   color: #1B5162;
# MAGIC   line-height: 1.7;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="acc-wrap">
# MAGIC
# MAGIC   <!-- Left: accordion list -->
# MAGIC   <div class="acc-list" id="acc-container"></div>
# MAGIC
# MAGIC   <!-- Right: content panel -->
# MAGIC   <div class="acc-panel" id="acc-panel">
# MAGIC     <div style="color:#618794; font-size:12pt; font-style:italic;">
# MAGIC       Select a term on the left to see its description.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var ACC_DATA = [
# MAGIC
# MAGIC /* OBJECTS */
# MAGIC {
# MAGIC   summary: 'Objects',
# MAGIC   detail: 'JSON data is made up of JSON objects, which are typically enclosed in curly brackets {}.',
# MAGIC   fix: `
# MAGIC <div style="font-family: sans-serif; color: #0B2026;">
# MAGIC   <div style="display: flex; gap: 20px; align-items: flex-start;">
# MAGIC     <div style="flex: 1; background: #fff; border: 2px solid #EEEDE9; border-radius: 10px; padding: 18px; font-family: monospace; font-size: 12pt; line-height: 1.7;">
# MAGIC       <span style="color: #00A972;">{</span><br>
# MAGIC       &nbsp;&nbsp;<span style="color: #00A972;">"name"</span>: "John Doe",<br>
# MAGIC       &nbsp;&nbsp;<span style="color: #00A972;">"age"</span>: 35,<br>
# MAGIC       &nbsp;&nbsp;<span style="color: #00A972;">"address"</span>: {<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;"city": "Anytown",<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;"state": "CA"<br>
# MAGIC       &nbsp;&nbsp;},<br>
# MAGIC       &nbsp;&nbsp;<span style="color: #00A972;">"children"</span>: [<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;{ "name": "Owen", "age": 10 },<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;{ "name": "Eva", "age": 8 }<br>
# MAGIC       &nbsp;&nbsp;]<br>
# MAGIC       <span style="color: #00A972;">}</span>
# MAGIC     </div>
# MAGIC     <div style="flex: 0 0 220px;">
# MAGIC       <div style="background: #fff; border: 2px solid #00A972; border-radius: 10px; padding: 16px; font-size: 13pt;">
# MAGIC         <strong style="color: #00A972;">JSON objects</strong> are enclosed in <strong>curly brackets</strong> <code>{ }</code>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC `
# MAGIC },
# MAGIC
# MAGIC /* KEYS */
# MAGIC {
# MAGIC   summary: 'Keys',
# MAGIC   detail: `Within the curly brackets, JSON objects contain key-value pairs.
# MAGIC     <ul style="margin:8px 0 0 0; padding-left:18px;">
# MAGIC       <li>Each key is always a <strong>string</strong> enclosed in quotation marks</li>
# MAGIC       <li>Each key contains a <strong>value</strong></li>
# MAGIC     </ul>`,
# MAGIC   fix: `
# MAGIC <div style="font-family: sans-serif; color: #0B2026;">
# MAGIC   <div style="display: flex; gap: 20px; align-items: flex-start;">
# MAGIC     <div style="flex: 1; background: #fff; border: 2px solid #EEEDE9; border-radius: 10px; padding: 18px; font-family: monospace; font-size: 12pt; line-height: 1.7;">
# MAGIC       {<br>
# MAGIC       &nbsp;&nbsp;<strong style="color: #FF5F46;">"name"</strong>: "John Doe",<br>
# MAGIC       &nbsp;&nbsp;<strong style="color: #FF5F46;">"age"</strong>: 35,<br>
# MAGIC       &nbsp;&nbsp;<strong style="color: #FF5F46;">"address"</strong>: {<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;"city": "Anytown",<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;"state": "CA"<br>
# MAGIC       &nbsp;&nbsp;},<br>
# MAGIC       &nbsp;&nbsp;<strong style="color: #FF5F46;">"children"</strong>: [<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;{ "name": "Owen", "age": 10 },<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;{ "name": "Eva", "age": 8 }<br>
# MAGIC       &nbsp;&nbsp;]<br>
# MAGIC       }
# MAGIC     </div>
# MAGIC     <div style="flex: 0 0 220px;">
# MAGIC       <div style="background: #fff; border: 2px solid #FF5F46; border-radius: 10px; padding: 16px; font-size: 13pt;">
# MAGIC         <strong style="color: #FF5F46;">Keys</strong> are enclosed in <strong>quotation marks</strong>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC `
# MAGIC },
# MAGIC
# MAGIC /* VALUES */
# MAGIC {
# MAGIC   summary: 'Values',
# MAGIC   detail: `The value of a key can be a string, number, boolean, array, object, or null.
# MAGIC     <ul style="margin:8px 0 0 0; padding-left:18px;">
# MAGIC       <li>Objects can be <strong>flat</strong> or <strong>nested</strong></li>
# MAGIC       <li>The complexity depends on how the data is structured</li>
# MAGIC       <li>Understanding this format is important for parsing and transformation</li>
# MAGIC     </ul>`,
# MAGIC   fix: `
# MAGIC <div style="font-family: sans-serif; color: #0B2026;">
# MAGIC   <div style="display: flex; gap: 20px; align-items: flex-start;">
# MAGIC     <div style="flex: 1; background: #fff; border: 2px solid #EEEDE9; border-radius: 10px; padding: 18px; font-family: monospace; font-size: 12pt; line-height: 1.9;">
# MAGIC       {<br>
# MAGIC       &nbsp;&nbsp;"name": "John Doe", <span style="color:white; background:#00A972; border-radius:4px; padding:1px 7px; font-size:10pt;">STRING</span><br>
# MAGIC       &nbsp;&nbsp;"age": 35, <span style="color:white; background:#00A972; border-radius:4px; padding:1px 7px; font-size:10pt;">NUMERIC</span><br>
# MAGIC       &nbsp;&nbsp;"address": { <span style="color:white; background:#00A972; border-radius:4px; padding:1px 7px; font-size:10pt;">OBJECT</span><br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;"city": "Anytown",<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;"state": "CA"<br>
# MAGIC       &nbsp;&nbsp;},<br>
# MAGIC       &nbsp;&nbsp;"children": [ <span style="color:white; background:#00A972; border-radius:4px; padding:1px 7px; font-size:10pt;">ARRAY of OBJECTS</span><br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;{ "name": "Owen", "age": 10 },<br>
# MAGIC       &nbsp;&nbsp;&nbsp;&nbsp;{ "name": "Eva", "age": 8 }<br>
# MAGIC       &nbsp;&nbsp;]<br>
# MAGIC       }
# MAGIC     </div>
# MAGIC     <div style="flex: 0 0 160px; background: #fff; border: 2px solid #00A972; border-radius: 10px; padding: 16px;">
# MAGIC       <div style="font-size: 14pt; font-weight: 700; margin-bottom: 10px; color: #00A972;">Values</div>
# MAGIC       <ul style="font-size: 12pt; padding-left: 16px; margin: 0; line-height: 2.0;">
# MAGIC         <li>String</li>
# MAGIC         <li>Number</li>
# MAGIC         <li>Boolean</li>
# MAGIC         <li>Array</li>
# MAGIC         <li>Object</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC `
# MAGIC }
# MAGIC
# MAGIC ];
# MAGIC
# MAGIC (function() {
# MAGIC   var container = document.getElementById('acc-container');
# MAGIC   ACC_DATA.forEach(function(item, i) {
# MAGIC     var div = document.createElement('div');
# MAGIC     div.className = 'acc-item';
# MAGIC     div.innerHTML =
# MAGIC       '<div class="acc-header" onclick="toggleAcc(' + i + ')">'
# MAGIC       + '<div class="acc-num">' + (i + 1) + '</div>'
# MAGIC       + '<div class="acc-summary">' + item.summary + '</div>'
# MAGIC       + '<div class="acc-chevron">&#9654;</div>'
# MAGIC       + '</div>';
# MAGIC     container.appendChild(div);
# MAGIC   });
# MAGIC })();
# MAGIC
# MAGIC function toggleAcc(idx) {
# MAGIC   var items   = document.querySelectorAll('.acc-item');
# MAGIC   var panel   = document.getElementById('acc-panel');
# MAGIC   var wasOpen = items[idx].classList.contains('open');
# MAGIC
# MAGIC   items.forEach(function(el) { el.classList.remove('open'); });
# MAGIC
# MAGIC   if (!wasOpen) {
# MAGIC     items[idx].classList.add('open');
# MAGIC     var d = ACC_DATA[idx];
# MAGIC     panel.innerHTML =
# MAGIC       '<div class="acc-panel-detail">' + d.detail + '</div>'
# MAGIC       + '<div>' + d.fix + '</div>';
# MAGIC   } else {
# MAGIC     panel.innerHTML = '<div style="color:#618794; font-size:12pt; font-style:italic;">Select a term on the left to see its description.</div>';
# MAGIC   }
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Working with JSON-Formatted Columns
# MAGIC
# MAGIC When working with JSON data, it is common that after ingestion one or more columns in your table might contain JSON-formatted strings as values.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. JSON-Formatted STRING Column
# MAGIC
# MAGIC The question here is, how do you work with columns that store JSON formatted strings?
# MAGIC
# MAGIC This is a common scenario when JSON isn't fully parsed during ingestion, or when JSON data is embedded within another field, like a log message or a nested structure.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <table style="width: 100%; border-collapse: collapse; font-size: 13pt;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; border: 1px solid #EEEDE9;">json_column</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-family: monospace;">
# MAGIC         '{"name": "John Doe", "age": 35, "address": {"city": "Anytown", "state": "CA"}, "children": [{"name": "Owen", "age": 10}, {"name": "Eva", "age": 8}]}'
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9; font-family: monospace;">
# MAGIC         '{"name": "Kristi Doe", "age": 40, "address": {"city": "Anytown", "state": "CA"}, "children": [{"name": "Steve", "age": 10}]}'
# MAGIC       </td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 10px 14px; border: 1px solid #EEEDE9;">...</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC <br>
# MAGIC <div style="background: #F9F7F4; border: 2px solid #1B5162; border-radius: 10px; padding: 16px; margin-bottom: 16px; text-align: center; font-size: 14pt;">
# MAGIC   Columns in tables can hold <strong>JSON formatted strings</strong> as values
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. JSON-Formatted String Column Methods
# MAGIC
# MAGIC We'll explore techniques to parse, extract, and manipulate those JSON strings using SQL or DataFrame operations, so you can flatten and or access the nested fields just like regular columns.
# MAGIC
# MAGIC ##### Click on the tabs to switch between the approaches used while working with a JSON-formatted string column.
# MAGIC <br>
# MAGIC <div style="width:900px; max-width:100%; margin:auto;font-family:sans-serif;color:#0b2026;">
# MAGIC
# MAGIC <style>
# MAGIC   .json-tabs { display:flex; border-bottom:2px solid #EEEDE9; margin-bottom:0; }
# MAGIC   .json-tab {
# MAGIC     padding:10px 18px; border:none; background:none;
# MAGIC     font-size:14pt; font-weight:bold; color:#888; cursor:pointer;
# MAGIC     border-bottom:3px solid transparent; margin-bottom:-2px;
# MAGIC     transition:color 0.2s, border-color 0.2s;
# MAGIC   }
# MAGIC   .json-tab.t1.active { color:#4299E0; border-bottom:3px solid #4299E0; }
# MAGIC   .json-tab.t2.active { color:#00A972; border-bottom:3px solid #00A972; }
# MAGIC   .json-tab.t3.active { color:#FF5F46; border-bottom:3px solid #FF5F46; }
# MAGIC
# MAGIC   .json-panel { display:none; padding:24px 0 4px 0; }
# MAGIC   .json-panel.active { display:block; animation:fadeIn 0.22s ease; }
# MAGIC   @keyframes fadeIn { from{opacity:0;transform:translateY(5px)} to{opacity:1;transform:translateY(0)} }
# MAGIC
# MAGIC   .json-infobox {
# MAGIC     background:#F9F7F4; border-radius:10px;
# MAGIC     padding:18px 22px; margin-bottom:20px;
# MAGIC   }
# MAGIC   .json-infobox .box-title { font-size:16pt; font-weight:bold; margin-bottom:12px; }
# MAGIC   .json-infobox ul { font-size:14pt; line-height:1.8; padding-left:20px; margin:0; }
# MAGIC   .json-infobox li { margin-bottom:8px; }
# MAGIC   .json-infobox li:last-child { margin-bottom:0; }
# MAGIC
# MAGIC   .sql-row { display:flex; align-items:center; gap:10px; flex-wrap:wrap; margin-bottom:16px; }
# MAGIC   .sql-kw { background:#FFAB00; color:white; border-radius:4px; padding:4px 14px; font-family:monospace; font-size:13pt; font-weight:700; }
# MAGIC   .sql-expr { font-family:monospace; font-size:12pt; }
# MAGIC   .sql-arrow { font-size:22pt; color:#00A972; line-height:1; }
# MAGIC   .sql-result { border:2px solid #EEEDE9; border-radius:6px; padding:7px 14px; font-family:monospace; font-size:12pt; background:#FAFAF8; }
# MAGIC
# MAGIC   .map-table { width:100%; border-collapse:collapse; font-size:14pt; border-radius:8px; overflow:hidden; margin-top:4px; }
# MAGIC   .map-table thead tr { background:#1B5162; color:white; }
# MAGIC   .map-table thead th { padding:12px 16px; border:1px solid #2a6a7f; text-align:left; font-weight:700; }
# MAGIC   .map-table tbody tr:nth-child(odd)  { background:#F9F7F4; }
# MAGIC   .map-table tbody tr:nth-child(even) { background:white; }
# MAGIC   .map-table tbody tr:hover { background:rgba(0,169,114,0.08); }
# MAGIC   .map-table tbody td { padding:11px 16px; border:1px solid #EEEDE9; font-size:14pt; }
# MAGIC
# MAGIC   .variant-badge {
# MAGIC     margin-top:18px; background:#FF5F46; color:white;
# MAGIC     border-radius:6px; padding:8px 16px; font-size:13pt;
# MAGIC     font-weight:700; display:inline-block;
# MAGIC   }
# MAGIC
# MAGIC   .opening-statement {
# MAGIC     font-size:14pt; line-height:1.85; color:#0b2026;
# MAGIC     margin-bottom:24px;
# MAGIC   }
# MAGIC   .pill {
# MAGIC     display:inline; padding:2px 9px; border-radius:999px;
# MAGIC     font-weight:700; font-size:13pt; white-space:nowrap;
# MAGIC   }
# MAGIC   .pill-blue   { background:rgba(66,153,224,0.12); color:#2574B5; border:1.5px solid #4299E0; }
# MAGIC   .pill-green  { background:rgba(0,169,114,0.12);  color:#00865e; border:1.5px solid #00A972; }
# MAGIC   .pill-orange { background:rgba(255,95,70,0.12);  color:#d94028; border:1.5px solid #FF5F46; }
# MAGIC </style>
# MAGIC
# MAGIC <!-- TAB BAR -->
# MAGIC <div class="json-tabs">
# MAGIC   <button class="json-tab t1 active" onclick="switchJsonTab(1)">Approach 1: STRING</button>
# MAGIC   <button class="json-tab t2"        onclick="switchJsonTab(2)">Approach 2: STRUCT</button>
# MAGIC   <button class="json-tab t3"        onclick="switchJsonTab(3)">Approach 3: VARIANT</button>
# MAGIC </div>
# MAGIC
# MAGIC <!-- PANEL 1: STRING -->
# MAGIC <div class="json-panel active" id="jpanel1">
# MAGIC   One technique for working with a JSON-formatted string column is to access values directly from the STRING data type column.<br><br>
# MAGIC   <div class="json-infobox" style="border:2px solid #4299E0;">
# MAGIC     <ul>
# MAGIC       <li>JSON can be stored as a simple <strong>STRING</strong></li>
# MAGIC       <li>Can hold any JSON content without constraints — it is just raw text</li>
# MAGIC       <li>Less performant compared to typed approaches</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <br>
# MAGIC   <div class="sql-row">
# MAGIC     <div class="sql-kw">SELECT</div>
# MAGIC     <div class="sql-expr">json_column:name</div>
# MAGIC     <div class="sql-arrow">→</div>
# MAGIC     <div class="sql-result">John Doe</div>
# MAGIC   </div>
# MAGIC   <div class="sql-row">
# MAGIC     <div class="sql-kw">SELECT</div>
# MAGIC     <div class="sql-expr">json_column:address:city</div>
# MAGIC     <div class="sql-arrow">→</div>
# MAGIC     <div class="sql-result">Anytown</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <!-- PANEL 2: STRUCT -->
# MAGIC <div class="json-panel" id="jpanel2">
# MAGIC   Another method to work with a JSON-formatted string column is to convert the column to a STRUCT data type.<br><br>
# MAGIC   <div class="json-infobox" style="border:2px solid #00A972;">
# MAGIC     <ul>
# MAGIC       <li>You can parse JSON data into a <strong>STRUCT</strong> type with a defined schema</li>
# MAGIC       <li><strong>STRUCT enforces the JSON schema</strong>, ensuring data types and structure are consistent</li>
# MAGIC       <li>Is <strong>more efficient for querying</strong> than a JSON-formatted STRING</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC   <table class="map-table" style="margin-left: auto; margin-right: auto;">
# MAGIC     <thead>
# MAGIC       <tr>
# MAGIC         <th>JSON String Types</th>
# MAGIC         <th>Databricks SQL Data Type</th>
# MAGIC       </tr>
# MAGIC     </thead>
# MAGIC     <tbody>
# MAGIC       <tr><td>String</td>  <td><strong>STRING</strong></td></tr>
# MAGIC       <tr><td>Number</td>  <td><strong>INT / FLOAT / DOUBLE</strong></td></tr>
# MAGIC       <tr><td>Boolean</td> <td><strong>BOOLEAN</strong></td></tr>
# MAGIC       <tr><td>Object</td>  <td><strong>STRUCT &lt;&gt;</strong></td></tr>
# MAGIC       <tr><td>Array</td>   <td><strong>ARRAY &lt;&gt;</strong></td></tr>
# MAGIC     </tbody>
# MAGIC   </table>
# MAGIC </div>
# MAGIC <!-- PANEL 3: VARIANT -->
# MAGIC <div class="json-panel" id="jpanel3">
# MAGIC   You can also use the new VARIANT column data type.
# MAGIC   <br><br>
# MAGIC   <div class="json-infobox" style="border:2px solid #FF5F46;">
# MAGIC     <ul>
# MAGIC       <li>Can store <strong>any type of data</strong>, including JSON — ideal for <strong>semi-structured</strong> data</li>
# MAGIC       <li><strong>Highly flexible</strong> — no schema required upfront</li>
# MAGIC       <li><strong>Improved performance</strong> over existing STRING and STRUCT methods</li>
# MAGIC     </ul>
# MAGIC     <div class="variant-badge">Public Preview as of 2025 Q2</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC function switchJsonTab(n) {
# MAGIC   var colors = ["#4299E0","#00A972","#FF5F46"];
# MAGIC   for (var i = 1; i <= 3; i++) {
# MAGIC     document.getElementById('jpanel' + i).classList.remove('active');
# MAGIC     var t = document.querySelectorAll('.json-tab')[i-1];
# MAGIC     t.classList.remove('active');
# MAGIC     t.style.color = '#888';
# MAGIC     t.style.borderBottom = '3px solid transparent';
# MAGIC   }
# MAGIC   document.getElementById('jpanel' + n).classList.add('active');
# MAGIC   var active = document.querySelectorAll('.json-tab')[n-1];
# MAGIC   active.classList.add('active');
# MAGIC   active.style.color = colors[n-1];
# MAGIC   active.style.borderBottom = '3px solid ' + colors[n-1];
# MAGIC }
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <ol>
# MAGIC <li><strong>STRING Data Type</strong><br>
# MAGIC One technique for working with a JSON-formatted string column is to access values directly from the STRING data type column.
# MAGIC
# MAGIC - A column can simply store JSON data as a plain STRING
# MAGIC - Since it is stored as a string, the column can hold any JSON string without constraints
# MAGIC - However, this approach is less performant compared to typed approaches like STRUCT
# MAGIC
# MAGIC To access subfields within JSON-formatted string columns, you can use the colon (:) syntax.
# MAGIC
# MAGIC For example, if your column is named json_column, and you want to access the subfield "name", you would specify it as: json_column:name
# MAGIC
# MAGIC This syntax allows you to extract specific fields directly from the JSON string stored in the column.</li>
# MAGIC
# MAGIC <li><strong>STRUCT Data Type</strong><br>
# MAGIC Another method to work with a JSON-formatted string column is to convert the column to a STRUCT data type.
# MAGIC
# MAGIC - You can parse JSON data into a STRUCT type by defining a schema
# MAGIC - The STRUCT enforces the JSON schema, ensuring data types and structure are consistent
# MAGIC - Querying a STRUCT is more efficient than working with a raw JSON-formatted STRING.</li>
# MAGIC
# MAGIC <br>
# MAGIC <li><strong>VARIANT Data Type</strong><br>
# MAGIC The VARIANT data type is the newest approach for working with JSON data in Databricks.
# MAGIC
# MAGIC As of 2025 Q2, VARIANT is in <strong>public preview</strong>. Key benefits include:
# MAGIC
# MAGIC - Can store <strong>any type of data</strong>, including JSON, making it ideal for semi-structured data
# MAGIC - <strong>Highly flexible</strong>, adapting to different data shapes without rigid schemas
# MAGIC - Offers <strong>improved performance</strong> over existing methods (STRING and STRUCT)
# MAGIC
# MAGIC Keep an eye out for its General Availability (GA) release.</li>
# MAGIC </ol>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Converting JSON Formatted Strings as STRUCTS
# MAGIC
# MAGIC ##### Click a step to highlight the relevant JSON and STRUCT portions. 
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div style="width:100%;max-width:1120px;margin:0 auto;font-family:'Segoe UI',sans-serif;color:#0B2026;">
# MAGIC
# MAGIC <style>
# MAGIC .struct3-shell{
# MAGIC  display:grid;
# MAGIC  grid-template-columns: minmax(290px, 0.92fr) minmax(340px, 1.16fr) minmax(290px, 0.92fr);
# MAGIC  gap:14px;
# MAGIC  align-items:start;
# MAGIC }
# MAGIC
# MAGIC .struct3-panel{
# MAGIC  background:#F9F7F4;
# MAGIC  border:1.5px solid #e8e5e0;
# MAGIC  border-radius:12px;
# MAGIC  padding:14px;
# MAGIC  box-sizing:border-box;
# MAGIC  min-height:560px;
# MAGIC }
# MAGIC
# MAGIC .struct3-title{
# MAGIC  font-size:14pt;
# MAGIC  font-weight:800;
# MAGIC  margin-bottom:10px;
# MAGIC  color:#1b3139;
# MAGIC }
# MAGIC
# MAGIC /* code columns */
# MAGIC .code-card{
# MAGIC  background:#fff;
# MAGIC  border:1.5px solid #e0ddd8;
# MAGIC  border-radius:10px;
# MAGIC  padding:12px 14px;
# MAGIC  min-height:470px;
# MAGIC  transition:border-color .2s, box-shadow .2s;
# MAGIC }
# MAGIC .code-card.focus{
# MAGIC  border-color:#2574B5;
# MAGIC  box-shadow:0 3px 12px rgba(0,0,0,.08);
# MAGIC }
# MAGIC
# MAGIC .code-block-lite{
# MAGIC  font-family:Consolas, Monaco, monospace;
# MAGIC  font-size:14px;
# MAGIC  line-height:0.7;
# MAGIC }
# MAGIC .line{
# MAGIC  display:block;
# MAGIC  padding:0 6px;
# MAGIC  margin:1px 0;
# MAGIC  border-radius:4px;
# MAGIC  transition:.2s;
# MAGIC }
# MAGIC .line.hl{
# MAGIC  background:#ffd9d2;
# MAGIC  font-weight:700;
# MAGIC }
# MAGIC
# MAGIC /* middle steps */
# MAGIC .struct-steps{
# MAGIC  display:flex;
# MAGIC  flex-direction:column;
# MAGIC  gap:8px;
# MAGIC }
# MAGIC
# MAGIC .struct-step{
# MAGIC  background:#fff;
# MAGIC  border:1.5px solid #e8e5e0;
# MAGIC  border-radius:10px;
# MAGIC  transition:.2s;
# MAGIC  cursor:pointer;
# MAGIC }
# MAGIC .struct-step.active{
# MAGIC  border-color:var(--step-color);
# MAGIC  box-shadow:0 3px 12px rgba(0,0,0,.08);
# MAGIC }
# MAGIC .struct-step-head{
# MAGIC  display:flex;
# MAGIC  gap:10px;
# MAGIC  align-items:flex-start;
# MAGIC  padding:11px 12px;
# MAGIC }
# MAGIC .struct-step-num{
# MAGIC  width:28px;
# MAGIC  height:28px;
# MAGIC  border-radius:50%;
# MAGIC  display:flex;
# MAGIC  align-items:center;
# MAGIC  justify-content:center;
# MAGIC  color:#fff;
# MAGIC  font-size:14px;
# MAGIC  font-weight:800;
# MAGIC  flex-shrink:0;
# MAGIC }
# MAGIC .struct-step-title{
# MAGIC  font-size:12pt;
# MAGIC  font-weight:700;
# MAGIC  line-height:1.35;
# MAGIC  color:#1b3139;
# MAGIC  flex:1;
# MAGIC  word-break:break-word;
# MAGIC }
# MAGIC
# MAGIC @media (max-width: 1050px){
# MAGIC  .struct3-shell{
# MAGIC   grid-template-columns: 1fr;
# MAGIC  }
# MAGIC  .struct3-panel{
# MAGIC   min-height:auto;
# MAGIC  }
# MAGIC  .code-card{
# MAGIC   min-height:auto;
# MAGIC  }
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="struct3-shell">
# MAGIC
# MAGIC  <!-- LEFT: JSON -->
# MAGIC  <div class="struct3-panel">
# MAGIC   <div class="struct3-title">JSON formatted string</div>
# MAGIC   <div class="code-card" id="jsonBox">
# MAGIC    <div class="code-block-lite">
# MAGIC <span class="line" id="j1">{</span>
# MAGIC <span class="line" id="j2"> "name": "John Doe",</span>
# MAGIC <span class="line" id="j3"> "age": 35,</span>
# MAGIC <span class="line" id="j4"> "address": {</span>
# MAGIC <span class="line" id="j5"> "city": "Anytown",</span>
# MAGIC <span class="line" id="j6"> "state": "CA"</span>
# MAGIC <span class="line" id="j7"> },</span>
# MAGIC <span class="line" id="j8"> "children": [</span>
# MAGIC <span class="line" id="j9"> {</span>
# MAGIC <span class="line" id="j10"> "name": "Owen",</span>
# MAGIC <span class="line" id="j11"> "age": 10</span>
# MAGIC <span class="line" id="j12"> },</span>
# MAGIC <span class="line" id="j13"> {</span>
# MAGIC <span class="line" id="j14"> "name": "Eva",</span>
# MAGIC <span class="line" id="j15"> "age": 8</span>
# MAGIC <span class="line" id="j16"> }</span>
# MAGIC <span class="line" id="j17"> ]</span>
# MAGIC <span class="line" id="j18">}</span>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC
# MAGIC  <!-- MIDDLE: Steps -->
# MAGIC  <div class="struct3-panel">
# MAGIC   <div class="struct3-title">Steps</div>
# MAGIC
# MAGIC   <div class="struct-steps">
# MAGIC
# MAGIC    <div class="struct-step active" style="--step-color:#2574B5;" data-step="1" onclick="setStructStep(1)">
# MAGIC     <div class="struct-step-head">
# MAGIC      <div class="struct-step-num" style="background:#2574B5;">1</div>
# MAGIC      <div class="struct-step-title">Define the schema of the JSON formatted <br>string</div>
# MAGIC     </div>
# MAGIC    </div>
# MAGIC
# MAGIC    <div class="struct-step" style="--step-color:#E05A2B;" data-step="2" onclick="setStructStep(2)">
# MAGIC     <div class="struct-step-head">
# MAGIC      <div class="struct-step-num" style="background:#E05A2B;">2</div>
# MAGIC      <div class="struct-step-title">Specify the STRUCT data type to hold the <br>JSON formatted string</div>
# MAGIC     </div>
# MAGIC    </div>
# MAGIC
# MAGIC    <div class="struct-step" style="--step-color:#02A36F;" data-step="3" onclick="setStructStep(3)">
# MAGIC     <div class="struct-step-head">
# MAGIC      <div class="struct-step-num" style="background:#02A36F;">3</div>
# MAGIC      <div class="struct-step-title">Specify the STRING and INT data types <br>for the name and age keys</div>
# MAGIC     </div>
# MAGIC    </div>
# MAGIC
# MAGIC    <div class="struct-step" style="--step-color:#618794;" data-step="4" onclick="setStructStep(4)">
# MAGIC     <div class="struct-step-head">
# MAGIC      <div class="struct-step-num" style="background:#618794;">4</div>
# MAGIC      <div class="struct-step-title">The address key holds a STRUCT data <br>type with the keys city and state</div>
# MAGIC     </div>
# MAGIC    </div>
# MAGIC
# MAGIC    <div class="struct-step" style="--step-color:#1C3037;" data-step="5" onclick="setStructStep(5)">
# MAGIC     <div class="struct-step-head">
# MAGIC      <div class="struct-step-num" style="background:#1C3037;">5</div>
# MAGIC      <div class="struct-step-title">The children key holds an ARRAY <br>of STRUCTS</div>
# MAGIC     </div>
# MAGIC    </div>
# MAGIC
# MAGIC   </div>
# MAGIC  </div>
# MAGIC
# MAGIC  <!-- RIGHT: STRUCT -->
# MAGIC  <div class="struct3-panel">
# MAGIC   <div class="struct3-title">STRUCT schema</div>
# MAGIC   <div class="code-card" id="structBox">
# MAGIC    <div class="code-block-lite">
# MAGIC <span class="line" id="s1">STRUCT&lt;</span>
# MAGIC <span class="line" id="s2"> name: STRING,</span>
# MAGIC <span class="line" id="s3"> age: INT,</span>
# MAGIC <span class="line" id="s4"> address: STRUCT&lt;</span>
# MAGIC <span class="line" id="s5"> city: STRING,</span>
# MAGIC <span class="line" id="s6"> state: STRING</span>
# MAGIC <span class="line" id="s7"> &gt;,</span>
# MAGIC <span class="line" id="s8"> children: ARRAY&lt;</span>
# MAGIC <span class="line" id="s9"> STRUCT&lt;</span>
# MAGIC <span class="line" id="s10"> name: STRING,</span>
# MAGIC <span class="line" id="s11"> age: INT</span>
# MAGIC <span class="line" id="s12"> &gt;</span>
# MAGIC <span class="line" id="s13"> &gt;</span>
# MAGIC <span class="line" id="s14">&gt;</span>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC const structStepMap = {
# MAGIC  1: {
# MAGIC   jsonBox: true,
# MAGIC   structBox: true,
# MAGIC   json: [],
# MAGIC   struct: []
# MAGIC  },
# MAGIC  2: {
# MAGIC   jsonBox: false,
# MAGIC   structBox: false,
# MAGIC   json: ["j1", "j18"],
# MAGIC   struct: ["s1", "s14"]
# MAGIC  },
# MAGIC  3: {
# MAGIC   jsonBox: false,
# MAGIC   structBox: false,
# MAGIC   json: ["j2","j3"],
# MAGIC   struct: ["s2","s3"]
# MAGIC  },
# MAGIC  4: {
# MAGIC   jsonBox: false,
# MAGIC   structBox: false,
# MAGIC   json: ["j4","j5","j6","j7"],
# MAGIC   struct: ["s4","s5","s6","s7"]
# MAGIC  },
# MAGIC  5: {
# MAGIC   jsonBox: false,
# MAGIC   structBox: false,
# MAGIC   json: ["j8","j9","j10","j11","j12","j13","j14","j15","j16","j17"],
# MAGIC   struct: ["s8","s9","s10","s11","s12","s13"]
# MAGIC  }
# MAGIC };
# MAGIC
# MAGIC function clearStructHighlights() {
# MAGIC  document.querySelectorAll('.line').forEach(el => el.classList.remove('hl'));
# MAGIC  document.getElementById('jsonBox').classList.remove('focus');
# MAGIC  document.getElementById('structBox').classList.remove('focus');
# MAGIC  document.querySelectorAll('.struct-step').forEach(el => el.classList.remove('active'));
# MAGIC }
# MAGIC
# MAGIC function setStructStep(step) {
# MAGIC  clearStructHighlights();
# MAGIC
# MAGIC  const cfg = structStepMap[step];
# MAGIC  document.querySelector('.struct-step[data-step="' + step + '"]').classList.add('active');
# MAGIC
# MAGIC  if (cfg.jsonBox) document.getElementById('jsonBox').classList.add('focus');
# MAGIC  if (cfg.structBox) document.getElementById('structBox').classList.add('focus');
# MAGIC
# MAGIC  cfg.json.forEach(id => document.getElementById(id).classList.add('hl'));
# MAGIC  cfg.struct.forEach(id => document.getElementById(id).classList.add('hl'));
# MAGIC }
# MAGIC
# MAGIC setStructStep(1);
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Now, let’s go through the process of mapping a JSON-formatted STRING into a STRUCT column.
# MAGIC
# MAGIC - The first step is to define the schema, or structure, of the JSON-formatted string. Defining the schema tells Databricks how to interpret each part of the JSON string and convert it into the appropriate data types within a STRUCT.
# MAGIC - First, the `STRUCT<>` data type is used to hold the entire structure of the JSON-formatted string. It acts as a container for all the fields defined in the JSON, preserving their data types and hierarchy within a single column.
# MAGIC - Next, go through the JSON key-value pairs one by one and define the structure for each field.
# MAGIC - The `name` key contains a string value, while the `age` key contains an integer value.
# MAGIC - The `address` key contains another object, which is represented as a nested `STRUCT`. This nested `STRUCT` includes two keys: `city` and `state`. Both `city` and `state` use the string data type.
# MAGIC - The `children` key contains an `ARRAY` of `STRUCTS`. Each `STRUCT` within the array includes two keys: `name` and `age`. The `name` key contains a string value, and the `age` key contains an integer value.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Structure of the JSON String
# MAGIC
# MAGIC After reviewing how to map a JSON-formatted STRING to a STRUCT column, let's learn how to easily determine the structure of the JSON string.
# MAGIC
# MAGIC This can be done in two steps:
# MAGIC
# MAGIC 1. Get the **schema** of the JSON-formatted string using `schema_of_json`
# MAGIC 2. Use the **`from_json`** function to apply the schema and parse the column

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Step 1: Deriving the Schema with `schema_of_json`
# MAGIC
# MAGIC Instead of manually defining the schema, you can use the built-in `schema_of_json` function to automatically derive the schema from an example JSON string.
# MAGIC
# MAGIC Simply pass an example JSON-formatted string as the argument to this function, and it will return the inferred schema (structure).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 1500px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <div style="display: flex; gap: 24px; align-items: flex-start;">
# MAGIC   <!-- Left: JSON -->
# MAGIC   <div style="flex: 0 0 340px; background: #F9F7F4; border: 2px solid #EEEDE9; border-radius: 10px; padding: 16px; font-family: monospace; font-size: 14pt; line-height: 1.5;">
# MAGIC     {<br>
# MAGIC     &nbsp;&nbsp;"name": "John Doe",<br>
# MAGIC     &nbsp;&nbsp;"age": 35,<br>
# MAGIC     &nbsp;&nbsp;"address": {<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;"city": "Anytown",<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;"state": "CA"<br>
# MAGIC     &nbsp;&nbsp;},<br>
# MAGIC     &nbsp;&nbsp;"children": [<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;{ "name": "Owen", "age": 10 },<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;{ "name": "Eva", "age": 8 }<br>
# MAGIC     &nbsp;&nbsp;]<br>
# MAGIC     }
# MAGIC   </div>
# MAGIC   <!-- Right: Steps -->
# MAGIC   <div style="flex: 1;">
# MAGIC     <div style="margin-bottom: 14px; font-size: 14pt;">
# MAGIC       We can <strong>derive the schema</strong> of the JSON-formatted STRING column with <code>schema_of_json</code>
# MAGIC     </div><br>
# MAGIC     <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 14px;">
# MAGIC       <div style="background: #FF5F46; color: white; border-radius: 50%; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center; font-weight: 700;">1</div>
# MAGIC       <div style="font-size: 14pt; font-weight: 600;">Get the schema</div>
# MAGIC     </div>
# MAGIC     <div style="background: #F9F7F4; border: 2px solid #FF5F46; border-radius: 8px; padding: 14px; font-family: monospace; font-size: 14pt; margin-bottom: 14px;">
# MAGIC       <span style="color: #4299E0; font-weight: 700;">SELECT</span> schema_of_json(<span style="color: #00A972;">'sample-json-string'</span>)
# MAGIC     </div>
# MAGIC     <div style="font-size: 13pt; color: #618794;">
# MAGIC       The function returns the <strong>structure</strong> of the JSON formatted string
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="display:flex; align-items:center; justify-content:center;">
# MAGIC     <div style="font-size:20px; color:#0b2c33;"><br><br><br><br><br><br>→</div>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; background: #F9F7F4; border: 2px solid #FF5F46; border-radius: 10px; padding: 18px; font-family: monospace; font-size: 14pt; line-height: 1.6;">
# MAGIC     <span style="color: #FF5F46; font-weight: 700;">STRUCT</span>&lt;<br>
# MAGIC     &nbsp;&nbsp;<span style="color: #4299E0;">name</span>: STRING,<br>
# MAGIC     &nbsp;&nbsp;<span style="color: #4299E0;">age</span>: INT,<br>
# MAGIC     &nbsp;&nbsp;<span style="color: #4299E0;">address</span>: <span style="color: #FF5F46; font-weight: 700;">STRUCT</span>&lt;<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;city: STRING,<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;state: STRING<br>
# MAGIC     &nbsp;&nbsp;&gt;,<br>
# MAGIC     &nbsp;&nbsp;<span style="color: #4299E0;">children</span>: <span style="color: #FF5F46; font-weight: 700;">ARRAY</span>&lt;<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #FF5F46; font-weight: 700;">STRUCT</span>&lt;<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;name: STRING,<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;age: INT<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;&gt;<br>
# MAGIC     &nbsp;&nbsp;&gt;<br>
# MAGIC     &gt;
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Simply pass an example JSON-formatted string as the argument to this function, and it will return the inferred schema (structure).
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Step 2: Parsing JSON with `from_json`
# MAGIC
# MAGIC Once you have the structure of the JSON-formatted string, you can use the Spark **`from_json`** function. This function takes the JSON string and the specified schema you obtained in the previous step, and returns a STRUCT column.
# MAGIC
# MAGIC Using `from_json` will create a new column with the STRUCT data type, containing the parsed JSON data according to the defined schema.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC <div style="display: flex; gap: 24px; align-items: flex-start;">
# MAGIC   <!-- Left: STRUCT schema -->
# MAGIC   <div style="flex: 0 0 320px; background: #F9F7F4; border: 2px solid #EEEDE9; border-radius: 10px; padding: 16px; font-family: monospace; font-size: 14pt; line-height: 1.5;">
# MAGIC     STRUCT&lt;<br>
# MAGIC     &nbsp;&nbsp;name: STRING,<br>
# MAGIC     &nbsp;&nbsp;age: INT,<br>
# MAGIC     &nbsp;&nbsp;address: STRUCT&lt;<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;city: STRING,<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;state: STRING<br>
# MAGIC     &nbsp;&nbsp;&gt;,<br>
# MAGIC     &nbsp;&nbsp;children: ARRAY&lt;<br>
# MAGIC     &nbsp;&nbsp;&nbsp;&nbsp;STRUCT&lt;name: STRING, age: INT&gt;<br>
# MAGIC     &nbsp;&nbsp;&gt;<br>
# MAGIC     &gt;
# MAGIC   </div>
# MAGIC   <!-- Right: from_json -->
# MAGIC   <div style="flex: 1;">
# MAGIC     <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 14px;">
# MAGIC       <div style="background: #FF5F46; color: white; border-radius: 50%; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center; font-weight: 700;">2</div>
# MAGIC       <div style="font-size: 14pt; font-weight: 600;">Apply the schema with from_json</div>
# MAGIC     </div>
# MAGIC     <div style="background: #F9F7F4; border: 2px solid #FF5F46; border-radius: 8px; padding: 14px; font-family: monospace; font-size: 14pt;">
# MAGIC       <span style="color: #4299E0; font-weight: 700;">SELECT</span> from_json(json_col, <span style="color: #00A972;">'json-struct-schema'</span>) <span style="color: #4299E0; font-weight: 700;">AS</span> struct_column
# MAGIC       <span style="color: #4299E0; font-weight: 700;">FROM</span> table
# MAGIC     </div><br>
# MAGIC     <div style="margin-bottom: 14px; font-size: 14pt;">
# MAGIC       The <code>from_json</code> function returns a <strong>struct column</strong> using the JSON string and specified schema
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Conclusion
# MAGIC
# MAGIC In this lecture, you learned how to work with semi-structured JSON data in Databricks:
# MAGIC
# MAGIC - **JSON structure**: Objects enclosed in curly brackets contain key-value pairs. Values can be strings, numbers, booleans, arrays, or nested objects.
# MAGIC - **Three approaches** for working with JSON-formatted columns:
# MAGIC   - **STRING**: Simple but less performant. JSON stored as raw text.
# MAGIC   - **STRUCT**: Parse JSON with a defined schema. Enforces structure and is more efficient for querying.
# MAGIC   - **VARIANT**: The newest approach (public preview). Highly flexible with improved performance.
# MAGIC - **JSON to STRUCT conversion** requires mapping JSON types to Databricks SQL types (STRING, INT, BOOLEAN, STRUCT<>, ARRAY<>).
# MAGIC - Use **`schema_of_json`** to automatically derive the schema from a sample JSON string.
# MAGIC - Use **`from_json`** to parse a JSON string column into a STRUCT column using the derived schema.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next section, you will work hands-on with JSON data, parsing and transforming JSON-formatted columns using these techniques.

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