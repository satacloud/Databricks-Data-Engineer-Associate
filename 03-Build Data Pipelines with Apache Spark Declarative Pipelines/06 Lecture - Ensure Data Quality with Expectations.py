# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Ensure Data Quality with Expectations
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how to create and add data quality expectations to your pipelines, apply expectations to validate streaming data, and understand the available actions for handling records that fail data quality checks.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Explain what expectations are** and how they are used to enforce data quality rules in Apache Spark™ Declarative Pipelines
# MAGIC 2. **Write the CONSTRAINT syntax** to define data quality expectations on streaming tables
# MAGIC 3. **Describe the three violation actions** — WARN, DROP, and FAIL — and explain when to use each
# MAGIC 4. **Apply expectations to a streaming table** using SQL with all three violation actions
# MAGIC 5. **Trace how a row of data is evaluated** through expectations in the pipeline and explain what happens when a constraint is violated

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A. What Are Expectations
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC   <div style="display:flex; gap:24px; align-items:flex-start; flex-wrap:wrap;">
# MAGIC     <!-- Column 1 -->
# MAGIC     <div style="flex:1 1 0; min-width:260px;">
# MAGIC       Apache Spark™ Declarative Pipelines lets you apply <b>data quality rules</b> called <b>expectations</b> during your ETL processes. 
# MAGIC       
# MAGIC   These rules validate your data row-by-row to ensure integrity.
# MAGIC     </div>
# MAGIC     <!-- Column 2 (empty placeholder for now) -->
# MAGIC     <div style="flex:1 1 0; min-width:260px;">
# MAGIC       <div style="background:#F9F7F4; border:2px solid #EEEDE9; border-radius:10px; padding:16px 18px;">
# MAGIC         <div style="font-size:9pt; font-weight:800; color:#618794; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:10px;">
# MAGIC           General Syntax
# MAGIC         </div>
# MAGIC <div class="code-block" data-language="sql">
# MAGIC CONSTRAINT constraint_name
# MAGIC EXPECT (column_condition)
# MAGIC [ON VIOLATION action]
# MAGIC </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC     if (block.getAttribute('data-processed')) return;
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML = 
# MAGIC       '<div style="position:relative;margin:16px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre>' +
# MAGIC       '</div>';
# MAGIC     var codeEl = document.getElementById(id);
# MAGIC     codeEl.textContent = code;
# MAGIC     Prism.highlightElement(codeEl);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var t = document.createElement('textarea');
# MAGIC       t.value = code;
# MAGIC       document.body.appendChild(t);
# MAGIC       t.select();
# MAGIC       document.execCommand('copy');
# MAGIC       document.body.removeChild(t);
# MAGIC       this.textContent = '✓ Copied!';
# MAGIC       setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. The Three Violation Actions
# MAGIC
# MAGIC ##### Click each tab below to explore the syntax and use case of the three violation actions.
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC
# MAGIC   <style>
# MAGIC     /* Card layout (mirrors Batch Ingestion style) */
# MAGIC     .exp-grid {
# MAGIC       display: flex;
# MAGIC       flex-direction: column;
# MAGIC       gap: 40px;
# MAGIC       justify-content: center;
# MAGIC       align-items: center;
# MAGIC       margin-top: 16px;
# MAGIC     }
# MAGIC     .exp-box {
# MAGIC       width: 100%;
# MAGIC       background: #F9F7F4;
# MAGIC       border-radius: 8px;
# MAGIC       box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC       padding: 16px 18px 18px 18px;
# MAGIC       box-sizing: border-box;
# MAGIC       position: relative;
# MAGIC       overflow: hidden;
# MAGIC       display: flex;
# MAGIC       flex-direction: column;
# MAGIC       gap: 12px;
# MAGIC     }
# MAGIC     .exp-box::before {
# MAGIC       content: "";
# MAGIC       position: absolute;
# MAGIC       top: 0; left: 0;
# MAGIC       width: 100%; height: 6px;
# MAGIC     }
# MAGIC     .exp-box.warn::before  { background: #FFAB00; }
# MAGIC     .exp-box.drop::before  { background: #FF5F46; }
# MAGIC     .exp-box.fail::before  { background: #98102A; }
# MAGIC
# MAGIC     .exp-box-header {
# MAGIC       display: flex;
# MAGIC       align-items: center;
# MAGIC       justify-content: space-between;
# MAGIC     }
# MAGIC     .exp-box-title {
# MAGIC       font-size: 16pt;
# MAGIC       font-weight: 700;
# MAGIC       color: #0B2026;
# MAGIC     }
# MAGIC     .exp-box-subtitle {
# MAGIC       font-size: 14pt;
# MAGIC       color: #64748b;
# MAGIC       margin-top: 2px;
# MAGIC     }
# MAGIC     .exp-pane {
# MAGIC       display: flex;
# MAGIC       flex-direction: column;
# MAGIC       gap: 10px;
# MAGIC     }
# MAGIC     .exp-sql-box {
# MAGIC       background:#FDFDFD;
# MAGIC       border-radius:8px;
# MAGIC       border:1px solid #E0E4E7;
# MAGIC       padding:10px 12px;
# MAGIC     }
# MAGIC     .exp-sql-title {
# MAGIC       font-size:14px;
# MAGIC       font-weight:700;
# MAGIC       color:#1B3139;
# MAGIC       margin-bottom:4px;
# MAGIC       text-transform:uppercase;
# MAGIC       letter-spacing:0.06em;
# MAGIC     }
# MAGIC     .exp-code {
# MAGIC       background:#FFFFFF;
# MAGIC       border-radius:6px;
# MAGIC       border:1px solid #E0E4E7;
# MAGIC       padding:10px 12px;
# MAGIC       font-family:Consolas, Monaco, 'Courier New', monospace;
# MAGIC       font-size:14px;
# MAGIC       line-height:1.7;
# MAGIC       color:#1B3139;
# MAGIC       overflow-x:auto;
# MAGIC       margin:0;
# MAGIC     }
# MAGIC
# MAGIC     .exp-result-box {
# MAGIC       background:#FFFFFF;
# MAGIC       border-radius:8px;
# MAGIC       border:1px solid #E0E4E7;
# MAGIC       padding:10px 14px;
# MAGIC       font-size:14px;
# MAGIC       color:#0B2026;
# MAGIC       line-height:1.7;
# MAGIC     }
# MAGIC     .exp-result-box ul {
# MAGIC       margin:6px 0 0 1.1em;
# MAGIC       padding:0;
# MAGIC     }
# MAGIC
# MAGIC     /* Tabs (same pattern as Batch carousel) */
# MAGIC     .expect-tabbar {
# MAGIC       display: flex;
# MAGIC       border-bottom: 2px solid #EEEDE9;
# MAGIC       margin-bottom: 0;
# MAGIC     }
# MAGIC     .expect-tab {
# MAGIC       padding: 10px 18px;
# MAGIC       border: none;
# MAGIC       border-bottom: 3px solid transparent;
# MAGIC       background: none;
# MAGIC       font-size: 14pt;
# MAGIC       font-weight: bold;
# MAGIC       color: #888;
# MAGIC       cursor: pointer;
# MAGIC       margin-bottom: -2px;
# MAGIC     }
# MAGIC
# MAGIC     .expect-panel { display:none; }
# MAGIC   </style>
# MAGIC
# MAGIC   <!-- Tabs -->
# MAGIC   <div class="expect-tabbar">
# MAGIC     <button class="expect-tab" onclick="showExpectTab(1)">WARN</button>
# MAGIC     <button class="expect-tab" onclick="showExpectTab(2)">DROP</button>
# MAGIC     <button class="expect-tab" onclick="showExpectTab(3)">FAIL</button>
# MAGIC   </div>
# MAGIC   <!-- WARN panel -->
# MAGIC   <div class="expect-panel">
# MAGIC     <div class="exp-grid">
# MAGIC       <div class="exp-box warn">
# MAGIC         <div class="exp-box-header">
# MAGIC           <div>
# MAGIC             <div class="exp-box-title">WARN — Log Violations, Keep Rows</div>
# MAGIC             <div class="exp-box-subtitle">Default behavior when no action is specified</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="exp-pane">
# MAGIC           <div class="exp-sql-box">
# MAGIC             <div class="exp-sql-title">SQL Syntax Example</div>
# MAGIC <div class="code-block" data-language="sql">
# MAGIC CONSTRAINT valid_notification
# MAGIC EXPECT (notifications IN ('Y','N'))
# MAGIC </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC     if (block.getAttribute('data-processed')) return;
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:16px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre>' +
# MAGIC       '</div>';
# MAGIC     var codeEl = document.getElementById(id);
# MAGIC     codeEl.textContent = code;
# MAGIC     Prism.highlightElement(codeEl);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var t = document.createElement('textarea');
# MAGIC       t.value = code;
# MAGIC       document.body.appendChild(t);
# MAGIC       t.select();
# MAGIC       document.execCommand('copy');
# MAGIC       document.body.removeChild(t);
# MAGIC       this.textContent = '✓ Copied!';
# MAGIC       setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>
# MAGIC           </div>
# MAGIC           <div class="exp-result-box">
# MAGIC             <strong style="color:#FFAB00;">Result</strong>
# MAGIC             <ul>
# MAGIC               <li>Invalid rows are still written to the target.</li>
# MAGIC               <li>Logs include counts of valid vs. invalid records and other metrics.</li>
# MAGIC             </ul>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- DROP panel -->
# MAGIC   <div class="expect-panel">
# MAGIC     <div class="exp-grid">
# MAGIC       <div class="exp-box drop">
# MAGIC         <div class="exp-box-header">
# MAGIC           <div>
# MAGIC             <div class="exp-box-title">DROP — Exclude Invalid Rows</div>
# MAGIC             <div class="exp-box-subtitle">Remove bad records from the target table</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="exp-pane">
# MAGIC           <div class="exp-sql-box">
# MAGIC             <div class="exp-sql-title">SQL Syntax Example</div>
# MAGIC <div class="code-block" data-language="sql">
# MAGIC CONSTRAINT valid_date
# MAGIC EXPECT (order_timestamp > "2021-01-01")
# MAGIC ON VIOLATION DROP ROW
# MAGIC </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC     if (block.getAttribute('data-processed')) return;
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:16px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre>' +
# MAGIC       '</div>';
# MAGIC     var codeEl = document.getElementById(id);
# MAGIC     codeEl.textContent = code;
# MAGIC     Prism.highlightElement(codeEl);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var t = document.createElement('textarea');
# MAGIC       t.value = code;
# MAGIC       document.body.appendChild(t);
# MAGIC       t.select();
# MAGIC       document.execCommand('copy');
# MAGIC       document.body.removeChild(t);
# MAGIC       this.textContent = '✓ Copied!';
# MAGIC       setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>
# MAGIC           </div>
# MAGIC           <div class="exp-result-box">
# MAGIC             <strong style="color:#FF5F46;">Result</strong>
# MAGIC             <ul>
# MAGIC               <li>Invalid rows are dropped from the table.</li>
# MAGIC               <li>The count of dropped rows is logged alongside other metrics.</li>
# MAGIC             </ul>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- FAIL panel -->
# MAGIC   <div class="expect-panel">
# MAGIC     <div class="exp-grid">
# MAGIC       <div class="exp-box fail">
# MAGIC         <div class="exp-box-header">
# MAGIC           <div>
# MAGIC             <div class="exp-box-title">FAIL — Stop the Flow on Violations</div>
# MAGIC             <div class="exp-box-subtitle">Fail a specific flow when constraints are broken</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="exp-pane">
# MAGIC           <div class="exp-sql-box">
# MAGIC             <div class="exp-sql-title">SQL Syntax Example</div>
# MAGIC <div class="code-block" data-language="sql">
# MAGIC CONSTRAINT valid_id
# MAGIC EXPECT (customer_id IS NOT NULL)
# MAGIC ON VIOLATION FAIL UPDATE
# MAGIC </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC     if (block.getAttribute('data-processed')) return;
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:16px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre>' +
# MAGIC       '</div>';
# MAGIC     var codeEl = document.getElementById(id);
# MAGIC     codeEl.textContent = code;
# MAGIC     Prism.highlightElement(codeEl);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var t = document.createElement('textarea');
# MAGIC       t.value = code;
# MAGIC       document.body.appendChild(t);
# MAGIC       t.select();
# MAGIC       document.execCommand('copy');
# MAGIC       document.body.removeChild(t);
# MAGIC       this.textContent = '✓ Copied!';
# MAGIC       setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>
# MAGIC           </div>
# MAGIC           <div class="exp-result-box">
# MAGIC             <strong style="color:#98102A;">Result</strong>
# MAGIC             <ul>
# MAGIC               <li>Causes a failure of a single flow and does not cause other flows in your pipeline to fail.</li>
# MAGIC               <li>Manual intervention is required to resolve the issue.</li>
# MAGIC             </ul>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC function showExpectTab(n) {
# MAGIC   var tabs   = document.getElementsByClassName("expect-tab");
# MAGIC   var panels = document.getElementsByClassName("expect-panel");
# MAGIC   var colors = ["#FFAB00", "#FF5F46", "#98102A"];
# MAGIC   for (var i = 0; i < tabs.length; i++) {
# MAGIC     tabs[i].style.color = "#888";
# MAGIC     tabs[i].style.borderBottom = "3px solid transparent";
# MAGIC     panels[i].style.display = "none";
# MAGIC   }
# MAGIC   tabs[n-1].style.color = colors[n-1];
# MAGIC   tabs[n-1].style.borderBottom = "3px solid " + colors[n-1];
# MAGIC   panels[n-1].style.display = "block";
# MAGIC }
# MAGIC // default to WARN
# MAGIC window.addEventListener("DOMContentLoaded", function(){ showExpectTab(1); });
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. Adding Expectations — Full SQL Example
# MAGIC
# MAGIC Let's look at an example of how to add constraints in Declarative Pipelines. Here we will create a streaming table named <code>orders_silver</code> with all three constraint types applied.
# MAGIC
# MAGIC ##### Click each step below to explore how the constraints are added.
# MAGIC <br>
# MAGIC
# MAGIC <div style="width:100%; font-family:'Segoe UI',sans-serif; max-width:1000px; margin:0 auto;">
# MAGIC
# MAGIC <style>
# MAGIC .journey-wrap8 { display:flex; flex-direction:column; gap:0; position:relative; }
# MAGIC .journey-wrap8::before {
# MAGIC   content:""; position:absolute; left:27px; top:44px; bottom:44px;
# MAGIC   width:2px; background:linear-gradient(to bottom, #2272B4, #FFAB00, #FF5F46, #98102A);
# MAGIC   z-index:0;
# MAGIC }
# MAGIC .step-row8 { display:flex; align-items:flex-start; gap:18px; cursor:pointer; position:relative; z-index:1; margin-bottom:6px; }
# MAGIC .step-badge8 {
# MAGIC   width:56px; height:56px; border-radius:50%; display:flex; align-items:center; justify-content:center;
# MAGIC   font-size:20px; font-weight:800; color:white; flex-shrink:0; border:3px solid white;
# MAGIC   box-shadow:0 2px 10px rgba(0,0,0,0.15); transition:transform 0.2s, box-shadow 0.2s; z-index:2; position:relative;
# MAGIC }
# MAGIC .step-row8:hover .step-badge8 { transform:scale(1.1); box-shadow:0 6px 18px rgba(0,0,0,0.2); }
# MAGIC .step-card8 { flex:1; background:#F9F7F4; border-radius:10px; border:1.5px solid #EEEDE9; overflow:hidden; transition:box-shadow 0.25s, border-color 0.25s; }
# MAGIC .step-card8:hover { box-shadow:0 4px 18px rgba(0,0,0,0.10); }
# MAGIC .step-card8.active { border-color:var(--sc8); box-shadow:0 4px 18px rgba(0,0,0,0.12); }
# MAGIC .step-header8 { display:flex; align-items:center; justify-content:space-between; padding:14px 18px; background:#F9F7F4; }
# MAGIC .step-header8-left { display:flex; align-items:center; gap:10px; }
# MAGIC .step-dot8 { width:10px; height:10px; border-radius:50%; background:var(--sc8); flex-shrink:0; }
# MAGIC .step-title8 { font-size:13pt; font-weight:700; color:#0B2026; }
# MAGIC .step-subtitle8 { font-size:11pt; color:#618794; margin-top:1px; }
# MAGIC .step-chevron8 { font-size:14px; color:#90A5B1; transition:transform 0.25s; user-select:none; }
# MAGIC .step-card8.active .step-chevron8 { transform:rotate(180deg); color:var(--sc8); }
# MAGIC .step-body8 { display:none; padding:0 18px 18px 18px; border-top:1.5px solid #EEEDE9; animation:expandIn8 0.25s ease; }
# MAGIC @keyframes expandIn8 { from{opacity:0;transform:translateY(-8px);} to{opacity:1;transform:translateY(0);} }
# MAGIC .step-card8.active .step-body8 { display:block; }
# MAGIC .diag-box8 { background:white; border-radius:8px; border:1.5px solid #DCE0E2; padding:14px 16px; margin-top:12px; }
# MAGIC .step-desc8 { font-size:11pt; color:#1B3139; line-height:1.8; margin-top:12px; }
# MAGIC .step-desc8 strong { color:#0B2026; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="journey-wrap8">
# MAGIC
# MAGIC   <!-- STEP 1 -->
# MAGIC   <div class="step-row8" onclick="toggleStep8('c1')">
# MAGIC     <div class="step-badge8" style="background:#2272B4;">1</div>
# MAGIC     <div class="step-card8" id="c1" style="--sc8:#2272B4;">
# MAGIC       <div class="step-header8">
# MAGIC         <div class="step-header8-left">
# MAGIC           <div class="step-dot8"></div>
# MAGIC           <div>
# MAGIC             <div class="step-title8">Define the Streaming Table</div>
# MAGIC             <div class="step-subtitle8">CREATE OR REFRESH STREAMING TABLE with constraint block</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="step-chevron8">▼</div>
# MAGIC       </div>
# MAGIC       <div class="step-body8">
# MAGIC         <div class="diag-box8">
# MAGIC           <div class="code-block" data-language="sql">
# MAGIC CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver
# MAGIC  (
# MAGIC    -- constraints go here
# MAGIC  )
# MAGIC AS
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 2: WARN -->
# MAGIC   <div class="step-row8" onclick="toggleStep8('c2')">
# MAGIC     <div class="step-badge8" style="background:#FFAB00;">2</div>
# MAGIC     <div class="step-card8" id="c2" style="--sc8:#FFAB00;">
# MAGIC       <div class="step-header8">
# MAGIC         <div class="step-header8-left">
# MAGIC           <div class="step-dot8"></div>
# MAGIC           <div>
# MAGIC             <div class="step-title8">WARN Constraint — valid_notifications</div>
# MAGIC             <div class="step-subtitle8">WARN in metrics</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="step-chevron8">▼</div>
# MAGIC       </div>
# MAGIC       <div class="step-body8">
# MAGIC         <div class="diag-box8">
# MAGIC           <div class="code-block" data-language="sql">
# MAGIC CONSTRAINT valid_notifications
# MAGIC EXPECT (notifications IN ('Y','N')),
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 3: FAIL -->
# MAGIC   <div class="step-row8" onclick="toggleStep8('c3')">
# MAGIC     <div class="step-badge8" style="background:#FF5F46;">3</div>
# MAGIC     <div class="step-card8" id="c3" style="--sc8:#FF5F46;">
# MAGIC       <div class="step-header8">
# MAGIC         <div class="step-header8-left">
# MAGIC           <div class="step-dot8"></div>
# MAGIC           <div>
# MAGIC             <div class="step-title8">FAIL Constraint — valid_date</div>
# MAGIC             <div class="step-subtitle8">FAIL the pipeline</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="step-chevron8">▼</div>
# MAGIC       </div>
# MAGIC       <div class="step-body8">
# MAGIC         <div class="diag-box8">
# MAGIC           <div class="code-block" data-language="sql">
# MAGIC CONSTRAINT valid_date
# MAGIC EXPECT (order_timestamp > "2021-01-01")
# MAGIC ON VIOLATION FAIL UPDATE,
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- STEP 4: DROP -->
# MAGIC   <div class="step-row8" onclick="toggleStep8('c4')">
# MAGIC     <div class="step-badge8" style="background:#98102A;">4</div>
# MAGIC     <div class="step-card8" id="c4" style="--sc8:#98102A;">
# MAGIC       <div class="step-header8">
# MAGIC         <div class="step-header8-left">
# MAGIC           <div class="step-dot8"></div>
# MAGIC           <div>
# MAGIC             <div class="step-title8">DROP Constraint — valid_id</div>
# MAGIC             <div class="step-subtitle8">DROP the row</div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <div class="step-chevron8">▼</div>
# MAGIC       </div>
# MAGIC       <div class="step-body8">
# MAGIC         <div class="diag-box8">
# MAGIC           <div class="code-block" data-language="sql">
# MAGIC CONSTRAINT valid_id
# MAGIC EXPECT (customer_id IS NOT NULL)
# MAGIC ON VIOLATION DROP ROW
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- RESULT -->
# MAGIC <div style="margin-top:20px; padding:18px 24px; background:#F9F7F4; border:3px solid #FF5F46; border-radius:10px;">
# MAGIC   <div style="font-weight:700; margin-bottom:8px; font-size:14pt; color:#0B2026;">
# MAGIC     <b>RESULT</b>: <code>orders_silver</code> with all three data quality constraints
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="code-block" data-language="sql">
# MAGIC CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver
# MAGIC  (
# MAGIC    CONSTRAINT valid_notifications EXPECT (notifications IN ('Y','N')),
# MAGIC    CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE,
# MAGIC    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC  )
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   timestamp(order_timestamp) AS order_timestamp,
# MAGIC   customer_id,
# MAGIC   notifications
# MAGIC FROM STREAM 1_bronze_db.orders_bronze;
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   // Copy + light-background code-blocks
# MAGIC   document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC     if (block.getAttribute('data-processed')) return;
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML =
# MAGIC       '<div style="position:relative;margin:16px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;">' +
# MAGIC           '<code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code>' +
# MAGIC         '</pre>' +
# MAGIC       '</div>';
# MAGIC     var codeEl = document.getElementById(id);
# MAGIC     codeEl.textContent = code;
# MAGIC     Prism.highlightElement(codeEl);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var t = document.createElement('textarea');
# MAGIC       t.value = code;
# MAGIC       document.body.appendChild(t);
# MAGIC       t.select();
# MAGIC       document.execCommand('copy');
# MAGIC       document.body.removeChild(t);
# MAGIC       this.textContent = '✓ Copied!';
# MAGIC       setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC
# MAGIC function toggleStep8(id) {
# MAGIC   var card = document.getElementById(id);
# MAGIC   var isActive = card.classList.contains("active");
# MAGIC   document.querySelectorAll(".step-card8").forEach(function(c){ c.classList.remove("active"); });
# MAGIC   if (!isActive) { card.classList.add("active"); }
# MAGIC }
# MAGIC window.addEventListener("DOMContentLoaded", function(){ toggleStep8("c1"); });
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## D. Actions Overview — How Expectations Work in the Pipeline
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_ensure_data_quality/actions_overview.png"
# MAGIC     alt="LakeFlow Connect Unified Ingestion"
# MAGIC     style="width: 900px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC <!-- Q2 2025 Note -->
# MAGIC <div style="border-left:4px solid #ff9800; background:#fff3e0; padding:14px 18px; border-radius:4px; margin:0;">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size:1.1em;">Important — As of Q2 2025</strong>
# MAGIC   <div style="color:#333; font-size:10.5pt; line-height:1.75;">
# MAGIC     Materialized views that use <b>expectations</b> are <strong>always fully refreshed</strong> during pipeline runs.
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Now let’s look at how expectations are applied within the pipeline architecture.
# MAGIC
# MAGIC Here’s what happens step by step:
# MAGIC
# MAGIC 1. A row of data enters the pipeline.
# MAGIC
# MAGIC 2. That row is evaluated against any defined expectations (constraints).
# MAGIC
# MAGIC - If the row passes all expectations, it’s kept, and the pipeline continues processing as normal.
# MAGIC
# MAGIC - If the row fails an expectation, the action defined in the constraint determines what happens next:
# MAGIC <ul>
# MAGIC <li>WARN (default): The failure is logged, the row is still included, and the pipeline continues.
# MAGIC <li>DROP: The row is discarded, but the pipeline continues processing remaining data.
# MAGIC <li>FAIL: The pipeline fails immediately for that specific flow, and manual intervention is required. Other flows are unaffected.
# MAGIC </li>
# MAGIC </ul>
# MAGIC
# MAGIC As of Q2 2025, any materialized views that use expectations will always be fully refreshed during pipeline runs.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## E. Conclusion
# MAGIC
# MAGIC In this lecture, you learned how to use expectations to enforce data quality within Apache Spark™ Declarative Pipelines:
# MAGIC
# MAGIC 1. **Expectations are data quality rules** defined using the `CONSTRAINT ... EXPECT (...)` syntax directly within your pipeline SQL — validating data row-by-row during ETL.
# MAGIC 2. **WARN** (the default action) logs violations and still writes invalid rows to the target — ideal for non-critical quality monitoring without interrupting the pipeline.
# MAGIC 3. **DROP** removes invalid rows from the output and logs the count — suitable for cases where bad data must be excluded but the pipeline should continue running.
# MAGIC 4. **FAIL** stops the specific flow when a violation occurs and requires manual intervention — used for critical data issues that must be resolved before downstream processing.
# MAGIC 5. **Multiple constraints** can coexist in a single table definition, each evaluated independently — giving you layered, granular control over data quality across all columns.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next section, you will explore hands-on demonstrations of data quality expectations in action — running pipelines with constraints, observing metrics in the execution insights panel, and understanding how violations are surfaced within the Spark Declarative Pipelines UI.

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