# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Handling Task Failures and Monitoring Jobs Performance
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how to recover from task failures and monitor Lakeflow Jobs performance. You will see how Repair and Rerun supports efficient recovery from unsuccessful job runs, and how system tables and Spark UI details help you monitor jobs performance.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. **Utilize** the repair run feature to efficiently recover from failed job runs
# MAGIC 2. **Optimize** performance monitoring and cost management using system tables (system.lakeflow) and Spark UI insights to identify bottlenecks, track SLAs, and implement resource efficiency measures

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Handling Task Failures

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Repair and Rerun
# MAGIC
# MAGIC Failure handling isn't just about restarting tasks - it's about building resilient systems that can recover efficiently and maintain data consistency even when components fail.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div class="htf-repair-rerun">
# MAGIC <style>
# MAGIC .htf-repair-rerun{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .htf-repair-rerun *{box-sizing:border-box;}
# MAGIC .htf-image-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:14px;display:flex;align-items:center;justify-content:center;}
# MAGIC .htf-image-card img{width:100%;max-height:520px;object-fit:contain;border-radius:10px;background:transparent;}
# MAGIC .htf-text-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;margin-top:18px;align-items:stretch;}
# MAGIC .htf-info{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:18px;min-height:130px;box-shadow:0 2px 8px rgba(27,49,57,.06);display:flex;align-items:center;}
# MAGIC .htf-info.maroon{border-left-color:#98102A;}
# MAGIC .htf-info.green{border-left-color:#00A972;}
# MAGIC .htf-info p,.htf-info li{font-size:16px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC .htf-info ul{margin:10px 0 0 0;padding-left:20px;}
# MAGIC @media screen and (max-width:900px){.htf-text-grid{grid-template-columns:1fr;}.htf-image-card img{max-height:380px;}}
# MAGIC </style>
# MAGIC <div class="htf-image-card">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_handling_task_failures/repair_rerun.png" alt="Repair and rerun workflow showing failed task recovery">
# MAGIC </div>
# MAGIC <div class="htf-text-grid">
# MAGIC <div class="htf-info">
# MAGIC <p><strong>Repair</strong> feature allows to <strong>re-run</strong> and override task parameters</p>
# MAGIC </div>
# MAGIC <div class="htf-info maroon">
# MAGIC <p><strong>Reduces the time</strong> and resources required to recover from unsuccessful job runs</p>
# MAGIC </div>
# MAGIC <div class="htf-info green">
# MAGIC <div>
# MAGIC <p>In case of <strong>Task Failure</strong>, You can:</p>
# MAGIC <ul>
# MAGIC <li>Modify the <strong>task</strong> and run again</li>
# MAGIC <li>Modify the <strong>parameters</strong> and run again</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p><strong>The Repair feature represents a sophisticated approach to failure recovery:</strong></p>
# MAGIC <ul>
# MAGIC <li><strong>Targeted Recovery:</strong> Instead of restarting entire workflows, you can modify specific failed tasks and rerun only what's necessary. This saves significant time and computational resources.</li>
# MAGIC <li><strong>Parameter Override Capability:</strong> The ability to modify parameters during repair runs enables you to fix configuration issues, adjust resource allocation, or change processing logic without rebuilding the entire job.</li>
# MAGIC <li><strong>Recovery Scenarios:</strong></li>
# MAGIC <ul>
# MAGIC <li><strong>Configuration Fixes:</strong> Correct parameter values that caused task failures</li>
# MAGIC <li><strong>Resource Adjustments:</strong> Increase memory or compute resources for tasks that failed due to resource constraints</li>
# MAGIC <li><strong>Code Updates:</strong> Deploy fixes for logic errors and rerun only affected tasks</li>
# MAGIC <li><strong>Data Quality Issues:</strong> Adjust processing logic to handle data quality problems discovered during execution</li>
# MAGIC </ul>
# MAGIC <li><strong>Cost Efficiency:</strong> By rerunning only failed tasks, you minimize unnecessary computation and reduce costs, especially important for large, complex workflows.</li>
# MAGIC </ul>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Repair Run
# MAGIC
# MAGIC Click the highlighted boxes to learn about fixing a task.
# MAGIC  
# MAGIC <div class="htf-repair-click">
# MAGIC <style>
# MAGIC .htf-repair-click{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .htf-repair-click *{box-sizing:border-box;}
# MAGIC .htf-repair-click input[type="checkbox"]{display:none;}
# MAGIC .htf-repair-layout{display:grid;grid-template-columns:1.35fr .65fr;gap:20px;align-items:stretch;}
# MAGIC .htf-run-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:18px;}
# MAGIC .htf-composite-box{position:relative;background:#fff;border:1px solid #DCE0E2;border-radius:12px;padding:12px;min-height:390px;overflow:hidden;display:flex;align-items:center;justify-content:center;}
# MAGIC .htf-base-image{width:100%;max-height:390px;object-fit:contain;border-radius:8px;background:#fff;}
# MAGIC .htf-failed-highlight{position:absolute;left:5%;top:11%;width:42%;height:30%;background:rgba(255,95,70,.08);border:3px solid #FF5F46;border-radius:6px;cursor:pointer;box-shadow:0 0 0 4px rgba(255,95,70,.16);animation:htfRepairPulse 1.2s ease-in-out infinite;z-index:4;}
# MAGIC .htf-details-overlay{position:absolute;right:12px;top:12px;bottom:12px;width:48%;background:#fff;border:2px solid #FF5F46;border-radius:10px;box-shadow:0 4px 18px rgba(27,49,57,.16);padding:10px;display:none;align-items:center;justify-content:center;z-index:5;}
# MAGIC .htf-details-overlay img{width:100%;max-height:360px;object-fit:contain;border-radius:8px;background:#fff;}
# MAGIC .htf-select-failed:checked ~ .htf-repair-layout .htf-details-overlay{display:flex;}
# MAGIC .htf-select-failed:checked ~ .htf-repair-layout .htf-failed-highlight{border-color:#98102A;background:rgba(152,16,42,.10);box-shadow:0 0 0 4px rgba(152,16,42,.16);}
# MAGIC .htf-repair-highlight{position:absolute;top:10px;right:60px;width:75px;height:30px;background:transparent;border:3px solid #FF5F46;border-radius:6px;cursor:pointer;box-shadow:0 0 0 4px rgba(255,95,70,.18);animation:htfRepairPulse 1.2s ease-in-out infinite;z-index:8;}
# MAGIC .htf-side-panel{background:#F9F7F4;border:1px solid #DCE0E2;border-radius:14px;padding:18px;box-shadow:0 3px 12px rgba(27,49,57,.07);display:flex;align-items:center;justify-content:center;min-height:430px;position:relative;}
# MAGIC .htf-side-empty{font-size:16px;line-height:1.5;text-align:center;color:#5A6F77;border:1px dashed #DCE0E2;border-radius:12px;padding:24px;background:#fff;}
# MAGIC .htf-empty-after{display:none;}
# MAGIC .htf-side-image{display:none;width:100%;}
# MAGIC .htf-side-image img{width:100%;max-height:430px;object-fit:contain;border:2px solid #FF5F46;border-radius:8px;background:#fff;}
# MAGIC .htf-select-failed:checked ~ .htf-repair-layout .htf-empty-before{display:none;}
# MAGIC .htf-select-failed:checked ~ .htf-repair-layout .htf-empty-after{display:block;}
# MAGIC .htf-repair-toggle:checked ~ .htf-repair-layout .htf-empty-before,.htf-repair-toggle:checked ~ .htf-repair-layout .htf-empty-after{display:none;}
# MAGIC .htf-repair-toggle:checked ~ .htf-repair-layout .htf-side-image{display:block;}
# MAGIC .htf-caption{margin-top:16px;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;background:#F9F7F4;padding:16px;font-size:18px;line-height:1.45;text-align:center;}
# MAGIC @keyframes htfRepairPulse{0%,100%{box-shadow:0 0 0 4px rgba(255,95,70,.18);}50%{box-shadow:0 0 0 8px rgba(255,95,70,.10);}}
# MAGIC @media screen and (max-width:980px){.htf-repair-layout{grid-template-columns:1fr}.htf-composite-box{min-height:auto;}.htf-details-overlay{position:relative;right:auto;top:auto;bottom:auto;width:100%;margin-top:12px;}.htf-base-image{max-height:360px;}}
# MAGIC </style>
# MAGIC <input class="htf-select-failed" type="checkbox" id="htf-select-failed-task">
# MAGIC <input class="htf-repair-toggle" type="checkbox" id="htf-show-repair-panel">
# MAGIC <div class="htf-repair-layout">
# MAGIC <div class="htf-run-card">
# MAGIC <div class="htf-composite-box">
# MAGIC <img class="htf-base-image" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_handling_task_failures/repair_failed_task_dag.png" alt="Failed task in the job DAG">
# MAGIC <label class="htf-failed-highlight" for="htf-select-failed-task" aria-label="Select failed transforming customers task"></label>
# MAGIC <div class="htf-details-overlay">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_handling_task_failures/job_run_details_ui.png" alt="Job run details with repair run button">
# MAGIC <label class="htf-repair-highlight" for="htf-show-repair-panel" aria-label="Show repair job run panel"></label>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="htf-caption">Allowing you to <strong>run only failed tasks</strong>, saving you time and money by only re-running the necessary tasks instead of the entire job</div>
# MAGIC </div>
# MAGIC <div class="htf-side-panel">
# MAGIC <div class="htf-side-empty htf-empty-before">Click the highlighted failed task to open the job run details.</div>
# MAGIC <div class="htf-side-empty htf-empty-after">Click the highlighted <strong>Repair run</strong> button to view the repair job run panel.</div>
# MAGIC <div class="htf-side-image"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_handling_task_failures/repair_job_run_panel.png" alt="Repair job run side panel"></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p><strong>The selective re-execution capability provides massive operational benefits:</strong></p>
# MAGIC <ul>
# MAGIC <li><strong>Resource Optimization:</strong> Running only failed tasks instead of entire workflows can reduce recovery time by 80-90% in complex pipelines, saving both time and money.</li>
# MAGIC <li><strong>Reduced Risk:</strong> Smaller recovery operations have less impact on system resources and reduce the risk of cascading failures during recovery attempts.</li>
# MAGIC <li><strong>Faster Resolution:</strong> Teams can respond to failures more quickly when they don't need to wait for entire workflows to complete, improving SLA adherence and business responsiveness.</li>
# MAGIC </ul>
# MAGIC <p><strong>Please note that fixing a task does not mean fixing a job.</strong></p>
# MAGIC <p>For example: If you pass the wrong parameter and fix it using the repair run feature, you still need to edit that in your job.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A3. After Repair Run
# MAGIC <div class="htf-final-run">
# MAGIC <style>
# MAGIC .htf-final-run{width:1220px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .htf-final-run *{box-sizing:border-box;}
# MAGIC .htf-final-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:20px;}
# MAGIC .htf-final-image{position:relative;background:#fff;border:1px solid #DCE0E2;border-radius:12px;padding:12px;overflow:hidden;}
# MAGIC .htf-final-image img{width:100%;height:auto;display:block;border-radius:8px;}
# MAGIC .htf-final-highlight{position:absolute;left:70.5%;top:30%;width:5%;height:6%;border:3px dashed #FF3621;border-radius:6px;animation:htfBlink 1s ease-in-out infinite;box-shadow:0 0 0 5px rgba(255,54,33,.12);}
# MAGIC .htf-final-pulse{position:absolute;left:74.2%;top:29.5%;width:9.7%;height:13%;border-radius:50%;background:rgba(255,54,33,.08);animation:htfPulse 1.6s ease-in-out infinite;}
# MAGIC .htf-final-text{margin-top:16px;border:1px solid #DCE0E2;border-left:6px solid #00A972;border-radius:12px;background:#F9F7F4;padding:16px;font-size:18px;line-height:1.45;text-align:center;}
# MAGIC @keyframes htfBlink{0%,100%{opacity:1}50%{opacity:.25}}
# MAGIC @keyframes htfPulse{0%{transform:scale(.75);opacity:.35}70%{transform:scale(1.25);opacity:0}100%{opacity:0}}
# MAGIC </style>
# MAGIC <div class="htf-final-card">
# MAGIC <div class="htf-final-image">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_handling_task_failures/final_repaired_job_dag.png" alt="Final job DAG after rerunning the failed task">
# MAGIC <div class="htf-final-pulse"></div>
# MAGIC <div class="htf-final-highlight" aria-label="Blinking highlight around repaired task attempts"></div>
# MAGIC </div>
# MAGIC <div class="htf-final-text">After re-running task, your final job will look like this.</div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC - <strong>Audit Trail</strong><p>Complete visibility into what was repaired, when, and by whom, providing essential information for troubleshooting and process improvement.</p>
# MAGIC - <strong>Success Validation</strong><p>Clear indication of which tasks were recovered successfully, enabling confidence in the repair process.</p>
# MAGIC - <strong>Learning Opportunities</strong><p>Historical repair data helps teams identify patterns in failures and improve initial job design to prevent future issues.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Monitoring Jobs Performance
# MAGIC
# MAGIC Failure handling isn't just about restarting tasks - it's about building resilient systems that can recover efficiently and maintain data consistency even when components fail.
# MAGIC
# MAGIC
# MAGIC Select each tab below to learn more about monitoring jobs performance.
# MAGIC
# MAGIC <div class="mjp-monitor-tabs">
# MAGIC <style>
# MAGIC .mjp-monitor-tabs{width:1220px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .mjp-monitor-tabs *{box-sizing:border-box;}
# MAGIC .mjp-monitor-tabs input[type="radio"]{display:none;}
# MAGIC .mjp-tabbar{display:flex;justify-content:flex-start;align-items:center;border-bottom:2px solid #EEEDE9;margin-bottom:18px;}
# MAGIC .mjp-tabbar label{padding:10px 18px;border:none;border-bottom:3px solid transparent;background:none;font-size:14pt;font-weight:bold;color:#888;cursor:pointer;margin-bottom:-2px;}
# MAGIC #mjp-tab-system:checked ~ .mjp-tabbar label[for="mjp-tab-system"]{color:#FF5F46;border-bottom-color:#FF5F46;}
# MAGIC #mjp-tab-spark:checked ~ .mjp-tabbar label[for="mjp-tab-spark"]{color:#4299E0;border-bottom-color:#4299E0;}
# MAGIC .mjp-panel{display:none;}
# MAGIC #mjp-tab-system:checked ~ .mjp-panels .mjp-system-panel{display:block;}
# MAGIC #mjp-tab-spark:checked ~ .mjp-panels .mjp-spark-panel{display:block;}
# MAGIC .mjp-system-section,.mjp-spark-section{width:100%;max-width:100%;margin:0 auto;}
# MAGIC .mjp-system-grid{display:grid;grid-template-columns:0.95fr 1.05fr;gap:22px;align-items:stretch;}
# MAGIC .mjp-system-left{display:grid;gap:14px;}
# MAGIC .mjp-system-card{background:#fff;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:14px;padding:18px;box-shadow:0 2px 8px rgba(27,49,57,0.06);}
# MAGIC .mjp-system-card.green{border-left-color:#00A972;}
# MAGIC .mjp-system-card h3{font-size:18px;line-height:1.25;font-weight:700;margin:0 0 10px 0;color:#0B2026;}
# MAGIC .mjp-system-card p,.mjp-system-card li{font-size:16px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC .mjp-system-card ul{margin:0;padding-left:20px;}
# MAGIC .mjp-key-table{width:100%;border-collapse:collapse;margin-top:10px;font-size:16px;line-height:1.35;}
# MAGIC .mjp-key-table th{background:#F9F7F4;border:1px solid #DCE0E2;text-align:left;padding:9px;font-weight:700;}
# MAGIC .mjp-key-table td{border:1px solid #DCE0E2;padding:9px;vertical-align:top;}
# MAGIC .mjp-key-table code{font-family:Consolas,Monaco,"Courier New",monospace;background:#F9F7F4;border-radius:4px;padding:2px 4px;}
# MAGIC .mjp-system-image{position:relative;background:#fff;border:1px solid #DCE0E2;border-radius:14px;padding:16px;display:flex;align-items:center;justify-content:center;box-shadow:0 3px 12px rgba(27,49,57,0.07);overflow:hidden;}
# MAGIC .mjp-system-image img{width:100%;max-height:520px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC .mjp-system-highlight{position:absolute;left:3%;top:14%;width:32%;height:6%;border:4px solid #FF5F46;border-radius:10px;background:rgba(255,95,70,.08);box-shadow:0 0 0 9999px rgba(11,32,38,.05),0 0 0 7px rgba(255,95,70,.14);animation:mjpRedPulse 1.5s ease-in-out infinite;pointer-events:none;}
# MAGIC .mjp-spark-grid{position:relative;display:grid;grid-template-columns:0.92fr 1.08fr;gap:22px;align-items:stretch;}
# MAGIC .mjp-detail-card,.mjp-right-card,.mjp-action-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,0.07);overflow:hidden;}
# MAGIC .mjp-card-bar{height:8px;background:#FF5F46;}
# MAGIC .mjp-card-bar.green{background:#00A972;}
# MAGIC .mjp-card-bar.maroon{background:#98102A;}
# MAGIC .mjp-card-body{padding:18px;}
# MAGIC .mjp-card-body h3{font-size:18px;line-height:1.25;font-weight:700;margin:0 0 12px 0;color:#0B2026;}
# MAGIC .mjp-card-body ul{margin:0;padding-left:22px;}
# MAGIC .mjp-card-body li{font-size:16px;line-height:1.55;margin-bottom:10px;}
# MAGIC .mjp-img-wrap{background:#fff;border:1px solid #DCE0E2;border-radius:12px;padding:12px;display:flex;align-items:center;justify-content:center;}
# MAGIC .mjp-img-wrap img{width:100%;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC .mjp-detail-card .mjp-img-wrap img{max-height:520px;}
# MAGIC .mjp-timeline-image{position:relative;overflow:hidden;}
# MAGIC .mjp-timeline-image img{max-height:230px;}
# MAGIC .mjp-right-stack{display:grid;gap:16px;}
# MAGIC .mjp-insights{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:16px;}
# MAGIC .mjp-insight{background:#F9F7F4;border:1px solid #DCE0E2;border-left:5px solid #FF5F46;border-radius:12px;padding:14px;font-size:16px;line-height:1.45;}
# MAGIC .mjp-insight.green{border-left-color:#00A972;}
# MAGIC .mjp-pulse{position:relative;}
# MAGIC .mjp-pulse:after{content:"";position:absolute;left:5%;top:47%;width:110px;height:20px;border:3px solid #FF5F46;border-radius:10px;animation:mjpPulse 1.6s ease-in-out infinite;pointer-events:none;}
# MAGIC .mjp-spark-connector{position:absolute;left:0;top:0;width:100%;height:100%;pointer-events:none;z-index:6;}
# MAGIC .mjp-connector-path{fill:none;stroke:#FF5F46;stroke-width:4;stroke-linecap:round;stroke-linejoin:round;}
# MAGIC .mjp-connector-label{position:absolute;left:34%;top:18%;background:#FFF1EE;border:1px solid #FFD2C9;border-left:5px solid #FF5F46;border-radius:10px;padding:8px 12px;font-size:14px;line-height:1.25;font-weight:800;color:#0B2026;box-shadow:0 2px 8px rgba(27,49,57,.08);z-index:7;}
# MAGIC @keyframes mjpPulse{0%,100%{opacity:.25;transform:scale(1);}50%{opacity:1;transform:scale(1.04);}}
# MAGIC @keyframes mjpRedPulse{0%,100%{border-color:#FF5F46;box-shadow:0 0 0 9999px rgba(11,32,38,.05),0 0 0 3px rgba(255,95,70,.18);}50%{border-color:#98102A;box-shadow:0 0 0 9999px rgba(11,32,38,.05),0 0 0 8px rgba(255,95,70,.12);}}
# MAGIC @keyframes mjpConnectorMove{0%{stroke-dashoffset:72;}100%{stroke-dashoffset:0;}}
# MAGIC @media screen and (max-width:900px){.mjp-system-grid,.mjp-spark-grid,.mjp-insights{grid-template-columns:1fr;}.mjp-system-image img{max-height:none;}.mjp-pulse:after,.mjp-spark-connector,.mjp-connector-label,.mjp-system-highlight,.mjp-tabbar{flex-wrap:wrap;}}
# MAGIC </style>
# MAGIC <input type="radio" id="mjp-tab-system" name="mjp-monitor-tab" checked>
# MAGIC <input type="radio" id="mjp-tab-spark" name="mjp-monitor-tab">
# MAGIC <div class="mjp-tabbar">
# MAGIC <label for="mjp-tab-system">System Tables</label>
# MAGIC <label for="mjp-tab-spark">Spark UI</label>
# MAGIC </div>
# MAGIC <div class="mjp-panels">
# MAGIC <div class="mjp-panel mjp-system-panel">
# MAGIC <div class="mjp-system-section">
# MAGIC <div class="mjp-system-grid">
# MAGIC <div class="mjp-system-left">
# MAGIC <div class="mjp-system-card"><h3>system.lakeflow</h3><p><strong>system.lakeflow</strong> is a built-in, read-only catalog that logs all job activity across workspaces in the region.</p></div>
# MAGIC <div class="mjp-system-card green"><h3>Timeline tables</h3><p>Timeline tables slice long runs hourly using <strong>period_start_time</strong> and <strong>period_end_time</strong>, enabling reliable duration, concurrency, and SLA analytics.</p></div>
# MAGIC <div class="mjp-system-card"><h3>Key Tables</h3><table class="mjp-key-table"><thead><tr><th>Table</th><th>Description</th></tr></thead><tbody><tr><td><code>jobs</code></td><td>Job basic info</td></tr><tr><td><code>job_tasks</code></td><td>Task basic definitions</td></tr><tr><td><code>job_run_timeline</code></td><td>Each job run over time</td></tr><tr><td><code>job_task_run_timeline</code></td><td>Each task run over time</td></tr><tr><td><code>Pipelines</code></td><td>Pipelines basic info</td></tr></tbody></table></div>
# MAGIC </div>
# MAGIC <div class="mjp-system-image">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_monitoring_jobs_performance/system_lakeflow_catalog_ui.png" alt="system.lakeflow catalog system tables UI">
# MAGIC <!-- <div class="mjp-system-highlight"></div> -->
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="mjp-panel mjp-spark-panel">
# MAGIC <div class="mjp-spark-section">
# MAGIC <div class="mjp-spark-grid">
# MAGIC <svg class="mjp-spark-connector" viewBox="0 0 1000 520" preserveAspectRatio="none" aria-hidden="true">
# MAGIC   <defs>
# MAGIC     <marker id="mjp-red-arrow-left" markerWidth="14" markerHeight="14" refX="12" refY="7" orient="auto" markerUnits="userSpaceOnUse">
# MAGIC       <path d="M0,0 L14,7 L0,14 Z" fill="#FF5F46"></path>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC  <path
# MAGIC   class="mjp-connector-path"
# MAGIC   d="M510,130 L410,130"
# MAGIC   marker-end="url(#mjp-red-arrow-left)">
# MAGIC </path>
# MAGIC </svg>
# MAGIC <div class="mjp-detail-card"><div class="mjp-card-bar maroon"></div><div class="mjp-card-body"><h3>Query/code details</h3><div class="mjp-img-wrap"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_monitoring_jobs_performance/spark_query_details_ui.png" alt="Spark UI query and code details panel"></div></div></div>
# MAGIC <div class="mjp-right-stack">
# MAGIC <div class="mjp-right-card"><div class="mjp-card-bar green"></div><div class="mjp-card-body"><h3>Timeline</h3><div class="mjp-img-wrap mjp-timeline-image mjp-pulse"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_monitoring_jobs_performance/spark_timeline_ui.png" alt="Spark UI job run timeline showing task duration and overlap"></div></div></div>
# MAGIC <div class="mjp-action-card"><div class="mjp-card-bar"></div><div class="mjp-card-body"><ul><li><strong>Timeline</strong> in job run highlights task start/end, duration, and overlap to spot bottlenecks fast.</li><li>Click a task to see status, timestamps, duration, cluster/runtime, logs, and quick I/O.</li><li>Click on <strong>query/code</strong> details for full text, run ID, wall-clock split (optimizing/pruning vs executing), files read/written details, files &amp; partitions and spill details.</li></ul><div class="mjp-insights"><div class="mjp-insight"><strong>High planning time</strong><br>Improve pruning/partitioning</div><div class="mjp-insight green"><strong>High execution time</strong><br>Optimize joins/aggregations (broadcast, skew fixes)</div></div></div></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>The <strong>system.lakeflow</strong> catalog provides enterprise-grade monitoring capabilities:</p><ul><li><strong>Comprehensive Logging:</strong> All job activity across all workspaces in the region is automatically logged, providing complete visibility into workflow execution patterns and performance trends.</li><li><strong>Timeline Analysis:</strong> Timeline tables use <strong>period_start_time</strong> and <strong>period_end_time</strong> to slice long-running jobs into hourly segments, enabling accurate duration analysis, concurrency tracking, and SLA measurement even for complex, long-running workflows.</li><li><strong>Key Table Functions:</strong><ul><li><strong>jobs:</strong> Basic job metadata and configuration information</li><li><strong>job_tasks:</strong> Task definitions and configuration details</li><li><strong>job_run_timeline:</strong> Complete execution history for every job run</li><li><strong>job_task_run_timeline:</strong> Detailed execution history for individual tasks</li><li><strong>pipelines:</strong> Information about Delta Live Tables pipelines</li></ul></li><li><strong>Analytics Capabilities:</strong> This data enables sophisticated analytics including cost analysis, performance trending, SLA compliance tracking, and resource utilization optimization.</li></ul>
# MAGIC
# MAGIC <p>The Spark UI provides detailed performance insights for optimization:</p><ul><li><strong>Timeline Analysis:</strong> The execution timeline immediately highlights task duration, overlap, and bottlenecks, enabling quick identification of performance issues.</li><li><strong>Task-Level Details:</strong> Clicking individual tasks reveals comprehensive information including execution timestamps, resource utilization, cluster configuration, logs, and I/O statistics.</li><li><strong>Query Performance Details:</strong> Detailed query analysis shows execution plans, optimization decisions, file access patterns, partition information, and data spill details - essential for performance tuning.</li><li><strong>Actionable Insights:</strong><ul><li><strong>High Planning Time:</strong> Indicates need for better partitioning strategies or metadata optimization</li><li><strong>High Execution Time:</strong> Suggests opportunities for join optimization, broadcast strategies, or skew handling</li><li><strong>Resource Bottlenecks:</strong> Identifies memory, CPU, or I/O constraints that require cluster configuration adjustments</li></ul></li></ul>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Conclusion
# MAGIC
# MAGIC - Repair runs let you re-run only failed tasks, override parameters, reduce recovery time/cost, and track the recovery in job run history.
# MAGIC - <strong>system.lakeflow</strong> logs job activity and provides timeline tables for duration, concurrency, and SLA analytics.
# MAGIC - Spark UI helps identify bottlenecks by showing task timing, overlaps, planning time, and execution time for optimization.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next demo, you will get hands-on experience with the complete failure handling lifecycle.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>