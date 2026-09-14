# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Lakeflow Jobs Core Components
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will understand the fundamental building blocks that make up every Lakeflow job and explore how tasks are orchestrated using DAG concepts.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC 1. **Understand** the building blocks of Lakeflow Jobs, including the concepts of jobs and tasks
# MAGIC 2. **Identify** and **describe** the various task types and configuration options available in Lakeflow Jobs
# MAGIC 3. **Explain** how Directed Acyclic Graphs (DAGs) enable task orchestration and workflow patterns
# MAGIC 4. **Learn** how to create and configure jobs using the Lakeflow Jobs UI

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Building Blocks of Lakeflow Jobs

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Job and Tasks
# MAGIC
# MAGIC Click the expandable boxes to know more about Jobs and Tasks.
# MAGIC
# MAGIC <div class="jt-wrap">
# MAGIC   <div class="jt-left">
# MAGIC     <button class="jt-tab" data-key="jobs" aria-expanded="false" type="button">
# MAGIC       <span class="jt-tab-title">Jobs</span>
# MAGIC       <span class="jt-tab-icon">+</span>
# MAGIC     </button>
# MAGIC     <div class="jt-panel" id="panel-jobs">
# MAGIC       <p>A Job is the primary resource for:</p>
# MAGIC       <ul>
# MAGIC         <li>Scheduling</li>
# MAGIC         <li>Coordinating</li>
# MAGIC         <li>Running operations such as data processing, ETL, analytics, and machine learning workloads within the Databricks environment.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <button class="jt-tab" data-key="tasks" aria-expanded="false" type="button">
# MAGIC       <span class="jt-tab-title">Tasks</span>
# MAGIC       <span class="jt-tab-icon">+</span>
# MAGIC     </button>
# MAGIC     <div class="jt-panel" id="panel-tasks">
# MAGIC       <p>A task is a single unit of work within a job that executes a specific workload such as:</p>
# MAGIC       <ul>
# MAGIC         <li>Notebook</li>
# MAGIC         <li>Script</li>
# MAGIC         <li>Query and more.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="jt-right">
# MAGIC     <div class="jt-image-wrap">
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/jobs_tasks_overview.png" alt="Job and Tasks slide image">
# MAGIC       <div class="jt-highlight jt-highlight-main"></div>
# MAGIC       <div class="jt-highlight jt-highlight-task task-1">Task</div>
# MAGIC       <div class="jt-highlight jt-highlight-task task-2">Task</div>
# MAGIC       <div class="jt-highlight jt-highlight-task task-3">Task</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div class="jt-callout">
# MAGIC     Each job consists of one or more tasks, which are the individual units of work that make up the job.
# MAGIC </div>
# MAGIC <style>
# MAGIC .jt-wrap {
# MAGIC   display: flex;
# MAGIC   gap: 28px;
# MAGIC   align-items: stretch;
# MAGIC   margin: 24px auto;
# MAGIC   width: 1100px;
# MAGIC   max-width: 100%;
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .jt-left,
# MAGIC .jt-right {
# MAGIC   box-sizing: border-box;
# MAGIC   min-width: 0;
# MAGIC }
# MAGIC .jt-left {
# MAGIC   flex: 0 0 calc(40% - 14px);
# MAGIC   max-width: calc(40% - 14px);
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   gap: 12px;
# MAGIC }
# MAGIC .jt-right {
# MAGIC   flex: 0 0 calc(60% - 14px);
# MAGIC   max-width: calc(60% - 14px);
# MAGIC   display: flex;
# MAGIC   justify-content: center;
# MAGIC }
# MAGIC .jt-intro {
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.6;
# MAGIC   color: #0B2026;
# MAGIC   margin-bottom: 4px;
# MAGIC }
# MAGIC .jt-callout {
# MAGIC   width: 1100px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 0 auto 24px auto;
# MAGIC   background: #FFF1EE;
# MAGIC   border: 1px solid #FFD2C9;
# MAGIC   border-left: 6px solid #FF5F46;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 14px 16px;
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.45;
# MAGIC   font-weight: 700;
# MAGIC   color: #0B2026;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .jt-tab {
# MAGIC   width: 100%;
# MAGIC   border: 2px solid #FF5F46;
# MAGIC   border-radius: 8px;
# MAGIC   background: #FF5F46;
# MAGIC   color: #ffffff;
# MAGIC   padding: 18px 20px;
# MAGIC   text-align: left;
# MAGIC   font-size: 18px;
# MAGIC   font-weight: 700;
# MAGIC   cursor: pointer;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC   display: flex;
# MAGIC   justify-content: space-between;
# MAGIC   align-items: center;
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .jt-tab:hover {
# MAGIC   background: #FF735F;
# MAGIC   border-color: #FF735F;
# MAGIC }
# MAGIC .jt-tab.active {
# MAGIC   border-color: #98102A;
# MAGIC   background: #FF5F46;
# MAGIC }
# MAGIC .jt-tab-title,
# MAGIC .jt-tab-icon {
# MAGIC   color: #ffffff;
# MAGIC }
# MAGIC .jt-tab-icon {
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1;
# MAGIC   flex: 0 0 auto;
# MAGIC }
# MAGIC .jt-panel {
# MAGIC   display: none;
# MAGIC   border-radius: 8px;
# MAGIC   padding: 18px 20px;
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.6;
# MAGIC   color: #0B2026;
# MAGIC   margin-top: -2px;
# MAGIC   box-sizing: border-box;
# MAGIC   overflow-wrap: break-word;
# MAGIC   word-break: normal;
# MAGIC }
# MAGIC .jt-panel.active {
# MAGIC   display: block;
# MAGIC }
# MAGIC .jt-panel p {
# MAGIC   margin: 0 0 8px 0;
# MAGIC }
# MAGIC .jt-panel ul {
# MAGIC   margin: 0;
# MAGIC   padding-left: 22px;
# MAGIC }
# MAGIC .jt-panel li {
# MAGIC   margin-bottom: 6px;
# MAGIC }
# MAGIC .jt-image-wrap {
# MAGIC   position: relative;
# MAGIC   width: 100%;
# MAGIC   max-width: 100%;
# MAGIC   overflow: hidden;
# MAGIC   border-radius: 8px;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC   background: #ffffff;
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .jt-image-wrap img {
# MAGIC   display: block;
# MAGIC   width: 100%;
# MAGIC   max-width: 100%;
# MAGIC   height: auto;
# MAGIC   object-fit: contain;
# MAGIC }
# MAGIC .jt-highlight {
# MAGIC   position: absolute;
# MAGIC   border-radius: 8px;
# MAGIC   pointer-events: none;
# MAGIC   transition: all 0.25s ease;
# MAGIC   display: none;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   text-align: center;
# MAGIC   font-size: 14px;
# MAGIC   font-weight: 700;
# MAGIC   color: #ffffff;
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .jt-highlight-main {
# MAGIC   border: 4px solid #FF5F46;
# MAGIC   background: rgba(255, 95, 70, 0);
# MAGIC   box-shadow: 0 0 0 9999px rgba(11,32,38,0.10);
# MAGIC }
# MAGIC .jt-highlight-main::before {
# MAGIC   content: "Job";
# MAGIC   position: absolute;
# MAGIC   top: 8px;
# MAGIC   left: 50%;
# MAGIC   transform: translateX(-50%);
# MAGIC   background: #FF5F46;
# MAGIC   color: #ffffff;
# MAGIC   border-radius: 6px;
# MAGIC   padding: 5px 18px;
# MAGIC   font-size: 16px;
# MAGIC   font-weight: 800;
# MAGIC   line-height: 1.2;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.14);
# MAGIC }
# MAGIC .jt-highlight-task {
# MAGIC   border: 2px solid #FF5F46;
# MAGIC   background: #FF5F46;
# MAGIC }
# MAGIC @media screen and (max-width: 940px) {
# MAGIC   .jt-wrap {
# MAGIC     flex-direction: column;
# MAGIC   }
# MAGIC   .jt-left,
# MAGIC   .jt-right {
# MAGIC     flex: 1 1 auto;
# MAGIC     max-width: 100%;
# MAGIC   }
# MAGIC }
# MAGIC </style>
# MAGIC <script>
# MAGIC (function () {
# MAGIC   const root = document.currentScript.previousElementSibling.previousElementSibling;
# MAGIC   const wrap = root && root.classList.contains('jt-wrap') ? root : document.querySelector('.jt-wrap');
# MAGIC   if (!wrap) return;
# MAGIC   const config = {
# MAGIC     jobs: {
# MAGIC       panelId: "panel-jobs",
# MAGIC       highlights: [
# MAGIC         { cls: ".jt-highlight-main", x: 0.0, y: 4.2, w: 100, h: 35 }
# MAGIC       ]
# MAGIC     },
# MAGIC     tasks: {
# MAGIC       panelId: "panel-tasks",
# MAGIC       highlights: [
# MAGIC         { cls: ".task-1", x: 37, y: 22, w: 8, h: 6 },
# MAGIC         { cls: ".task-2", x: 37, y: 37, w: 8, h: 6 },
# MAGIC         { cls: ".task-3", x: 63, y: 30, w: 8, h: 6 }
# MAGIC       ]
# MAGIC     }
# MAGIC   };
# MAGIC   const tabs = wrap.querySelectorAll(".jt-tab");
# MAGIC   const panels = wrap.querySelectorAll(".jt-panel");
# MAGIC   const highlights = wrap.querySelectorAll(".jt-highlight");
# MAGIC   function resetAll() {
# MAGIC     tabs.forEach(tab => {
# MAGIC       tab.classList.remove("active");
# MAGIC       tab.setAttribute("aria-expanded", "false");
# MAGIC       const icon = tab.querySelector(".jt-tab-icon");
# MAGIC       if (icon) icon.textContent = "+";
# MAGIC     });
# MAGIC     panels.forEach(panel => panel.classList.remove("active"));
# MAGIC     highlights.forEach(h => {
# MAGIC       h.style.display = "none";
# MAGIC       h.style.left = "0%";
# MAGIC       h.style.top = "0%";
# MAGIC       h.style.width = "0%";
# MAGIC       h.style.height = "0%";
# MAGIC     });
# MAGIC   }
# MAGIC   function activate(key) {
# MAGIC     resetAll();
# MAGIC     const tab = wrap.querySelector('.jt-tab[data-key="' + key + '"]');
# MAGIC     const panel = wrap.querySelector("#" + config[key].panelId);
# MAGIC     if (!tab || !panel) return;
# MAGIC     tab.classList.add("active");
# MAGIC     tab.setAttribute("aria-expanded", "true");
# MAGIC     tab.querySelector(".jt-tab-icon").textContent = "−";
# MAGIC     panel.classList.add("active");
# MAGIC     config[key].highlights.forEach(item => {
# MAGIC       const el = wrap.querySelector(item.cls);
# MAGIC       if (!el) return;
# MAGIC       el.style.display = "flex";
# MAGIC       el.style.left = item.x + "%";
# MAGIC       el.style.top = item.y + "%";
# MAGIC       el.style.width = item.w + "%";
# MAGIC       el.style.height = item.h + "%";
# MAGIC     });
# MAGIC   }
# MAGIC   tabs.forEach(tab => {
# MAGIC     tab.addEventListener("click", function () {
# MAGIC       const key = this.dataset.key;
# MAGIC       const isActive = this.classList.contains("active");
# MAGIC       if (isActive) {
# MAGIC         resetAll();
# MAGIC       } else {
# MAGIC         activate(key);
# MAGIC       }
# MAGIC     });
# MAGIC   });
# MAGIC })();
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Here are the two most fundamental concepts you need to understand:
# MAGIC
# MAGIC A Job is the primary resource for scheduling, coordinating, and running operations such as data processing, ETL, analytics, and machine learning workloads within the Databricks environment. Think of a job as the container that holds your entire workflow.
# MAGIC
# MAGIC A task is a single unit of work within a job that executes a specific workload such as a notebook, script, query, and more. Tasks are the individual building blocks that do the actual work.
# MAGIC
# MAGIC The relationship is hierarchical: each job consists of one or more tasks, which are the individual units of work that make up the job. The visual shows this clearly - one job containing multiple tasks.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Jobs and Tasks: A Hierarchical Relationship
# MAGIC
# MAGIC <div class="bb-task-types">
# MAGIC <style>
# MAGIC .bb-task-types{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .bb-task-types *{box-sizing:border-box;}
# MAGIC .bb-row{display:grid;grid-template-columns:.34fr 44px .66fr;gap:14px;align-items:start;margin:22px 0;}
# MAGIC .bb-text-block{background: #FF5F46; color: #ffffff; border-radius:12px;padding:18px 20px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .bb-text-block p{font-size:16px;line-height:1.55;margin:0;color:#ffffff;font-weight:600;}
# MAGIC .bb-connector{position:relative;height:68px;min-height:68px;align-self:start;display:flex;align-items:flex-start;justify-content:center;}
# MAGIC .bb-connector:before{content:"";position:absolute;left:0;right:0;top:34px;border-top:3px solid #FF5F46;}
# MAGIC .bb-connector:after{content:"";position:absolute;right:-1px;top:34px;transform:translateY(-50%);width:0;height:0;border-left:12px solid #FF5F46;border-top:8px solid transparent;border-bottom:8px solid transparent;}
# MAGIC .bb-image-block{background:#ffffff;border:1px solid #DCE0E2;border-radius:12px;padding:12px;box-shadow:0 3px 12px rgba(27,49,57,.07);display:flex;align-items:center;justify-content:center;}
# MAGIC .bb-image-block img{width:100%;max-height:390px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC @media screen and (max-width:900px){.bb-row{grid-template-columns:1fr;}.bb-connector{min-height:32px;height:32px;}.bb-connector:before{top:50%;left:50%;right:auto;width:3px;height:32px;border-top:0;border-left:3px solid #FF5F46;transform:translateX(-50%);}.bb-connector:after{right:auto;left:50%;top:auto;bottom:-2px;transform:translateX(-50%);border-left:8px solid transparent;border-right:8px solid transparent;border-top:12px solid #FF5F46;border-bottom:0;}.bb-image-block img{max-height:320px;}}
# MAGIC </style>
# MAGIC <div class="bb-row">
# MAGIC <div class="bb-text-block">
# MAGIC <p><b>Jobs</b> consist of one or more <b>Tasks</b></p>
# MAGIC </div>
# MAGIC <div class="bb-connector"></div>
# MAGIC <div class="bb-image-block">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/one_job_more_task.png" alt="Different task types available in Lakeflow Jobs">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Jobs consist of one or more tasks, and there are many different task types available. You can use Databricks Notebooks in any supported language, Python Scripts, Python Wheels for packaged code, SQL Files and Queries for data transformations,Spark Declarative Pipelines, dbt for data transformation, Java JAR files, Spark Submit jobs for legacy Spark applications, AI/BI Dashboards for visualization, and even Power BI integration.
# MAGIC
# MAGIC This variety ensures that you can orchestrate virtually any type of workload within your job.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A3. Task Configuration Options
# MAGIC
# MAGIC <div class="task-config-options">
# MAGIC <style>
# MAGIC .task-config-options{width:1100px;max-width:100%;margin:0 auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .task-config-options *{box-sizing:border-box;}
# MAGIC .tco-wrap{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin:32px 0;align-items:stretch;}
# MAGIC .tco-left{display:grid;gap:16px;}
# MAGIC .tco-block{background:#F9F7F4;border-radius:8px;padding:24px 22px;box-shadow:0 2px 8px rgba(11,32,38,0.07);font-size:16px;line-height:1.7;color:#0B2026;}
# MAGIC .tco-block.one{border-left:4px solid #4299E0;}
# MAGIC .tco-block.two{border-left:4px solid #00A972;}
# MAGIC .tco-block ul{margin:0;padding-left:22px;}
# MAGIC .tco-block li{margin-bottom:8px;}
# MAGIC .tco-image{background:#F9F7F4;border-top:4px solid #FF5F46;border-radius:8px;padding:24px 22px;box-shadow:0 2px 8px rgba(11,32,38,0.07);display:flex;align-items:center;justify-content:center;}
# MAGIC .tco-image img{max-width:100%;height:auto;display:block;background:transparent;border-radius:4px;}
# MAGIC @media screen and (max-width:900px){.tco-wrap{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="tco-wrap">
# MAGIC <div class="tco-left">
# MAGIC <div class="tco-block one">
# MAGIC With <b>Jobs</b>, we can set a <b>particular task</b>
# MAGIC </div>
# MAGIC <div class="tco-block two">
# MAGIC <ul>
# MAGIC <li>We have different <b>options available</b> based on the task:
# MAGIC <ul>
# MAGIC <li>Defining path</li>
# MAGIC <li>Adding libraries</li>
# MAGIC <li>Adding parameters</li>
# MAGIC <li>Enabling notification</li>
# MAGIC <li>All options are specific to the selected task type</li>
# MAGIC </ul>
# MAGIC </li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="tco-image">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/task_options.png" alt="Slide showing task-specific configuration options in Lakeflow Jobs">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC When you create a job, you can set specific configurations for each particular task. The options available depend on the task type you select. Common configuration options include defining the path to your code, adding libraries, setting parameters, enabling notifications, and configuring retry policies.
# MAGIC
# MAGIC These configurations allow you to better orchestrate each particular task according to your specific requirements.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A4. Task Types: Notebook and SQL
# MAGIC
# MAGIC Click on the tabs to switch between the task types.
# MAGIC <br>
# MAGIC
# MAGIC <div class="task-types-carousel">
# MAGIC <style>
# MAGIC .task-types-carousel{width:1100px;max-width:100%;margin:0 auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .task-types-carousel *{box-sizing:border-box;}
# MAGIC .flow-grid{display:flex;flex-direction:column;gap:40px;justify-content:center;align-items:center;}
# MAGIC .flow-box{width:100%;min-height:420px;background:#F9F7F4;border:none;border-radius:8px;box-shadow:0 2px 8px rgba(27,49,57,0.06);overflow:hidden;display:flex;flex-direction:column;gap:12px;padding:20px;text-align:center;position:relative;box-sizing:border-box;}
# MAGIC .flow-box::before{content:"";position:absolute;top:0;left:0;width:100%;height:8px;}
# MAGIC .flow-box.flow1::before{background:#FF7A59;}
# MAGIC .flow-box.flow2::before{background:#4299E0;}
# MAGIC .flow-box-title{font-size:20px;font-weight:bold;text-align:center;color:#0B2026;}
# MAGIC .flow-box-icon{display:inline-flex;align-items:center;justify-content:center;gap:8px;}
# MAGIC .flow-box-icon img{width:50px;height:auto;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC .flow-box-content{display:grid;grid-template-columns:.32fr .68fr;gap:24px;width:100%;align-items:stretch;}
# MAGIC .flow-box-text{font-size:18px;text-align:left;line-height:1.7;color:#0B2026;background:#ffffff;border:1px solid #DCE0E2;border-radius:10px;padding:18px 20px;box-sizing:border-box;display:flex;flex-direction:column;justify-content:center;}
# MAGIC .flow-box-text p{margin:0 0 12px 0;}
# MAGIC .flow-box-text ul{padding-left:22px;margin:8px 0 0 0;}
# MAGIC .flow-box-text li{margin-bottom:8px;}
# MAGIC .flow-box-text li:last-child{margin-bottom:0;}
# MAGIC .task-example{margin-top:12px;border-left:5px solid #FF7A59;background:#FFF1EE;border-radius:10px;padding:12px 14px;}
# MAGIC .flow2 .task-example{border-left-color:#4299E0;background:#F0F7FD;}
# MAGIC .flow-box-img{background:#ffffff;border:1px solid #DCE0E2;border-radius:10px;padding:12px;display:flex;align-items:center;justify-content:center;text-align:center;}
# MAGIC .flow-box-img img{width:100%;max-height:520px;object-fit:contain;border-radius:6px;box-shadow:0 2px 10px rgba(0,0,0,0.10);background:transparent;}
# MAGIC .flow-tabbar{
# MAGIC display:flex;
# MAGIC justify-content:flex-start;
# MAGIC align-items:center;
# MAGIC border-bottom:2px solid #EEEDE9;
# MAGIC margin-bottom:0;
# MAGIC width:100%;
# MAGIC }
# MAGIC .flow-tab{
# MAGIC padding:10px 18px;
# MAGIC border:none;
# MAGIC border-bottom:3px solid transparent;
# MAGIC background:none;
# MAGIC font-size:20px;
# MAGIC font-weight:bold;
# MAGIC color:#888;
# MAGIC cursor:pointer;
# MAGIC margin-bottom:-2px;
# MAGIC }
# MAGIC .flow-tab-active-1{color:#FF7A59;border-bottom-color:#FF7A59;}
# MAGIC .flow-tab-active-2{color:#4299E0;border-bottom-color:#4299E0;}
# MAGIC .flow-panel{display:none;}
# MAGIC .flow-panel-active{display:block;}
# MAGIC @media screen and (max-width:900px){.flow-box-content{grid-template-columns:1fr;}.flow-box-img img{max-height:380px;}.flow-tabbar{flex-wrap:wrap;}}
# MAGIC </style>
# MAGIC <div class="flow-tabbar">
# MAGIC <button class="flow-tab" onclick="showFlowTab(1)">Notebook Task</button>
# MAGIC <button class="flow-tab" onclick="showFlowTab(2)">SQL Task</button>
# MAGIC </div>
# MAGIC <br>
# MAGIC <div class="flow-panel">
# MAGIC <div class="flow-grid">
# MAGIC <div class="flow-box flow1">
# MAGIC <div class="flow-box-title">
# MAGIC <div class="flow-box-icon">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/notebook_icon.png" alt="Notebook Task Options">
# MAGIC <span>Notebook Task Options</span>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="flow-box-content">
# MAGIC <div class="flow-box-text">
# MAGIC <p>Based on task type selected, you get specific options.</p>
# MAGIC <div class="task-example">
# MAGIC <p><strong>Example</strong></p>
# MAGIC <ul>
# MAGIC <li>Source</li>
# MAGIC <li>Path</li>
# MAGIC <li>Compute options</li>
# MAGIC <li>And More</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="flow-box-img">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/notebook_task_options.png" alt="Notebook Task Options">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="flow-panel">
# MAGIC <div class="flow-grid">
# MAGIC <div class="flow-box flow2">
# MAGIC <div class="flow-box-title">
# MAGIC <div class="flow-box-icon">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/sql_icon.png" alt="SQL Task Options">
# MAGIC <span>SQL Task Options</span>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="flow-box-content">
# MAGIC <div class="flow-box-text">
# MAGIC <p>Based on task type selected, you get specific options.</p>
# MAGIC <div class="task-example">
# MAGIC <p><strong>Example</strong></p>
# MAGIC <ul>
# MAGIC <li>Task name</li>
# MAGIC <li>SQL query</li>
# MAGIC <li>SQL warehouse</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="flow-box-img">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/sql_task_options.png" alt="SQL Task Options">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC function showFlowTab(n){
# MAGIC var tabs=document.getElementsByClassName("flow-tab");
# MAGIC var panels=document.getElementsByClassName("flow-panel");
# MAGIC for(var i=0;i<tabs.length;i++){
# MAGIC tabs[i].className="flow-tab";
# MAGIC panels[i].className="flow-panel";
# MAGIC }
# MAGIC panels[n-1].className="flow-panel flow-panel-active";
# MAGIC tabs[n-1].className="flow-tab flow-tab-active-"+n;
# MAGIC }
# MAGIC window.addEventListener("load",function(){showFlowTab(1);});
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC **Notebook Task Options**:<br>
# MAGIC Here's a specific example for Notebook tasks. When you select a Notebook task type, you get options like specifying the Source path to your notebook, choosing Compute options (cluster configuration), and many more settings specific to running notebooks.
# MAGIC
# MAGIC The interface adapts based on your task type selection, providing relevant configuration options.
# MAGIC
# MAGIC **SQL Task Options**:<br>
# MAGIC Similarly, for SQL tasks, you get different options. You can specify the SQL task details, write or reference your SQL query, and select the SQL warehouse that will execute your query.
# MAGIC
# MAGIC Each task type provides the specific configuration options needed for that type of workload.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A5. Languages Supported
# MAGIC
# MAGIC <div class="language-icons-horizontal">
# MAGIC <style>
# MAGIC .language-icons-horizontal{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .language-icons-horizontal *{box-sizing:border-box;}
# MAGIC .lang-row{display:flex;align-items:center;justify-content:space-between;gap:18px;background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;padding:28px 34px;box-shadow:0 3px 12px rgba(27,49,57,.07);}
# MAGIC .lang-item{flex:1;min-height:130px;display:flex;align-items:center;justify-content:center;background:#ffffff;border-radius:12px;padding:16px;}
# MAGIC .lang-item img{max-width:150px;max-height:88px;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:grayscale(1) contrast(1.05) brightness(1.08);opacity:1;border-radius:4px;}
# MAGIC .lang-item.sql img{max-width:100px;max-height:76px;}
# MAGIC @media screen and (max-width:900px){.lang-row{flex-wrap:wrap;justify-content:center;}.lang-item{flex:0 0 30%;min-height:110px;}.lang-item img{max-width:130px;max-height:74px;}}
# MAGIC @media screen and (max-width:600px){.lang-item{flex:0 0 45%;}}
# MAGIC </style>
# MAGIC <div class="lang-row">
# MAGIC <div class="lang-item"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/python_logo.png" alt="Python logo"></div>
# MAGIC <div class="lang-item"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/scala_logo.png" alt="Scala logo"></div>
# MAGIC <div class="lang-item"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/java_logo.png" alt="Java logo"></div>
# MAGIC <div class="lang-item"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/r_logo.png" alt="R logo"></div>
# MAGIC <div class="lang-item sql"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/sql_logo.png" alt="SQL logo"></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Lakeflow Jobs supports multiple programming languages: Python, SQL, Scala, R, and Java through JAR files. This broad language support ensures that teams can use their preferred languages and existing code assets.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A6. Jobs Orchestration: Control Flow, Triggers, and Compute
# MAGIC
# MAGIC Click each expandable block to learn more about them.
# MAGIC
# MAGIC <div class="inline-acc-wrap">
# MAGIC <style>
# MAGIC .inline-acc-wrap {
# MAGIC  width: 1100px;
# MAGIC  max-width: 100%;
# MAGIC  margin: 24px auto;
# MAGIC  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC  color: #0B2026;
# MAGIC }
# MAGIC .inline-acc-stack {
# MAGIC  display: flex;
# MAGIC  flex-direction: column;
# MAGIC  gap: 14px;
# MAGIC }
# MAGIC .inline-acc-row {
# MAGIC  display: flex;
# MAGIC  align-items: stretch;
# MAGIC  gap: 14px;
# MAGIC  width: 100%;
# MAGIC }
# MAGIC .inline-acc-tab {
# MAGIC  flex: 0 0 220px;
# MAGIC  max-width: 220px;
# MAGIC  min-width: 220px;
# MAGIC  background: #FF5F46;
# MAGIC  color: #ffffff;
# MAGIC  border: 2px solid #FF5F46;
# MAGIC  border-radius: 8px;
# MAGIC  padding: 16px 18px;
# MAGIC  font-size: 18px;
# MAGIC  font-weight: 700;
# MAGIC  text-align: left;
# MAGIC  cursor: pointer;
# MAGIC  box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC  box-sizing: border-box;
# MAGIC  line-height: 1.5;
# MAGIC }
# MAGIC .inline-acc-tab:hover {
# MAGIC  background: #FF735F;
# MAGIC  border-color: #FF735F;
# MAGIC }
# MAGIC .inline-acc-tab.active {
# MAGIC  background: #FF5F46;
# MAGIC  border-color: #98102A;
# MAGIC  box-shadow: 0 0 0 3px rgba(255,95,70,0.18);
# MAGIC }
# MAGIC .inline-acc-tab b {
# MAGIC  color: #ffffff;
# MAGIC }
# MAGIC .inline-acc-panel {
# MAGIC  display: none;
# MAGIC  flex: 1 1 auto;
# MAGIC  min-height: 72px;
# MAGIC  border-radius: 8px;
# MAGIC  padding: 18px 22px;
# MAGIC  box-sizing: border-box;
# MAGIC  color: #0B2026;
# MAGIC  font-size: 18px;
# MAGIC  line-height: 1.6;
# MAGIC  box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC  border-left: 6px solid #FF5F46;
# MAGIC }
# MAGIC .inline-acc-panel.active {
# MAGIC  display: block;
# MAGIC }
# MAGIC .inline-acc-panel p {
# MAGIC  margin: 0;
# MAGIC }
# MAGIC .inline-acc-panel img {
# MAGIC  max-width: 100%;
# MAGIC  height: auto;
# MAGIC  background: transparent;
# MAGIC  mix-blend-mode: multiply;
# MAGIC  filter: contrast(1.15) brightness(1);
# MAGIC  border-radius: 4px;
# MAGIC  display: block;
# MAGIC }
# MAGIC .inline-hover-row {
# MAGIC  width: 100%;
# MAGIC  min-height: 88px;
# MAGIC  background: #FFF1EE;
# MAGIC  border: 1px solid #FFD2C9;
# MAGIC  border-left: 6px solid #FF5F46;
# MAGIC  border-radius: 10px;
# MAGIC  box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC  position: relative;
# MAGIC  overflow: hidden;
# MAGIC  display: flex;
# MAGIC  align-items: center;
# MAGIC  justify-content: center;
# MAGIC  padding: 18px 20px;
# MAGIC  box-sizing: border-box;
# MAGIC  text-align: center;
# MAGIC  margin-top: 14px;
# MAGIC }
# MAGIC .inline-hover-title {
# MAGIC  font-size: 18px;
# MAGIC  font-weight: 800;
# MAGIC  color: #0B2026;
# MAGIC }
# MAGIC .inline-hover-text {
# MAGIC  position: absolute;
# MAGIC  inset: 0;
# MAGIC  background: #FF5F46;
# MAGIC  color: #ffffff;
# MAGIC  display: flex;
# MAGIC  align-items: center;
# MAGIC  justify-content: center;
# MAGIC  padding: 16px 20px;
# MAGIC  box-sizing: border-box;
# MAGIC  font-size: 18px;
# MAGIC  font-weight: 700;
# MAGIC  line-height: 1.5;
# MAGIC  opacity: 0;
# MAGIC  transition: opacity 0.22s ease;
# MAGIC }
# MAGIC .inline-hover-row:hover .inline-hover-text {
# MAGIC  opacity: 1;
# MAGIC }
# MAGIC @media screen and (max-width: 900px) {
# MAGIC  .inline-acc-row {
# MAGIC   flex-direction: column;
# MAGIC  }
# MAGIC  .inline-acc-tab {
# MAGIC   max-width: 100%;
# MAGIC   min-width: 100%;
# MAGIC   flex: 1 1 auto;
# MAGIC  }
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="inline-acc-stack">
# MAGIC  <div class="inline-acc-row">
# MAGIC   <button class="inline-acc-tab" onclick="openInlineAcc(this, 'inline-panel-1')"><b>Jobs</b> consist of one or more <b>Tasks</b></button>
# MAGIC   <div class="inline-acc-panel" id="inline-panel-1">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/jobs_many_tasks.png" alt="Jobs consist of one or more tasks">
# MAGIC   </div>
# MAGIC  </div>
# MAGIC
# MAGIC  <div class="inline-acc-row">
# MAGIC   <button class="inline-acc-tab" onclick="openInlineAcc(this, 'inline-panel-2')"><b>Control flows</b> can be established between <b>Tasks</b></button>
# MAGIC   <div class="inline-acc-panel" id="inline-panel-2">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/control_flows_between_tasks.png" alt="Control flows can be established between tasks">
# MAGIC   </div>
# MAGIC  </div>
# MAGIC
# MAGIC  <div class="inline-acc-row">
# MAGIC   <button class="inline-acc-tab" onclick="openInlineAcc(this, 'inline-panel-3')"><b>Jobs</b> supports different <b>Triggers</b></button>
# MAGIC   <div class="inline-acc-panel" id="inline-panel-3">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/jobs_different_triggers.png" alt="Jobs support different triggers">
# MAGIC   </div>
# MAGIC  </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="inline-hover-row">
# MAGIC  <div class="inline-hover-title">Compute</div>
# MAGIC  <div class="inline-hover-text">The compute layer supports all these different orchestration patterns.</div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function openInlineAcc(btn, panelId) {
# MAGIC  var root = btn.closest(".inline-acc-wrap");
# MAGIC  var buttons = root.querySelectorAll(".inline-acc-tab");
# MAGIC  var panels = root.querySelectorAll(".inline-acc-panel");
# MAGIC  buttons.forEach(function(button) {
# MAGIC   button.classList.remove("active");
# MAGIC  });
# MAGIC  panels.forEach(function(panel) {
# MAGIC   panel.classList.remove("active");
# MAGIC  });
# MAGIC  btn.classList.add("active");
# MAGIC  document.getElementById(panelId).classList.add("active");
# MAGIC }
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC We already learned about Jobs and Tasks.
# MAGIC
# MAGIC This comprehensive view shows how tasks can be connected with different control flow patterns. You can implement Sequential execution (one after another), Parallel execution (multiple tasks at once), Conditional execution with If/else logic, Run Job tasks for modular design, and For each loops for iterative processing.
# MAGIC
# MAGIC Additionally, Jobs supports different trigger types: Manual triggers for on-demand execution, Scheduled triggers using cron expressions, API triggers for programmatic execution, File Arrival triggers for event-driven processing, Table triggers for data change events, and Continuous triggers for streaming workloads.
# MAGIC
# MAGIC The compute layer supports all these different orchestration patterns.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A7. Compute Options
# MAGIC
# MAGIC Jobs can be executed on different compute.
# MAGIC
# MAGIC <div class="bblj-compute-options">
# MAGIC <style>
# MAGIC .bblj-compute-options{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .bblj-compute-options *{box-sizing:border-box;}
# MAGIC .bblj-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:18px;align-items:stretch;}
# MAGIC .bblj-card{position:relative;background:#fff;border:2px solid var(--c);border-radius:4px;box-shadow:0 3px 10px rgba(27,49,57,.12);padding:0 22px 22px 22px;min-height:520px;}
# MAGIC .bblj-ribbon{position:relative;background:var(--c);color:#fff;font-size:18px;line-height:1.18;font-weight:600;text-align:center;min-height:52px;padding:9px 18px;margin:-66px -4px 16px -4px;display:flex;align-items:center;justify-content:center;}
# MAGIC .bblj-ribbon:before{content:"";position:absolute;left:0;top:0;border-top:26px solid transparent;border-bottom:26px solid transparent;border-left:22px solid #fff;}
# MAGIC .bblj-ribbon:after{content:"";position:absolute;right:-22px;top:0;border-top:26px solid transparent;border-bottom:26px solid transparent;border-left:22px solid var(--c);}
# MAGIC .bblj-spacer{height:66px;}
# MAGIC .bblj-title{font-size:20px;line-height:1.2;font-weight:600;color:#000;text-align:center;margin:10px 0 12px 0;}
# MAGIC .bblj-icon{width:70px;height:70px;object-fit:contain;display:block;margin:0 auto 28px auto;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC .bblj-text{font-size:16px;line-height:1.18;color:#000;margin:0;}
# MAGIC .bblj-text p{margin:0 0 22px 0;}
# MAGIC .bblj-red{color:#98102A;}
# MAGIC .bblj-teal{color:#0E5668;}
# MAGIC .bblj-green{color:#00A972;}
# MAGIC .bblj-blue{color:#2B91E8;}
# MAGIC @media screen and (max-width:1100px){.bblj-grid{grid-template-columns:repeat(2,1fr);row-gap:78px;}.bblj-card{min-height:460px;}}
# MAGIC @media screen and (max-width:700px){.bblj-grid{grid-template-columns:1fr;row-gap:78px;}}
# MAGIC </style>
# MAGIC <div class="bblj-spacer"></div>
# MAGIC <div class="bblj-grid">
# MAGIC <div class="bblj-card" style="--c:#98102A;">
# MAGIC <div class="bblj-ribbon">Development, Ad-Hoc Analysis &amp;<br>Exploration</div>
# MAGIC <div class="bblj-title">Interactive Clusters</div>
# MAGIC <img class="bblj-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/Interactive_clusters_icon.png" alt="Interactive clusters icon">
# MAGIC <div class="bblj-text">
# MAGIC <p><span class="bblj-red">Interactive or all-purpose clusters</span> can be shared by multiple users.</p>
# MAGIC <p>It is best for performing <span class="bblj-red">ad-hoc analysis, data exploration, or development.</span></p>
# MAGIC <p>Interactive <span class="bblj-red">should not be used in Production</span> as they are not cost-efficient.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="bblj-card" style="--c:#0E5668;">
# MAGIC <div class="bblj-ribbon">Production Grade &amp; Operational<br>Use-Cases</div>
# MAGIC <div class="bblj-title">Job Clusters</div>
# MAGIC <img class="bblj-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/job_clusters_icon.png" alt="Job clusters icon">
# MAGIC <div class="bblj-text">
# MAGIC <p><span class="bblj-teal">Job clusters</span> are approximately <span class="bblj-teal">50% cheaper</span> as they terminate when the job ends, reducing resource usage and costs.</p>
# MAGIC <p>However, Job clusters are subject to <span class="bblj-teal">cloud providers start-up times.</span></p>
# MAGIC <p>With Databricks Jobs, you can <span class="bblj-teal">re-use the same cluster across Tasks for better price performance!</span></p>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="bblj-card" style="--c:#00A972;">
# MAGIC <div class="bblj-ribbon">Simple, Fast, Reliable, Cost<br>Efficient!</div>
# MAGIC <div class="bblj-title">Serverless</div>
# MAGIC <img class="bblj-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/serverless_icon.png" alt="Serverless icon">
# MAGIC <div class="bblj-text">
# MAGIC <p><span class="bblj-green">Serverless Workflows</span> are a <span class="bblj-green">fully managed service</span> that are operationally simpler and more reliable</p>
# MAGIC <p>They provide you with <span class="bblj-green">faster clusters &amp; auto-scaling capabilities</span> providing you with a <span class="bblj-green">better user experience for a lower cost.</span></p>
# MAGIC <p>With <span class="bblj-green">out of the box performance optimizations</span>, Serverless provides you with a <span class="bblj-green">lower overall TCO.</span></p>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="bblj-card" style="--c:#2B91E8;">
# MAGIC <div class="bblj-ribbon">Compute for SQL Queries and BI,<br>Serverless by default</div>
# MAGIC <div class="bblj-title">SQL Warehouse</div>
# MAGIC <img class="bblj-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/SQL_warehouse_icon.png" alt="SQL warehouse icon">
# MAGIC <div class="bblj-text">
# MAGIC <p><span class="bblj-blue">Purpose-built for SQL queries, dashboards, and BI;</span> Can be attached to notebook as well.</p>
# MAGIC <p><span class="bblj-blue">High concurrency + autoscaling</span> via Intelligent Workload Management for consistent low latency.</p>
# MAGIC <p><span class="bblj-blue">auto-start/auto-stop</span> and <span class="bblj-blue">adjustable cluster size</span> and max clusters for peak loads <span class="bblj-blue">helps control costs.</span></p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Jobs can be executed on different types of compute, and choosing the right compute is crucial for both performance and cost:
# MAGIC
# MAGIC - Interactive Clusters can be shared by multiple users and are best for ad-hoc analysis, data exploration, or development. However, they should not be used in production as they are not cost-efficient.
# MAGIC
# MAGIC - Job Clusters are approximately 50% cheaper as they terminate when the job ends, reducing resource usage and costs. They're ideal for production workloads, though they are subject to cloud provider start-up times. With Databricks Jobs, you can reuse the same cluster across tasks for better price performance.
# MAGIC
# MAGIC - Serverless provides a fully managed service that is operationally simpler and more reliable. It offers faster clusters and auto-scaling capabilities, providing better user experience for lower cost. With out-of-the-box performance optimizations, Serverless provides lower overall TCO.
# MAGIC
# MAGIC - SQL Warehouse is purpose-built for SQL queries, dashboards, and BI, and is serverless by default. It offers high concurrency and autoscaling via Intelligent Workload Management, with auto-start/auto-stop and adjustable cluster sizing to help control costs.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A8. Serverless Performance Mode
# MAGIC
# MAGIC Click the green highlighted boxes to know more about Serverless Performance Mode. 
# MAGIC
# MAGIC <div class="perf-wrap">
# MAGIC
# MAGIC <style>
# MAGIC .perf-wrap {
# MAGIC  width: 1100px;
# MAGIC  max-width: 100%;
# MAGIC  margin: 24px auto;
# MAGIC  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC  color: #0B2026;
# MAGIC }
# MAGIC
# MAGIC .perf-stage {
# MAGIC  display: flex;
# MAGIC  gap: 20px;
# MAGIC  align-items: flex-start;
# MAGIC }
# MAGIC
# MAGIC .perf-left {
# MAGIC  flex: 0 0 32%;
# MAGIC  max-width: 32%;
# MAGIC }
# MAGIC
# MAGIC .perf-right {
# MAGIC  flex: 0 0 68%;
# MAGIC  max-width: 68%;
# MAGIC  position: relative;
# MAGIC  min-height: 420px;
# MAGIC }
# MAGIC
# MAGIC .perf-image-wrap {
# MAGIC  position: relative;
# MAGIC  width: 100%;
# MAGIC  background: #fff;
# MAGIC }
# MAGIC
# MAGIC .perf-image-wrap img {
# MAGIC  display: block;
# MAGIC  width: 100%;
# MAGIC  height: auto;
# MAGIC  background: transparent;
# MAGIC  mix-blend-mode: multiply;
# MAGIC  filter: contrast(1.15) brightness(1);
# MAGIC  border-radius: 4px;
# MAGIC }
# MAGIC
# MAGIC .perf-highlight-btn {
# MAGIC  position: absolute;
# MAGIC  left: 5.5%;
# MAGIC  top: 35%;
# MAGIC  width: 51%;
# MAGIC  height: 10.5%;
# MAGIC  border: 3px solid #00A972;
# MAGIC  background: rgba(0,169,114,0.08);
# MAGIC  border-radius: 0;
# MAGIC  cursor: pointer;
# MAGIC  box-sizing: border-box;
# MAGIC }
# MAGIC
# MAGIC .perf-highlight-btn:hover {
# MAGIC  background: rgba(0,169,114,0.14);
# MAGIC }
# MAGIC
# MAGIC .perf-connector {
# MAGIC  display: none;
# MAGIC  position: absolute;
# MAGIC  left: 0;
# MAGIC  top: 0;
# MAGIC  width: 100%;
# MAGIC  height: 100%;
# MAGIC  pointer-events: none;
# MAGIC }
# MAGIC
# MAGIC .perf-connector.active {
# MAGIC  display: block;
# MAGIC }
# MAGIC
# MAGIC .perf-line-h1,
# MAGIC .perf-line-v,
# MAGIC .perf-line-h2 {
# MAGIC  position: absolute;
# MAGIC  background: #00A972;
# MAGIC }
# MAGIC
# MAGIC .perf-line-h1 {
# MAGIC  left: -23%;
# MAGIC  top: 45%;
# MAGIC  width: 21%;
# MAGIC  height: 3px;
# MAGIC }
# MAGIC
# MAGIC .perf-line-v {
# MAGIC  left: -2.4%;
# MAGIC  top: 15%;
# MAGIC  width: 3px;
# MAGIC  height: 30%;
# MAGIC }
# MAGIC
# MAGIC .perf-line-h2 {
# MAGIC  left: -2.4%;
# MAGIC  top: 15%;
# MAGIC  width: 2.9%;
# MAGIC  height: 3px;
# MAGIC }
# MAGIC
# MAGIC .perf-content {
# MAGIC  display: none;
# MAGIC }
# MAGIC
# MAGIC .perf-content.active {
# MAGIC  display: block;
# MAGIC }
# MAGIC
# MAGIC .perf-top-note {
# MAGIC  border: 2px solid #00A972;
# MAGIC  background: #F9F7F4;
# MAGIC  padding: 14px 18px;
# MAGIC  text-align: center;
# MAGIC  font-size: 15pt;
# MAGIC  line-height: 1.35;
# MAGIC  margin-bottom: 12px;
# MAGIC }
# MAGIC
# MAGIC .perf-options {
# MAGIC  display: flex;
# MAGIC  gap: 18px;
# MAGIC  align-items: flex-start;
# MAGIC }
# MAGIC
# MAGIC .perf-col {
# MAGIC  flex: 1 1 0;
# MAGIC }
# MAGIC
# MAGIC .perf-option-btn {
# MAGIC  width: 100%;
# MAGIC  border: 3px solid #00A972;
# MAGIC  color: #ffffff;
# MAGIC  font-size: 15pt;
# MAGIC  font-weight: 800;
# MAGIC  text-align: center;
# MAGIC  padding: 16px 12px;
# MAGIC  cursor: pointer;
# MAGIC  line-height: 1.2;
# MAGIC  box-sizing: border-box;
# MAGIC  display: flex;
# MAGIC  align-items: center;
# MAGIC  justify-content: center;
# MAGIC  min-height: 76px;
# MAGIC  border-radius: 8px;
# MAGIC }
# MAGIC
# MAGIC .perf-option-btn.off {
# MAGIC  background: #FF5F46;
# MAGIC  box-shadow: 0 -6px 0 #00A972, 0 0 0 4px rgba(0,169,114,0.10);
# MAGIC }
# MAGIC
# MAGIC .perf-option-btn.on {
# MAGIC  background: #00A972;
# MAGIC  box-shadow: 0 0 0 4px rgba(0,169,114,0.10);
# MAGIC }
# MAGIC
# MAGIC .perf-option-btn:hover {
# MAGIC  filter: brightness(0.96);
# MAGIC }
# MAGIC
# MAGIC .perf-option-btn.active {
# MAGIC  box-shadow: 0 -6px 0 #00A972, 0 0 0 5px rgba(0,169,114,0.20);
# MAGIC }
# MAGIC
# MAGIC .perf-btn-main {
# MAGIC  display: block;
# MAGIC }
# MAGIC
# MAGIC .perf-btn-sub {
# MAGIC  display: inline-flex;
# MAGIC  align-items: center;
# MAGIC  gap: 6px;
# MAGIC  font-size: 10.5pt;
# MAGIC  font-weight: 600;
# MAGIC  opacity: 0.95;
# MAGIC }
# MAGIC
# MAGIC .perf-btn-plus {
# MAGIC  display: inline-flex;
# MAGIC  align-items: center;
# MAGIC  justify-content: center;
# MAGIC  width: 18px;
# MAGIC  height: 18px;
# MAGIC  border: 1.5px solid #ffffff;
# MAGIC  border-radius: 50%;
# MAGIC  font-size: 11pt;
# MAGIC  line-height: 1;
# MAGIC }
# MAGIC
# MAGIC .perf-detail {
# MAGIC  display: none;
# MAGIC  margin-top: 0;
# MAGIC  border: 1px solid #8f8f8f;
# MAGIC  border-top: none;
# MAGIC  background: #ffffff;
# MAGIC  padding: 14px 18px 10px 18px;
# MAGIC  min-height: 180px;
# MAGIC  box-sizing: border-box;
# MAGIC }
# MAGIC
# MAGIC .perf-detail.active {
# MAGIC  display: block;
# MAGIC }
# MAGIC
# MAGIC .perf-detail ul {
# MAGIC  margin: 0;
# MAGIC  padding-left: 22px;
# MAGIC }
# MAGIC
# MAGIC .perf-detail li {
# MAGIC  margin-bottom: 12px;
# MAGIC  font-size: 13.5pt;
# MAGIC  line-height: 1.45;
# MAGIC }
# MAGIC
# MAGIC .perf-detail li:last-child {
# MAGIC  margin-bottom: 0;
# MAGIC }
# MAGIC
# MAGIC @media screen and (max-width: 980px) {
# MAGIC  .perf-stage {
# MAGIC  flex-direction: column;
# MAGIC  }
# MAGIC
# MAGIC  .perf-left,
# MAGIC  .perf-right {
# MAGIC  flex: 1 1 auto;
# MAGIC  max-width: 100%;
# MAGIC  }
# MAGIC
# MAGIC  .perf-options {
# MAGIC  flex-direction: column;
# MAGIC  }
# MAGIC
# MAGIC  .perf-right {
# MAGIC  min-height: auto;
# MAGIC  }
# MAGIC
# MAGIC  .perf-line-h1,
# MAGIC  .perf-line-v,
# MAGIC  .perf-line-h2 {
# MAGIC  display: none;
# MAGIC  }
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="perf-stage">
# MAGIC  <div class="perf-left">
# MAGIC   <div class="perf-image-wrap">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/serverless_performance_model.png" alt="Job details page with Performance optimized setting highlighted">
# MAGIC    <button class="perf-highlight-btn" onclick="showPerfIntro()" aria-label="Show Performance optimized details"></button>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <div class="perf-right">
# MAGIC   <div class="perf-connector" id="perf-connector">
# MAGIC    <div class="perf-line-h1"></div>
# MAGIC    <div class="perf-line-v"></div>
# MAGIC    <div class="perf-line-h2"></div>
# MAGIC   </div>
# MAGIC   <div class="perf-content" id="perf-content">
# MAGIC    <div class="perf-top-note">
# MAGIC     Use the <b>Performance optimized</b> setting in the job details page to choose between <b>lower cost</b> and <b>faster execution</b> for serverless tasks.
# MAGIC    </div>
# MAGIC    <div class="perf-options">
# MAGIC     <div class="perf-col">
# MAGIC      <button class="perf-option-btn off" id="perf-off-btn" onclick="showPerfCard('off')">
# MAGIC  Performance Optimized<br>(Off)
# MAGIC </button>
# MAGIC      <div class="perf-detail" id="perf-off-card">
# MAGIC       <ul>
# MAGIC        <li><b>Standard mode</b> focuses on <b>cost-efficiency</b></li>
# MAGIC        <li><b>Longer startup time</b> (typically 4–6 minutes)</li>
# MAGIC        <li>Best for <b>non-urgent workloads</b> with flexible timing</li>
# MAGIC       </ul>
# MAGIC      </div>
# MAGIC     </div>
# MAGIC     <div class="perf-col">
# MAGIC      <button class="perf-option-btn on" id="perf-on-btn" onclick="showPerfCard('on')">
# MAGIC  Performance Optimized<br>(On)
# MAGIC </button>
# MAGIC      <div class="perf-detail" id="perf-on-card">
# MAGIC       <ul>
# MAGIC        <li><b>Enables</b> faster job startup and execution</li>
# MAGIC        <li>Ideal for <b>time-sensitive</b> workloads</li>
# MAGIC        <li><b>Applies</b> only to tasks with <b>serverless</b> compute in job</li>
# MAGIC       </ul>
# MAGIC      </div>
# MAGIC     </div>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function showPerfIntro() {
# MAGIC  document.getElementById("perf-connector").classList.add("active");
# MAGIC  document.getElementById("perf-content").classList.add("active");
# MAGIC }
# MAGIC
# MAGIC function showPerfCard(mode) {
# MAGIC  showPerfIntro();
# MAGIC  if (mode === "off") {
# MAGIC   document.getElementById("perf-off-card").classList.add("active");
# MAGIC   document.getElementById("perf-off-btn").classList.add("active");
# MAGIC  }
# MAGIC  if (mode === "on") {
# MAGIC   document.getElementById("perf-on-card").classList.add("active");
# MAGIC   document.getElementById("perf-on-btn").classList.add("active");
# MAGIC  }
# MAGIC }
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Standard Mode focuses on cost-efficiency with longer startup time (typically 4–6 minutes), making it best for non-urgent workloads with flexible timing.
# MAGIC
# MAGIC Optimized Mode enables faster job startup and execution, making it ideal for time-sensitive workloads. This setting applies only to tasks with serverless compute in your job.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A9. Selecting Compute
# MAGIC
# MAGIC The diagram illustrates an important principle: A job can have one or more tasks inside it, and each task can be assigned its own compute resource. Tasks in the same job can either share the same compute or use different compute as required.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_building_blocks_lakeflow_jobs/selecting_compute.png"
# MAGIC     alt="Slide showing that each task in a job can use its own compute resource or share compute"
# MAGIC     style="width: 1100px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC This diagram illustrates an important principle: A job can have one or more tasks inside it, and each task can be assigned its own compute resource. Tasks in the same job can either share the same compute or use different compute as required.
# MAGIC
# MAGIC You might have Task-1 running on an All Purpose Cluster, Task-2 on Serverless, Task-3 on a Job Cluster, and so on. This flexibility allows you to optimize both performance and cost for each specific task.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Task Orchestration

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. What is a DAG?
# MAGIC
# MAGIC <div style="width:1100px; max-width:100%; margin:32px auto; display:flex; gap:20px; align-items:stretch; box-sizing:border-box;">
# MAGIC   <div style="flex:0 0 calc(60% - 10px); max-width:calc(60% - 10px); background:#F9F7F4; border-radius:8px; padding:24px 22px; box-shadow:0 2px 8px rgba(11,32,38,0.07); box-sizing:border-box;">
# MAGIC     <div style="font-size:14pt; color:#0b2026; line-height:1.7;">
# MAGIC       <p style="margin-top:0;">A DAG is a <b>conceptual representation</b> of series of activities, including data processing flows.</p>
# MAGIC       <br>
# MAGIC       <ul style="margin:0 0 16px 0; padding-left:22px;">
# MAGIC         <li><b>D</b>irected - unambiguous direction for each edge</li>
# MAGIC         <li><b>A</b>cyclic - contains no cycles</li>
# MAGIC         <li><b>G</b>raph - collection of vertices connected by edges.</li>
# MAGIC       </ul>
# MAGIC       <p style="margin-bottom:0;">The diagram shows a simple DAG where <b>Task-1</b> leads to both <b>Task-2</b> and <b>Task-3</b>, representing the flow of execution.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="flex:0 0 calc(40% - 10px); max-width:calc(40% - 10px); background:#F9F7F4; border-radius:8px; padding:16px; box-shadow:0 2px 8px rgba(11,32,38,0.07); box-sizing:border-box; display:flex; align-items:center; justify-content:center;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_task_orchestration/task_DAG.png" alt="DAG Diagram" style="max-width:90%; height:auto; display:block; background:transparent; mix-blend-mode:multiply; filter:contrast(1.15) brightness(1); border-radius:4px;">
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Task Orchestration Overview
# MAGIC
# MAGIC <div class="dag-demo">
# MAGIC <style>
# MAGIC .dag-demo {
# MAGIC  width: 1100px;
# MAGIC  max-width: 100%;
# MAGIC  margin: 24px auto;
# MAGIC  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC  color: #0B2026;
# MAGIC }
# MAGIC .dag-demo * {
# MAGIC  box-sizing: border-box;
# MAGIC }
# MAGIC .dag-text-layout {
# MAGIC  display: grid;
# MAGIC  grid-template-columns: 0.9fr 1.1fr;
# MAGIC  gap: 16px;
# MAGIC  margin-bottom: 22px;
# MAGIC  align-items: stretch;
# MAGIC }
# MAGIC .dag-text-main {
# MAGIC  background: #FF5F46;
# MAGIC  color: #ffffff;
# MAGIC  border-radius: 12px;
# MAGIC  padding: 22px 24px;
# MAGIC  display: flex;
# MAGIC  align-items: center;
# MAGIC  justify-content: center;
# MAGIC  text-align: center;
# MAGIC  box-shadow: 0 2px 8px rgba(27,49,57,0.08);
# MAGIC }
# MAGIC .dag-text-main h3 {
# MAGIC  margin: 0;
# MAGIC  font-size: 22px;
# MAGIC  line-height: 1.25;
# MAGIC  font-weight: 800;
# MAGIC }
# MAGIC .dag-text-cards {
# MAGIC  display: grid;
# MAGIC  grid-template-columns: 1fr 1fr;
# MAGIC  gap: 14px;
# MAGIC }
# MAGIC .dag-text-card {
# MAGIC  background: #F9F7F4;
# MAGIC  border: 1px solid #DCE0E2;
# MAGIC  border-left: 6px solid #FF5F46;
# MAGIC  border-radius: 12px;
# MAGIC  padding: 18px 20px;
# MAGIC  box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC }
# MAGIC .dag-text-card h4 {
# MAGIC  margin: 0 0 8px 0;
# MAGIC  font-size: 18px;
# MAGIC  line-height: 1.25;
# MAGIC  color: #0B2026;
# MAGIC  font-weight: 800;
# MAGIC }
# MAGIC .dag-text-card p {
# MAGIC  margin: 0;
# MAGIC  font-size: 16px;
# MAGIC  line-height: 1.5;
# MAGIC  color: #0B2026;
# MAGIC }
# MAGIC .dag-text-card.green {
# MAGIC  border-left-color: #00A972;
# MAGIC }
# MAGIC .dag-text-callout {
# MAGIC  margin: 0 0 24px 0;
# MAGIC  background: #FFF1EE;
# MAGIC  border: 1px solid #FFD2C9;
# MAGIC  border-left: 6px solid #FF5F46;
# MAGIC  border-radius: 12px;
# MAGIC  padding: 16px 20px;
# MAGIC  font-size: 17px;
# MAGIC  line-height: 1.5;
# MAGIC  font-weight: 700;
# MAGIC  color: #0B2026;
# MAGIC  box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC }
# MAGIC .dag-canvas {
# MAGIC  position: relative;
# MAGIC  width: 100%;
# MAGIC  height: 430px;
# MAGIC  background: #ffffff;
# MAGIC  border-radius: 12px;
# MAGIC }
# MAGIC .task-box {
# MAGIC  position: absolute;
# MAGIC  min-width: 120px;
# MAGIC  padding: 14px 22px;
# MAGIC  border-radius: 10px;
# MAGIC  background: #FF5F46;
# MAGIC  color: white;
# MAGIC  font-size: 18pt;
# MAGIC  font-weight: 700;
# MAGIC  text-align: center;
# MAGIC  box-shadow: 0 2px 8px rgba(27,49,57,0.08);
# MAGIC  border: 2px solid #0b2026;
# MAGIC  z-index: 2;
# MAGIC }
# MAGIC .dep-text {
# MAGIC  position: absolute;
# MAGIC  font-size: 12pt;
# MAGIC  color: #0b2026;
# MAGIC  font-weight: 600;
# MAGIC  line-height: 1.3;
# MAGIC  text-align: center;
# MAGIC  z-index: 2;
# MAGIC  white-space: nowrap;
# MAGIC }
# MAGIC .arrow {
# MAGIC  position: absolute;
# MAGIC  height: 4px;
# MAGIC  background: #FF5F46;
# MAGIC  border-radius: 4px;
# MAGIC  transform-origin: left center;
# MAGIC  z-index: 1;
# MAGIC }
# MAGIC .arrow::after {
# MAGIC  content: "";
# MAGIC  position: absolute;
# MAGIC  right: -2px;
# MAGIC  top: 50%;
# MAGIC  transform: translateY(-50%);
# MAGIC  width: 0;
# MAGIC  height: 0;
# MAGIC  border-left: 12px solid #FF5F46;
# MAGIC  border-top: 7px solid transparent;
# MAGIC  border-bottom: 7px solid transparent;
# MAGIC }
# MAGIC .task1 { left: 70px; top: 170px; }
# MAGIC .task2 { left: 390px; top: 70px; }
# MAGIC .task3 { left: 390px; top: 275px; }
# MAGIC .task4 { left: 735px; top: 170px; }
# MAGIC .dep2 { left: 394px; top: 36px; width: 160px; }
# MAGIC .dep3 { left: 394px; top: 356px; width: 160px; }
# MAGIC .dep4 { left: 690px; top: 254px; width: 250px; }
# MAGIC .arrow1 { left: 252px; top: 190px; width: 132px; --rot: -28deg; }
# MAGIC .arrow2 { left: 252px; top: 230px; width: 132px; --rot: 28deg; }
# MAGIC .arrow3 { left: 572px; top: 118px; width: 160px; --rot: 24deg; }
# MAGIC .arrow4 { left: 572px; top: 312px; width: 160px; --rot: -24deg; }
# MAGIC .task1 {
# MAGIC  opacity: 0;
# MAGIC  transform: translateY(10px);
# MAGIC  animation: task1Loop 12.5s ease-in-out infinite;
# MAGIC }
# MAGIC .arrow1, .arrow2 {
# MAGIC  transform: rotate(var(--rot)) scaleX(0);
# MAGIC  animation: arrow12Loop 12.5s ease-in-out infinite;
# MAGIC }
# MAGIC .task2, .task3, .dep2, .dep3 {
# MAGIC  opacity: 0;
# MAGIC  transform: translateY(10px);
# MAGIC  animation: midNodesLoop 12.5s ease-in-out infinite;
# MAGIC }
# MAGIC .arrow3, .arrow4 {
# MAGIC  transform: rotate(var(--rot)) scaleX(0);
# MAGIC  animation: arrow34Loop 12.5s ease-in-out infinite;
# MAGIC }
# MAGIC .task4, .dep4 {
# MAGIC  opacity: 0;
# MAGIC  transform: translateY(10px);
# MAGIC  animation: task4Loop 12.5s ease-in-out infinite;
# MAGIC }
# MAGIC @keyframes task1Loop {
# MAGIC  0% { opacity: 0; transform: translateY(10px); }
# MAGIC  12% { opacity: 1; transform: translateY(0); }
# MAGIC  100% { opacity: 1; transform: translateY(0); }
# MAGIC }
# MAGIC @keyframes arrow12Loop {
# MAGIC  0%, 12% { transform: rotate(var(--rot)) scaleX(0); }
# MAGIC  24% { transform: rotate(var(--rot)) scaleX(1); }
# MAGIC  100% { transform: rotate(var(--rot)) scaleX(1); }
# MAGIC }
# MAGIC @keyframes midNodesLoop {
# MAGIC  0%, 24% { opacity: 0; transform: translateY(10px); }
# MAGIC  36% { opacity: 1; transform: translateY(0); }
# MAGIC  100% { opacity: 1; transform: translateY(0); }
# MAGIC }
# MAGIC @keyframes arrow34Loop {
# MAGIC  0%, 36% { transform: rotate(var(--rot)) scaleX(0); }
# MAGIC  48% { transform: rotate(var(--rot)) scaleX(1); }
# MAGIC  100% { transform: rotate(var(--rot)) scaleX(1); }
# MAGIC }
# MAGIC @keyframes task4Loop {
# MAGIC  0%, 48% { opacity: 0; transform: translateY(10px); }
# MAGIC  60% { opacity: 1; transform: translateY(0); }
# MAGIC  100% { opacity: 1; transform: translateY(0); }
# MAGIC }
# MAGIC @media screen and (max-width: 980px) {
# MAGIC  .dag-text-layout {
# MAGIC   grid-template-columns: 1fr;
# MAGIC  }
# MAGIC  .dag-text-cards {
# MAGIC   grid-template-columns: 1fr;
# MAGIC  }
# MAGIC  .dag-canvas {
# MAGIC   height: 540px;
# MAGIC  }
# MAGIC  .task1 { left: 20px; top: 210px; }
# MAGIC  .task2 { left: 300px; top: 80px; }
# MAGIC  .task3 { left: 300px; top: 320px; }
# MAGIC  .task4 { left: 585px; top: 210px; }
# MAGIC  .dep2 { left: 295px; top: 46px; width: 170px; }
# MAGIC  .dep3 { left: 295px; top: 404px; width: 170px; }
# MAGIC  .dep4 { left: 545px; top: 294px; width: 220px; }
# MAGIC  .arrow1 { left: 202px; top: 225px; width: 105px; }
# MAGIC  .arrow2 { left: 202px; top: 255px; width: 105px; }
# MAGIC  .arrow3 { left: 485px; top: 126px; width: 115px; }
# MAGIC  .arrow4 { left: 485px; top: 362px; width: 115px; }
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="dag-text-layout">
# MAGIC  <div class="dag-text-main">
# MAGIC   <h3>Databricks Jobs supports task orchestration</h3>
# MAGIC  </div>
# MAGIC  <div class="dag-text-cards">
# MAGIC   <div class="dag-text-card">
# MAGIC    <h4>Run multiple tasks</h4>
# MAGIC    <p>The ability to run multiple tasks as a <strong>Directed Acyclic Graph (DAG)</strong>.</p>
# MAGIC   </div>
# MAGIC   <div class="dag-text-card green">
# MAGIC    <h4>Orchestrate tasks</h4>
# MAGIC    <p>Orchestrate tasks using the <strong>Databricks UI</strong>, <strong>API</strong>, <strong>SDK</strong>, or <strong>Databricks Asset Bundles</strong>.</p>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dag-text-callout">
# MAGIC  You can define the <strong>order of execution of tasks</strong> in a job by configuring task dependencies, creating a <strong>DAG of task execution</strong>.
# MAGIC </div>
# MAGIC
# MAGIC <div class="dag-canvas">
# MAGIC  <div class="task-box task1">Task-1</div>
# MAGIC  <div class="arrow arrow1"></div>
# MAGIC  <div class="arrow arrow2"></div>
# MAGIC  <div class="dep-text dep2">Depends On: Task-1</div>
# MAGIC  <div class="task-box task2">Task-2</div>
# MAGIC  <div class="task-box task3">Task-3</div>
# MAGIC  <div class="dep-text dep3">Depends On: Task-1</div>
# MAGIC  <div class="arrow arrow3"></div>
# MAGIC  <div class="arrow arrow4"></div>
# MAGIC  <div class="task-box task4">Task-4</div>
# MAGIC  <div class="dep-text dep4">Depends On: <br>Task-2, Task-3</div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Databricks Jobs supports task orchestration through the ability to run multiple tasks as a Directed Acyclic Graph (DAG). You can orchestrate tasks using the Databricks UI, API, SDK, or Databricks Asset Bundles.
# MAGIC
# MAGIC The example shows Task-4 depends on Task-1, Task-4 depends on Task-2 and Task-3, and Task-3 depends on Task-1. You define the order of execution by configuring these task dependencies, creating a DAG of task execution.
# MAGIC
# MAGIC This approach allows you to build complex workflows while maintaining clear execution order and dependencies.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. Common Workloads Patterns
# MAGIC
# MAGIC <br>
# MAGIC <div style="max-width:1100px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC   <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:14px;">
# MAGIC     <div style="border-radius:10px; overflow:hidden; border:2px solid #2272B4; box-shadow:0 3px 12px rgba(34,114,180,0.15);">
# MAGIC       <div style="background:#2272B4; padding:10px 14px; color:white; font-weight:700; font-size:16pt; text-align:center; letter-spacing:0.03em;">
# MAGIC         Sequence
# MAGIC       </div>
# MAGIC       <div style="background:#f8f8f8; margin:0; padding:12px 14px; font-size:14pt; color:#1B3139; line-height:1.7;">
# MAGIC         <div style="height:180px; display:flex; align-items:center; justify-content:center; margin-bottom:12px;">
# MAGIC           <img
# MAGIC             src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_task_orchestration/sequence_diagram.png"
# MAGIC             alt="Sequence Diagram"
# MAGIC             style="width:100%; max-width:260px; height:160px; object-fit:contain; display:block; background:transparent; mix-blend-mode:multiply; filter:contrast(1.15) brightness(1); border-radius:4px;">
# MAGIC         </div>
# MAGIC         <ul>
# MAGIC           <li>Data transformation/ processing/ cleaning</li>
# MAGIC           <li>Bronze/ silver/ gold tables</li>
# MAGIC           <br>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="border-radius:10px; overflow:hidden; border:2px solid #02A36F; box-shadow:0 3px 12px rgba(2,163,111,0.15);">
# MAGIC       <div style="background:#02A36F; padding:10px 14px; color:white; font-weight:700; font-size:16pt; text-align:center; letter-spacing:0.03em;">
# MAGIC         Funnel
# MAGIC       </div>
# MAGIC       <div style="background:#f8f8f8; margin:0; padding:12px 14px; font-size:14pt; color:#1B3139; line-height:1.7;">
# MAGIC         <div style="height:180px; display:flex; align-items:center; justify-content:center; margin-bottom:12px;">
# MAGIC           <img
# MAGIC             src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_task_orchestration/funnel_diagram.png"
# MAGIC             alt="Funnel Diagram"
# MAGIC             style="width:100%; max-width:260px; height:160px; object-fit:contain; display:block; background:transparent; mix-blend-mode:multiply; filter:contrast(1.15) brightness(1); border-radius:4px;">
# MAGIC         </div>
# MAGIC         <ul>
# MAGIC           <li>Multiple data sources</li>
# MAGIC           <li>Data collection</li>
# MAGIC           <br>
# MAGIC           <br>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="border-radius:10px; overflow:hidden; border:2px solid #FFAB00; box-shadow:0 3px 12px rgba(255,171,0,0.15);">
# MAGIC       <div style="background:#FFAB00; padding:10px 14px; color:#1B3139; font-weight:700; font-size:16pt; text-align:center; letter-spacing:0.03em;">
# MAGIC         Fan-out
# MAGIC       </div>
# MAGIC       <div style="background:#f8f8f8; margin:0; padding:12px 14px; font-size:14pt; color:#1B3139; line-height:1.7;">
# MAGIC         <div style="height:180px; display:flex; align-items:center; justify-content:center; margin-bottom:12px;">
# MAGIC           <img
# MAGIC             src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_task_orchestration/fan-out_diagram.png"
# MAGIC             alt="Fan-Out Diagram"
# MAGIC             style="width:100%; max-width:260px; height:160px; object-fit:contain; display:block; background:transparent; mix-blend-mode:multiply; filter:contrast(1.15) brightness(1); border-radius:4px;">
# MAGIC         </div>
# MAGIC         <ul>
# MAGIC           <li>Fan-out, star pattern</li>
# MAGIC           <li>Single data source</li>
# MAGIC           <li>Data ingestion and distribution</li>
# MAGIC           <br>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC There are three common workflow patterns you'll encounter:
# MAGIC - Sequence Pattern is used for data transformation, processing, cleaning, and building bronze/silver/gold tables in a medallion architecture.
# MAGIC - Funnel Pattern brings together multiple data sources for data collection and consolidation.
# MAGIC - Fan-out or Star Pattern takes a single data source and distributes it for data ingestion and distribution to multiple downstream systems.
# MAGIC
# MAGIC Understanding these patterns helps you design effective workflows for your specific use cases.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Conclusion
# MAGIC
# MAGIC - Lakeflow Jobs combines jobs, tasks, options, languages, triggers, control flows, and compute for flexible orchestration.
# MAGIC - It uses DAG-based orchestration to define clear task dependencies.
# MAGIC - Tasks can be arranged in sequence, funnel, or fan-out patterns to build reliable data workflows.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next lecture, you will check out Course Project Overview.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>