# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Creating and Scheduling Jobs
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how to configure Lakeflow Jobs tasks using parameters, dynamic values, notifications, and retry policies, and how to automate job execution with schedules and event-driven triggers.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC 1. **Configure** task parameters,task values,dynamic value references, notifications, and retry policies for Lakeflow Jobs
# MAGIC 2. **Set up** and **manage** different job scheduling options, including scheduled, file arrival, Table update and continuous triggers
# MAGIC 3. **Automate** job execution using triggers to support time-based and event-driven workflows
# MAGIC 4. **Explore** and **apply** scheduling options in the Databricks Lakeflow Jobs UI through hands-on demonstration

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Common Task Configuration Options

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1.  Overview - Major Categories
# MAGIC
# MAGIC There are three major categories of task configuration options.
# MAGIC
# MAGIC <div class="ctco-overview-simple">
# MAGIC <style>
# MAGIC .ctco-overview-simple {
# MAGIC   width: 1100px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 16px auto 28px auto;
# MAGIC   font-family: "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-overview-simple * {
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .ctco-overview-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: repeat(3, 1fr);
# MAGIC   gap: 18px;
# MAGIC }
# MAGIC .ctco-overview-card {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   box-shadow: 0 2px 8px rgba(27, 49, 57, 0.07);
# MAGIC   overflow: hidden;
# MAGIC   min-height: 280px;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC }
# MAGIC .ctco-card-bar {
# MAGIC   height: 8px;
# MAGIC   background: #FF5F46;
# MAGIC }
# MAGIC .ctco-card-bar.maroon {
# MAGIC   background: #98102A;
# MAGIC }
# MAGIC .ctco-card-bar.green {
# MAGIC   background: #00A972;
# MAGIC }
# MAGIC .ctco-card-body {
# MAGIC   padding: 22px 20px;
# MAGIC   text-align: center;
# MAGIC   flex: 1;
# MAGIC }
# MAGIC .ctco-card-body img {
# MAGIC   width: 100px;
# MAGIC   height: 100px;
# MAGIC   object-fit: contain;
# MAGIC   display: block;
# MAGIC   margin: 0 auto 16px auto;
# MAGIC }
# MAGIC .ctco-card-body h3 {
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 700;
# MAGIC   color: #0B2026;
# MAGIC   margin: 0 0 12px 0;
# MAGIC }
# MAGIC .ctco-card-body p {
# MAGIC   font-size: 16px;
# MAGIC   line-height: 1.5;
# MAGIC   color: #0B2026;
# MAGIC   margin: 0;
# MAGIC }
# MAGIC @media screen and (max-width: 900px) {
# MAGIC   .ctco-overview-grid {
# MAGIC     grid-template-columns: 1fr;
# MAGIC   }
# MAGIC }
# MAGIC </style>
# MAGIC <div class="ctco-overview-grid">
# MAGIC   <div class="ctco-overview-card">
# MAGIC     <div class="ctco-card-bar"></div>
# MAGIC     <div class="ctco-card-body">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/overview_parameters_dynamic_references_icon.png"
# MAGIC         alt="Parameters and dynamic value references icon">
# MAGIC       <h3>Parameters &amp; Dynamic Value References</h3>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ctco-overview-card">
# MAGIC     <div class="ctco-card-bar maroon"></div>
# MAGIC     <div class="ctco-card-body">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/overview_notification_alerts_icon.png"
# MAGIC         alt="Notification alerts icon">
# MAGIC       <h3>Notification Alerts</h3>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ctco-overview-card">
# MAGIC     <div class="ctco-card-bar green"></div>
# MAGIC     <div class="ctco-card-body">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/overview_retries_icon.png"
# MAGIC         alt="Retries icon">
# MAGIC       <h3>Retries</h3>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Let me walk you through the three major categories of task configuration options:</p>
# MAGIC
# MAGIC - <p>Parameters &amp; Dynamic Value References are the foundation of flexible workflows. These can be set at the task level and job level. It allow you to create reusable, adaptable tasks that can behave differently based on input values or runtime context.</p>
# MAGIC - <p>Retries are your first line of defense against transient failures. You can configure retry behavior at both job and task levels, allowing for different retry strategies depending on the criticality and expected failure patterns of different parts of your workflow.</p>
# MAGIC - <p>Notification Alerts keep your team informed and enable rapid response to issues. Like retries, these can be configured at both job and task levels, giving you granular control over who gets notified about what events.</p>
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### A2. Parameter Overview - Types
# MAGIC
# MAGIC Click each tab to explore more about the parameters.
# MAGIC
# MAGIC <div class="ctco-param-large">
# MAGIC <style>
# MAGIC .ctco-param-large {
# MAGIC   width: 1100px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 14px auto 28px auto;
# MAGIC   font-family: "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-param-large * {
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .ctco-param-large input[type="radio"] {
# MAGIC   display: none;
# MAGIC }
# MAGIC .ctco-tab-buttons {
# MAGIC   display: flex;
# MAGIC   justify-content: flex-start;
# MAGIC   align-items: center;
# MAGIC   gap: 0;
# MAGIC   border-bottom: 2px solid #EEEDE9;
# MAGIC   margin-bottom: 18px;
# MAGIC   width: 100%;
# MAGIC }
# MAGIC .ctco-tab-buttons label {
# MAGIC   padding: 10px 18px;
# MAGIC   border: none;
# MAGIC   border-bottom: 3px solid transparent;
# MAGIC   background: none;
# MAGIC   font-size: 20px;
# MAGIC   font-weight: bold;
# MAGIC   color: #888888;
# MAGIC   cursor: pointer;
# MAGIC   margin-bottom: -2px;
# MAGIC }
# MAGIC #ctco-task-tab-large:checked ~ .ctco-tab-buttons label[for="ctco-task-tab-large"] {
# MAGIC   color: #FF7A59;
# MAGIC   border-bottom-color: #FF7A59;
# MAGIC }
# MAGIC #ctco-job-tab-large:checked ~ .ctco-tab-buttons label[for="ctco-job-tab-large"] {
# MAGIC   color: #4299E0;
# MAGIC   border-bottom-color: #4299E0;
# MAGIC }
# MAGIC .ctco-card {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 14px;
# MAGIC   box-shadow: 0 3px 12px rgba(27,49,57,0.07);
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .ctco-card-bar {
# MAGIC   height: 8px;
# MAGIC   background: #FF7A59;
# MAGIC }
# MAGIC #ctco-job-tab-large:checked ~ .ctco-card .ctco-card-bar {
# MAGIC   background: #4299E0;
# MAGIC }
# MAGIC .ctco-card-body {
# MAGIC   padding: 22px;
# MAGIC }
# MAGIC .ctco-panel {
# MAGIC   display: none;
# MAGIC   grid-template-columns: 0.65fr 1.35fr;
# MAGIC   gap: 28px;
# MAGIC   align-items: stretch;
# MAGIC   min-height: 560px;
# MAGIC }
# MAGIC #ctco-task-tab-large:checked ~ .ctco-card .task-panel,
# MAGIC #ctco-job-tab-large:checked ~ .ctco-card .job-panel {
# MAGIC   display: grid;
# MAGIC }
# MAGIC .ctco-text-box {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 24px;
# MAGIC   min-height: 560px;
# MAGIC }
# MAGIC .ctco-heading-row {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   gap: 18px;
# MAGIC   margin-bottom: 14px;
# MAGIC }
# MAGIC .ctco-heading-row h3 {
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 700;
# MAGIC   margin: 0;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-heading-icon {
# MAGIC   width: 126px;
# MAGIC   height: 136px;
# MAGIC   object-fit: contain;
# MAGIC   display: block;
# MAGIC   margin: 0 auto 20px auto;
# MAGIC   background: transparent;
# MAGIC   mix-blend-mode: multiply;
# MAGIC   filter: contrast(1.15) brightness(1);
# MAGIC   border-radius: 4px;
# MAGIC }
# MAGIC .ctco-heading-icon.job-icon {
# MAGIC   width: 230px;
# MAGIC   height: 170px;
# MAGIC   margin: 0 auto 20px auto;
# MAGIC }
# MAGIC .ctco-text-box p,
# MAGIC .ctco-text-box li {
# MAGIC   font-size: 16px;
# MAGIC   line-height: 1.55;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-text-box p {
# MAGIC   margin: 0 0 16px 0;
# MAGIC }
# MAGIC .ctco-text-box ul {
# MAGIC   margin: 0;
# MAGIC   padding-left: 22px;
# MAGIC }
# MAGIC .ctco-text-box li {
# MAGIC   margin-bottom: 9px;
# MAGIC }
# MAGIC .ctco-text-box ul ul {
# MAGIC   margin-top: 8px;
# MAGIC }
# MAGIC .ctco-image-box {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 16px;
# MAGIC   min-height: 560px;
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC }
# MAGIC .ctco-image-box img {
# MAGIC   width: 100%;
# MAGIC   max-height: 530px;
# MAGIC   object-fit: contain;
# MAGIC   border-radius: 8px;
# MAGIC }
# MAGIC @media screen and (max-width: 900px) {
# MAGIC   .ctco-panel {
# MAGIC     grid-template-columns: 1fr;
# MAGIC   }
# MAGIC   .ctco-tab-buttons {
# MAGIC     flex-wrap: wrap;
# MAGIC   }
# MAGIC   .ctco-text-box,
# MAGIC   .ctco-image-box,
# MAGIC   .ctco-panel {
# MAGIC     min-height: auto;
# MAGIC   }
# MAGIC   .ctco-heading-icon {
# MAGIC     width: 104px;
# MAGIC     height: 112px;
# MAGIC   }
# MAGIC   .ctco-heading-icon.job-icon {
# MAGIC     width: 170px;
# MAGIC     height: 130px;
# MAGIC   }
# MAGIC }
# MAGIC </style>
# MAGIC <input type="radio" id="ctco-task-tab-large" name="ctco-param-tab-large" checked>
# MAGIC <input type="radio" id="ctco-job-tab-large" name="ctco-param-tab-large">
# MAGIC <div class="ctco-tab-buttons">
# MAGIC   <label for="ctco-task-tab-large">Task Parameters</label>
# MAGIC   <label for="ctco-job-tab-large">Job Parameters</label>
# MAGIC </div>
# MAGIC <div class="ctco-card">
# MAGIC   <div class="ctco-card-bar"></div>
# MAGIC   <div class="ctco-card-body">
# MAGIC     <div class="ctco-panel task-panel">
# MAGIC       <div class="ctco-text-box">
# MAGIC         <div class="ctco-heading-row">
# MAGIC           <h3>Task Parameters</h3>
# MAGIC         </div>
# MAGIC         <img class="ctco-heading-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/task_overview.png" alt="Task parameters icon">
# MAGIC         <p>A key-value pair defined at the task level.</p>
# MAGIC         <ul>
# MAGIC           <li>Task Parameters are key-value pairs that lets you pass values into tasks.</li>
# MAGIC           <li>
# MAGIC             Support advanced orchestration such as:
# MAGIC             <ul>
# MAGIC               <li>Conditional execution</li>
# MAGIC               <li>Looping</li>
# MAGIC               <li>Passing context between tasks</li>
# MAGIC             </ul>
# MAGIC           </li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC       <div class="ctco-image-box">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/Task_Parameter_UI.png" alt="Task parameter configuration screenshot">
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="ctco-panel job-panel">
# MAGIC       <div class="ctco-text-box">
# MAGIC         <div class="ctco-heading-row">
# MAGIC           <h3>Job Parameters</h3>
# MAGIC         </div>
# MAGIC         <img class="ctco-heading-icon job-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/job_overview.png" alt="Job parameters icon">
# MAGIC         <p>A key-value pair defined at the job level and pushed down to all tasks.</p>
# MAGIC         <p>Job Parameters are key-value pairs defined at the job level that provide default values for the entire workflow.</p>
# MAGIC         <ul>
# MAGIC           <li>Applied to all tasks in the job automatically.</li>
# MAGIC           <li>Override task parameters with the same key name.</li>
# MAGIC           <li>Can be overridden at runtime when triggering job runs.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC       <div class="ctco-image-box">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/Job_Parameter_UI.png" alt="Job parameter configuration screenshot">
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Understanding the parameter hierarchy is crucial for effective job design:</p>
# MAGIC
# MAGIC - <p>Task Parameters are key-value pairs or JSON arrays defined at the individual task level. These are specific to each task and allow for fine-grained control over task behavior.</p>
# MAGIC - <p>Job Parameters are defined at the job level and automatically propagate to all tasks within that job. This creates a powerful inheritance model where you can set common defaults while still allowing task-specific overrides.</p>
# MAGIC
# MAGIC <p>The precedence rule is critical to remember: Job parameters always override task parameters when the same key exists. Task parameters define per-task defaults, while job parameters provide centralized runtime overrides that apply consistently across the entire job.</p>
# MAGIC
# MAGIC <p>Task parameters are far more than simple configuration values - they're the building blocks of intelligent workflows. These key-value pairs enable sophisticated orchestration patterns:</p>
# MAGIC
# MAGIC - <p>Conditional Execution: Use parameters to control which branches of your workflow execute based on data conditions, environment settings, or business rules.</p>
# MAGIC - <p>Looping: Parameters can control iteration counts, define arrays for for-each loops, and manage complex processing scenarios.</p>
# MAGIC - <p>Context Passing: Share information between tasks by setting parameters that downstream tasks can read, creating a data flow alongside your control flow.</p>
# MAGIC
# MAGIC <p>The real power comes from combining parameters with dynamic value references, allowing your workflows to adapt intelligently to changing conditions and data characteristics.</p>
# MAGIC
# MAGIC <p>Job parameters serve as the foundation for consistent, maintainable workflows. They're key-value pairs that provide default values for your entire workflow, ensuring consistency across all tasks.</p>
# MAGIC
# MAGIC <p>Here's what makes them powerful:</p>
# MAGIC
# MAGIC - <p>Automatic Application: Every task in the job automatically receives these parameters, eliminating the need to manually configure common settings across multiple tasks.</p>
# MAGIC - <p>Override Capability: Tasks can still define their own parameters with the same key names, but job parameters take precedence, giving you centralized control.</p>
# MAGIC - <p>Runtime Flexibility: You can override job parameters when triggering job runs, allowing the same job definition to behave differently for different scenarios - perhaps different environments, date ranges, or processing modes.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A3. Setting and Accessing Parameters (UI)
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div class="ctco-ui-simple">
# MAGIC <style>
# MAGIC .ctco-ui-simple {
# MAGIC   width: 1100px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 14px auto 28px auto;
# MAGIC   font-family: "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-ui-simple * {
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .ctco-ui-card {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 14px;
# MAGIC   box-shadow: 0 3px 12px rgba(27,49,57,0.07);
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .ctco-ui-bar {
# MAGIC   height: 8px;
# MAGIC   background: #FF5F46;
# MAGIC }
# MAGIC .ctco-ui-body {
# MAGIC   padding: 22px;
# MAGIC }
# MAGIC .ctco-ui-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr;
# MAGIC   gap: 22px;
# MAGIC   align-items: stretch;
# MAGIC   margin-bottom: 22px;
# MAGIC }
# MAGIC .ctco-ui-box {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 22px;
# MAGIC }
# MAGIC .ctco-ui-box h3 {
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 700;
# MAGIC   color: #0B2026  !important;
# MAGIC   margin: 0 0 14px 0;
# MAGIC }
# MAGIC .ctco-ui-box p,
# MAGIC .ctco-ui-box li {
# MAGIC   font-size: 16px;
# MAGIC   line-height: 1.55;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-ui-box p {
# MAGIC   margin: 0 0 14px 0;
# MAGIC }
# MAGIC .ctco-ui-box strong {
# MAGIC   font-weight: 700;
# MAGIC }
# MAGIC .ctco-ui-box ul {
# MAGIC   margin: 6px 0 18px 0;
# MAGIC   padding-left: 22px;
# MAGIC }
# MAGIC .ctco-ui-box li {
# MAGIC   margin-bottom: 8px;
# MAGIC }
# MAGIC .ctco-ui-image-box {
# MAGIC   width: 100%;
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 16px;
# MAGIC   display: flex;
# MAGIC   justify-content: center;
# MAGIC   align-items: center;
# MAGIC }
# MAGIC .ctco-ui-image-box img {
# MAGIC   width: 100%;
# MAGIC   max-height: 520px;
# MAGIC   object-fit: contain;
# MAGIC   border-radius: 8px;
# MAGIC }
# MAGIC @media screen and (max-width: 900px) {
# MAGIC   .ctco-ui-grid {
# MAGIC     grid-template-columns: 1fr;
# MAGIC   }
# MAGIC }
# MAGIC </style>
# MAGIC <div class="ctco-ui-card">
# MAGIC   <div class="ctco-ui-bar"></div>
# MAGIC   <div class="ctco-ui-body">
# MAGIC     <div class="ctco-ui-grid">
# MAGIC       <div class="ctco-ui-box">
# MAGIC         <h3>Setting Parameter Values (UI)</h3>
# MAGIC         <p><strong>Setting Job Parameters:</strong></p>
# MAGIC         <ul>
# MAGIC           <li>In your job, go to the Parameters section, and add key–value pairs at the job level.</li>
# MAGIC         </ul>
# MAGIC         <p><strong>Setting Task Parameters:</strong></p>
# MAGIC         <ul>
# MAGIC           <li>Go to the specific task in the job.</li>
# MAGIC           <li>Go to parameters within the task configuration, alongside task name, type, and path.</li>
# MAGIC           <li>Add the key–value pairs.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC       <div class="ctco-ui-box">
# MAGIC         <h3>Accessing Parameter Values in a Task</h3>
# MAGIC         <ul>
# MAGIC           <li>Access both task and job parameters in notebook tasks with <strong>dbutils.widgets.get</strong>.</li>
# MAGIC           <li>Retrieval methods vary by task type, such as notebook, SQL, or Python wheel.</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="ctco-ui-image-box">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/setting_accessing_parameters.png"
# MAGIC         alt="Setting parameter values in the UI">
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Let's look at the practical implementation of parameters:</p>
# MAGIC
# MAGIC - <p>Setting Job Parameters: Navigate to your job's Parameters section and add key-value pairs at the job level. These become available to all tasks automatically. Common examples include catalog names, schema names, environment settings, and processing dates.</p>
# MAGIC - <p>Setting Task Parameters: Within each task's configuration (found alongside task name, type, and path settings), add task-specific key-value pairs. These are perfect for task-specific paths, processing options, or override values.</p>
# MAGIC - <p>Retrieving in Notebook Tasks: Use <code>dbutils.widgets.get("parameter_name")</code> to access both job and task parameters. The system automatically handles the precedence - if both job and task parameters exist with the same key, you'll get the job parameter value</p>
# MAGIC - <p>Language-Specific Retrieval: Remember that parameter retrieval methods vary by task type. SQL tasks access parameters differently than Python wheel tasks or JAR tasks. Always check the documentation for your specific task type.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A4. Dynamically Setting and Accessing Task Parameters
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div class="ctco-task-values-simple">
# MAGIC <style>
# MAGIC .ctco-task-values-simple {
# MAGIC   width: 1200px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 12px auto 24px auto;
# MAGIC   font-family: "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-task-values-simple * {
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .ctco-value-flow {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr;
# MAGIC   gap: 18px;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .ctco-card {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 14px;
# MAGIC   box-shadow: 0 3px 12px rgba(27,49,57,0.07);
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .ctco-card-top {
# MAGIC   height: 8px;
# MAGIC   background: #FF5F46;
# MAGIC }
# MAGIC .ctco-card-top.navy {
# MAGIC   background: #1B5162;
# MAGIC }
# MAGIC .ctco-card-body {
# MAGIC   padding: 20px;
# MAGIC }
# MAGIC .ctco-card-body h3 {
# MAGIC   font-size: 20px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 700;
# MAGIC   color: #0B2026;
# MAGIC   margin: 0 0 12px 0;
# MAGIC }
# MAGIC .ctco-code-block {
# MAGIC   position: relative;
# MAGIC   margin-top: 12px;
# MAGIC }
# MAGIC .ctco-copy {
# MAGIC   position: absolute;
# MAGIC   top: 8px;
# MAGIC   right: 8px;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   background: #ffffff;
# MAGIC   color: #0B2026;
# MAGIC   border-radius: 6px;
# MAGIC   padding: 5px 10px;
# MAGIC   cursor: pointer;
# MAGIC   font-weight: 700;
# MAGIC   font-size: 12px;
# MAGIC   z-index: 3;
# MAGIC }
# MAGIC .ctco-code-block pre {
# MAGIC   margin: 0;
# MAGIC   background: #F9F7F4;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 42px 16px 16px 16px;
# MAGIC   overflow-x: auto;
# MAGIC   min-height: 96px;
# MAGIC }
# MAGIC .ctco-code-block code {
# MAGIC   font-family: Consolas, Monaco, "Courier New", monospace;
# MAGIC   font-size: 14px;
# MAGIC   line-height: 1.55;
# MAGIC   color: #0B2026;
# MAGIC   white-space: pre;
# MAGIC }
# MAGIC .ctco-text-callouts {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr;
# MAGIC   gap: 18px;
# MAGIC   margin-top: 18px;
# MAGIC }
# MAGIC .ctco-ui-box {
# MAGIC   border: none;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 22px;
# MAGIC   color: #ffffff;
# MAGIC   box-shadow: 0 3px 12px rgba(27,49,57,0.07);
# MAGIC }
# MAGIC .ctco-ui-box h3 {
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.35;
# MAGIC   font-weight: 700;
# MAGIC   color: #ffffff;
# MAGIC   margin: 0;
# MAGIC }
# MAGIC .ctco-ui-box.orange {
# MAGIC   background: #FF5F46;
# MAGIC }
# MAGIC .ctco-ui-box.navy {
# MAGIC   background: #1B5162;
# MAGIC }
# MAGIC @media screen and (max-width: 900px) {
# MAGIC   .ctco-value-flow,
# MAGIC   .ctco-text-callouts {
# MAGIC     grid-template-columns: 1fr;
# MAGIC   }
# MAGIC }
# MAGIC </style>
# MAGIC <div class="ctco-value-flow">
# MAGIC   <div class="ctco-card">
# MAGIC     <div class="ctco-card-top"></div>
# MAGIC     <div class="ctco-card-body">
# MAGIC       <h3>Dynamically Set Task Values with Code</h3>
# MAGIC       <div class="ctco-code-block">
# MAGIC         <button class="ctco-copy" type="button">Copy</button>
# MAGIC <pre><code>dbutils.jobs.taskValues.set(
# MAGIC     key = "catalog_name",
# MAGIC     value = "dbacademy"
# MAGIC )</code></pre>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ctco-card">
# MAGIC     <div class="ctco-card-top navy"></div>
# MAGIC     <div class="ctco-card-body">
# MAGIC       <h3>Accessing a Parameter from another Task</h3>
# MAGIC       <div class="ctco-code-block">
# MAGIC         <button class="ctco-copy" type="button">Copy</button>
# MAGIC <pre><code>dbutils.jobs.taskValues.get(
# MAGIC     taskKey = "task-name",
# MAGIC     key = "catalog_name"
# MAGIC )</code></pre>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div class="ctco-text-callouts">
# MAGIC   <div class="ctco-ui-box orange">
# MAGIC     <h3>Task Values are dynamic key-value pairs that tasks create and share during workflow execution.</h3>
# MAGIC   </div>
# MAGIC   <div class="ctco-ui-box navy">
# MAGIC     <h3>You Reference specific upstream task parameter values into downstream tasks.</>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC (function () {
# MAGIC   var root = document.currentScript.closest(".ctco-task-values-simple");
# MAGIC   if (!root) return;
# MAGIC   root.querySelectorAll(".ctco-copy").forEach(function (button) {
# MAGIC     button.addEventListener("click", function () {
# MAGIC       var code = button.parentElement.querySelector("code");
# MAGIC       if (!code) return;
# MAGIC       var text = code.textContent;
# MAGIC       var originalText = button.textContent;
# MAGIC       function showCopied() {
# MAGIC         button.textContent = "Copied";
# MAGIC         setTimeout(function () {
# MAGIC           button.textContent = originalText;
# MAGIC         }, 1400);
# MAGIC       }
# MAGIC       if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC         navigator.clipboard.writeText(text).then(showCopied);
# MAGIC       } else {
# MAGIC         var textarea = document.createElement("textarea");
# MAGIC         textarea.value = text;
# MAGIC         document.body.appendChild(textarea);
# MAGIC         textarea.select();
# MAGIC         document.execCommand("copy");
# MAGIC         document.body.removeChild(textarea);
# MAGIC         showCopied();
# MAGIC       }
# MAGIC     });
# MAGIC   });
# MAGIC })();
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Task Values represent a more advanced pattern than parameters - they're computed at runtime and enable dynamic communication between tasks:</p>
# MAGIC
# MAGIC - <p>Setting Task Values: Use <code>dbutils.jobs.taskValues.set(key="computed_result", value="some_calculated_value")</code> to store results that other tasks need. This might include record counts, processing status, file paths, or any computed information.</p>
# MAGIC - <p>Retrieving Task Values: Use <code>dbutils.jobs.taskValues.get(taskKey="upstream-task-name", key="computed_result")</code> to access values from specific upstream tasks. This creates explicit dependencies between tasks based on data, not just execution order.</p>
# MAGIC - <p>Use Cases: Task values are perfect for sharing computed results like record counts for data quality checks, file paths for dynamically generated outputs, processing statistics for monitoring, or decision criteria for conditional logic.</p>
# MAGIC - <p>Execution Context: Unlike parameters which are set before execution, task values are created during execution, making them ideal for decisions that depend on processing results.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A5. Dynamic Value References
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div class="ctco-dvr-simple">
# MAGIC <style>
# MAGIC .ctco-dvr-simple {
# MAGIC   width: 1200px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 14px auto 28px auto;
# MAGIC   font-family: "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-dvr-simple * {
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .ctco-dvr-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 0.72fr 1.28fr;
# MAGIC   gap: 24px;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .ctco-dvr-left {
# MAGIC   display: grid;
# MAGIC   gap: 14px;
# MAGIC }
# MAGIC .ctco-dvr-card {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-left: 5px solid #FF5F46;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 18px;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC }
# MAGIC .ctco-dvr-card.green {
# MAGIC   border-left-color: #00A972;
# MAGIC }
# MAGIC .ctco-dvr-card.maroon {
# MAGIC   border-left-color: #98102A;
# MAGIC }
# MAGIC .ctco-dvr-card h3 {
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 700;
# MAGIC   color: #0B2026;
# MAGIC   margin: 0 0 10px 0;
# MAGIC }
# MAGIC .ctco-dvr-card p {
# MAGIC   font-size: 16px;
# MAGIC   line-height: 1.5;
# MAGIC   color: #0B2026;
# MAGIC   margin: 0;
# MAGIC }
# MAGIC .ctco-dvr-table {
# MAGIC   width: 100%;
# MAGIC   border-collapse: collapse;
# MAGIC   margin-top: 10px;
# MAGIC   font-size: 15px;
# MAGIC   line-height: 1.4;
# MAGIC }
# MAGIC .ctco-dvr-table th {
# MAGIC   background: #F9F7F4;
# MAGIC   color: #0B2026;
# MAGIC   font-weight: 700;
# MAGIC   text-align: left;
# MAGIC   padding: 10px;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC }
# MAGIC .ctco-dvr-table td {
# MAGIC   padding: 10px;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   vertical-align: top;
# MAGIC }
# MAGIC .ctco-dvr-table code{
# MAGIC   font-family:Consolas, Monaco, "Courier New", monospace;
# MAGIC   font-size:14px;
# MAGIC   color:#0B2026;
# MAGIC   background:#F9F7F4;
# MAGIC   padding:3px 5px;
# MAGIC   border-radius:4px;
# MAGIC
# MAGIC   white-space:normal;
# MAGIC   overflow-wrap:anywhere;
# MAGIC   word-break:break-word;
# MAGIC }
# MAGIC .ctco-dvr-image-box {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 16px;
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   min-height: 520px;
# MAGIC }
# MAGIC .ctco-dvr-image-box img {
# MAGIC   width: 100%;
# MAGIC   max-height: 500px;
# MAGIC   object-fit: contain;
# MAGIC   border-radius: 8px;
# MAGIC }
# MAGIC @media screen and (max-width: 900px) {
# MAGIC   .ctco-dvr-grid {
# MAGIC     grid-template-columns: 1fr;
# MAGIC   }
# MAGIC   .ctco-dvr-image-box {
# MAGIC     min-height: auto;
# MAGIC   }
# MAGIC }
# MAGIC </style>
# MAGIC <div class="ctco-dvr-grid">
# MAGIC   <div class="ctco-dvr-left">
# MAGIC     <div class="ctco-dvr-card">
# MAGIC       <h3>Dynamic Value References</h3>
# MAGIC       <p>
# MAGIC         Dynamic Value References allow referencing values at runtime from the job and task context
# MAGIC         (e.g., task outputs, parameters, retask values, registered task values, run metadata).
# MAGIC       </p>
# MAGIC     </div>
# MAGIC     <div class="ctco-dvr-card green">
# MAGIC       <h3>{{ }} notation</h3>
# MAGIC       <p>
# MAGIC         These references use <code>{{ }}</code> notation and allow workflows to adapt to different execution.
# MAGIC       </p>
# MAGIC     </div>
# MAGIC     <div class="ctco-dvr-card maroon">
# MAGIC       <h3>Common examples</h3>
# MAGIC       <table class="ctco-dvr-table">
# MAGIC         <thead>
# MAGIC           <tr>
# MAGIC             <th>Reference</th>
# MAGIC             <th>Use</th>
# MAGIC           </tr>
# MAGIC         </thead>
# MAGIC         <tbody>
# MAGIC           <tr>
# MAGIC             <td><code>{{job.start_time.day}}</code></td>
# MAGIC             <td>For accessing the day</td>
# MAGIC           </tr>
# MAGIC           <tr>
# MAGIC             <td><code>{{task.name}}</code></td>
# MAGIC             <td>For Accessing Task Name</td>
# MAGIC           </tr>
# MAGIC           <!-- <tr>
# MAGIC             <td><code>{{tasks&lt;task-name&gt;.values.my_value}}</code></td>
# MAGIC             <td>For accessing output from an upstream task</td>
# MAGIC           </tr> -->
# MAGIC         </tbody>
# MAGIC       </table>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ctco-dvr-image-box">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/dynamic_value_references_ui.png" alt="Dynamic value references in task parameters UI">
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Dynamic Value References using <code>{{ }}</code> notation unlock powerful runtime capabilities that make workflows truly adaptive:</p>
# MAGIC
# MAGIC <p>Job Context References:</p>
# MAGIC
# MAGIC - <p><code>{{job.start_time.day}}</code> - Access execution timing for date-based processing</p>
# MAGIC - <p><code>{{job.run_id}}</code> - Unique identifier for tracking and logging</p>
# MAGIC - <p><code>{{job.parameters.environment}}</code> - Access job-level parameters dynamically</p>
# MAGIC
# MAGIC <p>Task Context References:</p>
# MAGIC
# MAGIC - <p><code>{{task.name}}</code> - Useful for logging and dynamic path generation</p>
# MAGIC - <p><code>{{task.retry_count}}</code> - Track retry attempts for debugging</p>
# MAGIC
# MAGIC <p>Inter-Task Communication:</p>
# MAGIC
# MAGIC - <p><code>{{tasks.data-validation.values.record_count}}</code> - Access computed results from upstream tasks</p>
# MAGIC - <p><code>{{tasks.file-processor.values.output_path}}</code> - Use dynamic paths generated by other tasks</p>
# MAGIC
# MAGIC <p>Advanced Patterns: </p>
# MAGIC
# MAGIC - <p>These references enable workflows that adapt to different execution environments, process varying data volumes, and make intelligent decisions based on upstream results.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### A6. Notification Alerts
# MAGIC
# MAGIC <div class="ctco-notification-tree">
# MAGIC <style>
# MAGIC .ctco-notification-tree {
# MAGIC   width: 1100px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 18px auto 28px auto;
# MAGIC   font-family: "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-notification-tree * {
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .ctco-notification-center {
# MAGIC   text-align: center;
# MAGIC   margin-bottom: 0;
# MAGIC }
# MAGIC .ctco-notification-center img {
# MAGIC   width: 92px;
# MAGIC   height: 92px;
# MAGIC   object-fit: contain;
# MAGIC   display: block;
# MAGIC   margin: 0 auto 10px auto;
# MAGIC   background: transparent;
# MAGIC   mix-blend-mode: multiply;
# MAGIC   filter: contrast(1.15) brightness(1);
# MAGIC   border-radius: 4px;
# MAGIC }
# MAGIC .ctco-notification-title {
# MAGIC   font-size: 28px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 800;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-connector {
# MAGIC   position: relative;
# MAGIC   height: 74px;
# MAGIC   margin: 8px 0 0 0;
# MAGIC }
# MAGIC .ctco-connector .vertical {
# MAGIC   position: absolute;
# MAGIC   left: 50%;
# MAGIC   top: 0;
# MAGIC   width: 3px;
# MAGIC   height: 36px;
# MAGIC   background: #FF5F46;
# MAGIC   transform: translateX(-50%);
# MAGIC }
# MAGIC .ctco-connector .horizontal {
# MAGIC   position: absolute;
# MAGIC   left: 24%;
# MAGIC   right: 24%;
# MAGIC   top: 36px;
# MAGIC   height: 3px;
# MAGIC   background: #FF5F46;
# MAGIC }
# MAGIC .ctco-connector .down-left,
# MAGIC .ctco-connector .down-right {
# MAGIC   position: absolute;
# MAGIC   top: 36px;
# MAGIC   width: 3px;
# MAGIC   height: 28px;
# MAGIC   background: #FF5F46;
# MAGIC }
# MAGIC .ctco-connector .down-left {
# MAGIC   left: 24%;
# MAGIC }
# MAGIC .ctco-connector .down-right {
# MAGIC   right: 24%;
# MAGIC }
# MAGIC .ctco-connector .down-left::after,
# MAGIC .ctco-connector .down-right::after {
# MAGIC   content: "";
# MAGIC   position: absolute;
# MAGIC   left: 50%;
# MAGIC   bottom: -1px;
# MAGIC   transform: translateX(-50%);
# MAGIC   width: 0;
# MAGIC   height: 0;
# MAGIC   border-top: 12px solid #FF5F46;
# MAGIC   border-left: 7px solid transparent;
# MAGIC   border-right: 7px solid transparent;
# MAGIC }
# MAGIC .ctco-notification-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1fr 1fr;
# MAGIC   gap: 34px;
# MAGIC   align-items: start;
# MAGIC }
# MAGIC .ctco-notification-card {
# MAGIC   background: #ffffff;
# MAGIC   border: 1.5px solid #0B2026;
# MAGIC   border-radius: 2px;
# MAGIC   box-shadow: 0 3px 12px rgba(27,49,57,0.07);
# MAGIC   overflow: visible;
# MAGIC }
# MAGIC .ctco-card-heading {
# MAGIC   width: 74%;
# MAGIC   margin: -1px auto 0 auto;
# MAGIC   background: #FF5F46;
# MAGIC   color: #ffffff;
# MAGIC   border: 1.5px solid #0B2026;
# MAGIC   padding: 12px 18px;
# MAGIC   text-align: center;
# MAGIC   font-size: 22px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 800;
# MAGIC }
# MAGIC .ctco-card-body {
# MAGIC   padding: 30px 34px 28px 34px;
# MAGIC   min-height: 205px;
# MAGIC }
# MAGIC .ctco-card-body ul {
# MAGIC   margin: 0;
# MAGIC   padding-left: 22px;
# MAGIC }
# MAGIC .ctco-card-body li {
# MAGIC   font-size: 20px;
# MAGIC   line-height: 1.45;
# MAGIC   color: #0B2026;
# MAGIC   margin-bottom: 18px;
# MAGIC }
# MAGIC .ctco-card-body li:last-child {
# MAGIC   margin-bottom: 0;
# MAGIC }
# MAGIC .ctco-card-body strong {
# MAGIC   font-weight: 800;
# MAGIC }
# MAGIC @media screen and (max-width: 900px) {
# MAGIC   .ctco-notification-grid {
# MAGIC     grid-template-columns: 1fr;
# MAGIC     gap: 26px;
# MAGIC   }
# MAGIC   .ctco-connector {
# MAGIC     display: none;
# MAGIC   }
# MAGIC   .ctco-card-heading {
# MAGIC     width: 88%;
# MAGIC   }
# MAGIC   .ctco-card-body li {
# MAGIC     font-size: 16px;
# MAGIC   }
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="ctco-notification-center">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/overview_notification_alerts_icon.png"
# MAGIC     alt="Notification configurations icon">
# MAGIC   <div class="ctco-notification-title">Notification Configurations</div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ctco-connector">
# MAGIC   <div class="vertical"></div>
# MAGIC   <div class="horizontal"></div>
# MAGIC   <div class="down-left"></div>
# MAGIC   <div class="down-right"></div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="ctco-notification-grid">
# MAGIC   <div class="ctco-notification-card">
# MAGIC     <div class="ctco-card-heading">Job Level Notifications</div>
# MAGIC     <div class="ctco-card-body">
# MAGIC       <ul>
# MAGIC         <li>A notification alert is sent after <strong>successful completion of the job</strong></li>
# MAGIC         <li>This setting can be adjusted in the Job Details section (right-side pane).</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="ctco-notification-card">
# MAGIC     <div class="ctco-card-heading">Task Level Notifications</div>
# MAGIC     <div class="ctco-card-body">
# MAGIC       <ul>
# MAGIC         <li>A notification alert will be sent when the <strong>task is successfully completed</strong></li>
# MAGIC         <li>This notification setting can be customized at the task level.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES</summary>
# MAGIC <details>
# MAGIC
# MAGIC <p>Notification alerts form a critical part of operational excellence, and understanding the configuration levels helps you build effective alerting strategies:</p>
# MAGIC
# MAGIC - <p>Job Level Notifications: Configure these in the Job Details section's right-side pane. Job-level alerts are sent after the entire job completes successfully. This is perfect for stakeholders who need to know when complete workflows finish, such as business users waiting for daily reports or downstream systems that depend on your job's outputs.</p>
# MAGIC - <p>Task Level Notifications: Each task can have its own notification configuration, allowing granular alerting strategies. This is essential when different tasks have different stakeholders or when certain tasks are more critical than others. For example, you might want immediate alerts for data validation failures but only summary notifications for routine cleanup tasks.</p>
# MAGIC - <p>Strategic Considerations: Design your notification strategy based on operational needs, not technical convenience. Consider who needs to know what, when they need to know it, and what actions they can take based on the notification.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A7. Notification Alerts - Designing Effective Notification Strategies
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div class="ctco-alert-strategy-simple">
# MAGIC <style>
# MAGIC .ctco-alert-strategy-simple {
# MAGIC   width: 1200px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 16px auto 28px auto;
# MAGIC   font-family: "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .ctco-alert-strategy-simple * {
# MAGIC   box-sizing: border-box;
# MAGIC }
# MAGIC .ctco-alert-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 0.78fr 1.64fr 0.78fr;
# MAGIC   gap: 16px;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .ctco-alert-list {
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   justify-content: space-between;
# MAGIC   gap: 14px;
# MAGIC   min-height: 500px;
# MAGIC }
# MAGIC .ctco-alert-item {
# MAGIC   border-radius: 12px;
# MAGIC   padding: 18px;
# MAGIC   min-height: 112px;
# MAGIC   box-shadow: 0 3px 12px rgba(27,49,57,0.08);
# MAGIC }
# MAGIC .ctco-alert-left .ctco-alert-item {
# MAGIC   border: none;
# MAGIC   color: #ffffff;
# MAGIC }
# MAGIC .ctco-alert-left .ctco-alert-item:nth-child(1) {
# MAGIC   background: #FF5F46;
# MAGIC }
# MAGIC .ctco-alert-left .ctco-alert-item:nth-child(2) {
# MAGIC   background: #2272B4;
# MAGIC }
# MAGIC .ctco-alert-left .ctco-alert-item:nth-child(3) {
# MAGIC   background: #00A972;
# MAGIC }
# MAGIC .ctco-alert-right .ctco-alert-item {
# MAGIC   border: none;
# MAGIC   color: #ffffff;
# MAGIC }
# MAGIC .ctco-alert-right .ctco-alert-item.green {
# MAGIC   background: #FF3621;
# MAGIC }
# MAGIC .ctco-alert-right .ctco-alert-item.maroon {
# MAGIC   background: #CD7F32;
# MAGIC }
# MAGIC .ctco-alert-item strong {
# MAGIC   display: block;
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 800;
# MAGIC   margin-bottom: 8px;
# MAGIC }
# MAGIC .ctco-alert-left .ctco-alert-item strong,
# MAGIC .ctco-alert-left .ctco-alert-item div,
# MAGIC .ctco-alert-left .ctco-alert-item li {
# MAGIC   color: #ffffff;
# MAGIC }
# MAGIC .ctco-alert-right .ctco-alert-item strong,
# MAGIC .ctco-alert-right .ctco-alert-item div,
# MAGIC .ctco-alert-right .ctco-alert-item li {
# MAGIC   color: #ffffff;
# MAGIC }
# MAGIC .ctco-alert-item div,
# MAGIC .ctco-alert-item li {
# MAGIC   font-size: 16px;
# MAGIC   line-height: 1.5;
# MAGIC }
# MAGIC .ctco-alert-item ul {
# MAGIC   margin: 8px 0 0 0;
# MAGIC   padding-left: 20px;
# MAGIC }
# MAGIC .ctco-alert-center {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 14px;
# MAGIC   padding: 12px;
# MAGIC   min-height: 500px;
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   box-shadow: 0 3px 12px rgba(27,49,57,0.07);
# MAGIC }
# MAGIC .ctco-alert-center img {
# MAGIC   width: 100%;
# MAGIC   max-width: 650px;
# MAGIC   max-height: 485px;
# MAGIC   object-fit: contain;
# MAGIC   border-radius: 10px;
# MAGIC   background: #ffffff;
# MAGIC }
# MAGIC @media screen and (max-width: 900px) {
# MAGIC   .ctco-alert-grid {
# MAGIC     grid-template-columns: 1fr;
# MAGIC   }
# MAGIC   .ctco-alert-list,
# MAGIC   .ctco-alert-center {
# MAGIC     min-height: auto;
# MAGIC   }
# MAGIC }
# MAGIC </style>
# MAGIC <div class="ctco-alert-grid">
# MAGIC   <div class="ctco-alert-list ctco-alert-left">
# MAGIC     <div class="ctco-alert-item">
# MAGIC       <strong>Multiple supporting destinations</strong>
# MAGIC       <div>Emails, Teams, pagerDuty, Slack and Webhook</div>
# MAGIC     </div>
# MAGIC     <div class="ctco-alert-item">
# MAGIC       <strong>Each task in a Job</strong>
# MAGIC       <div>could be configured differently to send notification</div>
# MAGIC     </div>
# MAGIC     <div class="ctco-alert-item">
# MAGIC       <strong>Jobs triggers notifications</strong>
# MAGIC       <div>when the task begins, completes or fails</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="ctco-alert-center">
# MAGIC     <img
# MAGIC       src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/notification_configurations_ui.png"
# MAGIC       alt="Notification configurations UI screenshot">
# MAGIC   </div>
# MAGIC   <div class="ctco-alert-list ctco-alert-right">
# MAGIC     <div class="ctco-alert-item green">
# MAGIC       <strong>Late Jobs</strong>
# MAGIC       <div>These notification could also be triggered in the case of late Jobs:</div>
# MAGIC       <ul>
# MAGIC         <li>For duration threshold warning/timeout alerts</li>
# MAGIC         <li>Streaming backlog(Issue with getting streaming data)</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <div class="ctco-alert-item maroon">
# MAGIC       <strong>Webhook</strong>
# MAGIC       <div>allows custom integration with third-party API services</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Modern production environments require sophisticated notification strategies:</p>
# MAGIC
# MAGIC - <p>Multiple Destinations: Support for Emails, Microsoft Teams, PagerDuty, Slack, and Webhooks means you can integrate with your existing operational tools and communication patterns. Different teams might prefer different channels - developers might want Slack notifications while operations teams prefer PagerDuty integration.</p>
# MAGIC - <p>Per-Task Customization: Each task in a job can have completely different notification configurations. Your data ingestion tasks might send alerts to the data engineering team, while your reporting tasks notify business stakeholders.</p>
# MAGIC - <p>Advanced Trigger Conditions: Beyond simple success/failure, you can configure notifications for:</p>
# MAGIC - <p>Late Jobs: Duration threshold warnings and timeout alerts help you catch performance degradation early</p>
# MAGIC - <p>Streaming Backlog: Critical for streaming workloads where falling behind can cascade into larger issues</p>
# MAGIC - <p>Custom Conditions: Webhook integrations allow complex custom logic for notification decisions</p>
# MAGIC - <p>Lifecycle Notifications: Jobs can trigger notifications when tasks begin (useful for long-running processes), complete successfully (confirmation of completion), or fail (immediate response required).</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A8. Retry Policy
# MAGIC <br>
# MAGIC
# MAGIC <div class="retry-policy-visual">
# MAGIC <style>
# MAGIC .retry-policy-visual{width:1200px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .retry-policy-visual *{box-sizing:border-box;}
# MAGIC .rpv-top{width:100%;background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 22px;margin-bottom:20px;text-align:center;}
# MAGIC .rpv-definition{font-size:22px;line-height:1.35;font-weight:700;color:#0B2026;}
# MAGIC .rpv-definition strong{font-weight:900;}
# MAGIC .rpv-image-wrap{width:100%;background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:14px;display:flex;align-items:center;justify-content:center;}
# MAGIC .rpv-image-wrap img{display:block;width:100%;max-width:100%;height:auto;object-fit:contain;border-radius:10px;background:transparent;}
# MAGIC @media screen and (max-width:900px){.rpv-definition{font-size:18px;}.rpv-image-wrap{padding:10px;}}
# MAGIC </style>
# MAGIC <div class="rpv-top">
# MAGIC <div class="rpv-definition">A policy that determines when and how many times failed runs are <strong>retried</strong></div>
# MAGIC </div>
# MAGIC <div class="rpv-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_common_task_config/retry_policy_ui.png" alt="Retry policy UI">
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC A well-designed retry policy is essential for resilient workflows. The policy determines not just how many times to retry, but under what conditions and with what timing patterns.
# MAGIC
# MAGIC Consider factors like:
# MAGIC - Failure Type: Transient network issues might warrant immediate retries, while data quality issues might not
# MAGIC - Resource Impact: Retrying resource-intensive tasks too aggressively can cause cluster resource contention
# MAGIC - Downstream Dependencies: Failed tasks might impact other workflows, making retry timing critical
# MAGIC - Business SLA: Some processes have strict timing requirements that limit retry windows
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Job Schedules and Triggers - Overview
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div class="jst-overview">
# MAGIC <style>
# MAGIC .jst-overview {
# MAGIC   width: 1100px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 16px auto 28px auto;
# MAGIC   font-family: "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .jst-overview * { box-sizing: border-box; }
# MAGIC .jst-overview-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1.3fr 0.9fr;
# MAGIC   gap: 24px;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .jst-trigger-list {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 14px;
# MAGIC   padding: 22px;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC }
# MAGIC .jst-trigger-list-title {
# MAGIC   font-size: 18px;
# MAGIC   font-weight: 700;
# MAGIC   margin-bottom: 16px;
# MAGIC   color: #FF5F46;
# MAGIC }
# MAGIC .jst-trigger-cards {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: repeat(5, 1fr);
# MAGIC   gap: 12px;
# MAGIC }
# MAGIC .jst-trigger-card {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   min-height: 126px;
# MAGIC   display: flex;
# MAGIC   flex-direction: column;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   gap: 10px;
# MAGIC   padding: 12px 8px;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .jst-trigger-card img {
# MAGIC   width: 54px;
# MAGIC   height: 54px;
# MAGIC   object-fit: contain;
# MAGIC   background: transparent;
# MAGIC   mix-blend-mode: multiply;
# MAGIC   filter: contrast(1.15) brightness(1);
# MAGIC   border-radius: 4px;
# MAGIC }
# MAGIC .jst-trigger-card span {
# MAGIC   font-size: 16px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 700;
# MAGIC }
# MAGIC .jst-overview-card {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 14px;
# MAGIC   box-shadow: 0 3px 12px rgba(27,49,57,0.07);
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .jst-card-bar { height: 8px; background: #FF5F46; }
# MAGIC .jst-overview-body { padding: 24px; }
# MAGIC .jst-overview-body h3 {
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.25;
# MAGIC   font-weight: 700;
# MAGIC   margin: 0 0 14px 0;
# MAGIC }
# MAGIC .jst-overview-body p, .jst-overview-body li {
# MAGIC   font-size: 16px;
# MAGIC   line-height: 1.55;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .jst-overview-body p { margin: 0 0 14px 0; }
# MAGIC .jst-overview-body ul { margin: 0 0 14px 0; padding-left: 22px; }
# MAGIC .jst-overview-body li { margin-bottom: 8px; }
# MAGIC @media screen and (max-width: 900px) {
# MAGIC   .jst-overview-grid { grid-template-columns: 1fr; }
# MAGIC   .jst-trigger-cards { grid-template-columns: repeat(2, 1fr); }
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="jst-overview-grid">
# MAGIC   <div class="jst-trigger-list">
# MAGIC     <div class="jst-trigger-list-title">Common Trigger Types</div>
# MAGIC     <div class="jst-trigger-cards">
# MAGIC       <div class="jst-trigger-card">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/scheduled_trigger_icon.png" alt="Scheduled trigger icon">
# MAGIC         <span>Scheduled</span>
# MAGIC       </div>
# MAGIC       <div class="jst-trigger-card">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/continuous_trigger_icon.png" alt="Continuous trigger icon">
# MAGIC         <span>Continuous</span>
# MAGIC       </div>
# MAGIC       <div class="jst-trigger-card">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/file_arrival_trigger_icon.png" alt="File arrival trigger icon">
# MAGIC         <span>File Arrival</span>
# MAGIC       </div>
# MAGIC       <div class="jst-trigger-card">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/manual_trigger_icon.png" alt="Manual trigger icon">
# MAGIC         <span>Manual</span>
# MAGIC       </div>
# MAGIC       <div class="jst-trigger-card">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/table_update_trigger_icon.png" alt="Table update trigger icon">
# MAGIC         <span>Table Update</span>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="text-align: center; margin-top: 20px;">
# MAGIC         <img
# MAGIC             src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/job_schedules_triggers_overview.png"
# MAGIC             alt="Overview of Job Schedules and Triggers"
# MAGIC             style="width: 700px; max-width: 100%; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1); border-radius: 4px;">
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="jst-overview-card">
# MAGIC     <div class="jst-card-bar"></div>
# MAGIC     <div class="jst-overview-body">
# MAGIC       <h3>Triggers</h3>
# MAGIC       <p>A <strong>trigger</strong> is a rule that automatically starts a job run based on a <strong>specific condition</strong> or <strong>schedule</strong>.</p>
# MAGIC       <p><strong>Common</strong> trigger types include:</p>
# MAGIC       <ul>
# MAGIC         <li>Time-based schedules</li>
# MAGIC         <li>Continuous (always-on) execution</li>
# MAGIC         <li>File arrival events</li>
# MAGIC         <li>Manual trigger</li>
# MAGIC         <li>Table Update</li>
# MAGIC       </ul>
# MAGIC       <p>Triggers <strong>enable automation</strong>, so jobs can run without manual intervention.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>A trigger is fundamentally a rule engine that automatically initiates job execution based on specific conditions or schedules. This is not just about convenience - it is about building reliable, responsive data systems that can operate autonomously.</p>
# MAGIC <p><strong>Trigger Categories:</strong></p>
# MAGIC <ul>
# MAGIC <li><strong>Time-based schedules:</strong> Traditional cron-style scheduling for predictable, recurring workloads</li>
# MAGIC <li><strong>Continuous execution:</strong> Always-on processing for streaming data scenarios</li>
# MAGIC <li><strong>File arrival events:</strong> Event-driven processing that responds immediately to new data</li>
# MAGIC <li><strong>Manual triggers:</strong> On-demand execution for development, testing, and ad-hoc analysis</li>
# MAGIC </ul>
# MAGIC <p><strong>Automation Benefits:</strong> Triggers eliminate human intervention points that can cause delays, errors, or missed executions. They enable 24/7 operation and ensure consistent execution regardless of team availability.</p>
# MAGIC <p><strong>Reliability Considerations:</strong> Each trigger type has different reliability characteristics and failure modes. Understanding these helps you choose the right trigger for each use case.</p>
# MAGIC <p>For example, if you want to run a job daily at scheduled intervals, a scheduled trigger would be the best choice. If you need to run your job when a file arrives, a file trigger would be most appropriate.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Types of Triggers
# MAGIC
# MAGIC ##### Click each of the trigger types to learn about them.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div class="jst-trigger-tabs">
# MAGIC <style>
# MAGIC .jst-trigger-tabs {
# MAGIC   width: 1100px;
# MAGIC   max-width: 100%;
# MAGIC   margin: 16px auto 28px auto;
# MAGIC   font-family: "DM Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   color: #0B2026;
# MAGIC }
# MAGIC .jst-trigger-tabs * { box-sizing: border-box; }
# MAGIC .jst-trigger-tabs input[type="radio"] { display: none; }
# MAGIC .jst-tabs-grid {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 245px 1fr;
# MAGIC   gap: 22px;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .jst-trigger-nav {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 14px;
# MAGIC   padding: 16px;
# MAGIC   display: grid;
# MAGIC   gap: 10px;
# MAGIC }
# MAGIC .jst-trigger-nav-title {
# MAGIC   color: #FF5F46;
# MAGIC   font-weight: 800;
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.2;
# MAGIC   padding: 4px 6px 8px 6px;
# MAGIC }
# MAGIC .jst-trigger-nav label {
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   gap: 12px;
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-left: 5px solid transparent;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 12px;
# MAGIC   min-height: 74px;
# MAGIC   cursor: pointer;
# MAGIC   font-size: 16px;
# MAGIC   font-weight: 700;
# MAGIC   transition: all .2s ease;
# MAGIC }
# MAGIC .jst-trigger-nav label img {
# MAGIC   width: 42px;
# MAGIC   height: 42px;
# MAGIC   object-fit: contain;
# MAGIC   background: transparent;
# MAGIC   mix-blend-mode: multiply;
# MAGIC   filter: contrast(1.15) brightness(1);
# MAGIC   border-radius: 4px;
# MAGIC }
# MAGIC #jst-scheduled:checked ~ .jst-tabs-grid label[for="jst-scheduled"],
# MAGIC #jst-file:checked ~ .jst-tabs-grid label[for="jst-file"],
# MAGIC #jst-continuous:checked ~ .jst-tabs-grid label[for="jst-continuous"],
# MAGIC #jst-manual:checked ~ .jst-tabs-grid label[for="jst-manual"],
# MAGIC #jst-table:checked ~ .jst-tabs-grid label[for="jst-table"] {
# MAGIC   border-left-color: #FF5F46;
# MAGIC   background: #FFF1EE;
# MAGIC }
# MAGIC .jst-trigger-panels {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 14px;
# MAGIC   box-shadow: 0 3px 12px rgba(27,49,57,0.07);
# MAGIC   overflow: hidden;
# MAGIC }
# MAGIC .jst-panel { display: none; }
# MAGIC #jst-scheduled:checked ~ .jst-tabs-grid .panel-scheduled,
# MAGIC #jst-file:checked ~ .jst-tabs-grid .panel-file,
# MAGIC #jst-continuous:checked ~ .jst-tabs-grid .panel-continuous,
# MAGIC #jst-manual:checked ~ .jst-tabs-grid .panel-manual,
# MAGIC #jst-table:checked ~ .jst-tabs-grid .panel-table { display: block; }
# MAGIC .jst-panel-bar { height: 8px; background: #FF5F46; }
# MAGIC .jst-panel-bar.green { background: #00A972; }
# MAGIC .jst-panel-bar.maroon { background: #98102A; }
# MAGIC .jst-panel-bar.blue { background: #2272B4; }
# MAGIC .jst-panel-body { padding: 22px; }
# MAGIC .jst-panel-title {
# MAGIC   display: inline-block;
# MAGIC   background: #FF5F46;
# MAGIC   color: #ffffff;
# MAGIC   border-radius: 8px;
# MAGIC   padding: 10px 16px;
# MAGIC   font-size: 18px;
# MAGIC   line-height: 1.2;
# MAGIC   font-weight: 700;
# MAGIC   margin-bottom: 18px;
# MAGIC }
# MAGIC .jst-panel-title.green { background: #00A972; }
# MAGIC .jst-panel-title.maroon { background: #98102A; }
# MAGIC .jst-panel-title.blue { background: #2272B4; }
# MAGIC .jst-panel-content {
# MAGIC   display: grid;
# MAGIC   grid-template-columns: 1.28fr .72fr;
# MAGIC   gap: 22px;
# MAGIC   align-items: stretch;
# MAGIC }
# MAGIC .jst-ui-shot {
# MAGIC   background: #ffffff;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 14px;
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC   min-height: 365px;
# MAGIC }
# MAGIC .jst-ui-shot img {
# MAGIC   width: 100%;
# MAGIC   max-height: 345px;
# MAGIC   object-fit: contain;
# MAGIC   border-radius: 8px;
# MAGIC }
# MAGIC .jst-trigger-text {
# MAGIC   background: #F9F7F4;
# MAGIC   border: 1px solid #DCE0E2;
# MAGIC   border-radius: 12px;
# MAGIC   padding: 18px;
# MAGIC }
# MAGIC .jst-trigger-text ul { margin: 0; padding-left: 22px; }
# MAGIC .jst-trigger-text li {
# MAGIC   font-size: 16px;
# MAGIC   line-height: 1.55;
# MAGIC   margin-bottom: 10px;
# MAGIC }
# MAGIC @media screen and (max-width: 980px) {
# MAGIC   .jst-tabs-grid { grid-template-columns: 1fr; }
# MAGIC   .jst-panel-content { grid-template-columns: 1fr; }
# MAGIC }
# MAGIC </style>
# MAGIC <input type="radio" id="jst-scheduled" name="jst-trigger-select" checked>
# MAGIC <input type="radio" id="jst-file" name="jst-trigger-select">
# MAGIC <input type="radio" id="jst-continuous" name="jst-trigger-select">
# MAGIC <input type="radio" id="jst-manual" name="jst-trigger-select">
# MAGIC <input type="radio" id="jst-table" name="jst-trigger-select">
# MAGIC <div class="jst-tabs-grid">
# MAGIC   <div class="jst-trigger-nav">
# MAGIC     <div class="jst-trigger-nav-title">Triggers</div>
# MAGIC     <label for="jst-scheduled"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/scheduled_trigger_icon.png" alt="Scheduled trigger icon"><span>Scheduled</span></label>
# MAGIC     <label for="jst-file"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/file_arrival_trigger_icon.png" alt="File arrival trigger icon"><span>File Arrival</span></label>
# MAGIC     <label for="jst-continuous"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/continuous_trigger_icon.png" alt="Continuous trigger icon"><span>Continuous</span></label>
# MAGIC     <label for="jst-manual"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/manual_trigger_icon.png" alt="Manual trigger icon"><span>Manual</span></label>
# MAGIC     <label for="jst-table"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/table_update_trigger_icon.png" alt="Table update trigger icon"><span>Table Update</span></label>
# MAGIC   </div>
# MAGIC   <div class="jst-trigger-panels">
# MAGIC     <div class="jst-panel panel-scheduled">
# MAGIC       <div class="jst-panel-bar"></div>
# MAGIC       <div class="jst-panel-body">
# MAGIC         <div class="jst-panel-title">1. Scheduled Trigger</div>
# MAGIC         <div class="jst-panel-content">
# MAGIC           <div class="jst-ui-shot"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/scheduled_trigger_ui.png" alt="Scheduled trigger configuration UI"></div>
# MAGIC           <div class="jst-trigger-text">
# MAGIC             <ul>
# MAGIC               <li><strong>Automatically run jobs</strong> at set times or intervals, such as hourly, daily using the UI</li>
# MAGIC               <li>You can schedule jobs using <strong>cron expressions</strong> for automated execution</li>
# MAGIC               <li>They help <strong>automate</strong> recurring jobs, ensuring jobs run consistently without manual intervention</li>
# MAGIC             </ul>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="jst-panel panel-file">
# MAGIC       <div class="jst-panel-bar green"></div>
# MAGIC       <div class="jst-panel-body">
# MAGIC         <div class="jst-panel-title green">2. File Arrival Trigger</div>
# MAGIC         <div class="jst-panel-content">
# MAGIC           <div class="jst-ui-shot"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/file_arrival_trigger_ui.png" alt="File arrival trigger configuration UI"></div>
# MAGIC           <div class="jst-trigger-text">
# MAGIC             <ul>
# MAGIC               <li><strong>Automatically</strong> trigger jobs <strong>when new files are detected</strong> in a specified storage location, with support for: <br>
# MAGIC               AWS S3 | Azure Storage | GCP GS and Databricks Volumes</li>
# MAGIC               <li>Enable <strong>event-driven processing</strong> to start data workflows upon <strong>file arrival</strong></li>
# MAGIC               <li>Ideal for <strong>automating jobs</strong> with <strong>unpredictable or irregular</strong> data ingestion patterns</li>
# MAGIC             </ul>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="jst-panel panel-continuous">
# MAGIC       <div class="jst-panel-bar blue"></div>
# MAGIC       <div class="jst-panel-body">
# MAGIC         <div class="jst-panel-title blue">3. Continuous Trigger</div>
# MAGIC         <div class="jst-panel-content">
# MAGIC           <div class="jst-ui-shot"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/continuous_trigger_ui.png" alt="Continuous trigger configuration UI"></div>
# MAGIC           <div class="jst-trigger-text">
# MAGIC             <ul>
# MAGIC               <li><strong>Continuously</strong> runs jobs by starting a new run as soon as the previous one finishes or fails</li>
# MAGIC               <li><strong>Built-in retry</strong> logic is automatically managed by Databricks</li>
# MAGIC               <li>Ideal for <strong>streaming</strong> workloads</li>
# MAGIC             </ul>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="jst-panel panel-manual">
# MAGIC       <div class="jst-panel-bar maroon"></div>
# MAGIC       <div class="jst-panel-body">
# MAGIC         <div class="jst-panel-title maroon">4. Manual Triggers</div>
# MAGIC         <div class="jst-panel-content">
# MAGIC           <div class="jst-ui-shot"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/manual_trigger_ui.png" alt="Manual trigger configuration UI"></div>
# MAGIC           <div class="jst-trigger-text">
# MAGIC             <ul>
# MAGIC               <li><strong>Manual (None) trigger</strong> lets jobs run on demand without any schedule or event.</li>
# MAGIC               <li>Can be <strong>started from UI</strong> (Run now or Run now with different settings), API, CLI, SDK, or via DABS</li>
# MAGIC               <li>Best for <strong>ad hoc runs, debugging, and one-off backfills</strong></li>
# MAGIC               <li>Can be combined with other trigger types later</li>
# MAGIC             </ul>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div class="jst-panel panel-table">
# MAGIC       <div class="jst-panel-bar"></div>
# MAGIC       <div class="jst-panel-body">
# MAGIC         <div class="jst-panel-title">5. Table Update Trigger</div>
# MAGIC         <div class="jst-panel-content">
# MAGIC           <div class="jst-ui-shot"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/table_update_trigger_ui.png" alt="Table update trigger configuration UI"></div>
# MAGIC           <div class="jst-trigger-text">
# MAGIC             <ul>
# MAGIC               <li>Table Update Trigger automatically starts a job when <strong>specified tables</strong> are updated.</li>
# MAGIC               <li><strong>Monitors</strong> one or more tables for changes (insert, update, delete, or merge).</li>
# MAGIC               <li>Helps <strong>replace</strong> time-based scheduling (like cron jobs) with real-time orchestration, ensuring jobs run as soon as <strong>new data arrives.</strong></li>
# MAGIC             </ul>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC   <div style="font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color:#0B2026; font-size:16px; line-height:1.55; margin-top:12px;">
# MAGIC     <p><strong>Scheduled triggers</strong> are the backbone of most production data workflows, providing reliable, time-based execution:</p>
# MAGIC     <ul>
# MAGIC       <li><strong>UI-Based Scheduling:</strong> The Databricks interface provides intuitive scheduling options for common patterns - hourly, daily, weekly, monthly. This is perfect for business users and reduces the learning curve for cron syntax.</li>
# MAGIC       <li><strong>Cron Expression Power:</strong> For more complex timing requirements, full cron expression support enables sophisticated schedules like "every 15 minutes during business hours" or "first Monday of each month."</li>
# MAGIC       <li><strong>Use Case Patterns:</strong> 
# MAGIC       <ul>
# MAGIC       <li>Daily ETL: Process yesterday's data every morning at 6 AM
# MAGIC       <li>Weekly Reports: Generate executive dashboards every Monday morning
# MAGIC       <li>Monthly Aggregations: Calculate monthly KPIs on the first day of each month
# MAGIC       <li>Hourly Streaming Checkpoints: Regular maintenance for streaming jobs
# MAGIC         </li>
# MAGIC         </ul>
# MAGIC         </li>
# MAGIC       <li><strong>Timezone Considerations:</strong> Always specify the appropriate timezone for your business context, especially for organizations operating across multiple regions.</li>
# MAGIC     </ul>
# MAGIC     <p><strong>File arrival triggers</strong> represent a paradigm shift from time-based to event-driven processing, enabling immediate response to data availability:</p>
# MAGIC     <ul>
# MAGIC       <li><strong>Storage Platform Support:</strong> AWS S3, Azure Storage, Google Cloud Storage, and Databricks Volumes.</li>
# MAGIC       <li><strong>Event-Driven Architecture:</strong> Processing begins immediately when data becomes available, rather than waiting for the next scheduled execution.</li>
# MAGIC       <li><strong>Real-World Scenarios:</strong> 
# MAGIC       <ul>
# MAGIC       <li>Partner Data Feeds: Process files as soon as external partners upload them
# MAGIC       <li>IoT Data Processing: Handle sensor data files uploaded irregularly throughout the day
# MAGIC       <li>Financial Data: Process trading data files that arrive at unpredictable intervals
# MAGIC       <li>Log File Processing: Handle application logs uploaded by various systems
# MAGIC </ul>
# MAGIC </li>
# MAGIC       <li><strong>Pattern Matching:</strong> Configure file pattern matching to process only relevant files and ignore temporary or incomplete uploads.</li>
# MAGIC     </ul>
# MAGIC     <p><strong>Continuous triggers</strong> are designed specifically for workloads that need to maintain constant processing:</p>
# MAGIC     <ul>
# MAGIC       <li><strong>Automatic Restart Logic:</strong> Built-in retry logic automatically managed by Databricks ensures that streaming jobs maintain continuity even through transient failures.</li>
# MAGIC       <li><strong>Resource Management:</strong> Continuous jobs are automatically managed to prevent resource leaks and ensure optimal cluster utilization over extended periods.</li>
# MAGIC       <li><strong>Streaming Use Cases:</strong> 
# MAGIC       <ul>
# MAGIC       <li>Real-time Analytics: Continuous processing of clickstream data for real-time dashboards
# MAGIC       <li>Fraud Detection: Always-on processing of transaction streams for immediate fraud identification
# MAGIC       <li>IoT Processing: Continuous ingestion and processing of sensor data streams
# MAGIC       <li>Change Data Capture: Real-time processing of database change streams
# MAGIC         </ul>
# MAGIC       </li>
# MAGIC       <li><strong>Monitoring Considerations:</strong> Continuous jobs require different monitoring approaches since they are designed to run indefinitely rather than complete discrete tasks.</li>
# MAGIC     </ul>
# MAGIC     <p><strong>Manual triggers</strong> provide essential flexibility for development, testing, and ad-hoc processing scenarios:</p>
# MAGIC     <ul>
# MAGIC       <li><strong>Execution Options:</strong> 
# MAGIC       <ul>
# MAGIC       <li>UI Execution: "Run now" for immediate execution with current settings
# MAGIC       <li>Parameterized Execution: "Run now with different settings" allows runtime parameter overrides
# MAGIC       <li>Programmatic Execution: API, CLI, SDK, and Databricks Asset Bundles enable integration with external systems
# MAGIC       </ul>
# MAGIC         </li>
# MAGIC       <li><strong>Development Workflow:</strong> Manual triggers are essential during development for testing job logic, debugging issues, and validating changes before implementing automated triggers.</li>
# MAGIC       <li><strong>Operational Use Cases:</strong> 
# MAGIC       <ul>
# MAGIC       <li>Backfill Processing: Handle historical data processing outside normal schedules
# MAGIC       <li>Emergency Processing: Respond to urgent business needs that can't wait for scheduled execution
# MAGIC       <li>Data Recovery: Reprocess specific time periods after resolving data quality issues
# MAGIC       <li>Testing and Validation: Verify job behavior in production environments before enabling automation
# MAGIC       </ul>
# MAGIC         </li>
# MAGIC     </ul>
# MAGIC     <p><strong>Table Update Trigger</strong> - a new trigger type in Databricks Lakeflow Jobs.
# MAGIC     <ul>
# MAGIC     <li>It automatically starts a job whenever specified source tables are updated.
# MAGIC     <li>This means we no longer rely on manual or cron-based schedules. Instead, jobs run in real time — as soon as new data lands — which improves freshness and reduces wasted compute.
# MAGIC     <li>It works by monitoring one or more tables for any data change — such as insert, update, delete, or merge.
# MAGIC     <li>You can configure it easily by selecting the ‘Table update’ option within job triggers, and then listing the tables you want to watch.
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Table Update Trigger Configuration
# MAGIC
# MAGIC Click the step number below to see how the Table Update Trigger is configured.
# MAGIC
# MAGIC <div class="jst-click-steps">
# MAGIC <style>
# MAGIC .jst-click-steps{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .jst-click-steps *{box-sizing:border-box;}
# MAGIC .jst-click-steps input[type="radio"]{display:none;}
# MAGIC .jst-layout{display:grid;grid-template-columns:.55fr 1.45fr;gap:22px;align-items:stretch;}
# MAGIC .jst-step-list{display:grid;gap:10px;align-content:start;}
# MAGIC .jst-step-card{background:#F9F7F4;border:1.5px solid #DCE0E2;border-left:5px solid #FF5F46;border-radius:12px;padding:12px 14px;cursor:pointer;box-shadow:0 2px 8px rgba(27,49,57,.06);transition:all .2s ease;}
# MAGIC .jst-step-card:hover{background:#FFF1EE;}
# MAGIC .jst-step-head{display:flex;align-items:center;gap:10px;}
# MAGIC .jst-step-badge{background:#FF5F46;color:#ffffff;border-radius:5px;padding:4px 10px;font-size:12px;font-weight:800;line-height:1.2;white-space:nowrap;}
# MAGIC .jst-step-card h3{font-size:16px;line-height:1.2;margin:0;font-weight:800;color:#0B2026;}
# MAGIC .jst-step-detail{max-height:0;opacity:0;overflow:hidden;transition:max-height .25s ease,opacity .2s ease,margin-top .2s ease;margin-top:0;}
# MAGIC .jst-step-detail ul{margin:0;padding-left:18px;}
# MAGIC .jst-step-detail li{font-size:13.5px;line-height:1.32;margin-bottom:4px;color:#0B2026;}
# MAGIC #jst-s1:checked ~ .jst-layout label[for="jst-s1"],#jst-s2:checked ~ .jst-layout label[for="jst-s2"],#jst-s3:checked ~ .jst-layout label[for="jst-s3"],#jst-s4:checked ~ .jst-layout label[for="jst-s4"]{background:#FFF1EE;border-color:#FF5F46;box-shadow:0 0 0 3px rgba(255,95,70,.16);}
# MAGIC #jst-s1:checked ~ .jst-layout label[for="jst-s1"] .jst-step-detail,#jst-s2:checked ~ .jst-layout label[for="jst-s2"] .jst-step-detail,#jst-s3:checked ~ .jst-layout label[for="jst-s3"] .jst-step-detail,#jst-s4:checked ~ .jst-layout label[for="jst-s4"] .jst-step-detail{max-height:190px;opacity:1;margin-top:9px;}
# MAGIC .jst-image-panel{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;padding:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);display:flex;align-items:center;justify-content:center;}
# MAGIC .jst-image-wrap{position:relative;width:100%;overflow:hidden;border-radius:10px;background:#ffffff;}
# MAGIC .jst-image-wrap img{display:block;width:100%;height:auto;border-radius:10px;}
# MAGIC .jst-highlight{position:absolute;display:none;border:4px solid #FF5F46;border-radius:8px;background:rgba(255,95,70,.08);box-shadow:0 0 0 9999px rgba(11,32,38,.08);animation:jstBlink 1.2s ease-in-out infinite;pointer-events:none;}
# MAGIC .jst-h1{left:1.7%;top:20%;width:40%;height:9.5%;}
# MAGIC .jst-h2{left:1.7%;top:39.5%;width:98%;height:21%;}
# MAGIC .jst-h3{left:1.7%;top:61%;width:29%;height:12%;}
# MAGIC .jst-h4{left:1.7%;top:73%;width:98%;height:16%;}
# MAGIC #jst-s1:checked ~ .jst-layout .jst-h1,#jst-s2:checked ~ .jst-layout .jst-h2,#jst-s3:checked ~ .jst-layout .jst-h3,#jst-s4:checked ~ .jst-layout .jst-h4{display:block;}
# MAGIC @keyframes jstBlink{0%,100%{border-color:#FF5F46;box-shadow:0 0 0 9999px rgba(11,32,38,.08),0 0 0 0 rgba(255,95,70,.35);}50%{border-color:#98102A;box-shadow:0 0 0 9999px rgba(11,32,38,.08),0 0 0 7px rgba(255,95,70,.18);}}
# MAGIC @media screen and (max-width:980px){.jst-layout{grid-template-columns:1fr;}.jst-image-panel{overflow-x:auto;}.jst-image-wrap{min-width:720px;}}
# MAGIC </style>
# MAGIC <input type="radio" id="jst-s1" name="jst-step-select" checked>
# MAGIC <input type="radio" id="jst-s2" name="jst-step-select">
# MAGIC <input type="radio" id="jst-s3" name="jst-step-select">
# MAGIC <input type="radio" id="jst-s4" name="jst-step-select">
# MAGIC <div class="jst-layout">
# MAGIC <div class="jst-step-list">
# MAGIC <label class="jst-step-card" for="jst-s1">
# MAGIC <div class="jst-step-head"><div class="jst-step-badge">Step-1</div><h3>Selecting trigger type</h3></div>
# MAGIC <div class="jst-step-detail"><ul><li>Select <strong>Table update</strong> as the trigger type.</li></ul></div>
# MAGIC </label>
# MAGIC <label class="jst-step-card" for="jst-s2">
# MAGIC <div class="jst-step-head"><div class="jst-step-badge">Step-2</div><h3>Adding source tables</h3></div>
# MAGIC <div class="jst-step-detail"><ul><li>You can select up to 10 tables per trigger.</li><li>Works with Unity Catalog-managed Delta, Iceberg, external Delta tables, materialized views, and streaming tables.</li></ul></div>
# MAGIC </label>
# MAGIC <label class="jst-step-card" for="jst-s3">
# MAGIC <div class="jst-step-head"><div class="jst-step-badge">Step-3</div><h3>Defining triggering condition</h3></div>
# MAGIC <div class="jst-step-detail"><ul><li><u>Any table updated</u> – triggers when the first monitored table changes.</li><li><u>All tables updated</u> – triggers only after all selected tables update.</li></ul></div>
# MAGIC </label>
# MAGIC <label class="jst-step-card" for="jst-s4">
# MAGIC <div class="jst-step-head"><div class="jst-step-badge">Step-4</div><h3>Advanced functionality(Optional)</h3></div>
# MAGIC <div class="jst-step-detail"><ul><li><u>Min. time between triggers</u> – Prevents over-triggering by enforcing a mandatory gap between consecutive job runs</li><li><u>Wait after last change</u> – Delays job execution until all data updates have landed in source table</li></ul></div>
# MAGIC </label>
# MAGIC </div>
# MAGIC <div class="jst-image-panel">
# MAGIC <div class="jst-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_job_schedules_triggers/table_update_trigger_ui.png" alt="Table update trigger configuration UI">
# MAGIC <div class="jst-highlight jst-h1"></div>
# MAGIC <div class="jst-highlight jst-h2"></div>
# MAGIC <div class="jst-highlight jst-h3"></div>
# MAGIC <div class="jst-highlight jst-h4"></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Let us walk through how it works.</p>
# MAGIC <ol>
# MAGIC <li>You first select <strong>Table Update</strong> as the trigger type.</li>
# MAGIC <li>Then add your source tables. You can include up to ten tables in a single trigger, supporting Unity Catalog-managed Delta or Iceberg tables, materialized views, and streaming tables.</li>
# MAGIC <li>Then, define when the trigger should fire: <br>
# MAGIC <ul>
# MAGIC <li>It can run when <strong>any</strong> of the listed tables change
# MAGIC <li> Or it can wait until <strong>all</strong> of them are updated</li>
# MAGIC </ul></li>
# MAGIC <li>Finally, we have advanced options for more control.</li>
# MAGIC <ul>
# MAGIC <li><strong>Minimum time between triggers</strong> places a buffer between runs to prevent over-triggering during rapid table updates.<br> Example: For a frequently updated table, set a gap between consecutive runs to avoid multiple job executions in quick succession.</li>
# MAGIC <li><strong>Wait after last change</strong> delays the job until a set period has passed since the most recent update, ensuring all data has landed.<br> Example: When data arrives in multiple batches, define a waiting period so the job starts only after the entire batch is delivered.</li>
# MAGIC </ul>
# MAGIC </ol>
# MAGIC <p>Together, these settings make orchestration smarter, more reactive, and resource-efficient.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Conclusion
# MAGIC
# MAGIC - Common task configuration options, such as parameters, dynamic value references, notifications, and retry policies, make workflows reusable, adaptable, monitorable, and resilient.
# MAGIC - Triggers automatically start job runs based on schedules or conditions, including scheduled, continuous, file arrival, manual, and table update triggers.
# MAGIC - Table Update Trigger enables real-time orchestration by monitoring tables for changes and reducing reliance on time-based scheduling.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next demonstration, you will explore the practical implementation of different trigger types through the Databricks interface.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>