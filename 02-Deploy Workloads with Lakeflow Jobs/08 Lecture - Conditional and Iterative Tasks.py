# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Conditional and Iterative Tasks
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will explore intelligent Lakeflow Jobs workflows that can make decisions and adapt their behavior based on runtime conditions. These workflows can branch, loop, and make decisions based on data and processing results. 
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC 1. **Describe** Run-if Conditional Task Dependencies and how they control task execution based on upstream task outcomes
# MAGIC 2. **Explain** how If/Else Tasks add boolean conditional logic to workflows
# MAGIC 3. **Explain** how For Each Tasks enable iterative processing using input arrays and nested tasks
# MAGIC 4. **Identify** how conditional and iterative task patterns support dynamic, resilient workflows

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Overview - Advanced Task Types
# MAGIC
# MAGIC <p>We're now entering the realm of intelligent workflows that can make decisions and adapt their behavior based on runtime conditions. These aren't just linear sequences of tasks - they're dynamic workflows that can branch, loop, and make intelligent decisions based on data and processing results.</p>
# MAGIC <p>This capability transforms your workflows from simple automation to intelligent data processing systems. Three advanced task types enable sophisticated workflow patterns:</p>
# MAGIC
# MAGIC <div class="cit-overview">
# MAGIC <style>
# MAGIC .cit-overview{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cit-overview *{box-sizing:border-box;}
# MAGIC .cit-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;}
# MAGIC .cit-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;min-height:330px;display:flex;flex-direction:column;}
# MAGIC .cit-bar{height:8px;background:#FF5F46;}
# MAGIC .cit-card:nth-child(2) .cit-bar{background:#98102A;}
# MAGIC .cit-card:nth-child(3) .cit-bar{background:#00A972;}
# MAGIC .cit-body{padding:22px;text-align:center;display:flex;flex-direction:column;align-items:center;gap:14px;flex:1;}
# MAGIC .cit-body img{width:86px;height:86px;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC .ctco-heading-icon-box {
# MAGIC   width: 120px;
# MAGIC   height: 120px;
# MAGIC   min-width: 120px;
# MAGIC   border-radius: 12px;
# MAGIC   background: #FFF1EE;
# MAGIC   border: 1px solid #FFD2C9;
# MAGIC   display: flex;
# MAGIC   align-items: center;
# MAGIC   justify-content: center;
# MAGIC }
# MAGIC .ctco-heading-icon-box img {
# MAGIC   width: 110px;
# MAGIC   height: 110px;
# MAGIC   object-fit: contain;
# MAGIC }
# MAGIC .cit-body h3{font-size:18px;line-height:1.25;margin:0;font-weight:700;color:#0B2026;}
# MAGIC .cit-body p{font-size:16px;line-height:1.5;margin:0;color:#0B2026;text-align:left;}
# MAGIC @media screen and (max-width:900px){.cit-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="cit-grid">
# MAGIC <div class="cit-card"><div class="cit-bar"></div>
# MAGIC <div class="cit-body">
# MAGIC <div class="ctco-heading-icon-box">
# MAGIC <img
# MAGIC src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/run_if_conditional_task_icon.png" alt="Run-if conditional task dependencies icon">
# MAGIC </div>
# MAGIC <br>
# MAGIC <h3>Run-if Conditional Task Dependencies</h3></div></div>
# MAGIC <div class="cit-card"><div class="cit-bar"></div><div class="cit-body"><div class="ctco-heading-icon-box">
# MAGIC <img
# MAGIC src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/if_else_task_icon.png" alt="Run-if conditional task dependencies icon">
# MAGIC </div>
# MAGIC <br>
# MAGIC <h3>If/Else Tasks</h3></div></div>
# MAGIC <div class="cit-card"><div class="cit-bar"></div><div class="cit-body">
# MAGIC <div class="ctco-heading-icon-box">
# MAGIC <img
# MAGIC src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/for_each_task_icon.png" alt="Run-if conditional task dependencies icon">
# MAGIC </div>
# MAGIC <br>
# MAGIC <h3>For Each Tasks</h3></div></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Three advanced task types enable sophisticated workflow patterns:
# MAGIC - Run-if Conditional Task Dependencies: Control task execution based on the outcomes of upstream tasks, enabling workflows that can handle partial failures and complex dependency scenarios.
# MAGIC - If/Else Tasks: Implement boolean conditional logic directly in your workflow, allowing branches based on data conditions, processing results, or business rules.
# MAGIC - For Each Tasks: Enable iterative processing patterns where the same logic is applied to multiple data partitions or parameters, with configurable parallelism for performance optimization.
# MAGIC
# MAGIC These task types can be combined to create sophisticated workflows that handle complex business logic while maintaining clarity and maintainability.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Run if Conditional Task Dependencies
# MAGIC
# MAGIC Run-if conditional dependencies provide fine-grained control over task execution based on the outcomes of upstream tasks. You can define specific conditions that must be met for a task to run.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Available Dependency Conditions
# MAGIC
# MAGIC <div class="cit-runif">
# MAGIC <style>
# MAGIC .cit-runif{width:1200px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cit-runif *{box-sizing:border-box;}
# MAGIC .cit-runif-grid{display:grid;grid-template-columns:.72fr 1.28fr;gap:24px;align-items:stretch;}
# MAGIC .cit-runif-text{background:#F9F7F4;border:1px solid #DCE0E2;border-radius:14px;padding:22px;box-shadow:0 3px 12px rgba(27,49,57,.06);font-size:16px;line-height:1.55;color:#0B2026;}
# MAGIC .cit-runif-text h3{font-size:18px;line-height:1.25;margin:0 0 14px 0;color:#0B2026;}
# MAGIC .cit-runif-text p,.cit-runif-text li{font-size:16px;line-height:1.55;color:#0B2026;}
# MAGIC .cit-runif-text ul{margin:8px 0 16px 0;padding-left:22px;}
# MAGIC .cit-runif-highlight{border-left:5px solid #FF5F46;background:#FFF1EE;border-radius:10px;padding:12px 14px;margin-top:14px;font-size:16px;line-height:1.45;}
# MAGIC .cit-dag{position:relative;background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);min-height:470px;overflow:hidden;padding:18px;}
# MAGIC .cit-dag-title{position:absolute;left:22px;top:18px;font-size:18px;font-weight:800;color:#0B2026;opacity:0;animation:citStaticShow 25s ease-in-out infinite both;}
# MAGIC .cit-task{position:absolute;min-width:92px;padding:13px 18px;border-radius:10px;background:#00A972;color:#fff;font-size:16px;font-weight:800;text-align:center;box-shadow:0 2px 8px rgba(27,49,57,.12);z-index:3;opacity:0;transform:translateY(10px);animation-duration:25s;animation-timing-function:ease-in-out;animation-iteration-count:infinite;animation-fill-mode:both;}
# MAGIC .cit-task.t3{background:#98102A;}
# MAGIC .cit-task.t1{left:70px;top:210px;animation-name:citShow1;}
# MAGIC .cit-task.t2{left:300px;top:130px;animation-name:citShowMid;}
# MAGIC .cit-task.t3{left:300px;top:300px;animation-name:citShowMid;}
# MAGIC .cit-label{position:absolute;font-size:14px;line-height:1.25;font-weight:700;color:#0B2026;text-align:center;opacity:0;transform:translateY(10px);animation-duration:25s;animation-timing-function:ease-in-out;animation-iteration-count:infinite;animation-fill-mode:both;}
# MAGIC .cit-label.l2{left:278px;top:96px;animation-name:citShowMid;}
# MAGIC .cit-label.l3{left:278px;top:362px;animation-name:citShowMid;}
# MAGIC .cit-arrow{position:absolute;height:4px;background:#FF5F46;border-radius:4px;transform-origin:left center;z-index:2;opacity:0;transform:rotate(var(--rot)) scaleX(0);animation-duration:25s;animation-timing-function:ease-in-out;animation-iteration-count:infinite;animation-fill-mode:both;}
# MAGIC .cit-arrow:after{content:"";position:absolute;right:-2px;top:50%;transform:translateY(-50%);width:0;height:0;border-left:12px solid #FF5F46;border-top:7px solid transparent;border-bottom:7px solid transparent;}
# MAGIC .cit-a1{left:165px;top:222px;width:120px;--rot:-30deg;animation-name:citArrowFirst;}
# MAGIC .cit-a2{left:165px;top:252px;width:120px;--rot:30deg;animation-name:citArrowFirst;}
# MAGIC .cit-a3{left:395px;top:168px;width:128px;--rot:15deg;animation-name:citArrowSecond;}
# MAGIC .cit-a4{left:395px;top:322px;width:128px;--rot:-30deg;animation-name:citArrowSecond;}
# MAGIC .cit-task4-card{position:absolute;left:520px;top:132px;width:240px;min-height:210px;background:#F9F7F4;border:2px solid #0B2026;border-radius:2px;padding:18px 20px;z-index:4;opacity:0;transform:translateY(10px);animation:citShowFinal 25s ease-in-out infinite both;}
# MAGIC .cit-task4-pill{background:#00A972;color:#fff;border:2px solid #0B2026;border-radius:10px;padding:14px 18px;font-size:20px;font-weight:800;line-height:1.2;text-align:center;margin:0 auto 12px auto;width:150px;}
# MAGIC .cit-task4-dep{font-size:18px;line-height:1.25;color:#0B2026;font-weight:800;margin-bottom:6px;}
# MAGIC .cit-task4-list{margin:0 0 12px 0;padding-left:22px;font-size:18px;line-height:1.3;color:#0B2026;}
# MAGIC .cit-task4-condition{font-size:20px;line-height:1.25;color:#174D5C;font-weight:800;text-decoration:underline;text-underline-offset:4px;}
# MAGIC .cit-note-arrow{position:absolute;left:640px;top:342px;width:4px;height:48px;background:#174D5C;border-radius:4px;z-index:2;opacity:0;animation:citShowFinal 25s ease-in-out infinite both;}
# MAGIC .cit-note-arrow:after{content:"";position:absolute;left:50%;bottom:-2px;transform:translateX(-50%);width:0;height:0;border-top:14px solid #174D5C;border-left:8px solid transparent;border-right:8px solid transparent;}
# MAGIC .cit-final-note{position:absolute;left:285px;bottom:26px;width:455px;background:#F9F7F4;border:1.5px solid #0B2026;border-radius:2px;padding:12px 16px;text-align:center;font-size:16px;line-height:1.3;font-weight:800;color:#174D5C;opacity:0;transform:translateY(10px);animation:citShowFinal 25s ease-in-out infinite both;}
# MAGIC @keyframes citStaticShow{0%,4%{opacity:0;}8%,88%{opacity:1;}92%,100%{opacity:0;}}
# MAGIC @keyframes citShow1{0%,4%{opacity:0;transform:translateY(10px);}10%,88%{opacity:1;transform:translateY(0);}92%,100%{opacity:0;transform:translateY(10px);}}
# MAGIC @keyframes citArrowFirst{0%,10%{opacity:0;transform:rotate(var(--rot)) scaleX(0);}20%,88%{opacity:1;transform:rotate(var(--rot)) scaleX(1);}92%,100%{opacity:0;transform:rotate(var(--rot)) scaleX(0);}}
# MAGIC @keyframes citShowMid{0%,20%{opacity:0;transform:translateY(10px);}30%,88%{opacity:1;transform:translateY(0);}92%,100%{opacity:0;transform:translateY(10px);}}
# MAGIC @keyframes citArrowSecond{0%,30%{opacity:0;transform:rotate(var(--rot)) scaleX(0);}42%,88%{opacity:1;transform:rotate(var(--rot)) scaleX(1);}92%,100%{opacity:0;transform:rotate(var(--rot)) scaleX(0);}}
# MAGIC @keyframes citShowFinal{0%,42%{opacity:0;transform:translateY(10px);}48%,88%{opacity:1;transform:translateY(0);}92%,100%{opacity:0;transform:translateY(10px);}}
# MAGIC @media screen and (max-width:900px){.cit-runif-grid{grid-template-columns:1fr;}.cit-dag{overflow-x:auto;min-width:780px;}}
# MAGIC </style>
# MAGIC <div class="cit-runif-grid">
# MAGIC <div class="cit-runif-text">Control task execution based on outcomes of upstream tasks<br><br>
# MAGIC Supports a variety of dependency conditions like:
# MAGIC <ul><li>All succeeded</li><li>At least one succeeded</li><li>None failed</li><li>And more</li></ul>
# MAGIC <div class="cit-runif-highlight"><strong>Example - At Least One Succeeded</strong><br>In this scenario, even if Task-3 Fails, Task-4 still runs.</div></div>
# MAGIC <div class="cit-dag">
# MAGIC <div class="cit-dag-title">Run if Conditional Task Dependencies</div>
# MAGIC <div class="cit-task t1">Task-1</div>
# MAGIC <div class="cit-arrow cit-a1"></div>
# MAGIC <div class="cit-arrow cit-a2"></div>
# MAGIC <div class="cit-label l2">Depends On: Task-1</div>
# MAGIC <div class="cit-task t2">Task-2</div>
# MAGIC <div class="cit-task t3">Task-3</div>
# MAGIC <div class="cit-label l3">Depends On: Task-1</div>
# MAGIC <div class="cit-arrow cit-a3"></div>
# MAGIC <div class="cit-arrow cit-a4"></div>
# MAGIC <div class="cit-task4-card">
# MAGIC <div class="cit-task4-pill">Task-4</div>
# MAGIC <div class="cit-task4-dep">Depends On:</div>
# MAGIC <ul class="cit-task4-list"><li>Task-2</li><li>Task-3</li></ul>
# MAGIC <div class="cit-task4-condition">At least one succeeded</div>
# MAGIC </div>
# MAGIC <div class="cit-note-arrow"></div>
# MAGIC <div class="cit-final-note">In this scenario, even if Task-3 Fails, Task-4 still runs</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Run-if conditional dependencies provide fine-grained control over task execution based on the outcomes of upstream tasks. You can define specific conditions that must be met for a task to run.</p>
# MAGIC <p><strong>Available Dependency Conditions</strong></p>
# MAGIC <ul><li><strong>All succeeded:</strong> This traditional dependency requires all upstream tasks to complete successfully before the next task can run.</li><li><strong>At least one succeeded:</strong> This condition is useful when you have redundant data sources or processing paths, allowing the workflow to proceed if even one of the required upstream tasks is successful.</li><li><strong>None failed:</strong> This allows a task to execute even if some upstream tasks were skipped, as long as no upstream tasks have explicitly failed.</li><li><strong>Custom combinations:</strong> This option enables the implementation of complex business logic that requires specific combinations of task outcomes.</li></ul>
# MAGIC <p><strong>Benefits of Conditional Dependencies</strong></p>
# MAGIC <p>These dependencies enhance workflow resilience and enable sophisticated business logic. For instance, if Task-4</code> is set to run when "at least one" of its predecessors (Task-2</code> or Task-3</code>) succeeds, it can still execute if Task-3</code> fails while Task-2</code> completes successfully. This prevents cascade failures and allows workflows to continue processing even when some components fail. It also helps mirror real-world business processes where multiple paths to success exist and partial failures do not halt the entire operation.</p>
# MAGIC <p><strong>How to Configure</strong></p>
# MAGIC <p>You can select these dependency conditions for a task within its configuration settings, under the “run-if dependencies” section. Which we are going to see next.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Understanding Visual Representation of Conditional Dependencies
# MAGIC
# MAGIC The visual representation of conditional dependencies in the job DAG provides immediate understanding of workflow logic.
# MAGIC
# MAGIC <div class="cit-dag-visual">
# MAGIC <style>
# MAGIC .cit-dag-visual{width:1200px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cit-dag-visual *{box-sizing:border-box;}
# MAGIC .cit-note-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:18px;}
# MAGIC .cit-note-card{background:#F9F7F4;border:1px solid #DCE0E2;border-left:5px solid #FF5F46;border-radius:12px;padding:16px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .cit-note-card:nth-child(2){border-left-color:#2272B4;}
# MAGIC .cit-note-card:nth-child(3){border-left-color:#00A972;}
# MAGIC .cit-note-card h3{font-size:18px;line-height:1.25;margin:0 0 8px 0;color:#0B2026;}
# MAGIC .cit-note-card p{font-size:16px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC .cit-slide-img{background:#fff;border:1px solid #DCE0E2;border-radius:14px;padding:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);}
# MAGIC .cit-slide-img img{width:100%;height:auto;display:block;border-radius:8px;}
# MAGIC @media screen and (max-width:900px){.cit-note-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="cit-note-grid"><div class="cit-note-card"><h3>Dependency Visualization</h3><p>Different line styles and colors indicate different dependency types, making complex logic easy to understand at a glance.</p></div><div class="cit-note-card"><h3>Troubleshooting Benefits</h3><p>When failures occur, the visual representation immediately shows which tasks were affected and which could continue.</p></div><div class="cit-note-card"><h3>Team Communication</h3><p>Visual workflows serve as living documentation that both technical and business stakeholders can understand.</p></div></div>
# MAGIC <div class="cit-slide-img"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_conditional_iterative_tasks/run_if_dag_visualization.png" alt="Run-if conditional dependencies visualized in a job DAG"></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>The visual representation of conditional dependencies in the job DAG provides immediate understanding of workflow logic:</p>
# MAGIC <ul><li><strong>Dependency Visualization:</strong> Different line styles and colors indicate different dependency types, making complex logic easy to understand at a glance.</li><li><strong>Troubleshooting Benefits:</strong> When failures occur, the visual representation immediately shows which tasks were affected and which could continue, accelerating root cause analysis.</li><li><strong>Team Communication:</strong> Visual workflows serve as living documentation that both technical and business stakeholders can understand, improving collaboration and change management.</li></ul>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. Execution Flow Diagram
# MAGIC
# MAGIC The execution flow diagram illustrates how conditional dependencies work in practice.
# MAGIC
# MAGIC <div class="cit-flow-steps">
# MAGIC <style>
# MAGIC .cit-flow-steps{width:1200px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cit-flow-steps *{box-sizing:border-box;}
# MAGIC .cit-flow-steps input[type="radio"]{display:none;}
# MAGIC .cit-step-nav{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px;}
# MAGIC .cit-step-nav label{background:#fff;border:1px solid #DCE0E2;border-left:6px solid transparent;border-radius:12px;padding:14px 16px;font-size:16px;line-height:1.35;font-weight:700;cursor:pointer;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC #cit-step-1:checked ~ .cit-step-nav label[for="cit-step-1"],#cit-step-2:checked ~ .cit-step-nav label[for="cit-step-2"],#cit-step-3:checked ~ .cit-step-nav label[for="cit-step-3"]{border-left-color:#FF5F46;background:#FFF1EE;}
# MAGIC .cit-step-panel{display:none;background:#F9F7F4;border:1px solid #DCE0E2;border-radius:14px;padding:18px;box-shadow:0 3px 12px rgba(27,49,57,.07);}
# MAGIC #cit-step-1:checked ~ .cit-panels .p1,#cit-step-2:checked ~ .cit-panels .p2,#cit-step-3:checked ~ .cit-panels .p3{display:grid;grid-template-columns:1.18fr .82fr;gap:18px;align-items:stretch;}
# MAGIC .cit-panel-img{background:#fff;border:1px solid #DCE0E2;border-radius:12px;padding:12px;display:flex;align-items:center;justify-content:center;}
# MAGIC .cit-panel-img img{width:100%;max-height:430px;object-fit:contain;border-radius:8px;}
# MAGIC .cit-panel-text{background:#fff;border-left:5px solid #FF5F46;border-radius:12px;padding:18px;}
# MAGIC .cit-panel-text h3{font-size:18px;line-height:1.25;margin:0 0 12px 0;color:#0B2026;}
# MAGIC .cit-panel-text p{font-size:16px;line-height:1.55;margin:0;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.cit-step-nav{grid-template-columns:1fr;}#cit-step-1:checked ~ .cit-panels .p1,#cit-step-2:checked ~ .cit-panels .p2,#cit-step-3:checked ~ .cit-panels .p3{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <input type="radio" id="cit-step-1" name="cit-step-select" checked>
# MAGIC <input type="radio" id="cit-step-2" name="cit-step-select">
# MAGIC <input type="radio" id="cit-step-3" name="cit-step-select">
# MAGIC <div class="cit-step-nav"><label for="cit-step-1">1. Job Initialization<br><span style="font-weight:500;">The first three tasks are executed</span></label><label for="cit-step-2">2. Waiting for all task to complete<br><span style="font-weight:500;">(All succeeded)</span></label><label for="cit-step-3">3. Dependent Task Started</label></div>
# MAGIC <div class="cit-panels"><div class="cit-step-panel p1"><div class="cit-panel-img"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_conditional_iterative_tasks/run_if_step_job_initialization.png" alt="Job initialization with the first three tasks running"></div><div class="cit-panel-text"><h3>Job Initialization</h3><p>The system evaluates all task dependencies and determines the initial execution set. <br><br>Please note that all tasks are running concurrently.</p></div></div><div class="cit-step-panel p2"><div class="cit-panel-img"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_conditional_iterative_tasks/run_if_step_waiting_all_succeeded.png" alt="Waiting for all upstream tasks to complete successfully"></div><div class="cit-panel-text"><h3>Condition Evaluation</h3><p>As tasks complete, the system continuously re-evaluates which downstream tasks can execute based on their dependency conditions. <br><br>Please note that 'Ingest_Source_2' and 'Ingest_Source_3' have completed and are waiting for "Ingest_Source_1" to complete before proceeding to the 'All_Data_Ingested' task.</p></div></div><div class="cit-step-panel p3"><div class="cit-panel-img"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_conditional_iterative_tasks/run_if_step_dependent_started.png" alt="Dependent task started after all upstream tasks complete"></div><div class="cit-panel-text"><h3>Dynamic Execution</h3><p>This creates truly dynamic workflows where the execution path isn't predetermined but adapts based on actual processing results. <br><br>Please note that once all three tasks are completed, the final task has started running.</p></div></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>The execution flow diagram illustrates how conditional dependencies work in practice:</p>
# MAGIC <ul><li><strong>Job Initialization:</strong> The system evaluates all task dependencies and determines the initial execution set. Please note that all tasks are running concurrently.</li><li><strong>Condition Evaluation:</strong> As tasks complete (successfully or with failures), the system continuously re-evaluates which downstream tasks can execute based on their dependency conditions. Please note that 'Ingest_Source_2' and 'Ingest_Source_3' have completed and are waiting for "Ingest_Source_1" to complete before proceeding to the 'All_Data_Ingested' task.</li><li><strong>Dynamic Execution:</strong> This creates truly dynamic workflows where the execution path isn't predetermined but adapts based on actual processing results. Please note that once all three tasks are completed, the final task has started running.</li></ul>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. If Else Task
# MAGIC
# MAGIC If/Else tasks enable direct implementation of business logic within your workflows, moving beyond simple success/failure conditions to data-driven decision making.
# MAGIC
# MAGIC ##### Click the highlighted boxes to reveal task logic.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div class="cit-ifelse-click">
# MAGIC <style>
# MAGIC .cit-ifelse-click{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cit-ifelse-click *{box-sizing:border-box;}
# MAGIC .cit-ifelse-click input[type="radio"]{display:none;}
# MAGIC .cit-if-wrap{background:transparent;border:none;border-radius:0;padding:0;box-shadow:none;}
# MAGIC .cit-if-image{position:relative;background:transparent;border:1px solid #DCE0E2;border-radius:12px;padding:0;display:flex;align-items:center;justify-content:center;}
# MAGIC .cit-if-image img{width:100%;max-height:460px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC .cit-hotspot{position:absolute;border:4px solid #FF5F46;border-radius:10px;background:rgba(255,95,70,.08);cursor:pointer;transition:all .2s ease;box-shadow:none;}
# MAGIC .cit-hotspot:hover{background:rgba(255,95,70,.16);}
# MAGIC .cit-hotspot-1{left:42%;top:52%;width:17%;height:33%;}
# MAGIC .cit-hotspot-2{left:82%;top:52%;width:17%;height:33%;}
# MAGIC #cit-if-box1:checked ~ .cit-if-wrap .cit-hotspot-1,#cit-if-box2:checked ~ .cit-if-wrap .cit-hotspot-2{border-color:#98102A;background:rgba(152,16,42,.10);animation:citBlink 1.2s ease-in-out infinite;}
# MAGIC .cit-text-row{margin-top:16px;background:transparent;border:none;border-radius:0;padding:0;min-height:150px;}
# MAGIC .cit-text-panel{display:none;background:#F9F7F4;border:1px solid #DCE0E2;border-left:5px solid #FF5F46;border-radius:12px;padding:18px 20px;}
# MAGIC #cit-if-box1:checked ~ .cit-if-wrap .cit-panel-1,#cit-if-box2:checked ~ .cit-if-wrap .cit-panel-2{display:block;}
# MAGIC .cit-text-panel h3{font-size:18px;line-height:1.25;margin:0 0 10px 0;color:#0B2026;font-weight:800;}
# MAGIC .cit-text-panel p,.cit-text-panel li{font-size:16px;line-height:1.5;color:#0B2026;}
# MAGIC .cit-text-panel p{margin:0 0 10px 0;}
# MAGIC .cit-text-panel ul{margin:0;padding-left:22px;}
# MAGIC .cit-text-panel li{margin-bottom:7px;}
# MAGIC .cit-note{margin-top:12px;border-left:5px solid #98102A;background:#FFF1EE;border-radius:10px;padding:10px 12px;font-size:16px;line-height:1.45;color:#0B2026;}
# MAGIC @keyframes citBlink{0%,100%{box-shadow:0 0 0 0 rgba(255,95,70,.35);}50%{box-shadow:0 0 0 7px rgba(255,95,70,.18);}}
# MAGIC @media screen and (max-width:900px){.cit-if-image{overflow-x:auto;justify-content:flex-start;}.cit-if-image img{min-width:760px;}.cit-hotspot{min-width:120px;}}
# MAGIC </style>
# MAGIC <input type="radio" id="cit-if-box1" name="cit-ifelse-select" checked>
# MAGIC <input type="radio" id="cit-if-box2" name="cit-ifelse-select">
# MAGIC <div class="cit-if-wrap">
# MAGIC <div class="cit-if-image">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_conditional_iterative_tasks/if_else_conditional_task_ui.png" alt="If else conditional task configuration UI">
# MAGIC <label class="cit-hotspot cit-hotspot-1" for="cit-if-box1" aria-label="Show If Else task details"></label>
# MAGIC <label class="cit-hotspot cit-hotspot-2" for="cit-if-box2" aria-label="Show condition behavior details"></label>
# MAGIC </div>
# MAGIC <div class="cit-text-row">
# MAGIC <div class="cit-text-panel cit-panel-1">
# MAGIC <h3>If/Else Conditional Tasks</h3>
# MAGIC <ul>
# MAGIC <li>Adds boolean conditional logic to your workflow based on task results</li>
# MAGIC <li>Allows branching based on specific conditions, such as data quality checks and record counts</li>
# MAGIC <li>Uses boolean operators: <strong>==</strong>, <strong>!=</strong>, <strong>&gt;</strong>, <strong>&gt;=</strong>, <strong>&lt;</strong>, <strong>&lt;=</strong></li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC <div class="cit-text-panel cit-panel-2">
# MAGIC <h3>Condition behavior</h3>
# MAGIC <p>If none of dependency failed and at least one task executed.</p>
# MAGIC <div class="cit-note">The condition ensures that conditional evaluation only occurs when meaningful upstream results are available.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>If/else conditional tasks add sophisticated boolean logic to workflows:</p>
# MAGIC <ul><li><strong>Condition Evaluation:</strong> Boolean operators (==, !=, &gt;, &gt;=, &lt;, &lt;=) evaluate expressions against task results, parameter values, or computed metrics.</li><li><strong>Business Logic Examples:</strong><ul><li>Data Quality Gates: Branch based on record counts, null percentages, or validation results</li><li>Processing Volume Decisions: Use different processing strategies for large vs. small datasets</li><li>Environment-Specific Logic: Execute different tasks based on environment parameters</li><li>Business Rule Implementation: Implement complex business rules directly in workflow logic</li></ul></li><li><strong>Execution Requirements:</strong> The condition "If none of dependency failed and at least one task executed" ensures that conditional evaluation only occurs when meaningful upstream results are available.</li><li><strong>True/False Branches:</strong> Each branch can contain multiple tasks, enabling complex processing paths based on conditional outcomes.</li></ul>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. For Each Task
# MAGIC
# MAGIC For Each tasks enable powerful iterative processing patterns that maintain the benefits of visual workflow management while handling repetitive operations efficiently.
# MAGIC
# MAGIC <div class="cit-foreach">
# MAGIC <style>
# MAGIC .cit-foreach{width:1200px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cit-foreach *{box-sizing:border-box;}
# MAGIC .cit-foreach-demo{position:relative;background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;padding:14px;}
# MAGIC .cit-foreach-frame{position:relative;width:100%;background:#ffffff;overflow:hidden;border-radius:10px;}
# MAGIC .cit-foreach-frame img{display:block;width:100%;height:auto;background:transparent;border-radius:10px;}
# MAGIC .cit-fe-cards{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-top:18px;}
# MAGIC .cit-fe-card{background:#F9F7F4;border:1px solid #DCE0E2;border-left:5px solid #FF5F46;border-radius:12px;padding:16px;box-shadow:0 2px 8px rgba(27,49,57,.06);font-size:16px;line-height:1.5;color:#0B2026;}
# MAGIC .cit-fe-card:nth-child(2){border-left-color:#00A972;}
# MAGIC .cit-fe-card:nth-child(3){border-left-color:#2272B4;}
# MAGIC @media screen and (max-width:900px){.cit-foreach-demo{overflow-x:auto;}.cit-foreach-frame{min-width:760px;}.cit-fe-cards{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="cit-foreach-demo">
# MAGIC <div class="cit-foreach-frame">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_conditional_iterative_tasks/for_each_task_loop_diagram.png" alt="For Each task input array looping over nested tasks and downstream task">
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="cit-fe-cards">
# MAGIC <div class="cit-fe-card">For Each loops over an input array and runs the same nested task per item, passing it as <strong>{{input}}</strong>.</div>
# MAGIC <div class="cit-fe-card">Iterations can run in parallel with configurable concurrency to speed up execution.</div>
# MAGIC <div class="cit-fe-card">Downstream dependencies attach to the For Each container, not to the nested task.</div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>For Each tasks provide sophisticated iteration capabilities:</p>
# MAGIC <ul><li><strong>Input Processing:</strong> The task loops over an input array, passing each item as {{input}}</code> to the nested task. This creates clean, parameterized processing where the same logic handles different data partitions.</li><li><strong>Parallel Execution:</strong> Configurable concurrency allows multiple iterations to run simultaneously, dramatically improving performance for independent processing tasks.</li><li><strong>Dependency Management:</strong> Downstream tasks depend on the completion of the entire For Each container, not individual iterations. This simplifies dependency management while ensuring all iterations complete before downstream processing begins.</li></ul>
# MAGIC <p><strong>Use Case Examples:</strong></p>
# MAGIC <ul><li>Geographic Processing: Process data for each state/region in parallel</li><li>Time Period Processing: Handle different date ranges with the same logic</li><li>Customer Segment Processing: Apply the same analysis to different customer segments</li><li>File Processing: Process multiple files with identical logic</li></ul>
# MAGIC <p>The container concept is crucial for understanding For Each task behavior:</p>
# MAGIC <ul><li><strong>Container Management:</strong> The For Each task acts as a single logical unit in your workflow, even though it executes multiple iterations internally.</li><li><strong>Dependency Simplification:</strong> Downstream tasks only need to depend on the For Each container, not each individual iteration, keeping workflow diagrams clean and manageable.</li><li><strong>Resource Management:</strong> The container manages resource allocation across iterations, optimizing cluster utilization and preventing resource conflicts.</li></ul>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. For Each Task
# MAGIC
# MAGIC Implementing For Each tasks requires understanding two distinct components:
# MAGIC
# MAGIC
# MAGIC <div class="cit-carousel">
# MAGIC <style>
# MAGIC .cit-carousel{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cit-carousel *{box-sizing:border-box;}
# MAGIC .cit-carousel input[type="radio"]{display:none;}
# MAGIC .cit-car-nav{display:flex;justify-content:left;gap:12px;border-bottom:2px solid #EEEDE9;margin-bottom:16px;}
# MAGIC .cit-car-nav label{padding:12px 22px;font-size:16px;font-weight:700;color:#5A6F77;border-bottom:4px solid transparent;cursor:pointer;}
# MAGIC #cit-car-1:checked ~ .cit-car-nav label[for="cit-car-1"],#cit-car-2:checked ~ .cit-car-nav label[for="cit-car-2"]{color:#FF5F46;border-bottom-color:#FF5F46;}
# MAGIC .cit-car-panel{display:none;background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC #cit-car-1:checked ~ .cit-car-stage .cit-p1,#cit-car-2:checked ~ .cit-car-stage .cit-p2{display:grid;grid-template-columns:.72fr 1.28fr;gap:22px;align-items:stretch;padding:22px;}
# MAGIC .cit-car-text{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:22px;display:flex;flex-direction:column;justify-content:center;}
# MAGIC .cit-car-text.green{border-left-color:#00A972;}
# MAGIC .cit-car-text h3{font-size:18px;line-height:1.25;margin:0 0 12px 0;color:#0B2026;}
# MAGIC .cit-car-text p{font-size:16px;line-height:1.55;margin:0;color:#0B2026;}
# MAGIC .cit-car-img{background:#fff;border:1px solid #DCE0E2;border-radius:12px;padding:14px;display:flex;align-items:center;justify-content:center;min-height:410px;}
# MAGIC .cit-car-img img{width:100%;max-height:390px;object-fit:contain;border-radius:8px;}
# MAGIC @media screen and (max-width:900px){#cit-car-1:checked ~ .cit-car-stage .cit-p1,#cit-car-2:checked ~ .cit-car-stage .cit-p2{grid-template-columns:1fr;}.cit-car-nav{flex-wrap:wrap;}}
# MAGIC </style>
# MAGIC <input type="radio" id="cit-car-1" name="cit-for-each-carousel" checked>
# MAGIC <input type="radio" id="cit-car-2" name="cit-for-each-carousel">
# MAGIC <div class="cit-car-nav"><label for="cit-car-1">1. For Each Task</label><label for="cit-car-2">2. Nested Task</label></div>
# MAGIC <div class="cit-car-stage"><div class="cit-car-panel cit-p1"><div class="cit-car-text"><h3>The For Each task</h3><p>The top-level container task that manages the loop.</p></div><div class="cit-car-img"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_conditional_iterative_tasks/foreach_task_ui.png" alt="For Each task configuration UI"></div></div><div class="cit-car-panel cit-p2"><div class="cit-car-text green"><h3>A nested task</h3><p>The actual task that executes for each iteration.</p></div><div class="cit-car-img"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_conditional_iterative_tasks/nested_task_ui.png" alt="Nested task configuration UI"></div></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Implementing For Each tasks requires understanding two distinct components:</p>
# MAGIC <ul><li><strong>The For Each Container:</strong> This top-level task manages the iteration logic, input array processing, concurrency settings, and resource allocation. It defines how many iterations run in parallel and how the input array is processed.</li><li><strong>The Nested Task:</strong> This is the actual work that gets performed for each iteration. It can be any task type - notebook, SQL, Python script, etc. The nested task receives each array item as <code>{{input}}</code> and processes it according to your business logic.</li><li><strong>Configuration Flexibility:</strong> This separation allows you to configure iteration behavior independently from the processing logic, making For Each tasks both powerful and maintainable.</li></ul>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Conclusion
# MAGIC
# MAGIC <p style="margin:0 0 12px 0;">In this lecture, you explored advanced Lakeflow Jobs task patterns that make workflows dynamic and adaptable.</p>
# MAGIC <ul style="margin:0;padding-left:22px;">
# MAGIC <li><strong>Run-if Conditional Task Dependencies</strong> control task execution based on upstream task outcomes.</li>
# MAGIC <li><strong>If/Else Tasks</strong> add boolean conditional logic to workflow branching.</li>
# MAGIC <li><strong>For Each Tasks</strong> support iterative processing over input arrays using nested tasks.</li>
# MAGIC </ul>
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next demonstration, you'll build a dynamic workflow that showcases all the advanced task types working together. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>