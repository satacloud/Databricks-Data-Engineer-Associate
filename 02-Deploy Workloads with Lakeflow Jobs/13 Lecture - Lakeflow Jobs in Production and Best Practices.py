# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Lakeflow Jobs in Production and Best Practices
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how to run Lakeflow Jobs in production using appropriate compute, pricing, modular design, Git integration, and operational best practices.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC 1. Select appropriate compute (serverless vs. classic) and understand the Jobs pricing structure
# MAGIC 2. Apply modular orchestration design patterns using the Run Job task
# MAGIC 3. Configure Git integration for version-controlled job definitions
# MAGIC 4. Apply production best practices (service principals, parameterized tasks, alerting, maintainable design)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Common Best Practices
# MAGIC
# MAGIC Moving from development to production requires understanding how to design, deploy, and operate Lakeflow Jobs at enterprise scale with appropriate governance, security, and operational practices.
# MAGIC
# MAGIC
# MAGIC <div class="lfjp-practices">
# MAGIC <style>
# MAGIC .lfjp-practices{width:1200px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .lfjp-practices *{box-sizing:border-box;}
# MAGIC .lfjp-practice-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;}
# MAGIC .lfjp-practice-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:22px;text-align:center;min-height:250px;display:flex;flex-direction:column;align-items:center;justify-content:flex-start;}
# MAGIC .lfjp-practice-card.orange{border-top:8px solid #FF5F46;}
# MAGIC .lfjp-practice-card.green{border-top:8px solid #00A972;}
# MAGIC .lfjp-practice-card.maroon{border-top:8px solid #98102A;}
# MAGIC .lfjp-practice-card img{width:88px;height:88px;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;margin-bottom:16px;}
# MAGIC .lfjp-practice-card h3{font-size:18px;line-height:1.25;font-weight:700;margin:0 0 12px 0;color:#0B2026;}
# MAGIC .lfjp-practice-card p{font-size:16px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.lfjp-practice-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="lfjp-practice-grid">
# MAGIC <div class="lfjp-practice-card orange"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/compute_strategy_icon.png" alt="Compute strategy icon"><h3>Compute</h3><p>Selecting the right compute options for performance, cost, and operational requirements.</p></div>
# MAGIC <div class="lfjp-practice-card green"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/modular_design_icon.png" alt="Modular design icon"><h3>Modular Design</h3><p>Implementing architecture patterns that support maintainability, reusability, and team collaboration.</p></div>
# MAGIC <div class="lfjp-practice-card maroon"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/git_collaboration_icon.png" alt="Git integration icon"><h3>Git</h3><p>Version control and deployment practices that ensure consistency and enable CI/CD workflows.</p></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC Production deployments require attention to four critical areas:
# MAGIC <ul><li><strong>Compute Strategy:</strong> Selecting the right compute options for performance, cost, and operational requirements.</li><li><strong>Modular Design:</strong> Implementing architecture patterns that support maintainability, reusability, and team collaboration.</li><li><strong>Git Integration:</strong> Version control and deployment practices that ensure consistency and enable CI/CD workflows.</li><li><strong>Performance Monitoring:</strong> Proactive monitoring and optimization strategies that ensure SLA compliance and cost efficiency.</li></ul>
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Selecting Compute
# MAGIC
# MAGIC <div class="lfjp-compute">
# MAGIC <style>
# MAGIC .lfjp-compute{width:1220px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .lfjp-compute *{box-sizing:border-box;}
# MAGIC .lfjp-compute-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:18px;}
# MAGIC .lfjp-compute-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .lfjp-compute-bar{height:8px;background:#98102A;}
# MAGIC .lfjp-compute-bar.green{background:#00A972;}
# MAGIC .lfjp-compute-card-body{padding:20px;}
# MAGIC .lfjp-compute-head{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:12px;}
# MAGIC .lfjp-compute-head h3{font-size:18px;line-height:1.25;margin:0;font-weight:700;color:#0B2026;}
# MAGIC .lfjp-compute-head img{width:72px;height:72px;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC .lfjp-compute-card li,.lfjp-serverless li{font-size:16px;line-height:1.5;margin-bottom:8px;}
# MAGIC .lfjp-compute-card ul,.lfjp-serverless ul{margin:0;padding-left:22px;}
# MAGIC .lfjp-serverless{background:#F9F7F4;border:1px solid #DCE0E2;border-radius:14px;padding:20px;}
# MAGIC .lfjp-serverless-title{display:flex;align-items:center;gap:12px;margin-bottom:14px;}
# MAGIC .lfjp-serverless-pill{background:#00A972;color:#fff;border-radius:8px;padding:10px 14px;font-size:18px;line-height:1.2;font-weight:700;}
# MAGIC .lfjp-serverless-benefits{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;}
# MAGIC .lfjp-serverless-benefit{background:#fff;border:1px solid #DCE0E2;border-radius:12px;padding:14px;min-height:150px;}
# MAGIC .lfjp-serverless-benefit img{width:54px;height:54px;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;margin-bottom:10px;}
# MAGIC .lfjp-serverless-benefit strong{display:block;font-size:16px;line-height:1.25;margin-bottom:8px;}
# MAGIC .lfjp-serverless-benefit p{font-size:15px;line-height:1.42;margin:0;}
# MAGIC @media screen and (max-width:900px){.lfjp-compute-grid,.lfjp-serverless-benefits{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="lfjp-compute-grid">
# MAGIC <div class="lfjp-compute-card"><div class="lfjp-compute-bar"></div><div class="lfjp-compute-card-body"><div class="lfjp-compute-head"><h3>Interactive Clusters</h3><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/interactive_cluster_icon.png" alt="Interactive clusters icon"></div><ul><li>Best for performing ad-hoc analysis, data exploration, or development, but not production</li><li>Costly for Job runs</li><li>Limited Scalability</li><li>Availability could be an issue because of parallel usage</li></ul></div></div>
# MAGIC <div class="lfjp-compute-card"><div class="lfjp-compute-bar green"></div><div class="lfjp-compute-card-body"><div class="lfjp-compute-head"><h3>Job Clusters</h3><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/job_cluster_icon.png" alt="Job clusters icon"></div><ul><li>Cheaper as they terminate when the job ends, reducing resource usage and costs</li><li>Start-up Latency</li><li>Subject to cloud provider start-up time</li><li>Maintenance Burden</li></ul></div></div>
# MAGIC </div>
# MAGIC <div class="lfjp-serverless"><div class="lfjp-serverless-title"><div class="lfjp-serverless-pill">Serverless Compute</div></div><div class="lfjp-serverless-benefits"><div class="lfjp-serverless-benefit"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/simplicity_hand_icon.png" alt="Simplicity icon"><strong>Simplicity</strong><p>You no longer need to choose the VM type. Customers do not need dedicated skilled DevOps folks.</p></div><div class="lfjp-serverless-benefit"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/cost_value_icon.png" alt="High efficiency icon"><strong>High efficiency</strong><p>Photon turned on default.</p></div><div class="lfjp-serverless-benefit"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/faster_startup_icon.png" alt="Faster startup icon"><strong>Faster startup</strong><p>VMs run in Databricks account. We ensure enough nodes are available based on historical data and ML.</p></div><div class="lfjp-serverless-benefit"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/reliability_shield_icon.png" alt="Reliability icon"><strong>Reliability</strong><p>Shielded from cloud disruptions.</p></div></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC Understanding compute options is crucial for production success:
# MAGIC <ul><li><strong>Interactive Clusters</strong> are ideal for development and ad-hoc analysis but present significant challenges for production use:<ul><li><strong>Cost Issues:</strong> Interactive clusters remain running even when not executing jobs, leading to unnecessary costs</li><li><strong>Limited Scalability:</strong> Shared resources can create contention and performance unpredictability</li><li><strong>Availability Concerns:</strong> Multiple users sharing clusters can cause resource conflicts and job delays</li></ul></li><li><strong>Job Clusters</strong> offer better production characteristics:<ul><li><strong>Cost Efficiency:</strong> Clusters terminate when jobs complete, eliminating idle resource costs</li><li><strong>Dedicated Resources:</strong> Each job gets dedicated compute resources, ensuring predictable performance</li><li><strong>Startup Latency:</strong> Cloud provider startup times can delay job execution, requiring consideration in SLA planning</li><li><strong>Maintenance Overhead:</strong> Requires more configuration and management compared to serverless options</li></ul></li><li><strong>Serverless compute</strong> represents the optimal choice for most production workloads because it provides operational simplicity, performance optimization, reliability and speed, and cloud independence.</li></ul>
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Pricing Structure
# MAGIC
# MAGIC The traditional pricing model involves multiple cost components whereas serverless fundamentally simplifies the cost model.
# MAGIC
# MAGIC ##### Select each tab below to compare Classic and Serverless pricing.
# MAGIC
# MAGIC <div class="pricing-structure-carousel">
# MAGIC <style>
# MAGIC .pricing-structure-carousel{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#000;}
# MAGIC .pricing-structure-carousel *{box-sizing:border-box;}
# MAGIC .pricing-structure-carousel input[type="radio"]{display:none;}
# MAGIC .pricing-tabbar{display:flex;justify-content:flex-start;align-items:center;border-bottom:2px solid #EEEDE9;margin-bottom:18px;}
# MAGIC .pricing-tabbar label{padding:10px 18px;border:none;border-bottom:3px solid transparent;background:none;font-size:18px;font-weight:800;color:#888;cursor:pointer;margin-bottom:-2px;}
# MAGIC #pricing-classic-tab:checked ~ .pricing-tabbar label[for="pricing-classic-tab"]{color:#FF5F46;border-bottom-color:#FF5F46;}
# MAGIC #pricing-serverless-tab:checked ~ .pricing-tabbar label[for="pricing-serverless-tab"]{color:#00A972;border-bottom-color:#00A972;}
# MAGIC .pricing-panel{display:none;}
# MAGIC #pricing-classic-tab:checked ~ .pricing-panels .classic-panel{display:block;}
# MAGIC #pricing-serverless-tab:checked ~ .pricing-panels .serverless-panel{display:block;}
# MAGIC .pricing-canvas{position:relative;width:1100px;max-width:100%;height:430px;background:#ffffff;overflow:hidden;}
# MAGIC .pricing-title{position:absolute;text-align:center;font-size:28px;line-height:1.2;font-weight:800;color:#000;}
# MAGIC .pricing-stack{position:absolute;background:#ffffff;}
# MAGIC .pricing-stack-box{position:absolute;left:0;color:#ffffff;display:flex;align-items:center;justify-content:center;text-align:center;font-weight:400;}
# MAGIC .pricing-stack-box span{display:block;}
# MAGIC .pricing-stack-box small{display:block;font-size:20px;line-height:1.18;margin-top:4px;font-weight:400;}
# MAGIC .classic-title{left:0;top:0;width:300px;}
# MAGIC .classic-stack{left:0;top:52px;width:300px;height:360px;}
# MAGIC .classic-dbu{top:0;width:300px;height:70px;background:#00A972;font-size:28px;border-bottom:3px solid #ffffff;}
# MAGIC .classic-infra{top:70px;width:300px;height:184px;background:#668A95;font-size:26px;line-height:1.15;border-bottom:3px solid #ffffff;}
# MAGIC .classic-ops{top:254px;width:300px;height:106px;background:#F25F4C;font-size:26px;line-height:1.15;}
# MAGIC .classic-lines{position:absolute;left:0;top:0;width:1100px;height:430px;pointer-events:none;overflow:visible;}
# MAGIC .classic-text{position:absolute;left:480px;color:#000;font-size:20px;line-height:1.35;font-weight:400;white-space:nowrap;}
# MAGIC .classic-t1{top:98px;}
# MAGIC .classic-t2{top:151px;}
# MAGIC .classic-t3{top:204px;}
# MAGIC .classic-t4{top:332px;}
# MAGIC .classic-t5{top:385px;}
# MAGIC .serverless-title{left:0;top:0;width:300px;}
# MAGIC .serverless-stack{left:0;top:52px;width:300px;height:380px;}
# MAGIC .serverless-tco{position:absolute;left:0;top:0;width:300px;height:95px;background:#F7F6F3;border:1.5px dashed #FF5F46;border-bottom:0;color:#000;display:flex;align-items:center;justify-content:center;text-align:center;font-size:24px;font-weight:400;}
# MAGIC .serverless-dbu{position:absolute;left:0;top:95px;width:300px;height:285px;background:#00A972;color:#ffffff;display:flex;align-items:center;justify-content:center;text-align:center;font-size:28px;font-weight:400;line-height:1.2;padding:22px;}
# MAGIC .serverless-dbu span{display:block;font-size:20px;line-height:1.25;margin-top:6px;font-weight:400;}
# MAGIC .tco-arrow{position:absolute;right:48px;top:0;bottom:0;width:1.5px;background:#FF5F46;}
# MAGIC .tco-arrow:before{content:"";position:absolute;left:50%;top:-1px;transform:translateX(-50%);width:0;height:0;border-left:4px solid transparent;border-right:4px solid transparent;border-bottom:7px solid #FF5F46;}
# MAGIC .tco-arrow:after{content:"";position:absolute;left:50%;bottom:-1px;transform:translateX(-50%);width:0;height:0;border-left:4px solid transparent;border-right:4px solid transparent;border-top:7px solid #FF5F46;}
# MAGIC .value-title{position:absolute;left:335px;top:150px;font-size:28px;line-height:1.2;font-weight:800;color:#00A972;}
# MAGIC .value-text{position:absolute;left:335px;color:#000;font-size:20px;line-height:1.25;font-weight:400;max-width:720px;}
# MAGIC .value-1{top:208px;}
# MAGIC .value-2{top:286px;}
# MAGIC .value-3{top:365px;}
# MAGIC @media screen and (max-width:900px){.pricing-structure-carousel{overflow-x:auto;}.pricing-canvas{min-width:1100px;}.pricing-tabbar{flex-wrap:wrap;}}
# MAGIC </style>
# MAGIC
# MAGIC <input type="radio" id="pricing-classic-tab" name="pricing-carousel-tab" checked>
# MAGIC <input type="radio" id="pricing-serverless-tab" name="pricing-carousel-tab">
# MAGIC
# MAGIC <div class="pricing-tabbar">
# MAGIC <label for="pricing-classic-tab">Classic</label>
# MAGIC <label for="pricing-serverless-tab">Serverless</label>
# MAGIC </div>
# MAGIC <div class="pricing-panels">
# MAGIC <div class="pricing-panel classic-panel">
# MAGIC <div class="pricing-canvas">
# MAGIC <div class="pricing-title classic-title">Classic</div>
# MAGIC <div class="pricing-stack classic-stack">
# MAGIC <div class="pricing-stack-box classic-dbu"><span>DBUs<small>(paid to Databricks)</small></span></div>
# MAGIC <div class="pricing-stack-box classic-infra"><span>Infrastructure cost<small>(paid to cloud provider)</small></span></div>
# MAGIC <div class="pricing-stack-box classic-ops"><span>Operational cost<small>(incurred at organization level)</small></span></div>
# MAGIC </div>
# MAGIC <svg class="classic-lines" viewBox="0 0 1100 430" aria-hidden="true">
# MAGIC <path d="M300 170 H450" fill="none" stroke="#FF5F46" stroke-width="1.2"></path>
# MAGIC <path d="M475 80 C455 80 455 95 455 105 V155 C455 170 445 170 445 170 C455 170 455 170 455 185 V235 C455 245 455 258 475 258" fill="none" stroke="#FF5F46" stroke-width="1.2"></path>
# MAGIC <path d="M300 350 H450" fill="none" stroke="#FF5F46" stroke-width="1.2"></path>
# MAGIC <path d="M475 286 C455 286 455 301 455 311 V315 C455 350 445 350 445 350 C455 350 455 350 455 375 V389 C455 399 455 412 475 412" fill="none" stroke="#FF5F46" stroke-width="1.2"></path>
# MAGIC </svg>
# MAGIC <div class="classic-text classic-t1">Cost of VMs for clusters</div>
# MAGIC <div class="classic-text classic-t2">Cost of network (FW / NAT)</div>
# MAGIC <div class="classic-text classic-t3">Cost of security, utilization monitoring,</div>
# MAGIC <div class="classic-text classic-t4">Time spent on deploying, automating, maintaining infrastructure</div>
# MAGIC <div class="classic-text classic-t5">Time spent on managing cost, efficiency, utilization</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="pricing-panel serverless-panel">
# MAGIC <div class="pricing-canvas">
# MAGIC <div class="pricing-title serverless-title">Serverless</div>
# MAGIC <div class="pricing-stack serverless-stack">
# MAGIC <div class="serverless-tco">TCO savings<div class="tco-arrow"></div></div>
# MAGIC <div class="serverless-dbu"><div>DBUs<span>(Single bill that includes<br>infrastructure and operational<br>cost)</span></div></div>
# MAGIC </div>
# MAGIC <div class="value-title">Value</div>
# MAGIC <div class="value-text value-1">Fully managed service - operationally simpler, more<br>reliable</div>
# MAGIC <div class="value-text value-2">Fast clusters, auto-scaling - better user experience, lower<br>cost</div>
# MAGIC <div class="value-text value-3">Out of the box performance and optimizations - lower<br>overall TCO</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC The traditional pricing model involves multiple cost components:
# MAGIC <ul><li><strong>Direct Costs:</strong> DBUs paid to Databricks plus infrastructure costs paid directly to cloud providers (VMs, networking, security services).</li><li><strong>Operational Overhead:</strong> Often overlooked but significant costs including time spent on infrastructure deployment, automation development, maintenance activities, cost monitoring, and efficiency optimization.</li><li><strong>Hidden Complexity:</strong> Managing multiple billing relationships, optimizing across different cost categories, and maintaining expertise in cloud infrastructure management.</li></ul>
# MAGIC Serverless fundamentally simplifies the cost model:
# MAGIC <ul><li><strong>Unified Billing:</strong> Single DBU price that includes infrastructure and operational costs, eliminating the need to manage multiple vendor relationships and cost optimization strategies.</li><li><strong>Value Proposition:</strong> The fully managed service provides operational simplicity and reliability improvements that often justify higher per-unit costs through reduced operational overhead.</li><li><strong>Performance Benefits:</strong> Auto-scaling capabilities and out-of-the-box optimizations often deliver better performance at lower total cost than self-managed alternatives.</li><li><strong>TCO Advantages:</strong> When you factor in operational overhead, the total cost of ownership is typically lower with serverless, especially for organizations without dedicated platform engineering teams.</li></ul>
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Modular Design
# MAGIC
# MAGIC Implementing architecture patterns that support maintainability, reusability, and team collaboration.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Modular Design in Databricks LakeFlow Jobs
# MAGIC
# MAGIC Implementing architecture patterns that support maintainability, reusability, and team collaboration.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/modular_design_databricks_lakeFlow_jobs.png"
# MAGIC     alt="Modular Design in Databricks Lakeflow Jobs"
# MAGIC     style="width: 1100px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC Modular orchestration transforms large, monolithic workflows into maintainable, reusable components:
# MAGIC <ul><li><strong>Decomposition Strategy:</strong> Break complex DAGs into logical business units rather than technical components. Each module should represent a cohesive business function that can be developed, tested, and deployed independently.</li><li><strong>Parent-Child Relationships:</strong> Parent jobs orchestrate child jobs, creating clean separation of concerns while maintaining overall workflow coordination.</li><li><strong>Benefits Realization:</strong><ul><li><strong>Maintainability:</strong> Smaller jobs are easier to understand, modify, and troubleshoot</li><li><strong>Reusability:</strong> Child jobs can be reused across multiple parent workflows</li><li><strong>Team Collaboration:</strong> Different teams can own different modules while collaborating on the overall workflow</li><li><strong>Testing:</strong> Individual modules can be tested independently, improving quality and reducing deployment risk</li></ul></li></ul>
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Jobs and Git
# MAGIC <br>
# MAGIC <div class="lfjp-git">
# MAGIC <style>
# MAGIC .lfjp-git{width:1220px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .lfjp-git *{box-sizing:border-box;}
# MAGIC .lfjp-git-layout{display:grid;grid-template-columns:260px 1fr;gap:22px;align-items:stretch;}
# MAGIC .lfjp-git-visual{background:#F9F7F4;border:1px solid #DCE0E2;border-radius:14px;padding:22px;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;}
# MAGIC .lfjp-git-visual img{background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;object-fit:contain;}
# MAGIC .lfjp-git-logo{width:150px;height:auto;margin-bottom:18px;}
# MAGIC .lfjp-job-icon{width:104px;height:104px;margin-top:18px;}
# MAGIC .lfjp-git-arrow{font-size:34px;color:#FF5F46;font-weight:900;margin:6px 0;}
# MAGIC .lfjp-git-cards{display:grid;grid-template-columns:1fr 1fr;gap:16px;}
# MAGIC .lfjp-git-card{background:#fff;border:1px solid #DCE0E2;border-left:5px solid #FF5F46;border-radius:12px;padding:18px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .lfjp-git-card.green{border-left-color:#00A972;}.lfjp-git-card.blue{border-left-color:#2272B4;}.lfjp-git-card.maroon{border-left-color:#98102A;}
# MAGIC .lfjp-git-card h3{font-size:18px;line-height:1.25;font-weight:700;margin:0 0 10px 0;}
# MAGIC .lfjp-git-card p{font-size:16px;line-height:1.5;margin:0;}
# MAGIC .lfjp-git-banner{grid-column:1/-1;background:#FFF1EE;border:1px solid #FFD2C9;border-radius:12px;padding:14px 16px;font-size:16px;line-height:1.5;}
# MAGIC @media screen and (max-width:900px){.lfjp-git-layout,.lfjp-git-cards{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="lfjp-git-layout">
# MAGIC <div class="lfjp-git-visual"><img class="lfjp-git-logo" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/git_logo.png" alt="Git logo"><div class="lfjp-git-arrow">↓</div><img class="lfjp-job-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/job_workflow_icon.png" alt="Job workflow icon"><strong>Job</strong></div>
# MAGIC <div class="lfjp-git-cards"><div class="lfjp-git-banner"><strong>Jobs supports execution of notebooks from Git.</strong></div><div class="lfjp-git-card"><h3>Prevent unintentional changes</h3><p>This prevents unintentional changes to your production job, for example, when another user makes local edits in a ‘Prod’ repo or switches branches.</p></div><div class="lfjp-git-card green"><h3>Single source of truth</h3><p>Simplifying the job definition process by having a single source of truth.</p></div><div class="lfjp-git-card blue"><h3>CI/CD deployment</h3><p>Makes deployment easy for notebooks through CI/CD.</p></div><div class="lfjp-git-card maroon"><h3>Git platform support</h3><p>Users can connect with various Git platforms, as Databricks supports GitHub, GitLab, AWS CodeCommit, and other Git providers.</p></div></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC Git integration provides essential capabilities for production workflow management:
# MAGIC <ul><li><strong>Change Management:</strong> Prevents unintentional changes to production jobs by ensuring all modifications go through proper version control processes.</li><li><strong>Single Source of Truth:</strong> Eliminates confusion about which version of code is running in production by always executing from committed code in specific branches or tags.</li><li><strong>CI/CD Integration:</strong> Enables automated testing and deployment pipelines that can validate changes before they reach production environments.</li><li><strong>Platform Support:</strong> Broad compatibility with GitHub, GitLab, AWS CodeCommit, and other Git providers ensures you can integrate with existing development workflows regardless of your chosen platform.</li><li><strong>Collaboration Benefits:</strong> Multiple developers can collaborate on workflow development using standard Git workflows (branches, pull requests, code reviews) while maintaining production stability.</li></ul>
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Jobs and Git Configuration Steps
# MAGIC <br>
# MAGIC <div class="lfjp-git-steps">
# MAGIC <style>
# MAGIC .lfjp-git-steps{width:1220px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .lfjp-git-steps *{box-sizing:border-box;}
# MAGIC .lfjp-step-grid{display:grid;grid-template-columns:1fr 1fr;gap:22px;align-items:stretch;}
# MAGIC .lfjp-step-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .lfjp-step-bar{height:8px;background:#FF5F46;}.lfjp-step-bar.green{background:#00A972;}
# MAGIC .lfjp-step-body{padding:20px;}
# MAGIC .lfjp-step-head{display:flex;align-items:center;gap:12px;margin-bottom:14px;}
# MAGIC .lfjp-step-num{width:38px;height:38px;border-radius:50%;background:#FF5F46;color:#fff;display:flex;align-items:center;justify-content:center;font-size:18px;font-weight:800;flex:0 0 auto;}
# MAGIC .lfjp-step-num.green{background:#00A972;}
# MAGIC .lfjp-step-head h3{font-size:18px;line-height:1.25;margin:0;font-weight:700;}
# MAGIC .lfjp-step-body p{font-size:16px;line-height:1.5;margin:0 0 14px 0;}
# MAGIC .lfjp-step-img{background:#F9F7F4;border:1px solid #DCE0E2;border-radius:12px;padding:12px;display:flex;align-items:center;justify-content:center;min-height:200px;}
# MAGIC .lfjp-step-img img{width:100%;max-height:230px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC @media screen and (max-width:900px){.lfjp-step-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="lfjp-step-grid">
# MAGIC <div class="lfjp-step-card"><div class="lfjp-step-bar"></div><div class="lfjp-step-body"><div class="lfjp-step-head"><div class="lfjp-step-num">1</div><h3>Create task with Git provider as Source</h3></div><p>Create tasks with Git provider as the source, specifying the repository, branch/tag, and authentication credentials.</p><div class="lfjp-step-img"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/git_provider_source_ui.png" alt="Git provider as source configuration UI"></div></div></div>
# MAGIC <div class="lfjp-step-card"><div class="lfjp-step-bar green"></div><div class="lfjp-step-body"><div class="lfjp-step-head"><div class="lfjp-step-num green">2</div><h3>Configure path to main notebook under repository root</h3></div><p>Configure the path to your main notebook under the repository root, ensuring the job can locate and execute the correct entry point for your workflow.</p><div class="lfjp-step-img"><img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs_production/git_notebook_path_ui.png" alt="Main notebook path configuration UI"></div></div></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC Implementing Git integration involves two straightforward steps:
# MAGIC <ul><li><strong>Step 1 - Source Configuration:</strong> Create tasks with Git provider as the source, specifying the repository, branch/tag, and authentication credentials. This ensures your job always executes the committed version of your code.</li><li><strong>Step 2 - Path Configuration:</strong> Configure the path to your main notebook under the repository root, ensuring the job can locate and execute the correct entry point for your workflow.</li><li><strong>Best Practices:</strong> Use specific branches or tags for production rather than constantly changing main branches, implement proper code review processes before merging to production branches, and maintain clear documentation about which repositories and paths contain production job code.</li></ul>
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Best Practices
# MAGIC
# MAGIC <div class="bp-practice-map">
# MAGIC <style>
# MAGIC .bp-practice-map{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .bp-practice-map *{box-sizing:border-box;}
# MAGIC .bp-lanes{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;align-items:stretch;}
# MAGIC .bp-lane{background:#ffffff;border:1px solid #DCE0E2;border-radius:16px;padding:0 18px 18px 18px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .bp-lane.compute{--lane:#00A972;}
# MAGIC .bp-lane.orchestration{--lane:#2272B4;}
# MAGIC .bp-lane.governance{--lane:#165A67;}
# MAGIC .bp-ribbon{margin:0 -18px 18px -18px;padding:14px 18px;background:var(--lane);color:#ffffff;font-size:18px;font-weight:800;line-height:1.25;text-align:center;min-height:66px;display:flex;align-items:center;justify-content:center;}
# MAGIC .bp-lane-icon{width:82px;height:82px;object-fit:contain;display:block;margin:18px auto 22px auto;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC .bp-track{position:relative;padding-left:24px;}
# MAGIC .bp-track:before{content:"";position:absolute;left:8px;top:4px;bottom:8px;width:3px;background:linear-gradient(180deg,var(--lane),rgba(220,224,226,.35));border-radius:999px;}
# MAGIC .bp-item{position:relative;margin:0 0 17px 0;padding:0 0 0 18px;font-size:16px;line-height:1.45;color:#0B2026;}
# MAGIC .bp-item:before{content:"";position:absolute;left:-22px;top:5px;width:15px;height:15px;border:3px solid #ffffff;background:var(--lane);border-radius:50%;box-shadow:0 0 0 2px var(--lane);}
# MAGIC .bp-item strong{color:var(--lane);font-weight:800;}
# MAGIC .bp-note-chip{margin-top:18px;background:#F9F7F4;border:1px dashed var(--lane);border-radius:12px;padding:12px 14px;font-size:15px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC @media screen and (max-width:980px){.bp-lanes{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="bp-lanes">
# MAGIC <div class="bp-lane compute">
# MAGIC <div class="bp-ribbon">Compute &amp; Cost Optimization</div>
# MAGIC <img class="bp-lane-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/compute_cost_optimization.png" alt="Compute and cost optimization icon">
# MAGIC <div class="bp-track">
# MAGIC <div class="bp-item">Use job or <strong>serverless clusters</strong> in production</div>
# MAGIC <div class="bp-item">Avoid interactive clusters for production workloads</div>
# MAGIC <div class="bp-item"><strong>Enable Photon</strong> for faster and cheaper execution</div>
# MAGIC <div class="bp-item"><strong>Reuse clusters</strong> to reduce startup time and cost</div>
# MAGIC </div>
# MAGIC <div class="bp-note-chip">Focus: efficient production compute with lower startup overhead and cost.</div>
# MAGIC </div>
# MAGIC <div class="bp-lane orchestration">
# MAGIC <div class="bp-ribbon">Orchestration &amp; Modularity</div>
# MAGIC <img class="bp-lane-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/orchestration_modularity.png" alt="Orchestration and modularity icon">
# MAGIC <div class="bp-track">
# MAGIC <div class="bp-item"><strong>Break complex pipelines</strong> into modular jobs/run jobs</div>
# MAGIC <div class="bp-item">Use <strong>multi-task jobs</strong> for parallel and scalable execution</div>
# MAGIC <div class="bp-item">Apply conditional logic using Run If, If/Else, For Each task</div>
# MAGIC <div class="bp-item">Limit jobs to <strong>manageable task counts</strong> for maintainability, limited to 1000 task</div>
# MAGIC </div>
# MAGIC <div class="bp-note-chip">Focus: modular design that stays maintainable as workflows scale.</div>
# MAGIC </div>
# MAGIC <div class="bp-lane governance">
# MAGIC <div class="bp-ribbon">Monitoring &amp; Governance</div>
# MAGIC <img class="bp-lane-icon" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/monitoring_governance.png" alt="Monitoring and governance icon">
# MAGIC <div class="bp-track">
# MAGIC <div class="bp-item">Use <strong>service principals</strong> for job ownership and data access</div>
# MAGIC <div class="bp-item">Configure alerts for <strong>failures, delays, and completions</strong></div>
# MAGIC <div class="bp-item">Leverage <strong>Repair &amp; Run</strong> for reducing cost on re-processing</div>
# MAGIC <div class="bp-item">Parameterize tasks for <strong>reusability and flexibility</strong></div>
# MAGIC </div>
# MAGIC <div class="bp-note-chip">Focus: reliable ownership, alerting, recovery, and reusable execution patterns.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p><strong>Compute & Cost Optimization Practices:</strong></p>
# MAGIC <ul>
# MAGIC <li><strong>Production Compute:</strong> Always use job clusters or serverless compute in production environments to ensure cost efficiency and resource isolation</li>
# MAGIC <li><strong>Interactive Cluster Avoidance:</strong> Reserve interactive clusters strictly for development and ad-hoc analysis to prevent production cost overruns and resource conflicts</li>
# MAGIC <li><strong>Photon Enablement:</strong> Enable Photon acceleration across all eligible workloads for significant performance improvements and cost reductions</li>
# MAGIC <li><strong>Cluster Reuse:</strong> Design workflows to reuse clusters across tasks when possible, reducing startup overhead and improving cost efficiency</li>
# MAGIC </ul>
# MAGIC <p><strong>Orchestration & Modularity Practices:</strong></p>
# MAGIC <ul>
# MAGIC <li><strong>Modular Architecture:</strong> Break complex pipelines into logical, maintainable modules using the run job task pattern for better maintainability and team collaboration</li>
# MAGIC <li><strong>Multi-Task Design:</strong> Leverage multi-task jobs for parallel execution and improved resource utilization while maintaining workflow clarity</li>
# MAGIC <li><strong>Advanced Logic:</strong> Implement sophisticated business logic using Run If, If/Else, and For Each tasks to handle complex real-world scenarios</li>
# MAGIC <li><strong>Task Limitations:</strong> Keep individual jobs under 1000 tasks to maintain manageability and performance, using modular design to handle larger workflows</li>
# MAGIC </ul>
# MAGIC <p><strong>Monitoring & Governance Practices:</strong></p>
# MAGIC <ul>
# MAGIC <li><strong>Service Principal Usage:</strong> Use service principals rather than personal accounts for job ownership and data access to ensure continuity and proper access control</li>
# MAGIC <li><strong>Comprehensive Alerting:</strong> Configure alerts for failures, delays, and completions with appropriate escalation and notification strategies</li>
# MAGIC <li><strong>Recovery Optimization:</strong> Leverage Repair &amp; Run capabilities to minimize costs and recovery time when handling failures</li>
# MAGIC <li><strong>Parameterization:</strong> Design tasks with proper parameterization for maximum reusability and flexibility across environments and use cases</li>
# MAGIC </ul>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Conclusion
# MAGIC
# MAGIC <ul>
# MAGIC <li><strong>Production compute</strong><br>Select compute options that align with performance, cost, and operational requirements.</li>
# MAGIC <li><strong>Pricing structure</strong><br>Serverless simplifies billing by including infrastructure and operational costs in a single DBU price.</li>
# MAGIC <li><strong>Modular design</strong><br>Break large workflows into maintainable parent jobs and reusable child jobs.</li>
# MAGIC <li><strong>Jobs and Git</strong><br>Use Git-backed production notebooks to support CI/CD, consistency, and change management.</li>
# MAGIC <li><strong>Best practices</strong><br>Apply compute optimization, modular orchestration, monitoring, alerting, recovery, governance, and parameterization practices for reliable production workflows.</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>