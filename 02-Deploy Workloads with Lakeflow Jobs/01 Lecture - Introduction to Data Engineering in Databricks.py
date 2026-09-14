# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Introduction to Data Engineering in Databricks
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This lecture introduces how Databricks supports efficient data engineering by combining optimized storage techniques, unified data governance through **Unity Catalog**, and **Lakeflow's integrated capabilities** for data ingestion, transformation, and orchestration. It then examines the challenges created by external orchestration tools and introduces **Lakeflow Jobs** as unified orchestration for data, analytics, and AI on the Data Intelligence Platform.
# MAGIC
# MAGIC In this course, we focus on Jobs as the orchestration component of Lakeflow and explore what Lakeflow Jobs is and why it matters.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC 1. **Understand** the key components of data engineering on the Databricks platform, including Connect, Spark Declarative Pipelines and Jobs
# MAGIC 2. **Explain** what Lakeflow Jobs are and their main benefits for unified orchestration of data, analytics, and AI workloads
# MAGIC 3. **Identify** the core capabilities and use cases enabled by Lakeflow Jobs within the Databricks ecosystem

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Data Engineering in Databricks

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Data Engineering Platform Overview
# MAGIC
# MAGIC It all begins with optimized storage using Delta Lake, Parquet, or Iceberg, built upon by unified governance with Unity Catalog, and powered by Lakeflow to deliver end-to-end, high-quality data engineering for analytics and AI.
# MAGIC
# MAGIC <div style="max-width:1000px;margin:0 auto;font-family:sans-serif;color:#0b2026;line-height:1.2;"><div style="display:flex;justify-content:center;position:relative;z-index:2;margin-bottom:-1px;"><div style="border:1px solid #FFD3CB;border-bottom:none;background:#fff;border-radius:10px 10px 0 0;padding:6px 22px;"><span style="font-size:18pt;font-weight:800;letter-spacing:-0.5px;">databricks</span> <span style="font-size:18pt;font-weight:800;color:#FF5F46;margin-left:6px;">LAKEFLOW</span></div></div><div style="border:1px solid #FFD3CB;border-radius:10px;padding:14px;"><div style="text-align:center;color:#FF5F46;font-weight:800;font-size:14pt;letter-spacing:0.5px;margin-bottom:12px;">UNIFIED DATA ENGINEERING FOR THE DATA INTELLIGENCE PLATFORM</div><div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;"><div style="flex:1;min-width:200px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.06);"><div style="background:#FF5F46;color:#fff;font-weight:800;font-size:14pt;text-align:center;letter-spacing:0.5px;min-height:56px;display:flex;align-items:center;justify-content:center;padding:8px 10px;">CONNECT</div><div style="padding:11px 14px;text-align:center;font-weight:700;font-size:14pt;">Efficient ingestion connectors</div></div><div style="flex:1;min-width:200px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.06);"><div style="background:#FF5F46;color:#fff;font-weight:800;font-size:14pt;text-align:center;letter-spacing:0.5px;min-height:56px;display:flex;align-items:center;justify-content:center;padding:8px 10px;">APACHE SPARK™ DECLARATIVE PIPELINES (SDP)</div><div style="padding:11px 14px;text-align:center;font-weight:700;font-size:14pt;">Accelerated ETL development</div></div><div style="flex:1;min-width:200px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.06);"><div style="background:#FF5F46;color:#fff;font-weight:800;font-size:14pt;text-align:center;letter-spacing:0.5px;min-height:56px;display:flex;align-items:center;justify-content:center;padding:8px 10px;">JOBS</div><div style="padding:11px 14px;text-align:center;font-weight:700;font-size:14pt;">Reliable orchestration for analytics and AI</div></div></div><div style="background:#FF5F46;color:#fff;border-radius:6px;text-align:center;padding:13px 14px;"><div style="font-weight:800;font-size:16pt;letter-spacing:0.5px;">INDUSTRY LEADING DATA PROCESSING ENGINE</div><div style="font-weight:700;font-size:14pt;margin-top:3px;">(Apache Spark + Structured Streaming)</div></div></div><div style="display:flex;margin-top:8px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;"><div style="background:#618794;color:#fff;font-weight:800;font-size:14pt;letter-spacing:0.5px;padding:10px 14px;width:240px;display:flex;align-items:center;">UNIFIED GOVERNANCE</div><div style="flex:1;background:#fff;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:14pt;padding:8px;">Unity Catalog</div></div><div style="display:flex;margin-top:6px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;"><div style="background:#618794;color:#fff;font-weight:800;font-size:14pt;letter-spacing:0.5px;padding:10px 14px;width:240px;display:flex;align-items:center;">OPTIMIZED STORAGE</div><div style="flex:1;background:#fff;display:flex;align-items:center;justify-content:center;gap:24px;font-weight:800;font-size:14pt;padding:8px;flex-wrap:wrap;"><span>Delta Lake</span><span style="color:#DCE0E2;font-weight:400;">|</span><span>Parquet</span><span style="color:#DCE0E2;font-weight:400;">|</span><span>Iceberg</span></div></div></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC It all begins with optimized storage using Delta Lake, Parquet, or Iceberg.
# MAGIC
# MAGIC Built on top of this storage layer is unified governance with Unity Catalog. Unity Catalog is a centralized data catalog that provides access control, auditing, data lineage, quality monitoring, and data discovery across Databricks workspaces.
# MAGIC
# MAGIC Databricks then offers Lakeflow, an end-to-end data engineering solution that empowers data engineers, software developers, SQL developers, analysts, and data scientists to deliver high-quality data for downstream analytics, AI, and operational applications. Lakeflow provides a unified platform for data ingestion, transformation, and orchestration, and includes the following components:
# MAGIC
# MAGIC - **Lakeflow Connect**: A set of efficient ingestion connectors that simplify data ingestion from popular enterprise applications, databases, cloud storage, message buses, and local files.
# MAGIC
# MAGIC - **Spark Declarative Pipelines**: A framework for building batch and streaming data pipelines using SQL and Python, designed to accelerate ETL development.
# MAGIC
# MAGIC - **Lakeflow Jobs**: A workflow automation tool for Databricks that orchestrates data processing tasks and workflows. It enables coordination of multiple tasks within complex workflows, allowing for the scheduling, optimization, and management of repeatable processes.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Lakeflow Jobs
# MAGIC
# MAGIC In this course, we're focusing specifically on Lakeflow Jobs — the orchestration component.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_data_engineering_databricks/jobs.png"
# MAGIC     alt="Lakeflow Jobs Orchestration Overview"
# MAGIC     style="width: 900px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Lakeflow Jobs allows you to orchestrate every type of workload within Databricks, including notebooks, SQL queries, dashboards, pipelines, and much more. 
# MAGIC
# MAGIC This unified approach is what makes Lakeflow Jobs so powerful.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. What is Lakeflow Jobs?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Ways to Orchestrate Your Workloads
# MAGIC
# MAGIC There is a fundamental challenge in modern data architecture: **choosing the right orchestration approach for lakehouse workloads**.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/orchestrate_workloads.png"
# MAGIC     alt="Diagram showing multiple ways to orchestrate lakehouse workloads"
# MAGIC     style="width: 1100px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC The left panel shows multiple options including open source solutions (Apache Airflow, Prefect, Dagster, dbt), cloud-native services (AWS, Azure, Google Cloud), and custom in-house frameworks.
# MAGIC
# MAGIC Diagram on right illustrates a typical data workflow with multiple steps: ingesting sessions and clicks data, joining them, performing featurization and aggregation, analysis, and training models. Then you have various downstream uses including BI and Data Warehousing, Data Streaming, and Data Science and ML.
# MAGIC
# MAGIC The question mark in the diagram represents a critical challenge: there are many ways to orchestrate these workloads, but which approach is best? 
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. External Orchestrators Create Challenges
# MAGIC
# MAGIC It's difficult to work with external orchestration tools because they introduce productivity, quality, and reliability challenges.
# MAGIC <style>
# MAGIC .flip-grid{display:flex;justify-content:space-between;gap:32px;max-width:1100px;margin:32px auto;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}
# MAGIC .flip-card{background:transparent;width:320px;height:220px;perspective:1000px;flex-shrink:0;}
# MAGIC .flip-card-inner{position:relative;width:100%;height:100%;transition:transform .9s;transform-style:preserve-3d;}
# MAGIC .flip-card:hover .flip-card-inner{transform:rotateY(180deg);}
# MAGIC .flip-card-front,.flip-card-back{position:absolute;width:100%;height:100%;-webkit-backface-visibility:hidden;backface-visibility:hidden;border-radius:8px;box-sizing:border-box;display:flex;align-items:center;justify-content:center;text-align:center;padding:24px 28px;border:2px solid rgba(0,0,0,.08);box-shadow:0 4px 10px rgba(0,0,0,.08);}
# MAGIC .flip-card-front{background:#ffffff;color:#111111;font-size:20px;font-weight:600;}
# MAGIC .flip-card-back{background:#FF5F46;color:#ffffff;transform:rotateY(180deg);font-size:20px;line-height:1.4;text-align:left;justify-content:center;flex-direction:column;}
# MAGIC .lakehouse-callout{position:relative;max-width:1100px;height:270px;margin:26px auto 8px auto;background:#ffffff;overflow:hidden;border-radius:2px;box-sizing:border-box;}
# MAGIC .lakehouse-callout *{box-sizing:border-box;}
# MAGIC .orch-bg-icon{position:absolute;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:contrast(1.05) brightness(1);opacity:.24;z-index:1;}
# MAGIC .icon-airflow{left:36px;top:22px;width:220px;height:90px;}
# MAGIC .icon-pink{left:345px;top:28px;width:95px;height:95px;}
# MAGIC .icon-hex{left:610px;top:18px;width:120px;height:110px;}
# MAGIC .icon-factory{right:40px;top:0;width:140px;height:130px;}
# MAGIC .icon-purple{left:185px;bottom:-14px;width:95px;height:95px;}
# MAGIC .icon-dagster{left:455px;bottom:-6px;width:245px;height:72px;}
# MAGIC .icon-prefect{right:80px;bottom:14px;width:245px;height:70px;}
# MAGIC .lakehouse-callout-box{position:absolute;left:0;right:0;top:92px;height:96px;background:rgba(255,255,255,.96);border:1.5px solid #0B2026;z-index:3;display:flex;align-items:center;justify-content:center;text-align:center;padding:18px 34px;}
# MAGIC .lakehouse-callout-text{color:#FF5F46;font-size:36px;line-height:1.15;font-weight:800;letter-spacing:.2px;}
# MAGIC .lakehouse-callout-text u{text-decoration-thickness:3px;text-underline-offset:6px;}
# MAGIC @media screen and (max-width:900px){.flip-grid{flex-direction:column;align-items:center;}.flip-card{width:100%;max-width:360px;}.lakehouse-callout{height:220px;}.lakehouse-callout-box{top:78px;height:82px;}.lakehouse-callout-text{font-size:24px;}.icon-airflow{width:160px;}.icon-pink{left:42%;}.icon-hex{left:58%;width:92px;}.icon-factory{width:110px;}.icon-dagster{left:35%;width:190px;}.icon-prefect{right:30px;width:180px;}}
# MAGIC </style>
# MAGIC <div class="flip-grid">
# MAGIC <div class="flip-card"><div class="flip-card-inner"><div class="flip-card-front">Data teams are less productive</div><div class="flip-card-back">Hard to use for many practitioners</div></div></div>
# MAGIC <div class="flip-card"><div class="flip-card-inner"><div class="flip-card-front">Bad data lowers value of downstream applications</div><div class="flip-card-back">Difficult to understand the root cause when issues occur</div></div></div>
# MAGIC <div class="flip-card"><div class="flip-card-inner"><div class="flip-card-front">Higher cost of ownership and lower reliability</div><div class="flip-card-back">Complex architecture to manage and maintain</div></div></div>
# MAGIC </div>
# MAGIC <div class="lakehouse-callout">
# MAGIC <img class="orch-bg-icon icon-airflow" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/apache_airflow_logo.png" alt="Apache Airflow logo">
# MAGIC <img class="orch-bg-icon icon-pink" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/orchestrator_icon_magenta.png" alt="External orchestration icon">
# MAGIC <img class="orch-bg-icon icon-hex" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/google_cloud_composer_icon.png" alt="Cloud orchestration icon">
# MAGIC <img class="orch-bg-icon icon-factory" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/azure_data_factory_icon.png" alt="Factory orchestration icon">
# MAGIC <img class="orch-bg-icon icon-purple" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/airflow_related_icon.png" alt="External orchestration icon">
# MAGIC <img class="orch-bg-icon icon-dagster" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/dagster_logo.png" alt="Dagster logo">
# MAGIC <img class="orch-bg-icon icon-prefect" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/prefect_logo.png" alt="Prefect logo">
# MAGIC <div class="lakehouse-callout-box">
# MAGIC <div class="lakehouse-callout-text">These tools are <u><strong>not unified</strong></u> with your Lakehouse</div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Many organizations use external orchestration tools, but this creates significant challenges. Data teams become less productive because these tools are hard to use for many practitioners. Bad data quality lowers the value of downstream applications.
# MAGIC
# MAGIC You also face higher costs of ownership and lower reliability. When issues occur, it's difficult to understand root cause. The complex architecture becomes hard to manage and maintain.
# MAGIC
# MAGIC Most importantly, these external tools are not unified with your Lakehouse, creating integration challenges and data silos.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. What is Lakeflow Jobs?
# MAGIC
# MAGIC <div class="lfj-merged">
# MAGIC <style>
# MAGIC .lfj-merged{width:1100px;max-width:100%;margin:14px auto 24px auto;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .lfj-merged *{box-sizing:border-box;}
# MAGIC .lfj-wrap{display:grid;grid-template-columns:.85fr 1.15fr;gap:24px;align-items:stretch;background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;padding:24px;box-shadow:0 3px 12px rgba(27,49,57,.07);}
# MAGIC .lfj-left{background:#F9F7F4;border-radius:12px;padding:24px;display:flex;flex-direction:column;justify-content:center;gap:20px;}
# MAGIC .lfj-text{border-left:5px solid #FF5F46;background:#ffffff;border-radius:10px;padding:16px 18px;}
# MAGIC .lfj-text p{font-size:20px;line-height:1.55;margin:0;color:#0B2026;}
# MAGIC .lfj-benefit-title{text-align:center;}
# MAGIC .lfj-benefit-pill{display:inline-block;background:#0B2026;color:#F9F7F4;font-size:20px;font-weight:800;letter-spacing:.4px;padding:10px 30px;border-radius:8px;}
# MAGIC .lfj-benefit-line{width:2px;height:24px;background:#618794;margin:0 auto;}
# MAGIC .lfj-benefits{display:grid;gap:12px;}
# MAGIC .lfj-benefit{background:#ffffff;border:1px solid #DCE0E2;border-radius:10px;padding:16px 18px;text-align:center;box-shadow:0 2px 8px rgba(11,32,38,.06);font-size:20px;line-height:1.35;font-weight:800;color:#0B2026;}
# MAGIC .lfj-benefit.blue{border-top:4px solid #4299E0;}
# MAGIC .lfj-benefit.green{border-top:4px solid #00A972;}
# MAGIC .lfj-benefit.orange{border-top:4px solid #FF5F46;}
# MAGIC .lfj-image{display:flex;align-items:center;justify-content:center;background:#ffffff;border:1px solid #DCE0E2;border-radius:12px;padding:16px;min-height:500px;}
# MAGIC .lfj-image img{width:100%;max-height:500px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC @media screen and (max-width:900px){.lfj-wrap{grid-template-columns:1fr;}.lfj-image{min-height:420px;}.lfj-image img{max-height:400px;}}
# MAGIC </style>
# MAGIC <div class="lfj-wrap">
# MAGIC <div class="lfj-left">
# MAGIC <div class="lfj-text">
# MAGIC <p>Unified orchestration for data, analytics, and AI on the Data Intelligence Platform</p>
# MAGIC </div>
# MAGIC <div class="lfj-benefit-title">
# MAGIC <div class="lfj-benefit-pill">Key Benefits</div>
# MAGIC <div class="lfj-benefit-line"></div>
# MAGIC </div>
# MAGIC <div class="lfj-benefits">
# MAGIC <div class="lfj-benefit blue">Simple authoring</div>
# MAGIC <div class="lfj-benefit green">Actionable insights</div>
# MAGIC <div class="lfj-benefit orange">Proven reliability</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="lfj-image">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/lakeflow_jobs_overview.png" alt="Lakeflow Jobs orchestration overview">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC This is where Lakeflow Jobs comes in. It provides unified orchestration for data, analytics, and AI workloads directly on the Data Intelligence Platform.
# MAGIC
# MAGIC The key benefits are simple authoring, actionable insights, and proven reliability. Because it's native to the platform, it integrates seamlessly with data ingestion and transformation, the processing engine (Photon), governance (Unity Catalog), storage (Delta Lake), data warehousing, and machine learning capabilities.
# MAGIC
# MAGIC The workflow shown here - from sessions and clicks through join, featurize, aggregate, analyze, and train - all runs natively within the same platform, eliminating the integration challenges of external tools.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B4. Architecture of Lakeflow Jobs
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_lakeflow_jobs/architecture_lakeflow_jobs.png"
# MAGIC     alt="Lakeflow Jobs Architecture"
# MAGIC     style="width: 1100px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC - This diagram shows the complete architecture of Lakeflow Jobs. At the center is the Workflow engine that coordinates everything.
# MAGIC - The compute layer supports various workload types: ETL, ML/AI, and Analytics/BI operations.
# MAGIC - You have multiple trigger types: Scheduled (time-based), Continuous (always-running), File Arrival (event-driven), and Table Updates.
# MAGIC - Two critical components support the entire system: Observability for monitoring and troubleshooting, and Control Flow for managing task dependencies and execution order.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Conclusion
# MAGIC
# MAGIC - Databricks combines optimized storage, Unity Catalog governance, and Lakeflow for ingestion, transformation, and orchestration.
# MAGIC - Lakeflow Jobs orchestrates data, analytics, and AI workloads natively on the Data Intelligence Platform.
# MAGIC - It unifies workflows, triggers, compute, observability, and control flow, reducing reliance on external orchestrators.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next lecture, you will explore the core components that make up Lakeflow Jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>