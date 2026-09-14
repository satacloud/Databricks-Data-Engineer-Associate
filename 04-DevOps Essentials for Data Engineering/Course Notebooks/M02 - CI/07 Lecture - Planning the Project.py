# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Planning the Project
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you’ll learn how to plan a data visualization project by structuring environments, managing data across dev, stage, and prod, and setting up a CI/CD pipeline in Databricks for smooth, secure deployments. 
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. Learn to plan and structure a data engineering project, defining key components and isolation steps for successful execution.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Requirements
# MAGIC <div class="pp-plan-project">
# MAGIC <style>
# MAGIC .pp-plan-project{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .pp-plan-project *{box-sizing:border-box;}
# MAGIC .pp-hero{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 22px;margin-bottom:18px;text-align:center;}
# MAGIC .pp-hero h3{font-size:22px;line-height:1.25;font-weight:850;margin:0;color:#0B2026;}
# MAGIC .pp-grid{display:grid;grid-template-columns:.8fr 1.2fr .8fr;gap:18px;align-items:stretch;}
# MAGIC .pp-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;display:flex;flex-direction:column;}
# MAGIC .pp-bar{height:8px;background:#FF5F46;}
# MAGIC .pp-bar.green{background:#00A972;}
# MAGIC .pp-bar.blue{background:#2272B4;}
# MAGIC .pp-card-body{padding:20px;flex:1;display:flex;flex-direction:column;justify-content:center;}
# MAGIC .pp-card h3{font-size:18px;line-height:1.25;font-weight:850;margin:0 0 12px 0;color:#0B2026;text-align:center;}
# MAGIC .pp-focus{font-size:24px;line-height:1.2;font-weight:850;text-align:center;color:#FF5F46;}
# MAGIC .pp-card ul{margin:0;padding-left:22px;}
# MAGIC .pp-card li{font-size:16px;line-height:1.5;margin-bottom:10px;color:#0B2026;}
# MAGIC .pp-assets{background:#F9F7F4;border:1px dashed #DCE0E2;border-radius:12px;min-height:150px;display:flex;align-items:center;justify-content:center;text-align:center;padding:16px;}
# MAGIC .pp-assets img{display:block;width:100%;max-width:220px;max-height:130px;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;margin:0 auto;}
# MAGIC @media screen and (max-width:900px){.pp-grid{grid-template-columns:1fr;}.pp-assets img{max-width:260px;}}
# MAGIC </style>
# MAGIC <div class="pp-hero"><h3>Planning the Project</h3></div>
# MAGIC <div class="pp-grid">
# MAGIC <div class="pp-card">
# MAGIC <div class="pp-bar"></div>
# MAGIC <div class="pp-card-body">
# MAGIC <h3>Deliverable</h3>
# MAGIC <div class="pp-focus">Visualize Health Data</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="pp-card">
# MAGIC <div class="pp-bar green"></div>
# MAGIC <div class="pp-card-body">
# MAGIC <h3>Tasks</h3>
# MAGIC <ul>
# MAGIC <li>Ingest daily incremental CSV files to a bronze table</li>
# MAGIC <li>Create a clean silver table</li>
# MAGIC <li>Create gold tables to share with consumers</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="pp-card">
# MAGIC <div class="pp-bar blue"></div>
# MAGIC <div class="pp-card-body">
# MAGIC <h3>Databricks Assets</h3>
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_planning_the_project/databricks_assets.png" alt="Databricks Assets">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>This project focuses on visualizing health data by following a structured data pipeline. We begin by ingesting daily incremental CSV files into a Bronze table, then refining this raw data into a clean Silver table. From there, we create a summarized Gold table, which is shared with consumers and used to build the final visualization. Throughout the process, we make use of various database assets, including notebooks, Spark Declarative , Lakeflow Jobs, and compute resources, to ensure a smooth and efficient execution.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Setting Up Your Data Environments
# MAGIC <div class="pp-data-envs">
# MAGIC <style>
# MAGIC .pp-data-envs{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .pp-data-envs *{box-sizing:border-box;}
# MAGIC .pp-env-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;align-items:stretch;}
# MAGIC .pp-env-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;display:flex;flex-direction:column;}
# MAGIC .pp-env-top{padding:14px 18px;color:#ffffff;font-size:20px;font-weight:850;text-align:center;}
# MAGIC .pp-env-top.dev{background:#FF5F46;}
# MAGIC .pp-env-top.stage{background:#2272B4;}
# MAGIC .pp-env-top.prod{background:#00A972;}
# MAGIC .pp-env-body{background:#F9F7F4;padding:18px;flex:1;}
# MAGIC .pp-env-card ul{margin:0;padding-left:20px;}
# MAGIC .pp-env-card li{font-size:16px;line-height:1.45;margin-bottom:10px;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.pp-env-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="pp-env-grid">
# MAGIC <div class="pp-env-card">
# MAGIC <div class="pp-env-top dev"><img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/table_data_icon.png" alt="Data Icon" style="width: 80px; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1); border-radius: 4px;"><h3 align=center">Dev Data</h3></div>
# MAGIC <div class="pp-env-body">
# MAGIC <ul>
# MAGIC <li>Often a Small Static Subset of Production Data</li>
# MAGIC <li>Can be Anonymized or Synthetic Datasets</li>
# MAGIC <li>Supports Rapid Development and Testing</li>
# MAGIC <li>Ensures Privacy and Data Integrity</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="pp-env-card">
# MAGIC <div class="pp-env-top stage"><img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/table_data_icon.png" alt="Data Icon" style="width: 80px; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1); border-radius: 4px;"><h3 align=center">Stage Data</h3></div>
# MAGIC <div class="pp-env-body">
# MAGIC <ul>
# MAGIC <li>Staging Data Mirrors Production Structure &amp; Volume, Typically Static</li>
# MAGIC <li>Can be Anonymized or Scrubbed Sensitive Information</li>
# MAGIC <li>Ensures Realistic Testing and Validation</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="pp-env-card">
# MAGIC <div class="pp-env-top prod"><img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/table_data_icon.png" alt="Data Icon" style="width: 80px; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1); border-radius: 4px;"><h3 align=center">Prod Data</h3></div>
# MAGIC <div class="pp-env-body">
# MAGIC <ul>
# MAGIC <li>Production Data: Live &amp; Fully Operational</li>
# MAGIC <li>Contains Real User Data</li>
# MAGIC <li>Continuously Updated</li>
# MAGIC <li>Requires High Security, Privacy, &amp; Compliance Standards</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Within a CI/CD pipeline</p>
# MAGIC <p>In development, data is often anonymized or generated using synthetic datasets to allow rapid development and testing without compromising privacy or production data integrity.</p>
# MAGIC <p>If you have an staging environment, staging data should closely mirror production in structure and volume, with anonymized or scrubbed sensitive information to ensure realistic testing and validation.</p>
# MAGIC <p>Production data is live, fully operational, and continuously updated, containing real user data, and must be handled with high security, privacy, and compliance standards.</p>
# MAGIC <p>Each environment should have data suited for its specific purpose, balancing realism, security, and compliance at every stage. How this is implemented will depend on your organization and the sensitivity of your data.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Isolating Environments
# MAGIC <div class="pp-isolate-envs">
# MAGIC <style>
# MAGIC .pp-isolate-envs{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .pp-isolate-envs *{box-sizing:border-box;}
# MAGIC .pp-isolate-grid{display:grid;grid-template-columns:1fr 150px 1fr;gap:22px;align-items:stretch;}
# MAGIC .pp-isolate-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:22px;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;min-height:250px;}
# MAGIC .pp-isolate-card.workspaces{border-top:8px solid #FF5F46;}
# MAGIC .pp-isolate-card.catalogs{border-top:8px solid #00A972;}
# MAGIC .pp-isolate-icon{width:92px;height:92px;object-fit:contain;display:block;margin:0 auto 18px auto;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC .pp-middle-image{background:#F9F7F4;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.06);display:flex;align-items:center;justify-content:center;padding:18px;min-height:250px;}
# MAGIC .pp-middle-image img{width:120px;height:120px;object-fit:contain;display:block;margin:0 auto;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC .pp-isolate-card h3{font-size:22px;line-height:1.2;font-weight:850;margin:0 0 12px 0;color:#0B2026;}
# MAGIC .pp-isolate-card p{font-size:17px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.pp-isolate-grid{grid-template-columns:1fr;}.pp-middle-image{min-height:120px;}.pp-middle-image img{width:88px;height:88px;}}
# MAGIC </style>
# MAGIC <div class="pp-isolate-grid">
# MAGIC <div class="pp-isolate-card workspaces">
# MAGIC <img class="pp-isolate-icon" src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/workspaces_icon.png" alt="Workspaces">
# MAGIC <h3>Workspaces</h3>
# MAGIC <p>Utilizing multiple workspaces, one for each environment</p>
# MAGIC </div>
# MAGIC <div class="pp-middle-image">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_planning_the_project/isolating_environments.png" alt="Environment isolation">
# MAGIC </div>
# MAGIC <div class="pp-isolate-card catalogs">
# MAGIC <img class="pp-isolate-icon" src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/catalog_icon.png" alt="Catalogs">
# MAGIC <h3>Catalogs</h3>
# MAGIC <p>Utilizing multiple catalogs, one for each environment</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Isolating your environments for the different stages of development is key to developing a CI/CD pipeline. This ensures that code is developed and tested in the developing and staging prior to touching the production environment.</p>
# MAGIC <p>The minimal setup is to have two environments “Development &amp; Stage” and “Production”, but this can vary depending on your organizations requirements.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Workspace Isolation Overview
# MAGIC <div class="pp-uc-isolation">
# MAGIC <style>
# MAGIC .pp-uc-isolation{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .pp-uc-isolation *{box-sizing:border-box;}
# MAGIC .pp-uc-grid{display:grid;grid-template-columns:1.1fr .9fr;gap:20px;align-items:center;}
# MAGIC .pp-uc-diagram{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:22px;display:flex;align-items:center;justify-content:center;}
# MAGIC .pp-uc-diagram img{width:1000px;max-width:100%;height:auto;display:block;background:transparent;border-radius:8px;}
# MAGIC .pp-text-card{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #00A972;border-radius:12px;padding:24px;box-shadow:0 2px 8px rgba(27,49,57,.06);min-height:180px;display:flex;align-items:center;justify-content:center;text-align:center;}
# MAGIC .pp-text-card.orange{border-left-color:#FF5F46;}
# MAGIC .pp-text-card p{font-size:18px;line-height:1.5;margin:0;color:#0B2026;font-weight:700;}
# MAGIC @media screen and (max-width:900px){.pp-uc-grid{grid-template-columns:1fr;}.pp-text-card{min-height:auto;}}
# MAGIC </style>
# MAGIC <div class="pp-uc-grid">
# MAGIC <div class="pp-text-card orange">
# MAGIC <p>You can <b>isolate</b> your dev, stage and prod environments at the Workspace and storage level.</p>
# MAGIC </div>
# MAGIC <div class="pp-uc-diagram">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_planning_the_project/databricks_workspace.png" alt="Databricks Workspace">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Within Databricks, one methods for isolating your environments includes creating a separate Workspace for the dev, stage and prod environments. This ensures that all development is isolated from your production environment.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Unity Catalog Isolation
# MAGIC <div class="pp-uc-isolation">
# MAGIC <style>
# MAGIC .pp-uc-isolation{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .pp-uc-isolation *{box-sizing:border-box;}
# MAGIC .pp-uc-grid{display:grid;grid-template-columns:.75fr 1.25fr;gap:20px;align-items:stretch;}
# MAGIC .pp-uc-text{display:grid;gap:16px;align-content:center;}
# MAGIC .pp-text-card{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #00A972;border-radius:12px;padding:22px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .pp-text-card.orange{border-left-color:#FF5F46;}
# MAGIC .pp-text-card h3{font-size:18px;line-height:1.25;margin:0 0 10px 0;font-weight:850;color:#0B2026;}
# MAGIC .pp-text-card p{font-size:16px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC .pp-uc-diagram{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:16px;display:flex;align-items:center;justify-content:center;}
# MAGIC .pp-uc-diagram img{width:100%;max-height:460px;object-fit:contain;display:block;background:transparent;border-radius:8px;}
# MAGIC @media screen and (max-width:900px){.pp-uc-grid{grid-template-columns:1fr;}.pp-uc-diagram img{max-height:360px;}}
# MAGIC </style>
# MAGIC <div class="pp-uc-grid">
# MAGIC <div class="pp-uc-text">
# MAGIC <div class="pp-text-card orange">
# MAGIC <h3>Storage isolation</h3>
# MAGIC <p>This example separates the storage locations on catalog level.</p>
# MAGIC </div>
# MAGIC <div class="pp-text-card">
# MAGIC <h3>UC Access Control</h3>
# MAGIC <p>Users should only gain access to data/metadata based on agreed access rules.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="pp-uc-diagram">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_planning_the_project/unity_catalog_isolation.png" alt="Unity Catalog Isolation">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Another method for isolating your environments within Databricks is Unity Catalog isolation.</p>
# MAGIC <p>With this method, you create a catalog for each(dev, stage and prod). The dev environment includes the dev data, stage the staging data, and prod the production data.</p>
# MAGIC <p>With this method you can also utilize Unity Catalog access control for your developers, only giving them the required permissions for each.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. Course Project Architecture
# MAGIC
# MAGIC <br>
# MAGIC <div>
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_planning_the_project/course_project_architecture.png" alt="CD High Level Overview" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Now that we have an understanding of our project, our data, and how to isolate environments, let&#x27;s dive into the project setup.</p>
# MAGIC <p>In this project we will create a catalog for each environment: dev, stage, and prod.</p>
# MAGIC <p>The dev catalog will contain a small, static subset of our production data, used for development and initial testing.</p>
# MAGIC <p>The stage catalog will hold a larger subset of production data, enabling more comprehensive testing as we move through our CI/CD process.</p>
# MAGIC <p>Finally, the prod catalog will contain the live production data that we rely on for final operations.</p>
# MAGIC <p>Our workflow starts by developing on the dev data, running unit and integration tests as we progress through the pipeline. The pipeline is set up with Databricks Lakeflow Jobs, which execute the unit tests, Spark Declarative pipeline, and final visualizations.</p>
# MAGIC <p>As we test the pipeline through each stage, we will ensure everything is functioning correctly before deploying to production.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Conclusion
# MAGIC <ul>
# MAGIC <li>Planning the project starts with the deliverable: visualizing health data from daily CSV files through bronze, silver, and gold tables.</li>
# MAGIC <li>Development, staging, and production environments can be isolated using workspaces, storage, and Unity Catalog access controls.</li>
# MAGIC <li>The course project architecture connects catalogs, workflow tests, Spark Declarative Pipeline, and data visualization.</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC