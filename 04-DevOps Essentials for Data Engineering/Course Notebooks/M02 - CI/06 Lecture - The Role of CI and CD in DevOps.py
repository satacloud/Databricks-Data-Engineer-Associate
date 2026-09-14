# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - The Role of CI/CD in DevOps
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you’ll learn how CI/CD practices in DevOps streamline development, testing, and deployment for Data Engineering and Machine Learning projects.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. Understand the core concepts and benefits of CI/CD in modern software development, focusing on automation, continuous integration, and continuous delivery of data pipelines.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Continuous Integration (CI) and Continuous Deployment (CD)
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_role_ci-cd_devops/ci-cd_role.png" alt="Role of CI/CD" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>CI/CD is a key subset of DevOps practices that focuses on automating code integration, testing, and delivery pipelines, including DataOps pipelines. Within the DevOps lifecycle, continuous integration (CI) emphasizes planning, development, environment management, and testing of the pipelines. 
# MAGIC
# MAGIC On the other hand, continuous deployment (CD) focuses on automating release processes, deployment, operation, and monitoring of these pipelines.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. CI/CD Overview
# MAGIC
# MAGIC <div class="cicd-overview">
# MAGIC <style>
# MAGIC .cicd-overview{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cicd-overview *{box-sizing:border-box;}
# MAGIC .cicd-overview-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;align-items:stretch;}
# MAGIC .cicd-overview-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;display:flex;flex-direction:column;}
# MAGIC .cicd-overview-bar{height:8px;background:#FF5F46;}
# MAGIC .cicd-overview-card:nth-child(2) .cicd-overview-bar{background:#00A972;}
# MAGIC .cicd-overview-card:nth-child(3) .cicd-overview-bar{background:#2272B4;}
# MAGIC .cicd-overview-body{padding:20px;display:flex;flex-direction:column;gap:12px;flex:1;}
# MAGIC .cicd-overview-body h3{font-size:20px;line-height:1.25;font-weight:850;margin:0;color:#0B2026;}
# MAGIC .cicd-overview-body ul{margin:0;padding-left:20px;}
# MAGIC .cicd-overview-body li{font-size:16px;line-height:1.45;margin-bottom:8px;color:#0B2026;}
# MAGIC .cicd-overview-placeholder{margin-top:18px;border:2px dashed #DCE0E2;border-radius:14px;background:#F9F7F4;min-height:180px;display:flex;align-items:center;justify-content:center;text-align:center;color:#5A6F77;font-size:18px;font-weight:800;}
# MAGIC @media screen and (max-width:900px){.cicd-overview-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="cicd-overview-grid">
# MAGIC <div class="cicd-overview-card"><div class="cicd-overview-bar"></div><div class="cicd-overview-body"><h3>CI/CD Process</h3><ul><li>Enables development and delivery of software in short, frequent cycles.</li><li>Uses automated pipelines to ensure faster deployment and consistency.</li></ul></div></div>
# MAGIC <div class="cicd-overview-card"><div class="cicd-overview-bar"></div><div class="cicd-overview-body"><h3>CI/CD Overview</h3><ul><li>Automates and streamlines software development processes.</li><li>Improves code quality, speed, and reliability.</li><li>Code is deployed to production through an automated process.</li></ul></div></div>
# MAGIC <div class="cicd-overview-card"><div class="cicd-overview-bar"></div><div class="cicd-overview-body"><h3>CI/CD Adoption</h3><ul><li>Common practice in software development.</li><li>Growing importance in data engineering and data science.</li></ul></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>At a high level, let&#x27;s talk about CI/CD, a practice that is transforming the way we develop and deliver our data pipelines. 
# MAGIC
# MAGIC CI/CD stands for Continuous Integration and Continuous Deployment/Delivery. 
# MAGIC
# MAGIC It’s a process that automates and streamlines key aspects of software development. By automating repetitive tasks, CI/CD helps improve code quality, speed up development cycles, and ensure the reliability of the code being deployed. Using CI/CD, code is deployed to production through an automated process, incorporating a quality control process. 
# MAGIC
# MAGIC So, what exactly does the CI/CD process involve? Essentially, enables teams to develop and deliver software in short, frequent cycles. This is accomplished through using automated pipelines, which ensure that code changes are integrated, tested, and deployed quickly and efficiently. This allows developers to identify issues early and address them before they become bigger problems. 
# MAGIC
# MAGIC While CI/CD has been a standard practice in software development for years, it is now becoming increasingly important in data engineering and data science as well. As these fields evolve, the need for automation and quick, reliable delivery of data-driven applications is growing.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Continuous Integration (CI) High Level Overview
# MAGIC
# MAGIC <div class="cicd-ci-overview">
# MAGIC <style>
# MAGIC .cicd-ci-overview{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cicd-ci-overview *{box-sizing:border-box;}
# MAGIC .ci-callout{background:#ffffff;border:1.5px solid #0B2026;border-radius:2px;padding:18px 22px;text-align:center;font-size:22px;line-height:1.35;margin-bottom:20px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .ci-benefit-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;align-items:stretch;}
# MAGIC .ci-benefit{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:16px;min-height:120px;box-shadow:0 2px 8px rgba(27,49,57,.06);display:flex;align-items:center;justify-content:center;text-align:center;}
# MAGIC .ci-benefit:nth-child(2){border-left-color:#00A972;}
# MAGIC .ci-benefit:nth-child(3){border-left-color:#2272B4;}
# MAGIC .ci-benefit:nth-child(4){border-left-color:#98102A;}
# MAGIC .ci-benefit h3{font-size:17px;line-height:1.3;margin:0;color:#0B2026;font-weight:850;}
# MAGIC .ci-placeholder{margin-top:18px;border:2px dashed #DCE0E2;border-radius:14px;background:#F9F7F4;min-height:180px;display:flex;align-items:center;justify-content:center;text-align:center;color:#5A6F77;font-size:18px;font-weight:800;}
# MAGIC @media screen and (max-width:900px){.ci-benefit-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="ci-callout">CI involves regularly merging code changes from multiple contributors into a central repository and running automated tests to ensure code quality.</div>
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_role_ci-cd_devops/ci_high_level_overview.png" alt="CI High Level Overview" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="ci-benefit-grid"><div class="ci-benefit"><h3>Early Detection of Issues</h3></div><div class="ci-benefit"><h3>Faster Development Cycle</h3></div><div class="ci-benefit"><h3>Improved Collaboration and Code Quality</h3></div><div class="ci-benefit"><h3>Automated Testing and Validation</h3></div></div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>CI involves regularly merging code changes from multiple contributors into a central repository and running automated tests to ensure code quality. Test that fail do not make it into your source code. 
# MAGIC
# MAGIC How the testing and commits to version control is implemented depends on the branching strategy defined by your organization. Deciding on a branching and testing strategy is extremely important. 
# MAGIC
# MAGIC Here are four key benefits of Continuous Integration: 
# MAGIC
# MAGIC First, Early Detection of Issues—by integrating code often, bugs and conflicts are caught early, making them easier to fix. 
# MAGIC
# MAGIC Second, Faster Development Cycle—frequent integration speeds up the delivery of new features and fixes. 
# MAGIC
# MAGIC Third, Improved Collaboration and Code Quality—regular integration leads to cleaner, more modular code and better teamwork. 
# MAGIC
# MAGIC Finally, Automated Testing and Validation—automated tests run with each integration to ensure the code is stable and works with existing features. 
# MAGIC
# MAGIC These benefits—early issue detection, faster delivery, better collaboration, and automated testing—make Continuous Integration essential for smooth development.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. High-level Testing Steps
# MAGIC
# MAGIC <div class="cicd-testing">
# MAGIC <style>
# MAGIC .cicd-testing{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cicd-testing *{box-sizing:border-box;}
# MAGIC .testing-grid{display:grid;grid-template-columns:.9fr 1.1fr;gap:22px;align-items:stretch;}
# MAGIC .pyramid-wrap{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:22px;position:relative;}
# MAGIC .speed-label{position:absolute;right:18px;font-size:16px;font-weight:850;color:#5A6F77;}
# MAGIC .speed-slow{top:22px;}.speed-fast{bottom:22px;}
# MAGIC .pyramid{display:grid;gap:10px;margin:8px 54px;}
# MAGIC .pyramid-layer{color:#ffffff;border-radius:10px;padding:16px;text-align:center;font-size:18px;font-weight:850;box-shadow:0 2px 8px rgba(27,49,57,.08);}
# MAGIC .system{background:#98102A;width:52%;margin:0 auto;}
# MAGIC .integration{background:#2272B4;width:74%;margin:0 auto;}
# MAGIC .unit{background:#00A972;width:100%;margin:0 auto;}
# MAGIC .testing-detail{display:grid;gap:14px;}
# MAGIC .test-card{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:16px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .test-card.blue{border-left-color:#2272B4;}.test-card.green{border-left-color:#00A972;}
# MAGIC .test-card h3{font-size:18px;line-height:1.25;font-weight:850;margin:0 0 8px 0;color:#0B2026;}
# MAGIC .test-card p{font-size:16px;line-height:1.45;margin:0;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.testing-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="testing-grid">
# MAGIC <div class="pyramid-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_role_ci-cd_devops/high_level_testing_strip.png" alt="High Level Testing Overview" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC <div class="testing-detail"><div class="test-card"><h3>System Tests</h3><p>Test the entire application, ensuring that all parts function together in a real-world scenario.<br>Ex: End to end data pipeline in a Job</p></div><div class="test-card blue"><h3>Integration Tests</h3><p>Test the interaction between different components or systems.<br>Ex: Notebooks / SDP/ Jobs interactions</p></div><div class="test-card green"><h3>Unit Tests</h3><p>Test individual functions or methods in isolation. Fast, low cost, high coverage, and automated.<br>Ex: Custom pyspark functions</p></div></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Within CI/CD there are testing steps you should following within what&#x27;s called the testing pyramid. The testing pyramid categorizes different tests, unit tests, integration tests and system tests. 
# MAGIC
# MAGIC The base of the pyramid are unit tests which test individual functions or methods in isolation. Since they are small individual functions, they typically can run quickly, frequently and automatically, ensuring that the functions work as expected. 
# MAGIC
# MAGIC Unit tests form the foundation because they are inexpensive and provide the broadest coverage. For example, testing if a pyspark method works as expected. 
# MAGIC
# MAGIC Next is integration tests test the interaction between different components or systems. These are typically slower and more costly than unit tests, but provide greater assurance that components work together correctly. Within Databricks these typically will revolve around using Notebooks, SDP and or Lakeflow Jobs. For example, testing whether a pyspark method and SDP work correctly. 
# MAGIC
# MAGIC Lastly system tests test the entire application, ensuring that all parts function together in a real-world scenario. These are typically slow, costly, and often run in a production-like environment. For example, for our end to end data pipeline, testing whether the data pipeline works as expected within a Job, creating our desired results.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Continuous Delivery/Deployment (CD) Overview
# MAGIC
# MAGIC <div class="cicd-delivery">
# MAGIC <style>
# MAGIC .cicd-delivery{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cicd-delivery *{box-sizing:border-box;}
# MAGIC .cd-definition{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 22px;margin-bottom:18px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .cd-definition h3{font-size:20px;line-height:1.25;font-weight:850;margin:0 0 8px 0;color:#0B2026;}
# MAGIC .cd-definition p{font-size:17px;line-height:1.45;margin:0;color:#0B2026;}
# MAGIC </style>
# MAGIC <div class="cd-definition"><h3>Continuous Delivery (CD)</h3><p>Automatically pushing changes to staging/pre-production environments with the ability to manually deploy to production at any time.</p></div>
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_role_ci-cd_devops/cd_overview.png" alt="CD High Level Overview" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Continuous Delivery is all about automating the process of pushing changes to staging or pre-production environments. This setup allows for seamless updates and provides the flexibility to manually deploy to production whenever needed, ensuring smooth, controlled releases. For example, after the continuous integration steps are complete and testing has occurred we can decide to deploy our data pipeline to production.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. Continuous Delivery/Deployment (CD) Overview - Continue
# MAGIC
# MAGIC <div class="cicd-deployment">
# MAGIC <style>
# MAGIC .cicd-deployment{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cicd-deployment *{box-sizing:border-box;}
# MAGIC .cd-def-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:18px;align-items:stretch;}
# MAGIC .cd-def{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 22px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .cd-def.gray{background:#EEEDE9;border-left-color:#9AA6AC;color:#5A6F77;opacity:.75;}
# MAGIC .cd-def.green{border-left-color:#00A972;}
# MAGIC .cd-def h3{font-size:20px;line-height:1.25;font-weight:850;margin:0 0 8px 0;color:#0B2026;}
# MAGIC .cd-def p{font-size:17px;line-height:1.45;margin:0;color:#0B2026;}
# MAGIC .cd-def.gray h3,.cd-def.gray p{color:#5A6F77;}
# MAGIC .cicd-deployment img{width:100%;height:auto;display:block;border-radius:10px;background:transparent;}
# MAGIC @media screen and (max-width:900px){.cd-def-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="cd-def-grid">
# MAGIC <div class="cd-def gray">
# MAGIC <h3>Continuous Delivery (CD)</h3>
# MAGIC <p>Automatically pushing changes to staging/pre-production environments with the ability to manually deploy to production at any time.</p>
# MAGIC </div>
# MAGIC <div class="cd-def green">
# MAGIC <h3>Continuous Deployment (CD)</h3>
# MAGIC <p>Fully automated process where each change passing tests is immediately deployed to production.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_role_ci-cd_devops/cd_overview_auto.png" alt="CD Overview" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Continuous Deployment takes automation a step further. Once a change passes all tests, it’s automatically deployed to production, ensuring that new features or fixes are delivered quickly and seamlessly, without manual intervention. 
# MAGIC
# MAGIC For example, if your continuous integration processes are aligned and well implemented, we can automatically deploy our data pipeline from development, to staging, then production, avoiding manual deployment steps. Implementing this technique required well thought out tests to ensure the pipeline should be deployed.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## G. High-Level CI/CD Workflow Overview
# MAGIC
# MAGIC <div>
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_role_ci-cd_devops/high_level_ci-cd_overview.png" alt="High Level CI/CD Workflow" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>In the end, the CI/CD process streamlines development of your data pipelines by automating testing and deployment, leading to faster, more reliable releases. 
# MAGIC
# MAGIC This approach minimizes manual errors, improves collaboration, and ensures high-quality software delivered quickly and consistently.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC