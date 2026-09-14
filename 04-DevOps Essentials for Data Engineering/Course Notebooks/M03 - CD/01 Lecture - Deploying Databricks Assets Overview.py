# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Deploying Databricks Assets Overview
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC By implementing continuous deployment, Databricks engineers can significantly improve the efficiency, reliability, and scalability of their data operations. This ultimately leads to faster and more effective data-driven decision-making within their organizations. When we think of CD, we think of
# MAGIC - Faster time to market
# MAGIC - Enhanced collaboration
# MAGIC - Scalability
# MAGIC - Consistency across different Workspaces and environments
# MAGIC - Automation of deployments 
# MAGIC
# MAGIC In this lecture, you’ll get an overview of deploying Databricks assets, exploring deployment options, Declarative Automation Bundles, and how to integrate them into a development and CI/CD workflow.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. Understand and demonstrate the difference between use-cases of the Databricks REST API, CLI, and SDK
# MAGIC 2. Understand how software engineering best practices are supported with DABs
# MAGIC 3. Understand how DABs are used for CI/CD

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Deployment Options
# MAGIC
# MAGIC <div class="dbao-options">
# MAGIC <style>
# MAGIC .dbao-options{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .dbao-options *{box-sizing:border-box;}
# MAGIC .dbao-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;align-items:stretch;}
# MAGIC .dbao-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;min-height:255px;display:flex;flex-direction:column;}
# MAGIC .dbao-bar{height:8px;background:#FF5F46;}
# MAGIC .dbao-card.cli .dbao-bar{background:#2272B4;}
# MAGIC .dbao-card.sdk .dbao-bar{background:#00A972;}
# MAGIC .dbao-card-body{padding:22px;display:flex;flex-direction:column;gap:14px;flex:1;align-items:center;justify-content:flex-start;}
# MAGIC .dbao-icon-img{width:150px;height:150px;object-fit:contain;display:block;margin:0 auto;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC .dbao-option-title{font-size:22px;line-height:1.2;font-weight:800;color:#0B2026;text-align:center;margin:0;}
# MAGIC .dbao-card-text{font-size:16px;line-height:1.5;text-align:center;color:#0B2026;margin:0;}
# MAGIC @media screen and (max-width:900px){.dbao-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="dbao-grid">
# MAGIC <div class="dbao-card rest">
# MAGIC <div class="dbao-bar"></div>
# MAGIC <div class="dbao-card-body">
# MAGIC <img class="dbao-icon-img" src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_deploying_databricks_assets_overview/rest_API_databricks.png" alt="REST API icon">
# MAGIC <h3 class="dbao-option-title">REST API</h3>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="dbao-card cli">
# MAGIC <div class="dbao-bar"></div>
# MAGIC <div class="dbao-card-body">
# MAGIC <img class="dbao-icon-img" src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_deploying_databricks_assets_overview/databricks_CLI.png" alt="Databricks CLI icon">
# MAGIC <h3 class="dbao-option-title">Databricks CLI</h3>
# MAGIC <p class="dbao-card-text">Command-line interface that wraps the REST API<br>Ideal for one-off tasks, experimentation, and shell scripting</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="dbao-card sdk">
# MAGIC <div class="dbao-bar"></div>
# MAGIC <div class="dbao-card-body">
# MAGIC <img class="dbao-icon-img" src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_deploying_databricks_assets_overview/databricks_SDK.png" alt="Databricks SDK icon">
# MAGIC <h3 class="dbao-option-title">Databricks SDKs</h3>
# MAGIC <p class="dbao-card-text">Available for multiple programming languages (Python, Java, Go, R)<br>Allows development of applications, custom Databricks Lakeflow Jobs, and robust error-handling<br>Programmatic way to interact with Databricks resources</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p><strong>Key comparisons:</strong></p>
# MAGIC <ul>
# MAGIC <li><strong>Ease of use:</strong> SDK &gt; CLI &gt; REST API</li>
# MAGIC <li><strong>Flexibility:</strong> REST API &gt; SDK &gt; CLI</li>
# MAGIC </ul>
# MAGIC <p><strong>REST API</strong><br>Postman and Databricks</p>
# MAGIC <p><strong>CLI</strong><br>Command line</p>
# MAGIC <p><strong>SDK</strong><br>Python, Go, R, Java</p>
# MAGIC <p><strong>REST API:</strong> Most flexible but complex—best for custom integrations.</p>
# MAGIC <p><strong>CLI:</strong> Simplifies REST API operations but has limited flexibility.</p>
# MAGIC <p><strong>SDK:</strong> Most developer-friendly—best for embedding Databricks functionality in applications.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Declarative Automation Bundles and Software Engineering Practices
# MAGIC
# MAGIC <div class="dbao-dabs-practices">
# MAGIC <style>
# MAGIC .dbao-dabs-practices{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .dbao-dabs-practices *{box-sizing:border-box;}
# MAGIC .dbao-rec{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 20px;margin-bottom:18px;font-size:18px;line-height:1.45;font-weight:700;}
# MAGIC .dbao-dabs-ribbon{background:#FF5F46;color:#fff;text-align:center;border-radius:10px 10px 0 0;padding:14px 20px;font-size:24px;line-height:1.2;font-weight:800;box-shadow:0 3px 12px rgba(27,49,57,.07);}
# MAGIC .dbao-practice-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:0;border:1px solid #DCE0E2;border-top:0;border-radius:0 0 12px 12px;overflow:hidden;box-shadow:0 3px 12px rgba(27,49,57,.07);}
# MAGIC .dbao-practice{min-height:285px;padding:20px 16px;color:#fff;display:flex;flex-direction:column;justify-content:flex-start;gap:10px;}
# MAGIC .dbao-practice h3{font-size:18px;line-height:1.2;margin:0;font-weight:800;color:#fff;}
# MAGIC .dbao-practice p{font-size:15px;line-height:1.4;margin:0;color:#fff;}
# MAGIC .dbao-practice.version{background:#00A972;}
# MAGIC .dbao-practice.review{background:#98102A;}
# MAGIC .dbao-practice.testing{background:#FFC400;color:#0B2026;}
# MAGIC .dbao-practice.testing h3,.dbao-practice.testing p{color:#0B2026;}
# MAGIC .dbao-practice.ci{background:#668A95;}
# MAGIC .dbao-icon-wrap{margin-top:auto;display:flex;align-items:center;justify-content:center;padding-top:16px;}
# MAGIC .dbao-icon-wrap img{width:82px;height:82px;object-fit:contain;display:block;background:transparent;mix-blend-mode:multiply;border-radius:4px;}
# MAGIC .dbao-se-footer{background:#2272B4;color:#fff;text-align:center;border-radius:0 0 12px 12px;padding:14px 20px;font-size:22px;font-weight:800;margin-top:0;}
# MAGIC @media screen and (max-width:900px){.dbao-practice-grid{grid-template-columns:1fr;}.dbao-practice{min-height:auto;}}
# MAGIC </style>
# MAGIC <div class="dbao-rec">Databricks recommends Declarative Automation Bundles for creating, developing, deploying, and testing jobs and other Databricks resources</div>
# MAGIC <div class="dbao-dabs-ribbon">Declarative Automation Bundles</div>
# MAGIC <div class="dbao-practice-grid">
# MAGIC <div class="dbao-practice version">
# MAGIC <h3>Version Control</h3>
# MAGIC <p>The practice of tracking and managing changes to code and other development artifacts over time.</p>
# MAGIC <div class="dbao-icon-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_icon.png" alt="Databricks icon">
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="dbao-practice review">
# MAGIC <h3>Code Review</h3>
# MAGIC <p>Systematic examination of source code with the goal of identifying and squashing bugs, improve quality, and enforce coding standards.</p>
# MAGIC <div class="dbao-icon-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_icon.png" alt="Databricks icon">
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="dbao-practice testing">
# MAGIC <h3>Testing</h3>
# MAGIC <p>The process of validating expected output from relevant functions and adhering to predetermine requirements.</p>
# MAGIC <div class="dbao-icon-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_icon.png" alt="Databricks icon">
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="dbao-practice ci">
# MAGIC <h3>Continuous Integration</h3>
# MAGIC <p>The process of automating development, testing, and deployment to ensure reliability.</p>
# MAGIC <div class="dbao-icon-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_icon.png" alt="Databricks icon">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="dbao-se-footer">Software Engineering Practices</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Declarative Automation Bundles (DABs) are designed to facilitate the adoption of best practices in software engineering, particularly for data and AI projects. Here we will identify 4 core components of SWE practices that are supported by DABs.</p>
# MAGIC <ul>
# MAGIC <li><strong>Version Control:</strong> How are we tracking changes and maintaining a history of code modifications?</li>
# MAGIC <li><strong>Code Review:</strong> How are maintaining code quality and are we adhering to coding standards?</li>
# MAGIC <li><strong>Testing:</strong> Is our coding behavior consistent and predictable?</li>
# MAGIC <li><strong>Continuous Integration:</strong> Are we automating various processes for integrating code changes within our repository?</li>
# MAGIC </ul>
# MAGIC <p>Declarative Automation Bundles provide a structured approach to managing Databricks projects while adhering to software engineering best practices. By combining infrastructure-as-code principles with automation capabilities, they streamline collaboration, improve quality assurance, and enable efficient delivery of data-driven solutions.</p>
# MAGIC <ul>
# MAGIC <li>DABs integrates seamlessly with Git-Based Lakeflow Jobs, enabling users to version their Databricks resources alongside source code</li>
# MAGIC <li>By treating Databricks resources as code, DABs enables peer review through standard Git Lakeflow Jobs like pull requests</li>
# MAGIC <li>Developers can use the Databricks CLI with DABs to run tests on bundles in isolated environments, ensuring that Lakeflow Jobs behave as intended</li>
# MAGIC <li>DABs integrate with CI/CD tools like GitHub Actions or Azure DevOps to automate validation, deployment, and execution of Databricks Lakeflow Jobs</li>
# MAGIC </ul>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Declarative Automation Bundles
# MAGIC
# MAGIC <div class="dbao-what-dabs">
# MAGIC <style>
# MAGIC .dbao-what-dabs{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .dbao-what-dabs *{box-sizing:border-box;}
# MAGIC .dbao-topline{display:flex;align-items:center;justify-content:space-between;gap:18px;margin-bottom:18px;}
# MAGIC .dbao-top-title{font-size:26px;line-height:1.2;font-weight:800;color:#0B2026;}
# MAGIC .dbao-top-badge{background:#FF5F46;color:#fff;border-radius:999px;padding:10px 18px;font-size:16px;font-weight:800;white-space:nowrap;}
# MAGIC .dbao-dab-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;align-items:stretch;}
# MAGIC .dbao-dab-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;display:flex;flex-direction:column;}
# MAGIC .dbao-dab-card .bar{height:8px;background:#FF5F46;}
# MAGIC .dbao-dab-card:nth-child(2) .bar{background:#2272B4;}
# MAGIC .dbao-dab-card:nth-child(3) .bar{background:#00A972;}
# MAGIC .dbao-dab-body{padding:20px;}
# MAGIC .dbao-dab-body h3{font-size:18px;line-height:1.25;font-weight:800;margin:0 0 14px 0;color:#0B2026;}
# MAGIC .dbao-dab-body p{font-size:16px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC .dbao-emphasis{font-weight:800;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.dbao-dab-grid{grid-template-columns:1fr;}.dbao-topline{flex-direction:column;align-items:flex-start;}}
# MAGIC </style>
# MAGIC
# MAGIC <div class="dbao-dab-grid">
# MAGIC <div class="dbao-dab-card"><div class="bar"></div><div class="dbao-dab-body"><h3>Write code once, deploy everywhere</h3><p><span class="dbao-emphasis">YAML</span> files that specify the <span class="dbao-emphasis">artifacts, resources,</span> and <span class="dbao-emphasis">configurations</span> of a Databricks project. This leads to easy configuration of complex notebook and pipeline interactions and <span class="dbao-emphasis">reproducibility</span> of your Lakeflow Jobs.</p></div></div>
# MAGIC <div class="dbao-dab-card"><div class="bar"></div><div class="dbao-dab-body"><h3>What are Declarative Automation Bundles?</h3><p>DABs are a tool designed to <span class="dbao-emphasis">streamline</span> this process for <span class="dbao-emphasis">Databricks projects</span>. These bundles encapsulate all necessary configurations and artifacts.</p></div></div>
# MAGIC <div class="dbao-dab-card"><div class="bar"></div><div class="dbao-dab-body"><h3>How do bundles work?</h3><p>Bundles provide an exact <span class="dbao-emphasis">definition</span> of Databricks resources that are to be used within your project with support for <span class="dbao-emphasis">validation</span> and <span class="dbao-emphasis">deployment</span> instructions.</p></div></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Create code that can be deployed across multiple environments without modification. This ensures consistency, reduces manual errors, and accelerates delivery by automating deployment processes.</p>
# MAGIC <p>DABs are a tool designed to streamline this process for Databricks projects. They enable developers to define Databricks resources (like jobs, pipelines, and notebooks) as source files and metadata in YAML format.</p>
# MAGIC <p>DABs work by first defining your resources and requirements in a databricks.yml file. You then validate the bundle utilizing the Databricks CLI and deploy to your chosen workspace. Once deployed, Lakeflow Jobs or pipelines described in the bundle can be executed.</p>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Development and CI/CD with DABs
# MAGIC
# MAGIC <div class="dbao-cicd">
# MAGIC <style>
# MAGIC .dbao-cicd{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .dbao-cicd *{box-sizing:border-box;}
# MAGIC .dbao-cicd-grid{display:grid;grid-template-columns:0.35fr 1.65fr;gap:18px;align-items:stretch;}
# MAGIC .dbao-cicd-side{display:grid;gap:14px;align-content:start;}
# MAGIC .dbao-cicd-card{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:16px;box-shadow:0 2px 8px rgba(27,49,57,.06);font-size:16px;line-height:1.45;font-weight:700;}
# MAGIC .dbao-cicd-card.green{border-left-color:#00A972;}
# MAGIC .dbao-cicd-img{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;padding:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);display:flex;align-items:center;justify-content:center;}
# MAGIC .dbao-cicd-img img{width:100%;max-height:560px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC @media screen and (max-width:900px){.dbao-cicd-grid{grid-template-columns:1fr;}.dbao-cicd-img{overflow-x:auto;}.dbao-cicd-img img{min-width:850px;}}
# MAGIC </style>
# MAGIC <div class="dbao-cicd-grid">
# MAGIC <div class="dbao-cicd-side">
# MAGIC <div class="dbao-cicd-card">Development and CI/CD with DABs</div>
# MAGIC <div class="dbao-cicd-card green">Databricks workspaces</div>
# MAGIC </div>
# MAGIC <div class="dbao-cicd-img"><img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_deploying_databricks_assets_overview/development_ci_cd_with_dabs.png" alt="Development and CI/CD with DABs architecture"></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Here we present a high-level view of architecture for development and CI/CD with DABs.</p>
# MAGIC <ul>
# MAGIC <li>If you’re working locally, you build the project bundle with your team using a local environment setup.</li>
# MAGIC <li>Next, you perform version control with project repository, where users commit changes.</li>
# MAGIC <li>Users can manually deploy to test the changes in their development workspace</li>
# MAGIC <li>When users commit changes, a notification is triggered to implement the CI/CD pipeline to staging and production.</li>
# MAGIC </ul>
# MAGIC <p>In summary, Declarative Automation Bundles (DABs) are a tool designed to simplify the management and deployment of data and AI projects on the Databricks platform. They follow an Infrastructure-as-Code (IaC) approach, allowing users to define and manage Databricks resources—such as jobs, pipelines, notebooks, and machine learning models—through YAML configuration files. These bundles streamline collaboration, testing, deployment, and version control across various environments.</p>
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Conclusion
# MAGIC
# MAGIC <ul>
# MAGIC <li>Deployment options include REST API, Databricks CLI, and Databricks SDKs.</li>
# MAGIC <li>Declarative Automation Bundles help define Databricks resources, configurations, and artifacts for repeatable deployments.</li>
# MAGIC <li>DABs support software engineering practices and development and CI/CD workflows across Databricks workspaces.</li>
# MAGIC </ul>
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next demo, you will perform how to deploy the Project.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC