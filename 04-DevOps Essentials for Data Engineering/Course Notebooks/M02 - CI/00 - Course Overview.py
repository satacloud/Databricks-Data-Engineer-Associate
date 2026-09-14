# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # DevOps Essentials for Data Engineering
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This course explores software engineering best practices and DevOps principles, specifically designed for data engineers working with Databricks. Participants will build a strong foundation in key topics such as code quality, version control, documentation, and testing. The course emphasizes DevOps, covering core components, benefits, and the role of continuous integration and delivery (CI/CD) in optimizing data engineering workflows.
# MAGIC
# MAGIC You will learn how to apply modularity principles in PySpark to create reusable components and structure code efficiently. Hands-on experience includes designing and implementing unit tests for PySpark functions using the pytest framework, followed by integration testing for Databricks data pipelines with Spark Declarative Pipeline and Jobs to ensure reliability.
# MAGIC
# MAGIC The course also covers essential Git operations within Databricks, including using Databricks Git Folders to integrate continuous integration practices. Finally, you will take a high level look at various deployment methods for Databricks assets, such as REST API, CLI, SDK, and Declarative Automation Bundles (DABs), providing you with the knowledge of techniques to deploy and manage your pipelines.
# MAGIC
# MAGIC By the end of the course, you will be proficient in software engineering and DevOps best practices, enabling you to build scalable, maintainable, and efficient data engineering solutions.
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC
# MAGIC By the end of this course, you will be able to:
# MAGIC
# MAGIC - Explain the core principles of software engineering best practices, including code quality, version control, documentation, and testing.
# MAGIC - Explain the principles of DevOps including core components, benefits, and the role of continuous integration and continuous delivery (CI/CD) in DevOps.
# MAGIC - Apply principles of modularity to organize PySpark code into reusable functions and components.
# MAGIC - Design and implement effective unit tests and integration tests for your Databricks data pipelines.
# MAGIC - Apply basic Git operations in Databricks using Git Folders to implement CI practices effectively.
# MAGIC - Explain the different methods for deploying Databricks assets, including REST API, CLI, SDK, and Declarative Automation Bundles (DABs).
# MAGIC
# MAGIC ##### Course update and version can be found in the `Version Info` file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC - **Proficient knowledge** of the Databricks platform, including experience with Databricks Workspaces, Apache Spark, Delta Lake and the Medallion Architecture, Unity Catalog, Spark Declarative Pipeline, and Jobs. A basic understanding of Git version control is also required.
# MAGIC - Experience ingesting and transforming data, with proficiency in PySpark for data processing and DataFrame manipulations. Additionally, candidates should have experience writing intermediate level SQL queries for data analysis and transformation.
# MAGIC - Knowledge of Python programming, with proficiency in writing intermediate level Python code, including the ability to design and implement functions and classes. Users should also be skilled in creating, importing, and effectively utilizing Python packages.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Course Agenda
# MAGIC
# MAGIC This course is part of the **Data Engineer Learning Path** by Databricks Academy. It is organized into two modules that follow the DevOps lifecycle in the order you would adopt it on a real project.
# MAGIC
# MAGIC | Module | What You Will Cover |
# MAGIC |--------|---------------------|
# MAGIC | **M02 - CI (Continuous Integration)** | Software engineering best practices, modularizing PySpark code, DevOps fundamentals, the role of CI/CD, planning and isolating project environments, unit testing with `pytest`, integration testing with SDP and Lakeflow Jobs, and version control with Databricks Git Folders and GitHub. |
# MAGIC | **M03 - CD (Continuous Deployment)** | A high-level look at deploying Databricks assets — comparing the REST API, CLI, SDK, and Declarative Automation Bundles (DABs), and how DABs fit into a development and CI/CD workflow. |
# MAGIC
# MAGIC The modules are built around a single running project — an end-to-end medallion pipeline (bronze → silver → gold) that ingests data, transforms it with PySpark, and produces a final visualization. You progressively harden that project by applying each DevOps discipline to it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Workspace Setup Information
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Databricks Provided Vocareum Workspace (Recommended)
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - If you are running this notebook in a <strong>Databricks Academy provided Vocareum workspace</strong>, your Unity Catalog catalog is already created for you.
# MAGIC
# MAGIC - Your catalog name matches your Vocareum username and looks like: <strong>labuser12345</strong> (series of unique numbers)
# MAGIC
# MAGIC - If a <strong>Marketplace</strong> dataset is required, the share is already installed and available in the workspace.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC