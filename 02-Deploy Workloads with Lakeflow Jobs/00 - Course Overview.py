# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploy Workloads with Lakeflow Jobs
# MAGIC
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Deploy Workloads with Lakeflow Jobs course teaches how to orchestrate and automate data, analytics, and AI workflows using Lakeflow Jobs as a unified orchestration platform within the Databricks ecosystem. 
# MAGIC - You will learn to design and implement data workloads using Directed Acyclic Graphs (DAGs), configure various scheduling options, and implement advanced workflow features such as conditional task execution, run-if dependencies, and for each loops. 
# MAGIC - The course covers best practices for creating robust, production-ready pipelines with proper compute selection, modular orchestration, error handling techniques, and fault-tolerant design-all natively integrated within the Databricks Data Intelligence Platform.
# MAGIC
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC - Understand the role of Lakeflow Jobs within the Databricks ecosystem as a unified orchestration platform for data, analytics, and AI workloads.
# MAGIC - Design and implement data workloads using Directed Acyclic Graphs (DAGs), demonstrating the relationship between jobs, tasks, and dependencies.
# MAGIC - Configure various job scheduling options including manual, scheduled, file arrival, and continuous triggers to automate workflow execution.
# MAGIC - Implement advanced workflow features such as conditional task execution, run-if dependencies, and repair runs to create robust, fault-tolerant data pipelines.
# MAGIC - Apply best practices for production workloads including selecting appropriate compute options, implementing modular orchestration, and utilizing proper error handling techniques.
# MAGIC
# MAGIC
# MAGIC ##### Course update and version can be found in the `Version Info` file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC - Completion of the course **Get Started with Databricks for Data Engineering**, or a solid understanding of the Databricks Data Intelligence Platform
# MAGIC - Basic Understanding of topics like navigating a **Databricks Workspace, Apache Spark, Delta Lake, Medallion Architecture, and Unity Catalog**.
# MAGIC - Familiarity with **python/pyspark** 
# MAGIC - Experience in writing intermediate-level **SQL queries**."

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Workspace Setup Information
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Databricks Provided Vocareum Workspace (Recommended)
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
# MAGIC - The classroom setup scripts create the schemas, volumes, and source data required for the demos and labs.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the **Data Engineer Learning** Path by Databricks Academy.
# MAGIC
# MAGIC | # | Module Name |
# MAGIC | --- | --- |
# MAGIC | 01 | [01 Lecture - Introduction to Data Engineering in Databricks]($./01 Lecture - Introduction to Data Engineering in Databricks) |
# MAGIC | 02 | [02 Lecture - Lakeflow Jobs Core Components]($./02 Lecture - Lakeflow Jobs Core Components) |
# MAGIC | 03 | [03 Lecture - Course Project Overview]($./03 Lecture - Course Project Overview) |
# MAGIC | 04 | [04 Demo - Creating a Job Using the Lakeflow Jobs UI]($./04 Demo - Creating a Job Using the Lakeflow Jobs UI) |
# MAGIC | 05L | [05 Lab - Create your First Job]($./05 Lab - Create your First Job) |
# MAGIC | 06 | [06 Lecture - Creating and Scheduling Jobs]($./06 Lecture - Creating and Scheduling Jobs) |
# MAGIC | 07 | [07 Demo - Automating Workloads with Scheduling and Triggers]($./07 Demo - Automating Workloads with Scheduling and Triggers) |
# MAGIC | 08 | [08 Lecture - Conditional and Iterative Tasks]($./08 Lecture - Conditional and Iterative Tasks) |
# MAGIC | 09 | [09 Demo - Building Dynamic Workloads with Advanced Tasks]($./09 Demo - Building Dynamic Workloads with Advanced Tasks) |
# MAGIC | 10L | [10 Lab - Adding If-Else Task and Automating your Job]($./10 Lab - Adding If-Else Task and Automating your Job) |
# MAGIC | 11 | [11 Lecture - Handling Task Failures and Monitoring Jobs Performance]($./11 Lecture - Handling Task Failures and Monitoring Jobs Performance) |
# MAGIC | 12 | [12 Demo - Monitoring and Repairing Task]($./12 Demo - Monitoring and Repairing Task) |
# MAGIC | 13 | [13 Lecture - Lakeflow Jobs in Production and Best Practices]($./13 Lecture - Lakeflow Jobs in Production and Best Practices) |
# MAGIC | 14 | [14 - Summary and Next Steps]($./14 - Summary and Next Steps) |
# MAGIC | 15L | [15 BONUS LAB - Modular Orchestration]($./15 BONUS LAB - Modular Orchestration) |
# MAGIC ---
# MAGIC ### Workspace Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use the following Databricks runtime: **`Serverless V5`**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>