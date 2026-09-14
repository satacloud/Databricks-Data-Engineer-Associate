# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion with LakeFlow Connect
# MAGIC
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This course provides a comprehensive introduction to Lakeflow Connect, a scalable and simplified solution for ingesting data into Databricks from a wide range of sources. You’ll begin by exploring the different types of Lakeflow Connect connectors (Standard and Managed) and learn various data ingestion techniques, including batch, incremental batch, and streaming ingestion. You'll also review the key benefits of using UC tables and the Medallion architecture
# MAGIC
# MAGIC Next, you’ll develop practical skills for ingesting data from cloud object storage using Lakeflow Connect Standard Connectors. This includes working with methods such as CREATE TABLE AS SELECT (CTAS), COPY INTO, and Auto Loader, with an emphasis on the benefits and considerations of each approach. You’ll also learn how to append metadata columns to your bronze-level tables during ingestion into the Databricks Data Intelligence Platform. The course then covers how to handle records that don’t match your table schema using the rescued data column, along with strategies for managing and analyzing this data. You’ll also explore techniques for ingesting and flattening semi-structured JSON data.
# MAGIC
# MAGIC Following this, you’ll explore how to perform enterprise-grade data ingestion using Lakeflow Connect Managed Connectors to bring in data from databases and Software-as-a-Service (SaaS) applications. The course also introduces Partner Connect as an option for integrating partner tools into your ingestion workloads.
# MAGIC
# MAGIC Finally, the course wraps up with alternative ingestion strategies, including MERGE INTO operations and leveraging the Databricks Marketplace, equipping you with a strong foundation to support modern data engineering use cases.
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC - Describe Lakeflow Connect as a scalable and simplified solution for data ingestion into Databricks from a variety of sources.  
# MAGIC - Review the benefits of UC tables and the Medallion architecture.  
# MAGIC - Demonstrate how to ingest data from cloud object storage into UC tables using CREATE TABLE AS, COPY INTO, and Auto Loader, including capturing input file metadata in Bronze layer tables.  
# MAGIC - Explain how rescued columns are used during ingestion to manage malformed records.  
# MAGIC - Illustrate techniques for ingesting and flattening semi structured JSON data from cloud storage.  
# MAGIC - Describe available options for ingesting data from enterprise systems using Lakeflow Connect Managed Connectors.  
# MAGIC - Discuss alternative ingestion methods such as MERGE INTO and Databricks Marketplace.
# MAGIC
# MAGIC
# MAGIC ##### Course update and version can be found in the `Version Info` file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC - Basic understanding of the Databricks Data Intelligence platform, including Databricks Workspaces, Apache Spark, Delta Lake, the Medallion Architecture and Unity Catalog.
# MAGIC - Basic understanding of data ingestion workflows (batch, streaming, incremental) and general ETL principles
# MAGIC - Experience working with various file formats (e.g., Parquet, CSV, JSON, TXT).
# MAGIC - Proficiency in SQL and Python.
# MAGIC - Familiarity with running code in Databricks notebooks.

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
# MAGIC - If a <strong>Marketplace</strong> dataset is required, the share is already installed and available in the workspace.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Course Agenda
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC
# MAGIC <style>
# MAGIC .agenda-table td, .agenda-table th {
# MAGIC   font-size: 14pt !important;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <table class="agenda-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 50px;">#</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 80px;">Type</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Module Name</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">1</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Data Engineering in Databricks</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">2</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Exploring the Lab Environment</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">3</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Data Ingestion from Cloud Storage</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">4</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Data Ingestion with CREATE TABLE AS and COPY INTO</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">5</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Create Streaming Tables with SQL using Auto Loader</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">6</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Appending Metadata Columns on Ingest</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">7</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Adding Metadata Columns During Ingestion</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">8</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Working with the Rescued Data Column</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">9</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Handling CSV Ingestion with the Rescued Data Column</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">10</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e8f5e9; color: #2e7d32; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lab</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Creating Bronze Tables from CSV Files</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">11</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Ingesting Semi-Structured Data JSON</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">12</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Ingesting JSON files with Databricks</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">13</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e8f5e9; color: #2e7d32; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lab</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Creating Bronze Tables from JSON Files</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">14</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Ingesting Enterprise Data Overview</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">15</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Enterprise Data Ingestion with Lakeflow Connect</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">16</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Additional Features and Ingesting into Existing UC Tables</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">17</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">BONUS - Data Ingestion with MERGE INTO</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">18</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Summary</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Summary and Next Steps</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
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
# MAGIC - Use Databricks Runtime version: **`Serverless V5`** for running all demo and lab notebooks.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/>
# MAGIC <a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> |
# MAGIC <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> |
# MAGIC <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>