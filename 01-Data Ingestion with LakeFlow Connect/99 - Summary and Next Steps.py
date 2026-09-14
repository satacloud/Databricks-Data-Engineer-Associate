# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary and Next Steps

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <!-- Title -->
# MAGIC <div style="text-align: center; margin-bottom: 28px;">
# MAGIC   <div style="font-size: 15pt; color: #618794; margin-top: 6px;">The complete LakeFlow Connect ingestion toolkit</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- ===== ROW 1: Connectors → Ingestion Methods → Bronze Quality ===== -->
# MAGIC <div style="display: flex; justify-content: center; align-items: stretch; gap: 0;">
# MAGIC
# MAGIC   <!-- 1. LakeFlow Connect Connectors -->
# MAGIC   <div style="flex: 1; max-width: 280px; background: #F9F7F4; border-radius: 10px; padding: 18px 16px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); text-align: center; border-top: 6px solid #4299E0;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">LakeFlow Connect Connectors</div>
# MAGIC     <div style="font-size: 14pt; color: #618794; line-height: 1.6; text-align: left;">
# MAGIC       <div style="margin-bottom: 4px;">Upload Files</div>
# MAGIC       <div style="margin-bottom: 4px;">Standard Connectors</div>
# MAGIC       <div style="margin-bottom: 4px;">Managed Connectors</div>
# MAGIC       <div>Partner Connect</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Arrow -->
# MAGIC   <div style="display: flex; align-items: center; padding: 0 10px; font-size: 26pt; color: #618794;">&#10132;</div>
# MAGIC
# MAGIC   <!-- 2. Ingestion Methods -->
# MAGIC   <div style="flex: 1.1; max-width: 300px; background: #1B5162; border-radius: 10px; padding: 18px 16px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); text-align: center; border-top: 6px solid #FF5F46;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; color: white; margin-bottom: 10px;">Ingestion Methods</div>
# MAGIC     <div style="display: flex; flex-direction: column; gap: 5px;">
# MAGIC       <div style="background: rgba(255,255,255,0.15); border-radius: 5px; padding: 6px 10px; font-size: 14pt; color: white;">CREATE TABLE AS &amp; <code style="color: white;">read_files()</code></div>
# MAGIC       <div style="background: rgba(255,255,255,0.15); border-radius: 5px; padding: 6px 10px; font-size: 14pt; color: white;">COPY INTO</div>
# MAGIC       <div style="background: rgba(255,255,255,0.15); border-radius: 5px; padding: 6px 10px; font-size: 14pt; color: white;">Auto Loader (Python &amp; SQL)</div>
# MAGIC       <div style="background: rgba(255,255,255,0.15); border-radius: 5px; padding: 6px 10px; font-size: 14pt; color: white;">MERGE INTO</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Arrow -->
# MAGIC   <div style="display: flex; align-items: center; padding: 0 10px; font-size: 26pt; color: #618794;">&#10132;</div>
# MAGIC
# MAGIC   <!-- 3. Bronze Quality -->
# MAGIC   <div style="flex: 1; max-width: 280px; background: #F9F7F4; border-radius: 10px; padding: 18px 16px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); text-align: center; border-top: 6px solid #00A972;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Bronze Quality</div>
# MAGIC     <div style="font-size: 14pt; color: #618794; line-height: 1.6;">
# MAGIC       Enrich with <code>_metadata</code> columns, preserve mismatches in <code>_rescued_data</code>, and flatten JSON for analysis-ready UC tables
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- ===== ITERATE LOOP BAR ===== -->
# MAGIC <div style="margin: 16px auto 0 auto; max-width: 1050px; display: flex; align-items: center; justify-content: center; gap: 12px;">
# MAGIC   <div style="flex: 1; height: 4px; background: #FF5F46; border-radius: 2px;"></div>
# MAGIC   <div style="background: #FF5F46; color: white; padding: 6px 22px; border-radius: 20px; font-size: 14pt; font-weight: 700; white-space: nowrap;">
# MAGIC     INGEST &amp; REFINE
# MAGIC   </div>
# MAGIC   <div style="flex: 1; height: 4px; background: #FF5F46; border-radius: 2px;"></div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- ===== COURSE JOURNEY TABLE ===== -->
# MAGIC <div style="margin-top: 20px; background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 14px;">Your Journey Through the Course</div>
# MAGIC
# MAGIC <style>
# MAGIC .journey-table td, .journey-table th {
# MAGIC   font-size: 14pt !important;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <table class="journey-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 50px;">Step</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">What You Did</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Key Takeaway</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">1</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Reviewed data engineering fundamentals, LakeFlow Connect connectors, ingestion methods, Delta Lake, and the Medallion Architecture</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">LakeFlow Connect replaces patchwork ingestion tools with a unified, managed platform</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">2</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Ingested files from cloud storage using <code>CREATE TABLE AS</code>, <code>COPY INTO</code>, and Auto Loader</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Match the ingestion method to the workload &mdash; ad-hoc batch, incremental, or streaming</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">3</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Added <code>_metadata.file_name</code> and <code>_metadata.file_modification_time</code> columns during ingestion</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Bronze tables should always preserve source context for lineage, auditing, and debugging</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">4</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Handled malformed CSV records with the <code>_rescued_data</code> column and built bronze tables in a lab</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Rescued data prevents silent data loss when input does not match the target schema</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">5</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Ingested and flattened JSON using STRING, STRUCT, and VARIANT approaches with <code>schema_of_json</code> and <code>from_json</code></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">STRUCT enforces structure for analytics; VARIANT offers flexible, high-performance semi-structured storage</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">6</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Configured LakeFlow Connect Managed Connectors for enterprise databases and SaaS sources</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Managed connectors deliver fully managed, UI-driven ingestion for sources beyond cloud storage</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">7</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Explored additional features (Lakehouse Federation, Zerobus, Open Sharing, Marketplace) and applied <code>MERGE INTO</code> for upserts</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">MERGE INTO unifies updates, inserts, and deletes into one atomic operation &mdash; ideal for CDC and SCDs</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- ===== CLOSING CALLOUT ===== -->
# MAGIC <div style="margin-top: 20px; padding: 20px 28px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px; text-align: center;">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #FF5F46; margin-bottom: 8px;">Modern ingestion is unified, governed, and incremental.</div>
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.6;">The strongest pipelines use LakeFlow Connect to land governed bronze tables, capture source metadata, rescue non-conforming records, and progressively refine data through the Medallion Architecture &mdash; all inside the Databricks Data Intelligence Platform.</div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC #### The Complete LakeFlow Connect Toolkit
# MAGIC
# MAGIC This visual shows the three pillars of data ingestion you explored across all lectures, demos, and labs. Each builds on the others to deliver governed, analysis-ready bronze tables in the Lakehouse.
# MAGIC
# MAGIC #### LakeFlow Connect Connectors
# MAGIC
# MAGIC LakeFlow Connect is the ingestion layer of the Databricks Data Intelligence Platform &mdash; it replaces the traditional patchwork of ingestion tools with a single managed surface:
# MAGIC
# MAGIC - **Upload Files** &mdash; ad-hoc loads of local files into Unity Catalog volumes or tables
# MAGIC - **Standard Connectors** &mdash; cloud object storage, Kafka, and other sources, supporting batch, incremental batch, and streaming ingestion
# MAGIC - **Managed Connectors** &mdash; fully managed, UI-driven ingestion from enterprise databases and SaaS applications (Salesforce, Workday, SQL Server, and more)
# MAGIC - **Partner Connect** &mdash; validated partner solutions when a native managed connector isn't available
# MAGIC
# MAGIC #### Ingestion Methods
# MAGIC
# MAGIC The right method depends on data volume, freshness, and operational pattern:
# MAGIC
# MAGIC - **CREATE TABLE AS (CTAS)** with `read_files()` &mdash; simple batch ingestion that creates UC tables from raw files. Best for one-time or smaller ad-hoc loads.
# MAGIC - **COPY INTO** &mdash; idempotent, retriable incremental batch ingestion. Skips files already loaded; ideal for scheduled pipelines handling thousands of files.
# MAGIC - **Auto Loader** (Python `cloudFiles` or SQL streaming tables in Declarative Pipelines) &mdash; the most scalable option. Handles automatic schema evolution and processes billions of files for incremental or streaming workloads.
# MAGIC - **MERGE INTO** &mdash; atomic upserts into existing UC tables. The right tool for slowly changing dimensions, incremental loads, and change data capture.
# MAGIC
# MAGIC #### Bronze Quality
# MAGIC
# MAGIC Production bronze tables need more than raw values. Throughout the course you enriched and protected your ingested data:
# MAGIC
# MAGIC - **`_metadata` columns** &mdash; capture `file_name` and `file_modification_time` during ingestion for lineage, auditing, and debugging
# MAGIC - **`_rescued_data` column** &mdash; preserve records that don't match the target schema as JSON-formatted strings, preventing silent data loss
# MAGIC - **JSON handling** &mdash; flatten semi-structured data into queryable columns using STRING, STRUCT (with `schema_of_json` + `from_json`), or the new VARIANT data type
# MAGIC
# MAGIC #### The Ingest & Refine Loop
# MAGIC
# MAGIC The coral bar represents the continuous flow of data through the Medallion Architecture. Bronze tables produced by LakeFlow Connect feed cleaned Silver tables, which feed curated Gold tables &mdash; with each layer refined incrementally and governed by Unity Catalog.
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Apache Spark™ Declarative Pipelines
# MAGIC
# MAGIC <br></br>
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 14px;">Take Your Ingestion Pipelines Further</div>
# MAGIC
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">
# MAGIC     Now that you understand the fundamentals of data ingestion with LakeFlow Connect &mdash; from cloud storage and managed connectors to metadata, rescued data, JSON, and <code style="background: #EEEDE9; padding: 2px 6px; border-radius: 3px;">MERGE INTO</code> &mdash; the next step is to orchestrate end-to-end pipelines with <strong>Spark Declarative Pipelines</strong>.
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.7; margin-bottom: 18px;">
# MAGIC     Declarative Pipelines let you define streaming tables and materialized views with simple SQL or Python while Databricks handles dependency resolution, incremental refresh, data quality, and orchestration. Paired with Auto Loader, they are the recommended approach for production ingestion at scale.
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="display: flex; gap: 10px; margin-top: 12px;">
# MAGIC     <a href="https://docs.databricks.com/aws/en/dlt/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 15pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       AWS &rarr;
# MAGIC     </a>
# MAGIC     <a href="https://learn.microsoft.com/en-us/azure/databricks/dlt/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 15pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       Azure &rarr;
# MAGIC     </a>
# MAGIC     <a href="https://docs.databricks.com/gcp/en/dlt/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 15pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       GCP &rarr;
# MAGIC     </a>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Additional Resources
# MAGIC
# MAGIC Explore the following resources to learn more about LakeFlow Connect and stay up to date with the latest platform updates.
# MAGIC
# MAGIC ### A1. Documentation
# MAGIC
# MAGIC - What is LakeFlow Connect &mdash; Unified ingestion for the Databricks Data Intelligence Platform:
# MAGIC [AWS](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ingestion/lakeflow-connect/)
# MAGIC
# MAGIC - Ingest data from cloud object storage with Auto Loader:
# MAGIC [AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader)
# MAGIC
# MAGIC - Load data with `COPY INTO`:
# MAGIC [AWS](https://docs.databricks.com/aws/en/ingestion/copy-into/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ingestion/copy-into/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ingestion/copy-into/)
# MAGIC
# MAGIC - File metadata column &mdash; capture `_metadata.file_name` and `_metadata.file_modification_time` during ingestion:
# MAGIC [AWS](https://docs.databricks.com/aws/en/ingestion/file-metadata-column) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-metadata-column) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ingestion/file-metadata-column)
# MAGIC
# MAGIC - Rescued data column &mdash; capture records that don't match your schema:
# MAGIC [AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema#what-is-the-rescued-data-column) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema#what-is-the-rescued-data-column) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/schema#what-is-the-rescued-data-column)
# MAGIC
# MAGIC - Upsert into a Delta Lake table using `MERGE`:
# MAGIC [AWS](https://docs.databricks.com/aws/en/delta/merge) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/delta/merge) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/delta/merge)
# MAGIC
# MAGIC ### A2. Blog and Announcements
# MAGIC
# MAGIC - [Announcing the General Availability of Databricks Lakeflow](https://www.databricks.com/blog/announcing-general-availability-databricks-lakeflow) &mdash; Lakeflow Connect, Declarative Pipelines, and Jobs are now GA, with a new IDE for data engineering and expanded ingestion connectors.
# MAGIC
# MAGIC - [Accelerate business insights with Lakeflow Connect, now with a Free Tier](https://www.databricks.com/blog/accelerate-business-insights-lakeflow-connect-now-free-tier) &mdash; Every workspace gets 100 free DBUs per day for managed SaaS and database connectors, enabling up to 100M records ingested daily.
# MAGIC
# MAGIC - [What's New: Zerobus and Other Announcements Improve Data Ingestion for Lakeflow Connect](https://www.databricks.com/blog/whats-new-zerobus-and-other-announcements-improve-data-ingestion-lakeflow-connect) &mdash; Direct-write API for high-throughput event ingestion, plus expanded connectors for databases and enterprise applications.
# MAGIC
# MAGIC - [Lakeflow Connect: Efficient and Easy Data Ingestion using the SQL Server connector](https://www.databricks.com/blog/lakeflow-connect-efficient-and-easy-data-ingestion-using-sql-server-connector) &mdash; A deep dive into setting up and operating the SQL Server managed connector.
# MAGIC
# MAGIC ### A3. Additional Features Outside the Scope of this Course
# MAGIC
# MAGIC - Spark Declarative Pipelines &mdash; declarative, end-to-end streaming and batch pipelines:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dlt/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dlt/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dlt/)
# MAGIC
# MAGIC - Lakehouse Federation &mdash; query external sources without moving the data:
# MAGIC [AWS](https://docs.databricks.com/aws/en/query-federation/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/query-federation/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/query-federation/)
# MAGIC
# MAGIC - Open Sharing &mdash; an open protocol for secure data sharing across platforms:
# MAGIC [AWS](https://docs.databricks.com/aws/en/delta-sharing/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/delta-sharing/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/delta-sharing/)
# MAGIC
# MAGIC - Databricks Marketplace &mdash; discover and share data, AI, and analytics assets:
# MAGIC [AWS](https://docs.databricks.com/aws/en/marketplace/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/marketplace/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/marketplace/)
# MAGIC
# MAGIC - The VARIANT data type &mdash; flexible, high-performance storage for semi-structured data:
# MAGIC [AWS](https://docs.databricks.com/aws/en/semi-structured/variant) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/semi-structured/variant) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/semi-structured/variant)
# MAGIC
# MAGIC - Partner Connect &mdash; integrate validated ingestion partners (Fivetran, Informatica, Qlik, and more):
# MAGIC [AWS](https://docs.databricks.com/aws/en/partner-connect/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/partner-connect/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/partner-connect/)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Next Steps
# MAGIC
# MAGIC Continue building your Databricks skills with additional training and certification resources.
# MAGIC
# MAGIC ### B1. Continue Your Learning
# MAGIC
# MAGIC Expand your data and AI knowledge through Databricks self-paced and instructor-led training. These courses help you deepen your technical skills and gain hands-on experience with the Databricks platform.
# MAGIC
# MAGIC Visit the [Databricks Training and Certification](https://www.databricks.com/learn/training/home)
# MAGIC
# MAGIC - [Build Data Pipelines with Apache Spark Declarative Pipelines](https://www.databricks.com/training/catalog/build-data-pipelines-with-lakeflow-spark-declarative-pipelines-1686) &mdash; Author end-to-end pipelines with the declarative framework.
# MAGIC
# MAGIC - [Stream Processing and Analysis with Apache Spark](https://www.databricks.com/training/catalog/stream-processing-and-analysis-with-apache-spark-3944) &mdash; Go deeper on Spark Structured Streaming and Auto Loader.
# MAGIC
# MAGIC - [Advanced Data Engineering with Databricks](https://www.databricks.com/training/catalog/advanced-data-engineering-with-databricks-971) &mdash; Build production-grade data pipelines at scale with Delta Lake and Structured Streaming.
# MAGIC
# MAGIC ### B2. Earn a Certification
# MAGIC
# MAGIC Validate your Databricks expertise by earning an official credential. Certifications demonstrate your ability to apply Databricks technologies in real-world data and AI workloads.
# MAGIC
# MAGIC Visit the [Databricks Certification and Badging](https://www.databricks.com/learn/training/certification)
# MAGIC
# MAGIC - **Databricks Certified Data Engineer Associate** &mdash; the natural next credential after this course.
# MAGIC - **Databricks Certified Data Engineer Professional** &mdash; for advanced data engineering on the Lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>