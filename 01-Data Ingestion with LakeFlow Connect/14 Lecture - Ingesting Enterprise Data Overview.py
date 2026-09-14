# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Ingesting Enterprise Data Overview
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how Lakeflow Connect Managed Connectors and Partner Connect streamline enterprise data ingestion by enabling fast and reliable integration from databases and applications into the Databricks Lakehouse through flexible, fully managed, and partner-supported options.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Explain the need for managed connectors** when ingesting data from enterprise databases and SaaS applications beyond cloud object storage
# MAGIC 2. **Describe the benefits of LakeFlow Connect Managed Connectors** including simplified setup, UI-driven configuration, and fully managed infrastructure
# MAGIC 3. **Compare the SaaS and database ingestion architectures** used by LakeFlow Connect Managed Connectors
# MAGIC 4. **Describe Partner Connect** as an alternative for data sources without a native managed connector

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Data Ingestion to Databricks Overview
# MAGIC
# MAGIC So far, we have discussed ingesting data from cloud object storage into Databricks using techniques like CREATE TABLE, COPY INTO, and Auto Loader.
# MAGIC
# MAGIC But what about ingesting data from databases or enterprise applications?
# MAGIC
# MAGIC What techniques can we use to handle those sources?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align:center;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/lecture_enterprise_data/data-ingestion-overview-question.png"
# MAGIC     alt="Data Ingestion to Databricks Overview"
# MAGIC     style="width:900px; max-width:100%; height:auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. LakeFlow Connect Managed Connectors
# MAGIC
# MAGIC The first method for ingesting enterprise data is by using LakeFlow Connect Managed Connectors.
# MAGIC
# MAGIC LakeFlow Connect Managed Connectors are built into Databricks and are designed to simplify the process of ingesting data from a wide variety of enterprise databases and applications.
# MAGIC
# MAGIC They provide a low-code, fully managed experience, reducing the need for manual configuration or custom integration code.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <div style="display: flex; gap: 28px; align-items: flex-start;">
# MAGIC
# MAGIC   <!-- Left: Branding -->
# MAGIC   <div style="flex: 0 0 240px; text-align: center;">
# MAGIC     <div style="background: #FF5F46; color: white; border-radius: 8px; padding: 12px 16px; font-size: 15pt; font-weight: 500; margin-bottom: 14px;">
# MAGIC       LakeFlow Connect <strong>Managed Connectors</strong>
# MAGIC     </div>
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/manager_connectors.png" style="height: 70px;">
# MAGIC     <div style="margin-top: 10px;">
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/icons/databricks_logo.png" style="height: 30px;">
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Right: Benefits -->
# MAGIC   <div style="flex: 1; background: #F9F7F4; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(27,49,57,0.08);">
# MAGIC     <div style="font-size: 16pt; font-weight: 700; margin-bottom: 14px;">BENEFITS</div>
# MAGIC     <ul style="font-size: 14pt; line-height: 1.8; padding-left: 18px; margin: 0;">
# MAGIC       <li><strong>Simplify</strong> the process of ingesting data from a wide variety of enterprise databases and applications</li>
# MAGIC       <li>Provide an easy to use <strong>user interface (UI)</strong> (or you can use the API)</li>
# MAGIC       <li><strong>Fully managed by Databricks</strong>, reducing the need for manual configuration or custom code</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Data Ingestion with Lakeflow Connect Managed Connectors
# MAGIC
# MAGIC With Lakeflow Connect managed connectors, you can easily begin ingesting enterprise data from sources like Workday, Salesforce, PostgreSQL, SQL Server, and more.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align:center;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/lecture_enterprise_data/managed-connectors-data-sources.png"
# MAGIC     alt="LakeFlow Connect Managed Connectors Data Sources"
# MAGIC     style="width:900px; max-width:100%; height:auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Documentation
# MAGIC
# MAGIC Managed connectors in LakeFlow Connect are in various release states.
# MAGIC
# MAGIC Some may be in public preview, others in GA. Be sure to check the official <a href="https://docs.databricks.com/aws/en/release-notes/release-types" style="color:#1976D2;">documentation</a> for the latest details.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC - These are highly efficient, Databricks-managed connectors designed specifically for fast, reliable ingestion into your Lakehouse
# MAGIC - Setup can be done through a <strong>"point and click" UI</strong> or via <strong>API</strong>
# MAGIC - Managed connectors are in <strong>various release states</strong> (some in public preview, others in GA)
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Lakeflow Connect Managed Connectors: SaaS Ingestion 
# MAGIC
# MAGIC Let's start with an overview of the Lakeflow Connect managed connector architecture for SaaS applications.
# MAGIC
# MAGIC Lakeflow Connect enables data ingestion from external, publicly accessible sources such as APIs or OLAP endpoints into Streaming UC tables, using serverless, declarative pipelines. You can setup these pipelines using the user interface (UI) or the API. Managed connectors leverage efficient incremental reads and writes to make data ingestion faster, scalable, and more cost-efficient, while your data remains fresh for downstream consumption
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif; color:#0B2026;">
# MAGIC   <div style="display:flex; gap:24px; align-items:flex-start;">
# MAGIC     <!-- Left: Image -->
# MAGIC     <div style="flex:1; text-align:center;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/lecture_enterprise_data/database_ SaaS_Ingestion.png"
# MAGIC         alt="Saas Ingestion"
# MAGIC         style="max-width:100%; height:auto; border-radius:8px;">
# MAGIC     </div>
# MAGIC     <!-- Right: Text -->
# MAGIC     <div style="flex:1; font-size:12pt; line-height:1.7;">
# MAGIC       Lakeflow Connect collects data from external sources to Streaming UC tables using a serverless compute Declarative Pipelines pipeline:
# MAGIC
# MAGIC - A Lakeflow Serverless Declarative Pipelines job **collects credentials** from Unity Catalog.
# MAGIC - The job **reaches out** to the publicly accessible data source (e.g., API, open OLAP port, etc.).
# MAGIC - The service transforms the data and stores it to a **Streaming UC table**.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Documentation
# MAGIC
# MAGIC View the SaaS managed connector components <a href="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/#saas-connector-components" style="color:#1976D2;">documentation</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC To support this flow, we've introduced a new pipeline type: the managed ingestion pipeline. Its primary role is to connect to public SaaS sources (like Salesforce or Workday), extract the data, and ingest it directly into a streaming table. These pipelines are largely predefined and managed by Databricks, enabling seamless handling of source-specific complexities.
# MAGIC
# MAGIC Finally, for SaaS connectors, all data movement happens in the data plane. The control plane is only used for pipeline setup, monitoring (e.g., reading event logs), and management.
# MAGIC
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Database Ingestion Architecture
# MAGIC
# MAGIC Now let's look at how the architecture changes when using a LakeFlow Connect managed database connector.
# MAGIC
# MAGIC Like with SaaS connectors, this architecture is designed to move data into Streaming UC tables — but this time from external databases rather than public APIs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <div style="text-align:center;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/lecture_enterprise_data/database-ingestion-architecture.png"
# MAGIC     alt="Database Ingestion Architecture"
# MAGIC     style="width:900px; max-width:100%; height:auto;">
# MAGIC </div>
# MAGIC
# MAGIC <!-- Steps -->
# MAGIC <div style="margin-top: 18px; background: #F9F7F4; border-radius: 10px; padding: 16px; box-shadow: 0 2px 8px rgba(27,49,57,0.08);">
# MAGIC   <div style="font-size: 14pt; margin-bottom: 10px;">LakeFlow Connect collects data from <strong>external databases</strong> to Streaming UC tables:</div>
# MAGIC   <ol style="font-size: 13pt; line-height: 1.8; padding-left: 22px; margin: 0;">
# MAGIC     <li>The classic compute Declarative Pipelines job <strong>collects credentials</strong> from UC</li>
# MAGIC     <li>It uses the credentials to <strong>connect and collect data</strong> from your Database sources</li>
# MAGIC     <li>The latest <strong>state and staging data are saved</strong> to your Unity Catalog volume</li>
# MAGIC     <li>A Serverless Declarative Pipelines job <strong>processes the collected data</strong> to your Streaming UC tables</li>
# MAGIC   </ol>
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Documentation
# MAGIC
# MAGIC View the database managed connector components <a href="https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/#database-connector-components" style="color:#1976D2;">documentation</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC The database ingestion architecture is more involved than SaaS because it needs to connect to databases that may be on-prem or in a private cloud.
# MAGIC
# MAGIC <strong>Key Components Introduced</strong>
# MAGIC
# MAGIC We're introducing two new architectural elements here:
# MAGIC <ol>
# MAGIC   <li><strong>Ingestion Gateway:</strong>
# MAGIC     <ul>
# MAGIC       <li>A dedicated pipeline that connects to the database to extract:
# MAGIC         <ul>
# MAGIC           <li>Metadata</li>
# MAGIC           <li>Snapshots</li>
# MAGIC           <li>Change logs — It stages all of this in the Unity Catalog (UC) volume.</li>
# MAGIC         </ul>
# MAGIC       </li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <li><strong>Unity Catalog Volume:</strong>
# MAGIC     <ul>
# MAGIC       <li>This acts as the intermediate staging layer, enabling the next pipeline to pick up and stream data.</li>
# MAGIC       <li>It's secured using standard UC mechanics, and by default, access is limited to the user running the pipeline.</li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC </ol>
# MAGIC
# MAGIC So why did we even add this gateway step?
# MAGIC
# MAGIC For starters, it helps with networking. Many customers have databases that are (1) sitting behind a firewall, (2) not publicly accessible to the internet, and so on. If you don't have an option like Private Link, we can deploy the gateway inside of your network.
# MAGIC
# MAGIC The gateway also helps us limit the load that we're placing on the database. After all, you probably want to limit the number of direct connections to your data source. By splitting out the gateway from the pipeline, we'll be able to have one gateway speak to the database — and then fan out to N pipelines for that scalability. (For context, this isn't a problem in a SaaS connector because the load is typically controlled with API limits. But even there, we're using those limits as efficiently as we can.)
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Data Ingestion with Partner Connect
# MAGIC
# MAGIC If there is no managed connector available for your specific data source, you can also use Partner Connect.

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Partner Connect Overview
# MAGIC
# MAGIC Partner Connect lets you create trial accounts with select Databricks technology partners and connect your Databricks workspace to partner solutions from the Databricks UI. This allows you to try partner solutions using your data in the Databricks lakehouse, then adopt the solutions that best meet your business needs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width:1200px; margin:0 auto; font-family:'Segoe UI',sans-serif; color:#0B2026;">
# MAGIC   <div style="display:flex; gap:24px; align-items:flex-start;">
# MAGIC     <!-- Left: Image -->
# MAGIC     <div style="flex:1; text-align:center;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/lecture_enterprise_data/managed_connectors_left.png"
# MAGIC         alt="Left image"
# MAGIC         style="max-width:100%; height:auto; border-radius:8px;">
# MAGIC     </div>
# MAGIC     <!-- Middle: Text -->
# MAGIC     <div style="flex:1; font-size:12pt; line-height:1.7;">
# MAGIC       <ul>
# MAGIC       <li>Partner Connect allows you to create trial accounts with select Databricks technology partners.
# MAGIC       <li>It enables you to connect your Databricks workspace to partner solutions directly from the Databricks UI.
# MAGIC       <li>This allows you to test partner solutions using your data in the Databricks Lakehouse.
# MAGIC       <li>You can then evaluate and adopt the solutions that best meet your business needs.
# MAGIC       </li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <!-- Right: Image -->
# MAGIC     <div style="flex:1; text-align:center;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/lecture_enterprise_data/partner_connect_right.png"
# MAGIC         alt="Right image"
# MAGIC         style="max-width:100%; height:auto; border-radius:8px;">
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Partner Connect Ecosystem
# MAGIC
# MAGIC Our ingestion partners remain vital. They offer a really wide range of connectors, and those connectors tend to have deep functionality that’s maintained by dedicated teams of experienced engineers.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align:center;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/lecture_enterprise_data/partner-connect-ecosystem.png"
# MAGIC     alt="Partner Connect Ecosystem"
# MAGIC     style="width:900px; max-width:100%; height:auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Documentation
# MAGIC
# MAGIC For more information, refer to the links below:
# MAGIC <br>
# MAGIC <ul>
# MAGIC   <li><a href="https://docs.databricks.com/aws/en/partner-connect/" style="color:#1976D2;">What is Databricks Partner Connect documentation</a></li>
# MAGIC   <li><a href="https://www.databricks.com/partnerconnect#partner-demos" style="color:#1976D2;">Partner Connect</a></li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC And we want you to continue having that choice, so we will continue to work with those partners to offer as many high-quality connectors as possible. Even when there is a native connector for a given source, we’ll continue to support that optionality.
# MAGIC
# MAGIC We’re just now adding the critical connectors that customers have requested. It’s about customer choice.
# MAGIC
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Conclusion
# MAGIC
# MAGIC In this lecture, you learned how to ingest enterprise data into Databricks beyond cloud object storage:
# MAGIC
# MAGIC - **LakeFlow Connect Managed Connectors** simplify ingestion from enterprise databases and SaaS applications with a fully managed, UI-driven or API-driven experience.
# MAGIC - **SaaS ingestion architecture**: A serverless Declarative Pipelines job collects credentials from Unity Catalog, reaches out to the public data source, and writes to Streaming UC tables.
# MAGIC - **Database ingestion architecture**: Uses an additional Ingestion Gateway (classic compute) to connect to private databases, with staging and state management in Unity Catalog volumes, before a serverless pipeline writes to Streaming UC tables.
# MAGIC - **Partner Connect** provides an alternative when no native managed connector is available, offering a rich ecosystem of partner solutions (Informatica, Qlik, Rivery, Alteryx, Prophecy, Fivetran, and more).
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next section, you will work hands-on with LakeFlow Connect Managed Connectors to set up enterprise data ingestion pipelines.

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