-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Appendix - Python Auto Loader
-- MAGIC ### Extra material, not part of a live teach.
-- MAGIC
-- MAGIC In this demonstration we will introduce running Auto Loader in Python for incremental ingestion. In this example you will be execute Auto Loader manually to incrementally ingest the data.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
-- MAGIC
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #f44336;
-- MAGIC   background: #ffebee;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select Serverless Compute</strong>
-- MAGIC   <div style="color:#333;">
-- MAGIC
-- MAGIC Before starting this notebook, select the required compute environment listed below.
-- MAGIC
-- MAGIC - **Serverless Compute, Version 5**  
-- MAGIC ![Serverless Select](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/common/select-serverless.png)
-- MAGIC <br></br>
-- MAGIC   - How to select an environment version:
-- MAGIC [AWS](https://docs.databricks.com/aws/en/compute/serverless/dependencies#-select-an-environment-version) |
-- MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/dependencies#-select-an-environment-version) |
-- MAGIC [GCP](https://docs.databricks.com/gcp/en/compute/serverless/dependencies#-select-an-environment-version)
-- MAGIC
-- MAGIC **NOTE:**  This notebook was **developed and tested using Serverless V5**. Other compute options may work but are not guaranteed to behave the same or support all features demonstrated.
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this notebook.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-Auto-Loader

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View the available file(s) in your `/Volumes/labuser123_678/data_ingestion/csv_files_autoloader_source` volume. Notice only one file exists.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'LIST "/Volumes/{my_catalog}/data_ingestion/csv_files_autoloader_source"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC [What is Auto Loader?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
-- MAGIC
-- MAGIC [Using Auto Loader with Unity Catalog](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog)
-- MAGIC
-- MAGIC [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options#csv-options)
-- MAGIC
-- MAGIC Below is an example of Auto Loader.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## Create a volume to store the Auto Loader checkpoint files
-- MAGIC spark.sql(f'CREATE VOLUME IF NOT EXISTS {my_catalog}.data_ingestion.auto_loader_files')
-- MAGIC
-- MAGIC ## Set checkpoint location to the volume from above
-- MAGIC checkpoint_file_location = f'/Volumes/{my_catalog}/data_ingestion/auto_loader_files'
-- MAGIC
-- MAGIC ## Incrementally (or stream) data using Auto Loader
-- MAGIC (spark
-- MAGIC  .readStream
-- MAGIC    .format("cloudFiles")
-- MAGIC    .option("cloudFiles.format", "csv")
-- MAGIC    .option("header", "true")
-- MAGIC    .option("sep", "|")
-- MAGIC    .option("inferSchema", "true")
-- MAGIC    .option("cloudFiles.schemaLocation", f"{checkpoint_file_location}")
-- MAGIC    .load(f"/Volumes/{my_catalog}/data_ingestion/csv_files_autoloader_source/")
-- MAGIC  .writeStream
-- MAGIC    .option("checkpointLocation", f"{checkpoint_file_location}")
-- MAGIC    .trigger(once=True)
-- MAGIC    .toTable(f"{my_catalog}.data_ingestion.python_csv_autoloader")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View the new table **python_csv_autoloader**. Notice the data was ingested into a table and contains 3,149 rows.

-- COMMAND ----------

SELECT *
FROM python_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Add a CSV file to your `/Volumes/dbacademy/labuser/csv_files_autoloader_source/` volume.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC copy_files(copy_from = '/Volumes/dbacademy_ecommerce/v01/raw/sales-csv/', copy_to = f"/Volumes/{my_catalog}/data_ingestion/csv_files_autoloader_source/", n=2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Confirm your volume contains 2 CSV files.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'LIST "/Volumes/{my_catalog}/data_ingestion/csv_files_autoloader_source"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Rerun your Auto Loader ingestion from above (pasted for you below) to incrementally ingest only the new file.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC checkpoint_file_location = f'/Volumes/{my_catalog}/data_ingestion/auto_loader_files'
-- MAGIC
-- MAGIC (spark
-- MAGIC  .readStream
-- MAGIC    .format("cloudFiles")
-- MAGIC    .option("cloudFiles.format", "csv")
-- MAGIC    .option("header", "true")
-- MAGIC    .option("sep", "|")
-- MAGIC    .option("inferSchema", "true")
-- MAGIC    .option("cloudFiles.schemaLocation", f"{checkpoint_file_location}")
-- MAGIC    .load(f"/Volumes/{my_catalog}/data_ingestion/csv_files_autoloader_source/")
-- MAGIC  .writeStream
-- MAGIC    .option("checkpointLocation", f"{checkpoint_file_location}")
-- MAGIC    .trigger(once=True)
-- MAGIC    .toTable(f"{my_catalog}.data_ingestion.python_csv_autoloader")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View the **python_csv_autoloader** table. Notice that it now contains 6,081 rows.

-- COMMAND ----------

SELECT * 
FROM python_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View the history of the **python_csv_autoloader** table. Notice the two **STREAMING UPDATES**. In the **operationMetrics** column you can see how many rows were ingestion in each streaming update. Notice that it only is ingestion new files.

-- COMMAND ----------

DESCRIBE HISTORY python_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drop the **python_csv_autoloader** table.

-- COMMAND ----------

DROP TABLE IF EXISTS python_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>