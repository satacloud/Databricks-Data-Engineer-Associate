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
-- MAGIC # 02 - Exploring the Lab Environment
-- MAGIC
-- MAGIC This demonstration is meant as a review for understanding data objects registered to Unity Catalog (UC) using Databricks. UC is a unified data governance solution designed to centralize and streamline the management of data, metadata, and access control across multiple Databricks workspaces. It provides interoperability across lakehouse formats like Delta lake and Apache Iceberg in addition to providing open APIs and built-in governance for data and AI applications. 
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Identify and display available Unity Catalog objects, including catalogs, schemas, volumes, and tables within Databricks.
-- MAGIC - Execute SQL queries to display data directly from files in cloud storage.
-- MAGIC
-- MAGIC **References** For more additional reading and learning, check out the [official UC GitHub repository](https://github.com/unitycatalog/unitycatalog) and [this video on UC on Databricks](https://www.databricks.com/resources/demos/videos/data-governance/unity-catalog-overview).

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
-- MAGIC 1. Run the following cell to configure your working environment for this notebook.
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the following to explore your **data_ingestion** schema using the Catalog UI on the left.
-- MAGIC
-- MAGIC    a. In the left navigation bar, select the catalog icon:  ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/catalog_icon.png)
-- MAGIC
-- MAGIC    b. Locate the catalog that starts with **labuser123_678*** (It should be your labusername) and expand the catalog. 
-- MAGIC
-- MAGIC    c. Expand the **data_ingestion** schema (database). This is your schema for the course.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. We want to modify our default catalog and default schema to use **labuser123_678** and our **data_ingestion** schema to avoid writing the three-level namespace every time we query and create tables in this course.
-- MAGIC
-- MAGIC     However, before we proceed, note that each of us has a different catalog name. Your specific catalog name has been stored dynamically in the SQL variable `my_catalog` during the classroom setup script.
-- MAGIC
-- MAGIC     Run the code below and confirm that SQL variable matches your specific catalog name (e.g., **labuser123_678**).

-- COMMAND ----------

values(my_catalog)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Let's modify our default catalog and schema using the `USE CATALOG` and `USE SCHEMA` statements. This eliminates the need to specify the three-level name for objects (i.e., catalog.schema.object).
-- MAGIC
-- MAGIC     - `USE CATALOG` – Sets the current catalog.
-- MAGIC
-- MAGIC     - `USE SCHEMA` – Sets the current schema.
-- MAGIC
-- MAGIC     **NOTE:** Since our dynamic catalog name is stored in the SQL variable `my_catalog` as a string, we will need to use the `IDENTIFIER` clause to interpret the constant string in our variable as a catalog name. The `IDENTIFIER` clause can interpret a constant string as any of the following:
-- MAGIC     - Relation (table or view) name
-- MAGIC     - Function name
-- MAGIC     - Column name
-- MAGIC     - Field name
-- MAGIC     - Schema name
-- MAGIC     - Catalog name
-- MAGIC
-- MAGIC     [IDENTIFIER clause documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names-identifier-clause?language=SQL)
-- MAGIC
-- MAGIC     Run the following cell to set and view your default catalog and schema. Confirm that your default catalog is **labuser123_678*** and your schema is **data_ingestion** (this uses the `my_catalog` variable created in the classroom setup script).
-- MAGIC
-- MAGIC **NOTE:** Alternatively, you can simply add your catalog name without using the `IDENTIFIER` clause.
-- MAGIC

-- COMMAND ----------

-- Change the default catalog/schema
USE CATALOG IDENTIFIER(my_catalog);
USE SCHEMA data_ingestion;


-- View current catalog and schema
SELECT 
  current_catalog(), 
  current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Inspecting and Referencing Unity Catalog Objects

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Catalogs, Schemas, Volumes, and Tables
-- MAGIC In Unity Catalog, all metadata is registered in a metastore. The hierarchy of database objects in any Unity Catalog metastore is divided into three levels, represented as a three-level namespace (example, `<catalog>.<schema>.<object>`) when you reference tables, views, volumes, models, and functions.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Catalogs
-- MAGIC
-- MAGIC Use the `SHOW SCHEMAS` statement to view available schemas in the **labuser123_678*** catalog. Run the cell and view the results. Notice that your **data_ingestion** schema is within the **labuser123_678*** catalog. Since **labuser123_678*** is now the default catalog using the above command, we do not need to explicitly mention the catalog name when looking for schemas.

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Schemas
-- MAGIC Run the `DESCRIBE SCHEMA EXTENDED` statement to see information about your **data_ingestion** schema (database) that was created for you within the **labuser123_678*** catalog. In the output below, your schema name is in the row called *Namespace Name*.  
-- MAGIC
-- MAGIC **NOTE:** Remember, we are using the `IDENTIFIER` clause to dynamically reference your specific schema name in the lab, since each user will have a different schema name. Alternatively, you can type in the schema name.

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED data_ingestion;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. Tables
-- MAGIC Use the `DESCRIBE TABLE EXTENDED` statement to describe the table `mytable`.
-- MAGIC
-- MAGIC Run the cell and view the results. Notice the following:
-- MAGIC - In the first few cells, you can see column information.
-- MAGIC - Starting at cell 4, you can see additional **Delta Statistics Columns**.
-- MAGIC - Starting at cell 8, you can see additional **Detailed Table Information**.
-- MAGIC
-- MAGIC **NOTE:** Remember, we do not need to reference the three-level namespace (`catalog.schema.table`) because we set our default catalog and schema earlier.
-- MAGIC

-- COMMAND ----------

DESCRIBE TABLE EXTENDED mytable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B4. Volumes
-- MAGIC
-- MAGIC Volumes are Unity Catalog objects that enable governance over non-tabular datasets. Volumes represent a logical volume of storage in a cloud object storage location. Volumes provide capabilities for accessing, storing, governing, and organizing files.
-- MAGIC
-- MAGIC While tables provide governance over tabular datasets, volumes add governance over non-tabular datasets. You can use volumes to store and access files in **_any_** format, including structured, semi-structured, and unstructured data.
-- MAGIC
-- MAGIC Databricks recommends using volumes to govern access to all non-tabular data. Like tables, volumes can be managed or external.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### B4.1 UI Exploration
-- MAGIC
-- MAGIC Complete the following to explore the **dbacademy_ecommerce** catalog:
-- MAGIC
-- MAGIC 1. In the left navigation bar, select the catalog icon:  ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/catalog_icon.png)
-- MAGIC
-- MAGIC 2. Locate the catalog called **dbacademy_ecommerce** and expand the catalog.
-- MAGIC
-- MAGIC 3. Expand the **v01** schema. Notice that this catalog contains two volumes, **delta** and **raw**.
-- MAGIC
-- MAGIC 4. Expand the **raw** volume. Notice that the volume contains a series of folders.
-- MAGIC
-- MAGIC 5. Expand the **users-historical** folder. Notice that the folder contains a series of files.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### B4.2 Volume Exploration with SQL
-- MAGIC
-- MAGIC Run the `DESCRIBE VOLUME` statement to return the metadata for the **dbacademy_ecommerce.v01.raw** volume. The metadata includes the volume name, schema, catalog, type, comment, owner, and more.
-- MAGIC
-- MAGIC Notice the following:
-- MAGIC - Under the **storage_location** column, you can see the cloud storage location for this volume.
-- MAGIC
-- MAGIC - Under the **volume_type** column, it indicates this is a *MANAGED* volume.
-- MAGIC

-- COMMAND ----------

DESCRIBE VOLUME dbacademy_ecommerce.v01.raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### B4.3 List Files in a Volume
-- MAGIC
-- MAGIC
-- MAGIC Use the `LIST` statement to list the available files in the **raw** volume's **users-historical** directory (`/Volumes/dbacademy_ecommerce/v01/raw/users-historical`) and view the results.
-- MAGIC
-- MAGIC Notice the following:
-- MAGIC - Ignore any file names that begin with an underscore (_). These are temporary or intermediate files used when writing files to a location.
-- MAGIC - Scroll down in the results and expand one of the files where the **name** column begins with **part**. Confirm that this directory contains a series of Parquet files.
-- MAGIC
-- MAGIC
-- MAGIC **NOTE:**  When interacting with data in volumes, use the path provided by Unity Catalog, which always follows this format: */Volumes/catalog_name/schema_name/volume_name/*.
-- MAGIC
-- MAGIC For more information on exploring directories and data files managed with Unity Catalog volumes, check out the [Explore storage and find data files](https://docs.databricks.com/en/discover/files.html) documentation.
-- MAGIC

-- COMMAND ----------

LIST '/Volumes/dbacademy_ecommerce/v01/raw/users-historical'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>