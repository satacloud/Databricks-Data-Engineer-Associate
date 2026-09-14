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
-- MAGIC # Appendix - Ingesting XML Files with Databricks
-- MAGIC ### Extra material, not part of a live teach.
-- MAGIC In this demonstration we will go over how to ingest XML files and store them as Bronze UC tables.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC
-- MAGIC - Use the `CREATE TABLE AS SELECT` (CTAS) statement with the `read_files()` function to ingest XML files into a UC table, including any rescued data.
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

-- MAGIC %run ../Includes/Classroom-Setup-XML

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to view your default catalog and schema. Notice that the **labuser123_678** catalog is your catalog, which is also your unique lab username, and your default schema is the **data_ingestion** schema.
-- MAGIC
-- MAGIC **NOTE:** The default catalog and schema are pre-configured for you to avoid the need to specify the three-level name when writing your tables to your **data_ingestion** schema (i.e., catalog.schema.table).

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. CTAS with `read_files()` for Ingesting XML Files
-- MAGIC
-- MAGIC In this section, we'll explore how to ingest raw XML (Extensible Markup Language) files from cloud storage into a UC table. XML files are structured text files that use custom tags to organize data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the code below, we dynamically pass variables to `read_files()` since each user has a unique username within the Vocareum environment. This is done using the `my_catalog` object created during classroom setup. 
-- MAGIC
-- MAGIC For example, the expression:
-- MAGIC
-- MAGIC `'/Volumes/' || my_catalog || '/data_ingestion/landing_folder/xml_demo_files/example_1_data.xml'`
-- MAGIC
-- MAGIC evaluates to the string:
-- MAGIC
-- MAGIC `/Volumes/<username>/data_ingestion/landing_folder/xml_demo_files/example_1_data.xml`
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. View the XML File
-- MAGIC
-- MAGIC 1. Follow the steps below to view your XML file in your course volume: **labuser123_678.data_ingestion.landing_folder**
-- MAGIC
-- MAGIC    a. In the left navigation bar, select the catalog icon:  ![Catalog Icon](https://files.training.databricks.com/binder/prod_main/data-ingestion-with-lakeflow-connect-en_us-3.1.2/images/20260828T081722Z/Data Ingestion with LakeFlow Connect/Includes/images/catalog_icon.png)
-- MAGIC
-- MAGIC    b. Expand the **labuser123_678** catalog.
-- MAGIC
-- MAGIC    c. Expand the **data_ingestion** schema.
-- MAGIC
-- MAGIC    d. Expand **Volumes**. You should see **landing_folder** volume, which contains the source data to ingest.
-- MAGIC
-- MAGIC    e. Expand your **landing_folder** volume. This volume contains several subdirectories. We will use the **xml_demo_files** directory.
-- MAGIC
-- MAGIC    f. Expand the **xml_demo_files** subdirectory. It should contain the file: **example_1_data.xml**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Since XML files can be difficult to work with, let's start this demonstration by looking at the **example_1_data.xml** XML file:
-- MAGIC
-- MAGIC ```
-- MAGIC   <books>
-- MAGIC     <book id="222">
-- MAGIC       <author>Corets, Eva</author>
-- MAGIC       <title>Maeve Ascendant</title>
-- MAGIC     </book>
-- MAGIC     <book id="333">
-- MAGIC       <author>Corets, Eva</author>
-- MAGIC       <title>Oberon's Legacy</title>
-- MAGIC     </book>
-- MAGIC   </books>
-- MAGIC ```
-- MAGIC
-- MAGIC This XML contains:
-- MAGIC - The Top level element `<books>`. This is the root element. It acts as a container for all the `<book>` elements.
-- MAGIC - Each `<book>` element represents a single book and includes:
-- MAGIC   - The `id` attribute which uniquely identifies the book.
-- MAGIC   - The `<author>` child element containing the name of the author. 
-- MAGIC   - The `<title>` child element containing the title of the book. 
-- MAGIC
-- MAGIC
-- MAGIC Our goal in ingesting this XML file is to flatten it into a tabular form so we can store it as a Delta table. We'll define the following columns:
-- MAGIC
-- MAGIC - **book_id** (extracted from the `id` attribute)
-- MAGIC - **author** (extracted from the `<author>` element text)
-- MAGIC - **title** (extracted from the `<title>` element text)
-- MAGIC
-- MAGIC Each `<book>` element will be treated as a row. To achieve this, we set `rowTag => 'book'` when using `read_files()`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Using the `read_files()` Function to Ingest XML
-- MAGIC
-- MAGIC The code in the next cell creates a structured table using a `CREATE TABLE AS SELECT` (CTAS) statement along with the `read_files()` function.
-- MAGIC
-- MAGIC We are using the following options with the `read_files()` function:
-- MAGIC
-- MAGIC 1. `format => "xml"` – Specifies that the input data is in XML format.  
-- MAGIC 2. `rowTag => "book"` – Identifies the repeating XML element (`<book>`) that defines individual rows.  
-- MAGIC 3. `schema => '_id INT, author STRING, title STRING'` – Enforces a schema for known fields in the XML.  
-- MAGIC 4. `rescuedDataColumn => '_rescued_data'` – Captures any malformed or unexpected fields that do not match the schema into a separate column for later inspection.
-- MAGIC
-- MAGIC This example demonstrates how to parse structured XML using schema enforcement while preserving problematic or unknown data for troubleshooting. For brevity, we skip the actual troubleshooting process.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the following cell to read in this XML file. The following query also brings in a `_rescued_data` column. Note that this column will return `NULL` because this XML file is made up of clean data.

-- COMMAND ----------

SELECT *
FROM read_files(
       '/Volumes/' || my_catalog || '/data_ingestion/landing_folder/xml_demo_files/example_1_data.xml',
       format => "xml",
       rowTag => 'book',
       schema => '''
            _id INT, 
            author STRING, 
            title STRING
          ''',
       rescueddatacolumn => '_rescued_data'
     );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. We can also change the `rowTag` parameter to `"books"` to produce a different flattening of the XML file. 
-- MAGIC
-- MAGIC     In this case, we omit the explicit schema definition and allow schema inference. The resulting output is a single row containing a nested array.
-- MAGIC

-- COMMAND ----------

SELECT *
FROM read_files(
       '/Volumes/' || my_catalog || '/data_ingestion/landing_folder/xml_demo_files/example_1_data.xml',
       format => "xml",
       rowTag => 'books',
       rescueddatacolumn => '_rescued_data'
     );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Let's finish by writing to UC table based on the first schema presented above.

-- COMMAND ----------

-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS books_bronze_xml;


-- Create the UC table
CREATE TABLE books_bronze_xml 
AS
SELECT
  _id AS book_id,
  * EXCEPT (_id),
  current_timestamp AS ingestion_timestamp,
  _metadata.file_name AS source_file
FROM read_files(
       '/Volumes/' || my_catalog || '/data_ingestion/landing_folder/xml_demo_files/example_1_data.xml',
       format => "xml",
       rowTag => 'book',
       schema => '''
            _id INT, 
            author STRING, 
            title STRING
          ''',
       rescuedDataColumn => '_rescued_data'
     );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Inspect the newly created **bronze** table.

-- COMMAND ----------

SELECT *
FROM books_bronze_xml

-- COMMAND ----------

-- View the datatypes of the columns 
DESCRIBE books_bronze_xml

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>