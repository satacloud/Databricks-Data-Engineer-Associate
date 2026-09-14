# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 13 - Performing Integration Tests
# MAGIC
# MAGIC Integration tests for data engineering ensure that different components of the data pipeline, such as data ingestion, transformation, storage, and retrieval, work together seamlessly in a real-world environment. These tests validate the flow of data across systems, checking for issues like data consistency, format mismatches, and processing errors when components interact as expected.
# MAGIC
# MAGIC There are multiple ways to implement integration tests within Databricks:
# MAGIC
# MAGIC 1. **Apache Spark™ Declarative Pipeline (Formerly DLT)**: With Apache Spark™ Declarative Pipelines, you can use expectations to check pipeline’s results.
# MAGIC     - [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations#manage-data-quality-with-pipeline-expectations)
# MAGIC
# MAGIC 2. **Jobs**: You can also perform integration tests as a Databricks Jobs with tasks - similarly what is typically done for Non-Spark Declarative Pipeline code.
# MAGIC
# MAGIC In this demonstration, we will quickly introduce to you how to perform simple integration tests with Delta Live Tables and discuss how to implement them with Jobs. Prior knowledge of Spark Declarative Pipeline and Lakeflow Jobs is assumed.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Learn how to perform integration testing in Spark Declarative Pipelines using expectations.
# MAGIC - Understand how to perform integration tests on data from Spark Declarative Pipeline using Lakeflow Jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC    - Click **More** in the drop-down.
# MAGIC
# MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. 
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.
# MAGIC
# MAGIC ##### The notebook "03 - Modularizing PySpark Code - Required" sets up the catalogs for this course. If you have not run this notebook, the catalogs will not be available.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../Includes/Classroom-Setup-13

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Option 1 - Spark Declarative Pipeline with Integration Tests
# MAGIC
# MAGIC In this section, we will create a Spark Declarative Pipeline using the modularized functions from the `src.helpers` file, which we unit tested in the previous notebook. In the pipeline, we will use these functions to create tables and then implement some simple integration tests for the output tables in our ETL pipeline for this project.
# MAGIC
# MAGIC - With Spark Declarative Pipelines, you can use expectations to check pipeline’s results.
# MAGIC   - [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations#manage-data-quality-with-pipeline-expectations)
# MAGIC
# MAGIC   - [Expectation recommendations and advanced patterns](https://docs.databricks.com/aws/en/ldp/expectation-patterns#expectation-recommendations-and-advanced-patterns)
# MAGIC
# MAGIC   - [Applying software development & DevOps best practices to Spark Declarative Pipelines](https://www.databricks.com/blog/applying-software-development-devops-best-practices-delta-live-table-pipelines)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. We will create the Spark Declarative Pipeline for this project using the Databricks Academy **`DAPipelineConfig`** class, which was specifically designed for this course with the Databricks SDK. This avoids manually creating the pipeline for this demo. Typically during development you would manually build the pipeline with the UI during development.
# MAGIC
# MAGIC     **NOTE:** The Databricks SDK is outside the scope of this course. However, if you're interested in seeing the code that uses the SDK to automate building SDP in Databricks Academy, check out the **[../Includes/Classroom-Setup-Common]($../Includes/Classroom-Setup-Common)** notebook in **Cell 6**.
# MAGIC
# MAGIC     [Databricks SDK for Python](https://docs.databricks.com/en/dev-tools/sdk-python.html)
# MAGIC
# MAGIC     [Databricks SDK Documentation](https://databricks-sdk-py.readthedocs.io/en/latest/)
# MAGIC
# MAGIC
# MAGIC ![Full Spark Declarative Pipeline](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/04_dlt_pipeline.png)

# COMMAND ----------

pipeline = DeclarativePipelineCreator(
                            pipeline_name=f"sdk_health_etl_{DA.catalog_dev}", 
                            catalog_name = DA.catalog_name,
                            schema_name = 'default',
                            root_path_folder_name='src',
                            source_folder_names=[
                                'src/sdp/**', 
                                'tests/integration_test/**'],
                            configuration = {
                                'target': 'development',
                                'raw_data_path':f'/Volumes/{DA.catalog_name}/default/health'
                            })

pipeline.create_pipeline()

pipeline.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. While the Spark Declarative Pipeline is running, examine it through the UI by completing the following steps:
# MAGIC
# MAGIC    a. In the far left navigation pane, right-click on **Jobs and Pipelines** and select *Open in a New Tab*.
# MAGIC
# MAGIC    b. Find your pipeline named **sdk_health_etl_your_catalog_1_dev** and select it.
# MAGIC
# MAGIC    c. Click **Settings** at the top right.
# MAGIC
# MAGIC     - c1. In the **Compute** section notice that this pipeline is using **Serverless** compute.
# MAGIC
# MAGIC     - c2. On the right side in **Pipeline Settings**, you'll notice that the pipeline contains two **Configuration** variables:
# MAGIC
# MAGIC       - **target** = *'development'*
# MAGIC         - This `target` variable will be modified dynamically for each deployment to **development**, **stage**, and **production**.
# MAGIC
# MAGIC       - **raw_data_path** = *'/Volumes/your_catalog_1_dev/default/health'*
# MAGIC         - This `raw_data_path` variable will be modified dynamically for each deployment to **development data**, **stage data**, and **production data**.
# MAGIC
# MAGIC     - c3. Click **Cancel** at the bottom right.
# MAGIC
# MAGIC    d. At the top of the Pipelines select the kebab menu (three ellipses) and select **View settings YAML**. Notice that the UI provides the necessary YAML files for future deployment. We will talk more about this later. 
# MAGIC
# MAGIC    e. In the **Pipeline details** section on the far right, locate the Source code section. You will see a link indicating the number of folders along with an **Open in Editor** option. Click **Open in Editor** to view the notebooks used by the pipeline. Open each notebook in a new tab to examine them:
# MAGIC
# MAGIC     - **Notebook 1: [..../src/sdp/ingest-bronze-silver_sdp]($../../src/sdp/ingest-bronze-silver_sdp.py)** - Obtains the Spark Declarative Pipeline configuration variables that setup the target and raw data, and creates the bronze and silver tables based on those variable values.
# MAGIC
# MAGIC     - **Notebook 2: [..../src/sdp/gold_tables_sdp]($../../src/sdp/gold_tables_sdp.sql)** - Creates the gold table.
# MAGIC
# MAGIC     - **Notebook 3: [..../tests/integration_test/integration_tests_sdp]($../../tests/integration_test/integration_tests_sdp)** - Performs simple integration tests on the bronze, silver and gold tables based on the target environment.
# MAGIC
# MAGIC    f. Here is a diagram of the entire pipeline for **development, stage and production**. Depending on the values of the **target** and **raw_data_path** configuration variables that are set, the ingest data source and integration tests will vary (dev catalog, stage catalog, prod catalog), but the ETL pipeline will remain the same.
# MAGIC
# MAGIC   ![Explain SDP Pipeline](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/04_sdp_explain_integrations.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Option 2 - Integration Testing with Notebooks and Databricks Jobs
# MAGIC You can also perform integration testing using notebooks and add them as tasks in jobs for your pipeline. 
# MAGIC
# MAGIC **NOTE:** We will simply review how to implement integration tests with Jobs if that is the method you prefer. The final deployment for this course uses the Spark Declarative Pipeline integration tests with expectations.
# MAGIC
# MAGIC #### Steps to take:
# MAGIC 1. Create a setup notebook to handle any dynamic setup required using job parameters for your target environment and data locations.
# MAGIC
# MAGIC 2. Create additional notebooks or files to store the integration tests you want to run as tasks.
# MAGIC
# MAGIC 3. Organize the new notebooks or files within your **tests** folder.
# MAGIC
# MAGIC 4. Create a Workflow. Within the Workflow:
# MAGIC
# MAGIC    - a. Create the necessary tables or views using Spark Declarative Pipeline or code.
# MAGIC
# MAGIC    - b. Add tasks to set up your integration tests (e.g., setting up any dynamic job parameters that need to be set).
# MAGIC
# MAGIC    - c. Perform validation by using your notebooks as tasks and set the tasks to all should succeed.
# MAGIC
# MAGIC **NOTES:** One major drawback of this approach is that you will need to write more code for setup and validation tasks, as well as manage the job parameters to dynamically modify the code based on the target environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Integration testing can be performed in a variety of ways within Databricks. In this demonstration, we focused on how to perform simple integration tests using Spark Declarative Pipeline expectations. We also discussed how to implement them with Workflow tasks.
# MAGIC
# MAGIC Depending on your specific situation, you can choose the approach that best fits your needs.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC