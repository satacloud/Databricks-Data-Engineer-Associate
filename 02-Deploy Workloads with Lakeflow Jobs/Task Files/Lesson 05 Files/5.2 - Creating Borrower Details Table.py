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

# MAGIC %run ../../Includes/Classroom-Setup-05L

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Borrowers Table
# MAGIC CREATE TABLE IF NOT EXISTS borrower_details_silver (
# MAGIC     id INT PRIMARY KEY,
# MAGIC     age INT,
# MAGIC     yoe INT,
# MAGIC     income_in_usd INT,
# MAGIC     zip_code INT,
# MAGIC     family_size INT,
# MAGIC     education STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Deleting Records before Insertion
# MAGIC DELETE FROM borrower_details_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transforming and Inserting records into our newly created table
# MAGIC INSERT INTO borrower_details_silver
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     age,
# MAGIC     experience AS yoe,
# MAGIC     cast(income AS FLOAT) * 1000 AS income_in_usd,
# MAGIC     zip_code,
# MAGIC     family AS family_size,
# MAGIC     CASE 
# MAGIC         WHEN education = '1' THEN 'Undergraduate'
# MAGIC         WHEN education = '2' THEN 'Graduate'
# MAGIC         WHEN education = '3' THEN 'Postgraduate'
# MAGIC     END AS education
# MAGIC FROM bank_master_data_bronze;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>