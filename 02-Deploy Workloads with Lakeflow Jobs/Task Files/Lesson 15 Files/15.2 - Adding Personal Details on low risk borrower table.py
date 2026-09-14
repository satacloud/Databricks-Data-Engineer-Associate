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

# MAGIC %run ../../Includes/Classroom-Setup-15L

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE low_risk_borrowers_gold AS
# MAGIC SELECT
# MAGIC   lr.id,
# MAGIC   pd.age,
# MAGIC   pd.yoe,
# MAGIC   pd.income_in_usd,
# MAGIC   pd.zip_code,
# MAGIC   pd.family_size,
# MAGIC   pd.education,
# MAGIC   lr.has_cd_account,
# MAGIC   lr.has_credit_card,
# MAGIC   lr.has_personal_loan,
# MAGIC   lr.is_online_customer,
# MAGIC   lr.preferred_channel,
# MAGIC   lr.cd_profile,
# MAGIC
# MAGIC   -- Customer Type (for marketing segmentation)
# MAGIC   CASE
# MAGIC     WHEN lr.has_credit_card = true AND lr.has_personal_loan = false THEN 'Credit Only'
# MAGIC     WHEN lr.has_credit_card = false AND lr.has_personal_loan = true THEN 'Loan Only'
# MAGIC     ELSE 'Other'
# MAGIC   END AS customer_type,
# MAGIC
# MAGIC   -- Family Size Band
# MAGIC   CASE
# MAGIC     WHEN pd.family_size <= 2 THEN 'Small'
# MAGIC     WHEN pd.family_size <= 4 THEN 'Medium'
# MAGIC     ELSE 'Large'
# MAGIC   END AS family_size_segment
# MAGIC
# MAGIC FROM low_risk_borrowers_silver lr
# MAGIC JOIN borrower_details_silver pd
# MAGIC   ON lr.id = pd.id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from low_risk_borrowers_gold
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>