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

# MAGIC %run ../../Includes/Classroom-Setup-10L

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE high_risk_borrowers_silver AS
# MAGIC SELECT
# MAGIC   id,
# MAGIC   avg_cc_spending,
# MAGIC   total_mortgage_amount,
# MAGIC   has_cd_account,
# MAGIC   is_online_customer,
# MAGIC   has_securities_account,
# MAGIC   CASE
# MAGIC     WHEN avg_cc_spending > 5000 THEN 'High Spender'
# MAGIC     WHEN avg_cc_spending > 2000 THEN 'Medium Spender'
# MAGIC     ELSE 'Low Spender'
# MAGIC   END AS spending_category
# MAGIC FROM loan_details_silver
# MAGIC WHERE has_credit_card = true AND has_personal_loan = true
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>