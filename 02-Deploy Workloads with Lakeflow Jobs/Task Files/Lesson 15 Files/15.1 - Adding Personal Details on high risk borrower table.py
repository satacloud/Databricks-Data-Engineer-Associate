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

# ==== Parameter to simulate failure ====
should_fail = dbutils.widgets.get("should_fail")

if should_fail.lower() == "true":
    raise Exception("Simulated failure: Skipping gold_high_risk_borrower pipeline.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE high_risk_borrowers_gold AS
# MAGIC SELECT
# MAGIC   hr.id,
# MAGIC   pd.age,
# MAGIC   pd.yoe,
# MAGIC   pd.income_in_usd,
# MAGIC   pd.zip_code,
# MAGIC   pd.family_size,
# MAGIC   pd.education,
# MAGIC   hr.avg_cc_spending,
# MAGIC   hr.total_mortgage_amount,
# MAGIC   hr.has_cd_account,
# MAGIC   hr.is_online_customer,
# MAGIC   hr.has_securities_account,
# MAGIC   hr.spending_category,
# MAGIC
# MAGIC   -- Income Band
# MAGIC   CASE
# MAGIC     WHEN pd.income_in_usd >= 100000 THEN 'High Income'
# MAGIC     WHEN pd.income_in_usd >= 50000 THEN 'Middle Income'
# MAGIC     ELSE 'Low Income'
# MAGIC   END AS income_band,
# MAGIC
# MAGIC   -- Age Group
# MAGIC   CASE
# MAGIC     WHEN pd.age < 30 THEN 'Young'
# MAGIC     WHEN pd.age < 50 THEN 'Middle-aged'
# MAGIC     ELSE 'Senior'
# MAGIC   END AS age_group
# MAGIC
# MAGIC FROM high_risk_borrowers_silver hr
# MAGIC JOIN borrower_details_silver pd
# MAGIC   ON hr.id = pd.id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>