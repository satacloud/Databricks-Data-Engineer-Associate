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
# MAGIC -- Creating Loan Details Table
# MAGIC CREATE TABLE IF NOT EXISTS loan_details_silver (
# MAGIC     id INT,
# MAGIC     avg_cc_spending INT,
# MAGIC     total_mortgage_amount INT,
# MAGIC     has_personal_loan BOOLEAN,
# MAGIC     has_securities_account BOOLEAN,
# MAGIC     has_cd_account BOOLEAN,
# MAGIC     is_online_customer BOOLEAN,
# MAGIC     has_credit_card BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Deleting Records before Insertion
# MAGIC DELETE FROM loan_details_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transforming and Inserting records into our newly created table
# MAGIC INSERT INTO loan_details_silver
# MAGIC (
# MAGIC     SELECT 
# MAGIC         id,
# MAGIC         CAST(credit_card_average AS FLOAT) * 1000 AS avg_cc_spending,
# MAGIC         CAST(mortgage * 1000 AS FLOAT) AS total_mortgage_amount, 
# MAGIC         has_personal_loan,
# MAGIC         has_securities_account,
# MAGIC         has_cd_account,
# MAGIC         is_online AS is_online_customer,
# MAGIC         has_credit_card
# MAGIC     FROM bank_master_data_bronze
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setting Up Task Values
# MAGIC We will set up task values to be used in another task. 
# MAGIC - First, we extract the `loan_details_silver` table and save it as a DataFrame. 
# MAGIC - Next, we apply a filter to identify customers who hold both a credit card and a personal loan. 
# MAGIC - These customers are categorized as **risky customers**.
# MAGIC - We will count the number of such customers, and if the count exceeds 100, we will set the `risk_flag` to true.
# MAGIC
# MAGIC **NOTE: We are setting up task values for our next task. You will learn more about task values in upcoming lectures before actually using them in the next lab.**

# COMMAND ----------

## Getting Dataframe for setting Task Value
loan_details_df = spark.read.table("loan_details_silver")

# COMMAND ----------

# Checking for risky customers
## Condition: users with both a credit card and a personal loan

risky_customers_df = loan_details_df.filter(
    (loan_details_df['has_credit_card'] == True) & (loan_details_df['has_personal_loan'] == True)
)

# Getting count of risky customers
risk_count = risky_customers_df.count()
print(f"Number of users with both personal loan and credit card: {risk_count}")


# COMMAND ----------

# Decide flag
## We are going to set the flag to True if there are more than 100 customers with both personal loan and credit card
### This task value is going to used by next lab task
status = True if risk_count > 100 else False
dbutils.jobs.taskValues.set(key="risk_flag", value=status)


# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>