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

# MAGIC %run ../../Includes/Classroom-Setup-12

# COMMAND ----------

# Reading the data from each state
df_ca = spark.read.table('customers_orders_ca_silver')
df_ny = spark.read.table('customers_orders_ny_silver')
df_va = spark.read.table('customers_orders_va_silver')

# COMMAND ----------

# -- COMMON CLEANING AND STANDARDIZATION --
def clean_common(df):
    df = df.toDF(*[c.strip().lower() for c in df.columns])
    
    ## We intentionally used wrong column name so that task will fail
    df = df.withColumn("customer", F.initcap(F.trim(F.col("customer")))) 
    
    ''' Correct Code
    df = df.withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))
    '''
    return df

# COMMAND ----------

# -- CALIFORNIA TRANSFORMATION --
def prep_ca(df_ca):
    df = clean_common(df_ca)
    # Fill missing cities with 'Unknown'
    df = df.withColumn("city", F.when(F.col("city").isNull(), F.lit("Unknown")).otherwise(F.col("city")))
    # Extract order month and large order flag
    df = df.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    # Custom: Add column for Southern CA city marker
    so_cal_cities = ["Los Angeles", "San Diego", "Bakersfield", "Anaheim", "Long Beach"]
    df = df.withColumn(
        "is_southern_ca",
        F.lower(F.col("city")).isin([c.lower() for c in so_cal_cities])
    )
    return df

# COMMAND ----------

# -- NEW YORK TRANSFORMATION --
def prep_ny(df_ny):
    df = clean_common(df_ny)
    # Fill missing city with 'Unspecified'
    df = df.withColumn("city", F.when(F.col("city").isNull(), F.lit("Unspecified")).otherwise(F.col("city")))
    # Assigning NY region group
    ny_upstate_cities = ["Buffalo", "Rochester", "Albany", "Syracuse"]
    ny_downstate_cities = ["Yonkers", "White Plains"]
    df = df.withColumn(
    "ny_region",
    F.when(F.lower(F.col("city")).isin([c.lower() for c in ny_upstate_cities]), F.lit("Upstate"))
    .when(F.lower(F.col("city")).isin([c.lower() for c in ny_downstate_cities]), F.lit("Downstate"))
    .otherwise(F.lit("Other"))
    )
    # Extract order week for NY analysis
    df = df.withColumn("order_week", F.weekofyear("order_date"))
    return df

# COMMAND ----------

# -- VIRGINIA TRANSFORMATION --
def prep_va(df_va):
    df = clean_common(df_va)
    # Fill missing city as 'Other'
    df = df.withColumn("city", F.when(F.col("city").isNull(), F.lit("Other")).otherwise(F.col("city")))
    # Segment orders by size
    df = df.withColumn("order_size_label", F.when(F.col("is_large_order") == "true", F.lit("Large")).otherwise(F.lit("Regular")))
    # Extract order day of week
    df = df.withColumn("order_day_of_week", F.date_format("order_date", "E"))
    return df

# COMMAND ----------

# -- APPLY TRANSFORMATIONS --
df_ca_clean = prep_ca(df_ca)
df_ny_clean = prep_ny(df_ny)
df_va_clean = prep_va(df_va)

# -- WRITE TO TABLES --
df_ca_clean.write.mode("overwrite").saveAsTable("customers_orders_ca_gold")
df_ny_clean.write.mode("overwrite").saveAsTable("customers_orders_ny_gold")
df_va_clean.write.mode("overwrite").saveAsTable("customers_orders_va_gold")


# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>