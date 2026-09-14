# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);

# COMMAND ----------

display_config_values(
    [
        ('catalog', my_catalog),
        ('schema', 'data_ingestion'),
        ('volume', 'test'),
        ('user volume path', user_vol_path)
    ]
)