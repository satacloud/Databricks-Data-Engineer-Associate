# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

spark.sql('DROP TABLE IF EXISTS mytable')
spark.sql('CREATE TABLE IF NOT EXISTS mytable (id INT, name STRING)')
spark.sql('INSERT INTO mytable (id, name) VALUES (1,"Peter")')
print('Created UC table mytable for the demonstration.')

# COMMAND ----------

display_config_values(
    [
        ('catalog', my_catalog),
        ('schema', 'data_ingestion'),
        ('volume', 'test'),
        ('user volume path', user_vol_path)
    ]
)