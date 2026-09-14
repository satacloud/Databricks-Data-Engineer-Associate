# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

DA.display_config_values([('Course Catalog',DA.catalog_name),('Your Schema',DA.schema_name)])

# COMMAND ----------

@DBAcademyHelper.add_method
def copy_data(self):
    base_location = '/Volumes/dbacademy_retail/v01/source_files/customers.csv'
    target_location = f'/Volumes/dbacademy/{DA.schema_name}/trigger_storage_location/'
    dbutils.fs.cp(base_location, target_location, True)
    print(f'Data has been copied from {base_location} to {target_location}')

