# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common-Python

# COMMAND ----------

## Create schemas for the course
my_schema = 'sdp'

bronze_schema = f'{my_schema}_1_bronze'
silver_schema = f'{my_schema}_2_silver'
gold_schema = f'{my_schema}_3_gold'

create_schemas(in_catalog = my_catalog, 
               schemas_to_create = [
                   bronze_schema, 
                   silver_schema, 
                   gold_schema]
            )

## Set default schema
spark.sql(f'USE SCHEMA {bronze_schema}')

# COMMAND ----------

########################################
## CREATE USER SCHEMAS
########################################
import time

## Find data in workspace data folder
data_path = "/Volumes/dbacademy/default/data"

source_dir = [
    "customers",
    "orders",
    "status"
]

##
## Copy files to the 'source' file
##

## Create source volume
spark.sql(f'CREATE VOLUME IF NOT EXISTS {my_catalog}.{bronze_schema}.source')


for folder in source_dir:
    vol_path = f'/Volumes/{my_catalog}/{bronze_schema}/source/{folder}'

    ## If location exists, delete files in it.
    if os.path.isdir(vol_path):                      
      delete_files(vol_path)       

    ## Give it a few seconds before creating files after deleting them.
    time.sleep(5)

    copy_workspace_files_to_volume(
        src_workspace_folder=f'{data_path}/{folder}',
        target_volume_path=vol_path,
        n=1
    )

# COMMAND ----------

########################################
## Creates a Python/SQL variable called source_volume_path for path to source raw files
########################################
source_volume_path = f'/Volumes/{my_catalog}/{bronze_schema}/source'
_ = spark.sql(f'DECLARE OR REPLACE VARIABLE source_volume_path STRING')
_ = spark.sql(f'SET VAR source_volume_path = "{source_volume_path}"')

# COMMAND ----------

display_config_values(
    [
        ("Your Catalog", my_catalog),
        ("Bronze Schema", bronze_schema),
        ("Silver Schema", silver_schema),
        ("Gold Schema", gold_schema),
        ("Source Volume", source_volume_path)
    ]
)

# COMMAND ----------

compute_validation(recommend_dbr_classic_version=None, recommended_serverless_version=5)