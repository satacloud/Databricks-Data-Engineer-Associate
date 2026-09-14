from pyspark import pipelines as dp
import pyspark.sql.functions as F

## Add previous folder to python path to import our helpers package
import sys
sys.path.append('../.')
from helpers import project_functions

####################################################################################
## Obtain Configuration Variables

# This raw source data path and catalog will dynamically be set using the configuration variable set in the Spark Declarative Pipeline for each environment: development, stage or production

# development – Reads the dev CSV file from your_unique_catalog_1_dev.default.health.dev_health.csv

# stage – Reads the stage CSV file from your_unique_catalog_2_stage.default.health.stage_health.csv

# production – Reads CSV files in the production volume your_unique_catalog_2_stage.default.health/*.csv
####################################################################################

# Store the target configuration environment in the variable targert
target = spark.conf.get("target")

# Store the target raw data configuration in the variable raw_data_path
raw_data_path = spark.conf.get("raw_data_path")


####################################################################################
##                 A. Ingest CSV Files -> health_bronze                           ##
####################################################################################

## The health_bronze table is created using the value based on the target variable.
## development - import the DEV CSV
## stage - import the STAGE CSV
## production - import the daily CSV files from our production source volume

# Simple expectations for the bronze table
valid_rows = {
        "not_null_pii": "PII IS NOT NULL", 
        "valid_date": "date IS NOT NULL"
    }

@dp.table(
    comment = "This table will be used to ingest the raw CSV files and add metadata columns to the bronze table.",
    table_properties = {"quality": "bronze"}
)

# Fail process if expectation is not met
@dp.expect_all_or_fail(valid_rows)

def health_bronze():
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header","true")
        .schema(project_functions.get_health_csv_schema())   ## Use the custom schema we created
        .load(raw_data_path)   ## <--------------- Path is based on the configuration parameter set (DEV, STAGE, PROD)
        .select(
            "*",
            "_metadata.file_name",
            "_metadata.file_modification_time",
            F.current_timestamp().alias("processing_time")
            )
    )

####################################################################################
##                                B. Silver Table                                 ##
####################################################################################

@dp.table(
    comment = "This table will create, drop and categorize columns from the bronze table.",
    table_properties = {"quality": "bronze"}
)
def health_silver():
    return (
        dp
        .read_stream("health_bronze")
        .withColumn("HighCholest_Group", project_functions.high_cholest_map("HighCholest"))  # UDF - highcholest_map 
        .withColumn("Age_Group", project_functions.group_ages_map("Age"))                   # UDF - group_ages_map Age
        .drop("file_name", "file_modification_time", "processing_time")         # Drop unnecessary metadata columns
    )
