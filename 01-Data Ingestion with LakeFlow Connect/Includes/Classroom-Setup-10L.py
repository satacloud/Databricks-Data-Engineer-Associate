# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

import csv
import os

def corrupt_row(input_csv, output_csv):
    """
    Keeps only the first 5 rows and first 3 columns,
    and replaces the first order_id value with 'aaa'.
    """
    with open(input_csv, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter='|')
        rows = list(reader)

    # Keep only the first 5 rows and first 3 columns
    trimmed_rows = [row[:3] for row in rows[:5]]

    # Replace the first order_id in the first data row (row index 1) with 'aaa'
    if len(trimmed_rows) > 1:
        trimmed_rows[3][0] = 'M_PREM_A,Premium Queen Mattress,#$%^'
        trimmed_rows[3][0] = 'M_PREM_A,Premium Queen Mattress,$100.00'

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    with open(output_csv, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile, delimiter='|')
        writer.writerows(trimmed_rows)

# Example usage
corrupt_row(
    '/Volumes/dbacademy_ecommerce/v01/raw/products-csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv',
    f'/Volumes/{my_catalog}/data_ingestion/landing_folder/csv_demo_files/lab_malformed_data.csv'
)

# COMMAND ----------

# Builds the final lab table for students to use as a resource
query_1 = """
DROP TABLE IF EXISTS 10_lab_solution
"""

query_2 = f"""
CREATE TABLE 10_lab_solution 
USING DELTA AS
SELECT
  *,
  _METADATA.FILE_MODIFICATION_TIME AS file_modification_time,
  _METADATA.FILE_NAME AS source_file, 
  current_timestamp() as ingestion_time
FROM READ_FILES(
        '/Volumes/' || my_catalog || '/data_ingestion/landing_folder/csv_demo_files/lab_malformed_data.csv',
        FORMAT => "csv",
        SEP => ",",
        HEADER => true,
        SCHEMA => 'item_id STRING, name STRING, price DOUBLE', 
        RESCUEDDATACOLUMN => "_rescued_data"
      )
"""

# Execute the queries
spark.sql(query_1)
spark.sql(query_2)

# COMMAND ----------

# Builds the final lab challenge table for students to use as a resource
query_1 = """
DROP TABLE IF EXISTS 10_lab_challenge_solution
"""

query_2 = f"""
CREATE TABLE 10_lab_challenge_solution 
AS
SELECT
  item_id,
  name,
  price,
  coalesce(price,replace(_rescued_data:price,'$','')) AS price_fixed,
  _rescued_data,
  _metadata.file_modification_time AS file_modification_time,
  _metadata.file_name AS source_file, 
  current_timestamp() as ingestion_time
FROM read_files(
        '/Volumes/' || my_catalog || '/data_ingestion/landing_folder/csv_demo_files/lab_malformed_data.csv',
        format => "csv",
        sep => ",",
        header => true,
        schema => 'item_id STRING, name STRING, price DOUBLE', 
        rescueddatacolumn => "_rescued_data"
      )
"""


# Execute the queries
r = spark.sql(query_1)
r = spark.sql(query_2)