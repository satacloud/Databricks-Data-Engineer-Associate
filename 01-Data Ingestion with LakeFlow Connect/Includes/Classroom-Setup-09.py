# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

import csv

def corrupt_first_order_id(input_csv, output_csv):
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
        trimmed_rows[1][2] = 'aaa'

    with open(output_csv, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile, delimiter='|')
        writer.writerows(trimmed_rows)


# Example usage
create_volume(in_catalog = my_catalog, 
              in_schema = 'data_ingestion', 
              volume_name='csv_demo_files'
)


corrupt_first_order_id(
    '/Volumes/dbacademy_ecommerce/v01/raw/sales-csv/000.csv',
    f'/Volumes/{my_catalog}/data_ingestion/landing_folder/csv_demo_files/malformed_example_1_data.csv'
)

# COMMAND ----------

import csv

def blank_out_first_header_and_trim_five_rows(input_csv, output_csv):
    """
    Keeps only the first 5 rows and first 3 columns, and blanks out the first column header.
    """
    with open(input_csv, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter='|')  # <-- Key fix here
        rows = list(reader)

    # Keep only the first 5 rows and first 3 columns
    trimmed_rows = [row[:3] for row in rows[:5]]

    # Replace only the first header value with an empty string
    if trimmed_rows:
        trimmed_rows[0][0] = ''

    with open(output_csv, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile, delimiter='|')  # Match delimiter here too
        writer.writerows(trimmed_rows)

# Example usage
blank_out_first_header_and_trim_five_rows(
    '/Volumes/dbacademy_ecommerce/v01/raw/sales-csv/000.csv',
    f'/Volumes/{my_catalog}/data_ingestion/landing_folder/csv_demo_files/malformed_example_2_data.csv'
)

# COMMAND ----------

display_config_values(
    [
        ('catalog', my_catalog),
        ('schema', 'data_ingestion')
    ]
)