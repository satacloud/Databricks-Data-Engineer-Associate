# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Lab

# COMMAND ----------

## Find data in workspace data folder (source)
data_path = "/Volumes/dbacademy/default/data"

## target volume
volume_path = f'/Volumes/{my_catalog}/sdp_lab_1_bronze/lab_files/'

## Delete files in volume to redo lab if necessary
delete_files(volume_path)

## Copy a file into the volume
## Give it a few seconds before creating files after deleting them.
time.sleep(5)

copy_workspace_files_to_volume(
    src_workspace_folder=f'{data_path}/lab_files',
    target_volume_path=volume_path,
    n=1
)


display_config_values(
    [
        ("Your Catalog", my_catalog),
        ("Bronze Schema", 'sdp_lab_1_bronze'),
        ("Silver Schema", 'sdp_lab_2_silver'),
        ("Gold Schema", 'sdp_lab_3_gold'),
        ("Source Volume", volume_path)
    ]
)

compute_validation(recommend_dbr_classic_version=None, recommended_serverless_version=5)