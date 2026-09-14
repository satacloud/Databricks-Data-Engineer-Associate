# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA.display_config_values([('Course Catalog',DA.catalog_name),('Your Schema',DA.schema_name)])

# COMMAND ----------

import os

curr_path = os.getcwd()
print(f"Current Path: {curr_path}")

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

w = WorkspaceClient()

def _retry(func, max_retries=3, backoff=10):
    """Retry a function with exponential backoff on transient errors."""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt < max_retries - 1 and "DeadlineExceeded" in type(e).__name__:
                wait = backoff * (2 ** attempt)
                print(f"Attempt {attempt+1} failed ({type(e).__name__}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise

def create_query_file(catalog, schema, table_name):

    # Retrieve available data sources (SQL warehouses)
    srcs = _retry(lambda: w.data_sources.list())
    query_name = "4.2 - Creating sales table - SQL Query"

    # Check if a query with the same name already exists
    query_id = validate_job_query_file_exists(query_name)

    # If the query exists, delete it to avoid duplicates
    if query_id != -1:
        _retry(lambda: w.queries.delete(id=query_id))
        print(f'Query Deleted id {query_id}')

    # Create a new query to generate the sales table using CTAS
    query = _retry(lambda: w.queries.create(
        query=sql.CreateQueryRequestQuery(
            display_name=query_name,
            warehouse_id=srcs[0].warehouse_id,  # Use the first available warehouse
            description="CTAS for Sales Table for Demo 04",
            parent_path = f"{curr_path}/Task Files/Lesson 04 Files",
            query_text=f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name} AS SELECT * FROM dbacademy_retail.v01.sales
"""
            )
    ))
    print(f'Query Created id {query.id}')

    return query

def validate_job_query_file_exists(query_name):
    # List all queries and check if one matches the given name
    query_list = _retry(lambda: list(w.queries.list()))
    for query in query_list:
        if query.display_name == query_name:
            return query.id
    return -1  # Return -1 if no matching query is found

# COMMAND ----------

query = create_query_file(f"{DA.catalog_name}",f"{DA.schema_name}", "sales_bronze")