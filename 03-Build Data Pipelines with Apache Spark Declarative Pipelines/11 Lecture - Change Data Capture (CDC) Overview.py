# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Change Data Capture (CDC) Overview
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn the fundamentals of Change Data Capture (CDC), including what it is and how to implement Slowly Changing Dimensions (SCD) Type 1 and Type 2 to manage and track evolving data in your pipelines.
# MAGIC
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Define Change Data Capture (CDC)** and explain how it is used to track and apply changes from a source data system into the Lakehouse
# MAGIC 2. **Distinguish between SCD Type 1 and SCD Type 2** and explain how each handles inserts, updates, and deletes differently
# MAGIC 3. **Walk through a concrete SCD Type 1 example** showing how a target table is updated with the latest values from a source change stream
# MAGIC 4. **Describe the SCD Type 2 historical tracking pattern** including the role of `__START_AT` and `__END_AT` columns in managing record versions
# MAGIC 5. **Write the AUTO CDC INTO syntax** and explain what each clause does — `KEYS`, `APPLY AS DELETE WHEN`, `SEQUENCE BY`, `COLUMNS`, and `STORED AS`
# MAGIC 6. **Describe the Customers flow** added to the complete pipeline architecture, including how it uses CDC with SCD Type 1 to maintain a current customer table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## A. What Is Change Data Capture?
# MAGIC
# MAGIC Change Data Capture (CDC) is a technique used to track and capture changes in a data source (such as a database, lakehouse or data warehouse). Those changes are then applied to a target table, for example, your Lakehouse, to keep it up to date with the latest state from the source.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_change_data_capture/cdc.png"
# MAGIC     alt="Data Engineering in Databricks Overview"
# MAGIC     style="width: 900px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC CDC is also closely related to how we handle Slowly Changing Dimensions, or SCDs, which define how historical changes are tracked and stored in your target. There are two main types of SCD we’ll focus on:
# MAGIC - <b>SCD Type 1</b> – Overwrites existing data (no history tracking)
# MAGIC - <b>SCD Type 2</b> – Tracks historical changes by storing previous versions of records
# MAGIC
# MAGIC Let’s walk through a high-level example to make this concrete.
# MAGIC
# MAGIC Imagine we’re working with a <b>customer</b> table.
# MAGIC - Our source data contains new customer records, as well as updates and deletes to existing customers.
# MAGIC - We want to apply those changes to our target table using either SCD Type 1 or SCD Type 2 logic (We’ll dive deeper into what those types mean and how they’re implemented shortly) to keep our target customers table up to date with the latest information.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## B. SCD Type 1 — Overwrite Target with Latest Values

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### B1. SCD Type 1 — Overview
# MAGIC
# MAGIC Let’s start with an overview of Slowly Changing Dimension Type 1, or SCD Type 1. In SCD Type 1, target table is overwritten with the latest values.
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC
# MAGIC <!-- Rules -->
# MAGIC <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin:20px 0;">
# MAGIC   <div style="background:#F9F7F4; border-radius:10px; border-top:4px solid #2272B4; padding:16px 18px;">
# MAGIC     <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:8px;">On Update</div>
# MAGIC     <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">When a record updates, the <b>previous record is simply overwritten</b> by its <b>key</b> with the new value.</div>
# MAGIC   </div>
# MAGIC   <div style="background:#F9F7F4; border-radius:10px; border-top:4px solid #98102A; padding:16px 18px;">
# MAGIC     <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:8px;">On Delete</div>
# MAGIC     <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">When a record is deleted by its <b>key</b>, the <b>record is removed</b>.</div>
# MAGIC   </div>
# MAGIC   <div style="background:#F9F7F4; border-radius:10px; border-top:4px solid #00A972; padding:16px 18px;">
# MAGIC     <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:8px;">On Insert</div>
# MAGIC     <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">There is <b>no tracking of old keys (rows)</b>, only the <b>current data</b> is retained.</div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### B2. Worked Example — Step by Step
# MAGIC
# MAGIC <br>
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC   <div style="display:flex; gap:24px; align-items:flex-start; flex-wrap:wrap;">
# MAGIC     <!-- Left: Text -->
# MAGIC     <div style="flex:1 1 0; min-width:260px; font-size:14px; color:#0B2026; line-height:1.7;">
# MAGIC       <!-- Your text goes here -->
# MAGIC       <b>Our Scenario</b>:<br>
# MAGIC <ul>
# MAGIC <li>We have a <b>customers</b> table as our target. The table currently contains two customers:
# MAGIC <ul>
# MAGIC <li>customer_id 1, Peter
# MAGIC <li>customer_id 2, Samarth</ul><br>
# MAGIC <li>We have an <b>updates</b> table as our source, this contains:
# MAGIC <ul>
# MAGIC <li>Updates (Peter has had two update on his address. One on 5/15 and the other on 5/20, customer_id 1)
# MAGIC <li>Deletes (Samarth wants to be removed, customer_id 2)
# MAGIC <li>Inserts (New customer Kostas, customer_id 3)
# MAGIC </ul><br>
# MAGIC <li>Our goal is to update the customers table with the new customer information from the updates source table.</ul>
# MAGIC </div>
# MAGIC     <!-- Right: Image -->
# MAGIC     <div style="flex:1 1 0; min-width:260px; text-align:right;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_change_data_capture/scd_type_1_worked_example.png"
# MAGIC         alt="Worked Example"
# MAGIC         style="max-width:100%; height:auto; border-radius:4px;">
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC When we apply SCD Type 1, our target table is updated with the latest customer information, without keeping any historical versions of the data based on the CustomerID (ID column) and ProcessDate (sequence column).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="flex:1 1 0; min-width:260px; text-align:right;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_change_data_capture/scd_type_1_worked_example_apply.png"
# MAGIC         alt="Worked Example"
# MAGIC         style="max-width:100%; height:auto; border-radius:4px;">
# MAGIC     </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Let’s walk through what happens in this example:
# MAGIC
# MAGIC - Peter (customer_id 1): His address is updated to the latest address based on the ProcessDate to 123 Main St. from the updates table.
# MAGIC - Samarth (customer_id 2): He’s been deleted, so his row is removed from the target table.
# MAGIC - Kostas (customer_id 3): He’s a new customer, so his record is inserted into the table.
# MAGIC
# MAGIC The end result? The customers table contains a current snapshot of all active customers, with no history, just the most recent customers.
# MAGIC
# MAGIC This is the simplest CDC strategy, and it’s ideal when maintaining historical changes isn’t necessary. You just need the latest, most accurate data.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## C. SCD Type 2 — Historical Tracking/Versioning

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### C1. SCD Type 2 — Overview
# MAGIC
# MAGIC Let’s talk about Slowly Changing Dimensions Type 2, or SCD Type 2, which introduces historical tracking and versioning of records.
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC <!-- Rules -->
# MAGIC <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px; margin:20px 0;">
# MAGIC   <div style="background:#F9F7F4; border-radius:10px; border-top:4px solid #618794; padding:16px 18px;">
# MAGIC     <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:8px;">On Update or Insert</div>
# MAGIC     <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC       The old record is <strong style="color:#1B3139;">preserved</strong> with an additional column indicating its validity period
# MAGIC       (start date, end date, or a current flag). A <strong style="color:#1B3139;">new row is inserted</strong> with the updated information.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="background:#F9F7F4; border-radius:10px; border-top:4px solid #98102A; padding:16px 18px;">
# MAGIC     <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:8px;">On Delete</div>
# MAGIC     <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC       When a <strong style="color:#1B3139;">record is marked as deleted</strong>, the record is kept and a column indicates the
# MAGIC       record is <strong style="color:#1B3139;">inactive</strong>.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <!-- Usage block (last paragraph in similar format) -->
# MAGIC <div style="background:#F9F7F4; border-radius:10px; border-top:4px solid #FFAB00; padding:16px 18px; margin-top:8px;">
# MAGIC   <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:8px;">When to Use SCD Type 2</div>
# MAGIC   <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC     Used when historical data is important, and the system needs to track how attributes change over time
# MAGIC     (like tracking changes in a customer's address or status).
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### C2. Worked Example — Step by Step
# MAGIC <br>
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC   <div style="display:flex; gap:24px; align-items:flex-start; flex-wrap:wrap;">
# MAGIC     <!-- Left: Text -->
# MAGIC     <div style="flex:1 1 0; min-width:260px; font-size:14px; color:#0B2026; line-height:1.7;">
# MAGIC       <!-- Your text goes here -->
# MAGIC       <b>Example of SCD Type 2 in action</b>:
# MAGIC       <br>
# MAGIC In the example table, we’ve added two important metadata columns to the target table:
# MAGIC
# MAGIC - <code>__START_AT</code> – shows when the row became active
# MAGIC - <code>__END_AT</code> – shows when the row became inactive (if it has)
# MAGIC <ul>
# MAGIC <li>A null <code>__END_AT</code> means the row is currently active.</ul>
# MAGIC
# MAGIC </div>
# MAGIC     <!-- Right: Image -->
# MAGIC     <div style="flex:1 1 0; min-width:260px; text-align:right;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_change_data_capture/scd_type_2_worked_example.png"
# MAGIC         alt="Worked Example"
# MAGIC         style="max-width:100%; height:auto; border-radius:4px;">
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC Let's breakdown the example above:
# MAGIC
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC
# MAGIC <!-- Rules -->
# MAGIC <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin:20px 0;">
# MAGIC   <div style="background:#F9F7F4; border-radius:10px; padding:16px 18px;">
# MAGIC     <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:8px;">Customer ID 1 – Peter</div>
# MAGIC     <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC     <ol>
# MAGIC     <li>The active record shows Peter’s updated address.
# MAGIC <ul>
# MAGIC <li>__END_AT is null → still active
# MAGIC <li>__START_AT marks when the update occurred
# MAGIC </ul>
# MAGIC <li>The inactive record holds Peter’s old address. We can tell it is inactive because __END_AT is populated → no longer current
# MAGIC </div>
# MAGIC   </div>
# MAGIC   <div style="background:#F9F7F4; border-radius:10px; padding:16px 18px;">
# MAGIC     <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:8px;">Customer ID 2 – Samarth</div>
# MAGIC     <div style="font-size:10pt; color:#5A6F77; line-height:1.75;"><ul>
# MAGIC     <li>Since Samarth deleted his account, his existing record is now inactive.
# MAGIC     <li>A date is added to __END_AT to indicate when he was removed.</ul></div>
# MAGIC   </div>
# MAGIC   <div style="background:#F9F7F4; border-radius:10px; padding:16px 18px;">
# MAGIC     <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:8px;">Customer ID 3 – Kostas</div>
# MAGIC     <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">A new customer, so his record was inserted into the table.
# MAGIC     <ul>
# MAGIC     <li>__START_AT shows when he joined
# MAGIC     <li>__END_AT is null → the record is currently active</ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div style="flex:1 1 0; text-align:center;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_change_data_capture/scd_type_2_worked_example_apply.png"
# MAGIC         alt="Worked Example"
# MAGIC         style="max-width:100%; height:auto; border-radius:4px;">
# MAGIC     </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ###D. Using AUTO CDC INTO in Spark Declarative Pipelines (Formerly APPLY CHANGES INTO)
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####Documentation
# MAGIC - <a href="https://docs.databricks.com/aws/en/dlt/cdc" style="color:#1976D2;">The AUTO CDC APIs: Simplify change data capture with Apache Spark™Declarative Pipelines</a>
# MAGIC <br>NOTE: The AUTO CDC APIs were previously called APPLY CHANGES, and had the same syntax.</a>
# MAGIC - <a href="https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into" style="color:#1976D2;">AUTO CDC INTO (Apache Spark™ Declarative Pipelines)</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E. The Complete Pipeline — Customers Flow Added
# MAGIC
# MAGIC Let's look at the final CDC flow we are adding to our pipeline.
# MAGIC
# MAGIC <b>Customers Flow Overview:</b>
# MAGIC 1. Ingest customer JSON files - Bring raw data into the pipeline from cloud storage into the customers_bronze streaming table.
# MAGIC
# MAGIC 2. Create customers_bronze_clean table - A cleaned streaming table that filters and formats incremental updates, inserts, and deletes from the customers_bronze streaming table.
# MAGIC
# MAGIC 3. Use AUTO CDC INTO customers_silver
# MAGIC <ul>
# MAGIC <li>Uses SCD Type 1 to overwrite customer changes (updates, inserts, and deletes).
# MAGIC <li>Implemented using AUTO CDC INTO with STORED AS SCD TYPE 1.
# MAGIC
# MAGIC This final flow combines CDC logic with the medallion architecture to maintain an updated customers (with no historical information),  critical for downstream analytics, compliance, and personalization.
# MAGIC
# MAGIC
# MAGIC <div style="flex:1 1 0; text-align:center;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.1/images/20260902T093127Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_change_data_capture/complete_pipeline.png"
# MAGIC         alt="Worked Example"
# MAGIC         style="max-width:100%; height:auto; border-radius:4px;">
# MAGIC     </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####Documentation
# MAGIC - <a href="https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into#syntax" style="color:#1976D2;">AUTO CDC INTO (Apache Spark™ Declarative Pipelines)</a>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## F. Conclusion
# MAGIC
# MAGIC In this lecture, you learned how Change Data Capture works and how to implement it using Apache Spark™ Declarative Pipelines:
# MAGIC
# MAGIC 1. **Change Data Capture (CDC)** tracks and captures inserts, updates, and deletes from a source system and applies them to a target table — keeping the Lakehouse in sync with the latest state of the data source.
# MAGIC 2. **SCD Type 1** overwrites the target table with the latest values on every change — no history is retained, making it the simplest and most efficient approach when only current data matters.
# MAGIC 3. **SCD Type 2** preserves every historical version of a record by adding `__START_AT` and `__END_AT` metadata columns — active rows have a null `__END_AT`, while inactive and deleted rows carry a date — enabling full historical analysis.
# MAGIC 4. **AUTO CDC INTO** is Lakeflow's built-in declarative CDC mechanism — it replaces complex `MERGE INTO` batch logic with a concise, readable syntax where `KEYS`, `APPLY AS DELETE WHEN`, `SEQUENCE BY`, `COLUMNS`, and `STORED AS` each play a distinct role.
# MAGIC 5. **The Customers flow** completes the full pipeline architecture — ingesting raw customer JSON files into bronze, cleaning them into `customers_bronze_clean`, and applying SCD Type 1 via `AUTO CDC INTO` to produce a current, production-ready `type1_customers_silver` table.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next section, you will perform a demonstration on Change Data Capture with AUTO CDC INTO INTO.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/>
# MAGIC <a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> |
# MAGIC <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> |
# MAGIC <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>