# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Executing Integration Tests with SDP and Jobs
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you’ll learn how to execute integration tests in Databricks using Apache Spark™ Declarative Pipelines expectations and Databricks Lakeflow Jobs tasks to validate data pipelines end-to-end
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. Learn to run integration tests using Apache Spark™ Declarative Pipeline and Lakeflow Jobs to validate data pipeline functionality.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Executing Integration Tests
# MAGIC
# MAGIC <div class="eit-exec-tests">
# MAGIC <style>
# MAGIC .eit-exec-tests{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .eit-exec-tests *{box-sizing:border-box;}
# MAGIC .eit-top-callout{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 22px;margin-bottom:18px;text-align:center;font-size:24px;line-height:1.25;font-weight:800;color:#FF5F46;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .eit-method-grid{display:grid;grid-template-columns:1fr 28px 1fr;gap:18px;align-items:stretch;}
# MAGIC .eit-method-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;min-height:300px;display:flex;flex-direction:column;}
# MAGIC .eit-method-card.sdp{border-top:8px solid #FF5F46;}
# MAGIC .eit-method-card.jobs{border-top:8px solid #2272B4;}
# MAGIC .eit-card-body{padding:24px;display:flex;flex-direction:column;align-items:center;text-align:center;gap:16px;flex:1;}
# MAGIC .eit-card-body img{width:92px;height:92px;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC .eit-card-body h3{font-size:22px;line-height:1.25;margin:0;color:#0B2026;font-weight:850;}
# MAGIC .eit-card-body ul{margin:0;padding-left:22px;text-align:left;}
# MAGIC .eit-card-body li{font-size:18px;line-height:1.45;color:#0B2026;margin-bottom:8px;}
# MAGIC .eit-divider{position:relative;display:flex;align-items:center;justify-content:center;}
# MAGIC .eit-divider:before{content:"";position:absolute;top:0;bottom:0;width:3px;background:#FF5F46;border-radius:999px;}
# MAGIC .eit-divider:after{content:"";position:absolute;width:12px;height:12px;border-radius:50%;background:#FF5F46;top:50%;transform:translateY(-50%);}
# MAGIC @media screen and (max-width:900px){.eit-method-grid{grid-template-columns:1fr;}.eit-divider{min-height:32px;}.eit-divider:before{left:50%;right:auto;top:0;bottom:0;}.eit-divider:after{left:50%;transform:translate(-50%,-50%);}}
# MAGIC </style>
# MAGIC <div class="eit-top-callout">With Spark Declarative Pipelines (SDP) or Lakeflow Jobs</div>
# MAGIC <div class="eit-method-grid">
# MAGIC <div class="eit-method-card sdp">
# MAGIC <div class="eit-card-body">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_executing_integration_test/SDP_icon.png" alt="Spark Declarative Pipelines icon">
# MAGIC <h3>Spark Declarative Pipelines (SDP)</h3>
# MAGIC <ul><li>Use <strong>SDP expectations</strong> to check pipeline’s results (demo technique).</li></ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="eit-divider" aria-hidden="true"></div>
# MAGIC <div class="eit-method-card jobs">
# MAGIC <div class="eit-card-body">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_executing_integration_test/lakeflow_jobs_icon.png" alt="Lakeflow Jobs icon">
# MAGIC <h3>Lakeflow Jobs</h3>
# MAGIC <ul><li>Implement it as a <strong>Databricks Job with multiple tasks</strong> - similarly what is typically done for non-SDP code.</li></ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>In Databricks, two simple ways to execute integration tests are with Spark Declarative Pipelines and Lakeflow Jobs.</p>
# MAGIC <p>With Spark Declarative Pipelines, we can use SDP expectations to validate the results of tables within pipelines. This is the method we will use in our project.</p>
# MAGIC <p>Alternatively, you can implement integration tests using Databricks Lakeflow Jobs by creating multiple notebooks that test specific aspects of your data pipeline. These notebooks can be added as tasks within the Lakeflow Job. While the integration tests themselves may be similar, you&#x27;re using different tools in Databricks to execute them. Choose the tool that best fits your needs.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Integration Test Methods
# MAGIC <div class="eit-method-carousel">
# MAGIC <style>
# MAGIC .eit-method-carousel{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .eit-method-carousel *{box-sizing:border-box;}
# MAGIC .eit-method-carousel input[type="radio"]{display:none;}
# MAGIC .eit-tabs{display:flex;justify-content:flex-start;align-items:center;border-bottom:2px solid #EEEDE9;margin-bottom:18px;flex-wrap:wrap;}
# MAGIC .eit-tabs label{padding:10px 18px;border:none;border-bottom:3px solid transparent;background:none;font-size:18px;font-weight:800;color:#888;cursor:pointer;margin-bottom:-2px;}
# MAGIC #eit-tab-sdp:checked ~ .eit-tabs label[for="eit-tab-sdp"]{color:#FF5F46;border-bottom-color:#FF5F46;}
# MAGIC #eit-tab-jobs:checked ~ .eit-tabs label[for="eit-tab-jobs"]{color:#2272B4;border-bottom-color:#2272B4;}
# MAGIC .eit-panel{display:none;}
# MAGIC #eit-tab-sdp:checked ~ .eit-panels .eit-sdp-panel{display:block;}
# MAGIC #eit-tab-jobs:checked ~ .eit-panels .eit-jobs-panel{display:block;}
# MAGIC .eit-method-grid{display:grid;grid-template-columns:.34fr .66fr;gap:18px;align-items:stretch;}
# MAGIC .eit-left{display:grid;gap:14px;}
# MAGIC .eit-card{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:18px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .eit-card.green{border-left-color:#00A972;}
# MAGIC .eit-card.blue{border-left-color:#2272B4;}
# MAGIC .eit-card.purple{border-left-color:#8C83F7;}
# MAGIC .eit-card h3{font-size:18px;line-height:1.25;margin:0 0 10px 0;color:#0B2026;font-weight:850;}
# MAGIC .eit-card p,.eit-card li{font-size:16px;line-height:1.45;color:#0B2026;margin:0;}
# MAGIC .eit-card ul{margin:0;padding-left:20px;}
# MAGIC .eit-card li{margin-bottom:7px;}
# MAGIC .eit-image{background:#fff;border:1px solid #DCE0E2;border-radius:14px;padding:14px;display:flex;align-items:center;justify-content:center;box-shadow:0 3px 12px rgba(27,49,57,.07);}
# MAGIC .eit-image img{width:100%;max-height:560px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC @media screen and (max-width:900px){.eit-method-grid{grid-template-columns:1fr;}.eit-image img{max-height:420px;}.eit-tabs label{font-size:16px;}}
# MAGIC </style>
# MAGIC <input type="radio" id="eit-tab-sdp" name="eit-method-tab" checked>
# MAGIC <input type="radio" id="eit-tab-jobs" name="eit-method-tab">
# MAGIC <div class="eit-tabs">
# MAGIC <label for="eit-tab-sdp">SDP - Method 1 - Expectations</label>
# MAGIC <label for="eit-tab-jobs">Jobs - Method 2 - Tasks</label>
# MAGIC </div>
# MAGIC <div class="eit-panels">
# MAGIC <div class="eit-panel eit-sdp-panel">
# MAGIC <div class="eit-method-grid">
# MAGIC <div class="eit-left">
# MAGIC <div class="eit-card"><h3>Shared SDP Code</h3><p>Same SDP code (notebooks) defining transformation logic (using custom functions). Used in dev, stage and prod.</p></div>
# MAGIC <div class="eit-card green"><h3>Validation examples</h3><ul><li>Validate # of rows</li><li>Validates distinct values in the new columns</li></ul></div>
# MAGIC <div class="eit-card blue"><h3>Expectations in environments</h3><p>Test tables leverage <strong>expectations</strong> in <strong>dev</strong> and <strong>stage</strong>.</p></div>
# MAGIC </div>
# MAGIC <div class="eit-image">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_executing_integration_test/SDP_method1_expectations.png" alt="SDP expectations diagram">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="eit-panel eit-jobs-panel">
# MAGIC <div class="eit-method-grid">
# MAGIC <div class="eit-left">
# MAGIC <div class="eit-card blue"><h3>Unit tests</h3><p>Testing individual units of code in isolation. If the unit test pass continue on into the Job.</p></div>
# MAGIC <div class="eit-card green"><h3>Pipeline task</h3><p>Executing a Spark Declarative Pipeline to create the necessary tables without using expectations.</p></div>
# MAGIC <div class="eit-card purple"><h3>Tests can include</h3><ul><li>Validate # of rows</li><li>Tables were created successfully</li><li>Columns contain correct distinct values</li><li>Columns contain values between a specific range</li><li>Columns contain no duplicates</li></ul></div>
# MAGIC </div>
# MAGIC <div class="eit-image">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_executing_integration_test/jobs_method2_tasks.png" alt="Jobs method tasks integration tests diagram">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC SDP - Method 1 - Expectations:
# MAGIC
# MAGIC <p>Let&#x27;s example the integration test with SDP.</p>
# MAGIC <p>In this example we will use SDP to ingest CSV files from each catalog (dev, stage and prod) based on the pipeline configuration variable, it will simply read the data from the corresponding target environment.</p>
# MAGIC <p>Regardless of the target environment (dev, stage, or prod), the same SDP is executed. Focus on the black squares under the &quot;Shared SDP Code&quot; in the image. The same SDP transformation logic, which includes custom functions, is applied in each environment.</p>
# MAGIC <p>In this example, data from the target catalog is ingested into the &quot;health_bronze&quot; table, then cleaned in the &quot;health_silver&quot; table, and finally aggregated into a materialized view in the &quot;chol_age_agg&quot; gold table for each environment.</p>
# MAGIC <p>The purple boxes beneath the shared SDP represent test materialized views created with SDP expectations during the dev and stage runs. Since we&#x27;re using static data for dev and stage, we know the expected output, which allows us to test our shared SDP code. In this case, we’re performing simple expectations for demonstration purposed, like counting the number of rows in the tables, to confirm correct ingestion. Typically you would create much more focused tests.</p>
# MAGIC
# MAGIC Jobs - Method 2 - Tasks
# MAGIC
# MAGIC <p>Instead of using SDP and expectations for integration tests, you can also use Databricks Lakeflow Jobs with tasks.</p>
# MAGIC <p>Looking at the entire Job for this simple project, we start by executing unit tests to test individual functions in isolation. If any unit test fails, the job will fail.</p>
# MAGIC <p>Next, we execute the SDP to ingest the data. In this example, we&#x27;re running the same SDP as before but without the expectations.</p>
# MAGIC <p>After that, we perform integration tests using notebooks set as tasks within Lakeflow Job. For this job, you’ll need to configure the correct parameters for your target environment (dev, stage, or production). You can run a variety of integration tests, such as counting rows in a table, verifying that tables were created successfully, checking that tables contain the specified columns or distinct values, ensuring column values fall within a certain range, confirming there are no duplicates, and more.</p>
# MAGIC <p>Finally, once the unit tests, data pipeline, and integration tests are successfully executed, the final visualization is created.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC