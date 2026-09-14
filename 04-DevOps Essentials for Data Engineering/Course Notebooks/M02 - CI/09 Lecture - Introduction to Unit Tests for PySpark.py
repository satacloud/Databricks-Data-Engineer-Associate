# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Introduction to Unit Tests for PySpark
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you’ll learn how to write and run unit tests for PySpark using pyspark.testing.utils and pytest, exploring their benefits, key functions, and practical examples to ensure code quality and reliability.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. Learn to write unit tests for PySpark code to ensure functionality and catch errors early.
# MAGIC 2. Apply the pytest framework in Databricks to execute unit tests and analyze the test results for errors.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Unit Tests Benefits
# MAGIC
# MAGIC <div class="ut-benefits-section">
# MAGIC <style>
# MAGIC .ut-benefits-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .ut-benefits-section *{box-sizing:border-box;}
# MAGIC .utb-grid{display:grid;grid-template-columns:.95fr 1.05fr;gap:24px;align-items:stretch;}
# MAGIC .utb-pyramid-card,.utb-benefit-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:22px;}
# MAGIC .utb-kicker{font-size:18px;line-height:1.2;font-weight:800;color:#FF5F46;margin-bottom:16px;}
# MAGIC .utb-pyramid-wrap{position:relative;min-height:360px;display:flex;align-items:center;justify-content:center;}
# MAGIC .utb-speed{position:absolute;left:0;top:64px;bottom:60px;width:70px;}
# MAGIC .utb-speed-line{position:absolute;left:42px;top:26px;bottom:26px;width:2px;background:#5A6F77;}
# MAGIC .utb-speed-line:before{content:"";position:absolute;left:50%;top:-6px;transform:translateX(-50%);border-left:5px solid transparent;border-right:5px solid transparent;border-bottom:9px solid #5A6F77;}
# MAGIC .utb-speed-line:after{content:"";position:absolute;left:50%;bottom:-6px;transform:translateX(-50%);border-left:5px solid transparent;border-right:5px solid transparent;border-top:9px solid #5A6F77;}
# MAGIC .utb-speed-slow,.utb-speed-fast{position:absolute;left:0;font-size:14px;font-weight:800;color:#0B2026;}
# MAGIC .utb-speed-slow{top:0;}.utb-speed-fast{bottom:0;}
# MAGIC .utb-pyramid{position:relative;width:300px;height:310px;margin-left:60px;clip-path:polygon(50% 0,100% 100%,0 100%);background:#FF5F46;border:2px solid #0B2026;overflow:hidden;}
# MAGIC .utb-layer{position:absolute;left:0;right:0;display:flex;align-items:center;justify-content:center;text-align:center;color:#fff;font-weight:850;line-height:1.2;}
# MAGIC .utb-layer.system{top:10%;height:38%;font-size:16px;background:#FF5F46;}
# MAGIC .utb-layer.integration{top:38%;height:28%;font-size:16px;background:#FF5F46;border-top:2px solid #ffffff;}
# MAGIC .utb-layer.unit{top:66%;height:34%;font-size:22px;background:#FF5F46;border-top:2px solid #ffffff;}
# MAGIC .utb-benefit-title{font-size:20px;line-height:1.25;font-weight:900;color:#0B2026;margin:0 0 14px 0;}
# MAGIC .utb-benefit-list{display:grid;gap:12px;}
# MAGIC .utb-benefit{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:14px 16px;font-size:16px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.utb-grid{grid-template-columns:1fr;}.utb-pyramid-wrap{min-height:330px;}.utb-pyramid{margin-left:40px;width:260px;height:280px;}}
# MAGIC </style>
# MAGIC <div class="utb-grid">
# MAGIC <div class="utb-pyramid-card">
# MAGIC <div class="utb-pyramid-wrap">
# MAGIC <div class="utb-speed"><div class="utb-speed-slow">Slow</div><div class="utb-speed-line"></div><div class="utb-speed-fast">Fast</div></div>
# MAGIC <div class="utb-pyramid">
# MAGIC <div class="utb-layer system">System<br>Tests</div>
# MAGIC <div class="utb-layer integration">Integration<br>Tests</div>
# MAGIC <div class="utb-layer unit">Unit Tests</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="utb-benefit-card">
# MAGIC <div class="utb-benefit-title">UNIT TESTS Benefits</div>
# MAGIC <div class="utb-benefit-list">
# MAGIC <div class="utb-benefit">Test only one specific function, on small amount of data</div>
# MAGIC <div class="utb-benefit">Catch bugs before you deploy them in your project</div>
# MAGIC <div class="utb-benefit">Refactoring your code is easier</div>
# MAGIC <div class="utb-benefit">Tests make your debugging easier</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC Let’s quickly go over the benefits of unit tests:<br>First, they test a specific function with a small amount of data, which makes it easy to isolate problems.<br>Unit tests also help you catch bugs early, before deploying to production, saving time and reducing errors.<br>They make refactoring easier since you can ensure your changes don’t break anything.<br>And lastly, they make debugging simpler by pinpointing exactly where things go wrong.<br>In short, unit tests are key to keeping your code reliable and easy to maintain.
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Pyspark.testing.utils Testing Functions
# MAGIC
# MAGIC <div class="ut-testing-utils">
# MAGIC <style>
# MAGIC .ut-testing-utils{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .ut-testing-utils *{box-sizing:border-box;}
# MAGIC .uttu-intro{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 20px;font-size:18px;line-height:1.45;font-weight:700;margin-bottom:18px;}
# MAGIC .uttu-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;align-items:stretch;}
# MAGIC .uttu-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .uttu-bar{height:8px;background:#FF5F46;}
# MAGIC .uttu-card.schema .uttu-bar{background:#2272B4;}
# MAGIC .uttu-body{padding:20px;}
# MAGIC .uttu-body h3{font-size:20px;line-height:1.25;font-weight:850;color:#0B2026;margin:0 0 14px 0;}
# MAGIC .uttu-code{font-family:Consolas,Monaco,"Courier New",monospace;background:#F9F7F4;border:1px solid #DCE0E2;border-radius:10px;padding:14px 16px;font-size:16px;line-height:1.4;color:#0B2026;}
# MAGIC .uttu-note{margin-top:18px;background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FFC400;border-radius:12px;padding:16px 18px;font-size:16px;line-height:1.45;font-weight:700;}
# MAGIC @media screen and (max-width:900px){.uttu-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="uttu-intro"><strong>pyspark.testing.utils</strong> provides helper functions to make unit testing in PySpark easier.</div>
# MAGIC <div class="uttu-grid">
# MAGIC <div class="uttu-card"><div class="uttu-bar"></div><div class="uttu-body"><h3>assertDataFrameEqual</h3><div class="uttu-code">assertDataFrameEqual(actual, expected[, ...])</div></div></div>
# MAGIC <div class="uttu-card schema"><div class="uttu-bar"></div><div class="uttu-body"><h3>assertSchemaEqual</h3><div class="uttu-code">assertSchemaEqual(actual, expected)</div></div></div>
# MAGIC </div>
# MAGIC <div class="uttu-note">There are a variety of other methods to test your unit tests, we will focus on the pyspark testing utils.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC PySpark&#x27;s built-in testing utilities simplify the process, especially when testing Spark transformations and actions.<br>assertDataFrameEqual is a utility function to check equality between an actual and expected DataFrame, with optional parameters.<br>assertSchemaEqual is a utility function to check equality between DataFrame schemas actual and expected.<br> There are a variety of other methods to test your unit tests, we will focus on the pyspark testing utils.<br>Documentation: <br>https://spark.apache.org/docs/latest/api/python/reference/pyspark.testing.html
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Unit Test Example
# MAGIC
# MAGIC <div class="ut-example-section">
# MAGIC <style>
# MAGIC .ut-example-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .ut-example-section *{box-sizing:border-box;}
# MAGIC .ute-grid{display:grid;grid-template-columns:1.15fr .85fr;gap:20px;align-items:stretch;}
# MAGIC .ute-code-card,.ute-table-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .ute-title{background:#F9F7F4;border-bottom:1px solid #DCE0E2;padding:14px 18px;font-size:18px;line-height:1.25;font-weight:850;color:#0B2026;}
# MAGIC .ute-code-card{border-top:8px solid #FF5F46;}
# MAGIC .ute-table-card{border-top:8px solid #00A972;}
# MAGIC .ute-code-card pre{margin:0;padding:20px;background:#0B2026;min-height:290px;overflow:auto;}
# MAGIC .ute-code-card code{font-family:Consolas,Monaco,"Courier New",monospace;font-size:15px;line-height:1.55;color:#ffffff;white-space:pre;}
# MAGIC .ute-table-wrap{padding:20px;}
# MAGIC .ute-results{width:100%;border-collapse:collapse;font-size:17px;line-height:1.35;}
# MAGIC .ute-results th{background:#FF5F46;color:#ffffff;border:1px solid #DCE0E2;padding:10px;text-align:left;font-weight:850;}
# MAGIC .ute-results td{border:1px solid #DCE0E2;padding:10px;color:#0B2026;}
# MAGIC .ute-results tr:nth-child(even) td{background:#F9F7F4;}
# MAGIC @media screen and (max-width:900px){.ute-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="ute-grid">
# MAGIC <div class="ute-code-card">
# MAGIC <div class="ute-title">1. You have the following function to create a column</div>
# MAGIC <pre><code>from pyspark.sql.functions import col, when
# MAGIC def add_new_col(df, new, s_col):
# MAGIC  return (df
# MAGIC          .withColumn(new,                        
# MAGIC             when(col(s_col) == 0, &#x27;Normal&#x27;)                            
# MAGIC             .otherwise(&#x27;Unknown&#x27;)))</code></pre>
# MAGIC </div>
# MAGIC <div class="ute-table-card">
# MAGIC <div class="ute-title">2. Your desired results</div>
# MAGIC <div class="ute-table-wrap">
# MAGIC <table class="ute-results"><thead><tr><th>original</th><th>desired</th></tr></thead><tbody><tr><td>0</td><td>Normal</td></tr><tr><td>1</td><td>Unknown</td></tr><tr><td>-1</td><td>Unknown</td></tr><tr><td>null</td><td>Unknown</td></tr></tbody></table>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC Let&#x27;s take a look at a simple example of a unit test.<br>You first have the initial function you want to test. In this example it is the add_new_col function.
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Unit Test Goal
# MAGIC
# MAGIC <div class="ut-goal-section">
# MAGIC <style>
# MAGIC .ut-goal-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .ut-goal-section *{box-sizing:border-box;}
# MAGIC .utg-callout{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 20px;margin-bottom:18px;font-size:20px;line-height:1.35;font-weight:850;color:#0B2026;}
# MAGIC .utg-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;align-items:stretch;}
# MAGIC .utg-code{background:#0B2026;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.14);overflow:hidden;display:flex;flex-direction:column;height:100%;}
# MAGIC .utg-code-title{background:#FF5F46;color:#ffffff;font-size:18px;font-weight:850;padding:14px 18px;min-height:54px;display:flex;align-items:center;}
# MAGIC .utg-code pre{margin:0;padding:20px 22px;background:#0B2026;overflow:auto;min-height:420px;flex:1;}
# MAGIC .utg-code code{font-family:Consolas,Monaco,"Courier New",monospace;font-size:14px;line-height:1.55;color:#ffffff;white-space:pre;}
# MAGIC .utg-steps{display:grid;grid-template-rows:repeat(5,minmax(0,1fr));gap:12px;align-self:stretch;height:100%;}
# MAGIC .utg-step{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:14px 16px;font-size:15px;line-height:1.4;color:#0B2026;font-weight:700;box-shadow:0 2px 8px rgba(27,49,57,.06);display:flex;align-items:center;}
# MAGIC @media screen and (max-width:900px){.utg-grid{grid-template-columns:1fr;}.utg-steps{grid-template-rows:auto;height:auto;}.utg-step{min-height:auto;}.utg-code pre{min-height:320px;}}
# MAGIC </style>
# MAGIC <div class="utg-callout">Compare the actual result of the function with a defined expected result</div>
# MAGIC <div class="utg-grid">
# MAGIC <div class="utg-code">
# MAGIC <div class="utg-code-title">3. Create the unit test</div>
# MAGIC <pre><code>def test_add_new_col():
# MAGIC    data = [(0,), (1,), (-1,),(None,)]
# MAGIC    columns = ["value"]
# MAGIC    df = spark.createDataFrame(data, columns)
# MAGIC
# MAGIC    actual_df = add_new_col(df, "new_value", "value")
# MAGIC
# MAGIC    expected_data = [(0, 'Normal'), (1, 'Unknown'),
# MAGIC                     (-1, 'Unknown'), (None, 'Unknown')]
# MAGIC    expected_df = spark.createDataFrame(expected_data,
# MAGIC                                        ["value", "new_value"])
# MAGIC
# MAGIC    assertDataFrameEqual(actual_df, expected_df)</code></pre>
# MAGIC </div>
# MAGIC <div class="utg-steps">
# MAGIC <div class="utg-step">Create a unit test function to test the function. Name the unit test function accordingly.</div>
# MAGIC <div class="utg-step">Create a sample DataFrame to test any use cases you can think of.</div>
# MAGIC <div class="utg-step">Execute your function on the sample data and store the result.</div>
# MAGIC <div class="utg-step">Create an expected result DataFrame using the sample data.</div>
# MAGIC <div class="utg-step">Check the two DataFrames. If they are not identical an error will be returned.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC A well-implemented CI/CD process enables faster and more reliable releases, reduces errors in production through automated testing, and supports easier rollbacks with safe deployment practices.
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Unit Testing Framework - pytest
# MAGIC
# MAGIC <div class="pytest-framework-section">
# MAGIC <style>
# MAGIC .pytest-framework-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .pytest-framework-section *{box-sizing:border-box;}
# MAGIC .pytest-intro{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 20px;margin-bottom:18px;font-size:18px;line-height:1.45;font-weight:700;}
# MAGIC .pytest-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;align-items:stretch;}
# MAGIC .pytest-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .pytest-bar{height:8px;background:#FF5F46;}
# MAGIC .pytest-card:nth-child(2) .pytest-bar{background:#2272B4;}
# MAGIC .pytest-card:nth-child(3) .pytest-bar{background:#00A972;}
# MAGIC .pytest-card:nth-child(4) .pytest-bar{background:#98102A;}
# MAGIC .pytest-body{padding:18px;}
# MAGIC .pytest-body h3{font-size:18px;line-height:1.25;margin:0 0 10px 0;color:#0B2026;font-weight:850;}
# MAGIC .pytest-body p{font-size:16px;line-height:1.45;margin:0;color:#0B2026;}
# MAGIC .pytest-bottom{margin-top:18px;background:#FFF5D6;border:1px solid #FFC400;border-left:6px solid #FFC400;border-radius:12px;padding:16px 18px;font-size:16px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.pytest-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="pytest-intro"><strong>Pytest</strong> is popular testing framework for Python that makes it easy to write simple and scalable test cases.</div>
# MAGIC <div class="pytest-grid">
# MAGIC <div class="pytest-card"><div class="pytest-bar"></div><div class="pytest-body"><h3>Uses Simple Syntax</h3><p>Minimal syntax, just define functions starting with test_</p></div></div>
# MAGIC <div class="pytest-card"><div class="pytest-bar"></div><div class="pytest-body"><h3>Provides Assertions</h3><p>Use assert statements to provide detailed error messages on failure</p></div></div>
# MAGIC <div class="pytest-card"><div class="pytest-bar"></div><div class="pytest-body"><h3>Automatic Discovery</h3><p>Finds and runs all tests automatically with a simple configuration</p></div></div>
# MAGIC <div class="pytest-card"><div class="pytest-bar"></div><div class="pytest-body"><h3>Rich Ecosystem</h3><p>Extend functionality with plugins for coverage, parallel tests, and more</p></div></div>
# MAGIC </div>
# MAGIC <div class="pytest-bottom">This course provides a simple introduction to pytest. There are many testing frameworks available, select the one that best meets your organization's needs.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>
# MAGIC Next like talk about the pytest unit testing framework.<br>Pytest is a popular testing framework for Python that makes it easy to write simple and scalable test cases. It provides a variety of benefits for executing your unit tests.<br>First up, pytest has a very simple syntax. You don’t need to worry about complex setup. Just write test functions that start with test_, and pytest will automatically pick them up and run them.<br>Next, pytest leverages Python’s built-in assert statements (or you can use other assert statements like in pyspark). These are simple to use but provide detailed error messages when a test fails, making your function debugging much easier and faster by understanding what went wrong.<br>Pytest also automatically discovers and runs all your tests. There’s no need to manually configure which tests to run. Just name your test files and functions with the test_ prefix, and pytest will take care of the rest.<br>Lastly, pytest has a rich ecosystem of plugins to extend its functionality. Whether you need test coverage reports, parallel test execution, or integration with other tools, there&#x27;s a plugin for almost anything, allowing you to customize pytest to fit your needs.<br>This course provides a simple introduction to pytest. There are many testing frameworks available, select the one that best meets your organization&#x27;s needs.
# MAGIC </p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC