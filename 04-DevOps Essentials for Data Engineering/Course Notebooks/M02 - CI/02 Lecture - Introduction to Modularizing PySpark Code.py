# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Introduction to Modularizing PySpark Code
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, we will discuss how to modularize PySpark code and explore the benefits of doing so.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. Understand how to modularize PySpark code into reusable and maintainable modules.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Modularizing PySpark Code: Non-Modularized Code (Before)
# MAGIC
# MAGIC <div class="mpc-before">
# MAGIC <style>
# MAGIC .mpc-before{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .mpc-before *{box-sizing:border-box;}
# MAGIC .mpc-before-grid{display:grid;grid-template-columns:.85fr 1.15fr;gap:20px;align-items:stretch;}
# MAGIC .mpc-before-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .mpc-before-bar{height:8px;background:#FF5F46;}
# MAGIC .mpc-before-body{padding:22px;}
# MAGIC .mpc-before-title{font-size:22px;line-height:1.2;font-weight:800;margin:0 0 16px 0;color:#0B2026;}
# MAGIC .mpc-issue-list{display:grid;gap:14px;}
# MAGIC .mpc-issue{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 18px;}
# MAGIC .mpc-issue h3{font-size:18px;line-height:1.25;font-weight:800;margin:0 0 8px 0;color:#0B2026;}
# MAGIC .mpc-issue p{font-size:16px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC .mpc-code-card{background:#0B2026;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.14);overflow:hidden;}
# MAGIC .mpc-code-title{background:#FF5F46;color:#ffffff;font-size:18px;font-weight:800;padding:14px 18px;}
# MAGIC .mpc-code-card pre{margin:0;padding:20px 22px;overflow:auto;background:#0B2026;color:#ffffff;min-height:360px;}
# MAGIC .mpc-code-card code{font-family:Consolas,Monaco,"Courier New",monospace;font-size:15px;line-height:1.55;color:#ffffff;white-space:pre;}
# MAGIC @media screen and (max-width:900px){.mpc-before-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="mpc-before-grid">
# MAGIC <div class="mpc-before-card">
# MAGIC <div class="mpc-before-bar"></div>
# MAGIC <div class="mpc-before-body">
# MAGIC <div class="mpc-before-title">Issues</div>
# MAGIC <div class="mpc-issue-list">
# MAGIC <div class="mpc-issue"><h3>Everything is in one block</h3><p>Making it harder to modify or test specific parts, such as loading data or adding new columns.</p></div>
# MAGIC <div class="mpc-issue"><h3>Code duplication</h3><p>Could occur if the same operations are needed elsewhere in the project.</p></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="mpc-code-card">
# MAGIC <div class="mpc-code-title">Non-Modularized Code (Before)</div>
# MAGIC <pre><code># Load data
# MAGIC df = (spark
# MAGIC       .read
# MAGIC       .csv(&quot;health.csv&quot;,
# MAGIC            header=True,
# MAGIC            inferSchema=True))
# MAGIC
# MAGIC # Create column
# MAGIC df = (df
# MAGIC       .withColumn(&quot;NewColumn&quot;,
# MAGIC           when(col(&quot;Column&quot;) == 0, &#x27;Normal&#x27;)
# MAGIC           .otherwise(&#x27;Unknown&#x27;)))
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Let's begin by taking a look at some non-modularized code. Looking at the example here, who has written code like this before? Where code is split into several lines or cells, consistently writing specific logic to transforms or prepare or analyze your data? As you continue to build your flow the code becomes hard to read and unmanageable? My guess is, we've all been there.</p>
# MAGIC <p>While this approach can work, it creates several challenges when trying to implement CI/CD into your development process.</p>
# MAGIC <p>First, everything is lumped into one block, making it harder to modify or test specific parts of the code (for example, loading data or adding new columns).</p>
# MAGIC <p>Another issue is code duplication. If the same operations are needed elsewhere in the project, you might end up repeating yourself, leading to unnecessary complexity and maintenance headaches. When one thing changes in your logic, you have to change that everywhere.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Modularizing PySpark Code: Modularized Code (After)
# MAGIC
# MAGIC <div class="mpc-after">
# MAGIC <style>
# MAGIC .mpc-after{width:1200px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .mpc-after *{box-sizing:border-box;}
# MAGIC .mpc-transform-grid{display:flex;flex-direction:column;gap:14px;align-items:center;}
# MAGIC .mpc-mini-code{width:100%;max-width:680px;min-width:0;background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;display:flex;flex-direction:column;}
# MAGIC .mpc-mini-code.before{border-top:8px solid #FF5F46;}
# MAGIC .mpc-mini-code.after{border-top:8px solid #00A972;}
# MAGIC .mpc-mini-code h3{font-size:18px;line-height:1.25;font-weight:800;margin:0;padding:15px 18px;background:#F9F7F4;border-bottom:1px solid #DCE0E2;color:#0B2026;}
# MAGIC .mpc-mini-code pre{margin:0;padding:18px;background:#0B2026;overflow:auto;min-height:340px;flex:1;}
# MAGIC .mpc-mini-code code{font-family:Consolas,Monaco,"Courier New",monospace;font-size:13px;line-height:1.55;color:#ffffff;white-space:pre;}
# MAGIC .mpc-arrow{width:46px;height:46px;border-radius:50%;background:#FFF1EE;color:#FF5F46;display:flex;align-items:center;justify-content:center;font-size:34px;font-weight:800;margin:auto;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .mpc-function-row{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:18px;}
# MAGIC .mpc-function-card{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #00A972;border-radius:12px;padding:16px 18px;}
# MAGIC .mpc-function-card h3{font-size:18px;line-height:1.25;font-weight:800;margin:0 0 8px 0;color:#0B2026;}
# MAGIC .mpc-function-card p{font-size:16px;line-height:1.45;margin:0;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.mpc-function-row{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="mpc-transform-grid">
# MAGIC <div class="mpc-mini-code before"><h3>Non-Modularized Code</h3><pre><code># Load data
# MAGIC df = (spark
# MAGIC       .read
# MAGIC       .csv("health.csv",
# MAGIC            header=True,
# MAGIC            inferSchema=True))
# MAGIC
# MAGIC # Create column
# MAGIC df = (df
# MAGIC       .withColumn("NewColumn",
# MAGIC           when(col("Column") == 0, 'Normal')
# MAGIC           .otherwise('Unknown')))</code></pre></div>
# MAGIC <div class="mpc-arrow" aria-hidden="true">↓</div>
# MAGIC <div class="mpc-mini-code after"><h3>Modularized Code (After)</h3><pre><code>def load_data(file_path):
# MAGIC     return (spark
# MAGIC             .read
# MAGIC             .csv(file_path,
# MAGIC                  header=True,
# MAGIC                  inferSchema=True))
# MAGIC
# MAGIC
# MAGIC def add_new_col(df, new, s_col):
# MAGIC     return (df
# MAGIC             .withColumn(new,
# MAGIC                 when(col(s_col) == 0, 'Normal')
# MAGIC                 .otherwise('Unknown')))</code></pre></div>
# MAGIC </div>
# MAGIC <div class="mpc-function-row">
# MAGIC <div class="mpc-function-card"><h3>load_data()</h3><p>Turns the code used to read the CSV file into a reusable function.</p></div>
# MAGIC <div class="mpc-function-card"><h3>add_new_col()</h3><p>Refactors column creation logic into a reusable function.</p></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Instead, you want to focus on modularizing your code.</p>
# MAGIC <p>For instance, the code you wrote to read the CSV file into a Spark DataFrame can be turned into a function, such as def load_data().</p>
# MAGIC <p>Similarly, the function that creates a new column based on an existing one could be refactored into def add_new_col(), or, ideally, a more specific name that clearly reflects what the function is doing (example, def add_age_category() if you're categorizing ages).</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Modularized Code Benefits
# MAGIC
# MAGIC <div class="mpc-benefits">
# MAGIC <style>
# MAGIC .mpc-benefits{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .mpc-benefits *{box-sizing:border-box;}
# MAGIC .mpc-benefit-grid{display:grid;grid-template-columns:.9fr 1.1fr;gap:20px;align-items:stretch;}
# MAGIC .mpc-benefit-list{display:grid;gap:14px;}
# MAGIC .mpc-flip-card{height:126px;perspective:1000px;}
# MAGIC .mpc-flip-inner{position:relative;width:100%;height:100%;transition:transform .7s ease;transform-style:preserve-3d;}
# MAGIC .mpc-flip-card:hover .mpc-flip-inner{transform:rotateY(180deg);}
# MAGIC .mpc-flip-front,.mpc-flip-back{position:absolute;inset:0;border-radius:14px;backface-visibility:hidden;-webkit-backface-visibility:hidden;box-shadow:0 3px 12px rgba(27,49,57,.07);display:flex;align-items:center;padding:18px 20px;}
# MAGIC .mpc-flip-front{background:#FF5F46;color:#ffffff;border:1px solid #FF5F46;gap:14px;}
# MAGIC .mpc-flip-card.green .mpc-flip-front{background:#00A972;border-color:#00A972;}
# MAGIC .mpc-flip-card.blue .mpc-flip-front{background:#2272B4;border-color:#2272B4;}
# MAGIC .mpc-flip-back{background:#F9F7F4;border:1px solid #DCE0E2;transform:rotateY(180deg);border-left:6px solid #FF5F46;}
# MAGIC .mpc-flip-card.green .mpc-flip-back{border-left-color:#00A972;}
# MAGIC .mpc-flip-card.blue .mpc-flip-back{border-left-color:#2272B4;}
# MAGIC .mpc-num{width:38px;height:38px;border-radius:50%;background:rgba(255,255,255,.18);border:2px solid rgba(255,255,255,.65);color:#ffffff;display:flex;align-items:center;justify-content:center;font-weight:900;flex:0 0 38px;font-size:18px;}
# MAGIC .mpc-flip-front h3{font-size:20px;line-height:1.25;font-weight:850;margin:0;color:#ffffff;}
# MAGIC .mpc-flip-back p{font-size:16px;line-height:1.45;margin:0;color:#0B2026;font-weight:600;}
# MAGIC .mpc-benefit-code{background:#0B2026;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.14);overflow:hidden;}
# MAGIC .mpc-benefit-code-title{background:#00A972;color:#ffffff;font-size:18px;font-weight:800;padding:14px 18px;}
# MAGIC .mpc-benefit-code pre{margin:0;padding:20px 22px;overflow:auto;background:#0B2026;color:#ffffff;min-height:390px;}
# MAGIC .mpc-benefit-code code{font-family:Consolas,Monaco,"Courier New",monospace;font-size:15px;line-height:1.55;color:#ffffff;white-space:pre;}
# MAGIC @media screen and (max-width:900px){.mpc-benefit-grid{grid-template-columns:1fr;}.mpc-flip-card{height:120px;}}
# MAGIC </style>
# MAGIC <div class="mpc-benefit-grid">
# MAGIC <div class="mpc-benefit-list">
# MAGIC <div class="mpc-flip-card">
# MAGIC <div class="mpc-flip-inner">
# MAGIC <div class="mpc-flip-front"><div class="mpc-num">1</div><h3>Easier Maintenance</h3></div>
# MAGIC <div class="mpc-flip-back"><p>Update only specific functions without changing the entire script.</p></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="mpc-flip-card green">
# MAGIC <div class="mpc-flip-inner">
# MAGIC <div class="mpc-flip-front"><div class="mpc-num">2</div><h3>Reuse</h3></div>
# MAGIC <div class="mpc-flip-back"><p>Reuse functions in different projects.</p></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="mpc-flip-card blue">
# MAGIC <div class="mpc-flip-inner">
# MAGIC <div class="mpc-flip-front"><div class="mpc-num">3</div><h3>Testing</h3></div>
# MAGIC <div class="mpc-flip-back"><p>Test individual functions through unit tests to ensure code reliability.</p></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="mpc-benefit-code">
# MAGIC <div class="mpc-benefit-code-title">Modularized Code</div>
# MAGIC <pre><code>def load_data(file_path):
# MAGIC     return (spark
# MAGIC             .read
# MAGIC             .csv(file_path,
# MAGIC                  header=True,
# MAGIC                  inferSchema=True))
# MAGIC
# MAGIC
# MAGIC def add_new_col(df, new, s_col):
# MAGIC     return (df
# MAGIC             .withColumn(new,
# MAGIC                 when(col(s_col) == 0, &#x27;Normal&#x27;)
# MAGIC                 .otherwise(&#x27;Unknown&#x27;)))
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Let’s talk about some of the key benefits of modularizing your code.</p>
# MAGIC <p>First, functions make maintenance easier. When your code is organized into functions, you can update or change specific parts of the code without affecting the entire script. This makes it way easier to manage and fix issues down the line.</p>
# MAGIC <p>Another big benefit is reusability. Once you’ve written a function like load_data() or add_new_col(), you can reuse it across different projects or scenarios. No need to rewrite the same code over and over again. it saves time and reduces errors.</p>
# MAGIC <p>Lastly, functions improve testability. By isolating specific logic into functions, you can write unit tests for each function individually, ensuring that each part of your code is working as expected. This leads to more reliable and bug-free code.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Conclusion
# MAGIC <ul>
# MAGIC <li>Non-modularized code is harder to modify, test, and maintain when logic is kept in one block.</li>
# MAGIC <li>Modularized code turns repeatable logic into functions such as <strong>load_data()</strong> and <strong>add_new_col()</strong>.</li>
# MAGIC <li>Modularizing PySpark code improves maintenance, reuse, and testing.</li>
# MAGIC </ul>
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next demo, you will modularize PySpark Code.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC