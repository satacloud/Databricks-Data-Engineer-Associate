# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Information
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC In this training, we use <strong>Lakebase Provisioned</strong>. 
# MAGIC For future use, we recommend <strong>Lakebase Autoscaling</strong>, which supports additional features beyond the core concepts covered here.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Warning
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC PostgreSQL does not support cross-database queries, so you can only query the database selected as your default.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Error</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     This is an error message. Use this style to highlight critical issues, errors, or anti-patterns that should be avoided.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #4caf50;
# MAGIC   background: #e8f5e9;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#2e7d32; margin-bottom:6px; font-size: 1.1em;">Success</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     This is an error message. Use this style to highlight critical issues, errors, or anti-patterns that should be avoided.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #7b1fa2;
# MAGIC   background: #f3e5f5;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#4a148c; margin-bottom:6px; font-size: 1.1em;">Notes</strong>
# MAGIC   <div style="color:#333;">
# MAGIC     This is a note. Use this style to highlight important remarks or observations that the reader should keep in mind.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Theme Light
# MAGIC
# MAGIC | Color | Hex |
# MAGIC |------|------|
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#0b2026;border:1px solid #ccc;"></span> | #0b2026 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#F9F7F4;border:1px solid #ccc;"></span> | #F9F7F4 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FF5F46;border:1px solid #ccc;"></span> | #FF5F46 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#EEEDE9;border:1px solid #ccc;"></span> | #EEEDE9 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#1B5162;border:1px solid #ccc;"></span> | #1B5162 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#00A972;border:1px solid #ccc;"></span> | #00A972 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#98102A;border:1px solid #ccc;"></span> | #98102A |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FFAB00;border:1px solid #ccc;"></span> | #FFAB00 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#618794;border:1px solid #ccc;"></span> | #618794 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#4299E0;border:1px solid #ccc;"></span> | #4299E0 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Custom
# MAGIC
# MAGIC | Color | Hex |
# MAGIC |------|------|
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#C2C2C2;border:1px solid #ccc;"></span> | #C2C2C2 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#801C17;border:1px solid #ccc;"></span> | #801C17 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FABFBA;border:1px solid #ccc;"></span> | #FABFBA |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FF9E94;border:1px solid #ccc;"></span> | #FF9E94 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FF3621;border:1px solid #ccc;"></span> | #FF3621 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FF6952;border:1px solid #ccc;"></span> | #FF6952 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FF5F46;border:1px solid #ccc;"></span> | #FF5F46 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FF8774;border:1px solid #ccc;"></span> | #FF8774 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#CD7F32;border:1px solid #ccc;"></span> | #CD7F32 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#F9F7F4;border:1px solid #ccc;"></span> | #F9F7F4 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FFCC66;border:1px solid #ccc;"></span> | #FFCC66 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#FFAB00;border:1px solid #ccc;"></span> | #FFAB00 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#EEEDE9;border:1px solid #ccc;"></span> | #EEEDE9 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#00A972;border:1px solid #ccc;"></span> | #00A972 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#0B2026;border:1px solid #ccc;"></span> | #0B2026 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#618794;border:1px solid #ccc;"></span> | #618794 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#1B3139;border:1px solid #ccc;"></span> | #1B3139 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#5A6F77;border:1px solid #ccc;"></span> | #5A6F77 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#DCE0E2;border:1px solid #ccc;"></span> | #DCE0E2 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#303F47;border:1px solid #ccc;"></span> | #303F47 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#90A5B1;border:1px solid #ccc;"></span> | #90A5B1 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#2272B4;border:1px solid #ccc;"></span> | #2272B4 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#C4CCD6;border:1px solid #ccc;"></span> | #C4CCD6 |
# MAGIC | <span style="display:inline-block;width:18px;height:18px;background:#98102A;border:1px solid #ccc;"></span> | #98102A |
# MAGIC