# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Course Project Overview
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you'll build a course project.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. **Build** a retail data processing pipeline that demonstrates all the concepts learnt till date.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Building a Retail Data Processing Pipeline
# MAGIC
# MAGIC We'll build a retail data processing pipeline that demonstrates all the concepts we're learning.
# MAGIC
# MAGIC Click each step to build a retail data processing pipeline.
# MAGIC
# MAGIC <div class="cpo-image-steps">
# MAGIC <style>
# MAGIC .cpo-image-steps{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .cpo-image-steps *{box-sizing:border-box;}
# MAGIC .cpo-image-steps input[type="radio"]{display:none;}
# MAGIC .cpo-step-buttons{display:flex;gap:10px;flex-wrap:wrap;justify-content:center;margin-bottom:16px;}
# MAGIC .cpo-step-buttons label{background:#F9F7F4;color:#0B2026;border:2px solid #DCE0E2;border-radius:10px;padding:12px 14px;min-width:126px;min-height:58px;font-size:15px;font-weight:800;line-height:1.25;text-align:center;cursor:pointer;box-shadow:0 2px 8px rgba(27,49,57,.06);transition:all .2s ease;}
# MAGIC .cpo-step-buttons label:hover{background:#FFF1EE;border-color:#FF5F46;}
# MAGIC #cpo-step1:checked ~ .cpo-step-buttons label[for="cpo-step1"],#cpo-step2:checked ~ .cpo-step-buttons label[for="cpo-step2"],#cpo-step3:checked ~ .cpo-step-buttons label[for="cpo-step3"],#cpo-step4:checked ~ .cpo-step-buttons label[for="cpo-step4"],#cpo-step5:checked ~ .cpo-step-buttons label[for="cpo-step5"],#cpo-step6:checked ~ .cpo-step-buttons label[for="cpo-step6"],#cpo-step7:checked ~ .cpo-step-buttons label[for="cpo-step7"]{background:#FF5F46;border-color:#FF5F46;color:#ffffff;}
# MAGIC .cpo-image-card{background:#ffffff;border:none;border-radius:0;padding:0;box-shadow:none;overflow:hidden;}
# MAGIC .cpo-stage{position:relative;width:100%;overflow:hidden;background:#ffffff;}
# MAGIC .cpo-stage img{display:none;width:100%;height:auto;background:transparent;}
# MAGIC #cpo-step1:checked ~ .cpo-image-card .img-step1,#cpo-step2:checked ~ .cpo-image-card .img-step2,#cpo-step3:checked ~ .cpo-image-card .img-step3,#cpo-step4:checked ~ .cpo-image-card .img-step4,#cpo-step5:checked ~ .cpo-image-card .img-step5,#cpo-step6:checked ~ .cpo-image-card .img-step6,#cpo-step7:checked ~ .cpo-image-card .img-step7{display:block;animation:cpoFade .3s ease both;}
# MAGIC @keyframes cpoFade{0%{opacity:0;transform:translateY(6px);}100%{opacity:1;transform:translateY(0);}}
# MAGIC @media screen and (max-width:900px){.cpo-image-card{overflow-x:auto;}.cpo-stage{min-width:900px;}}
# MAGIC </style>
# MAGIC <input type="radio" id="cpo-step1" name="cpo-step-view" checked>
# MAGIC <input type="radio" id="cpo-step2" name="cpo-step-view">
# MAGIC <input type="radio" id="cpo-step3" name="cpo-step-view">
# MAGIC <input type="radio" id="cpo-step4" name="cpo-step-view">
# MAGIC <input type="radio" id="cpo-step5" name="cpo-step-view">
# MAGIC <input type="radio" id="cpo-step6" name="cpo-step-view">
# MAGIC <input type="radio" id="cpo-step7" name="cpo-step-view">
# MAGIC <div class="cpo-step-buttons">
# MAGIC <label for="cpo-step1">Step 1:<br>Cloud Storage</label>
# MAGIC <label for="cpo-step2">Step 2:<br>Ingesting Data</label>
# MAGIC <label for="cpo-step3">Step 3:<br>Joining Data</label>
# MAGIC <label for="cpo-step4">Step 4:<br>If/Else Block</label>
# MAGIC <label for="cpo-step5">Step 5:<br>For Each Task</label>
# MAGIC <label for="cpo-step6">Step 6:<br>Transforming Data</label>
# MAGIC <label for="cpo-step7">Step 7:<br>Dashboard Creation</label>
# MAGIC </div>
# MAGIC <div class="cpo-image-card">
# MAGIC <div class="cpo-stage">
# MAGIC <img class="img-step1" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_course_project_overview/course_project_step_1_cloud_storage.png" alt="Step 1 Cloud Storage">
# MAGIC <img class="img-step2" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_course_project_overview/course_project_step_2_ingesting_data.png" alt="Step 2 Ingesting Data">
# MAGIC <img class="img-step3" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_course_project_overview/course_project_step_3_joining_data.png" alt="Step 3 Joining Data">
# MAGIC <img class="img-step4" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_course_project_overview/course_project_step_4_if_else_block.png" alt="Step 4 If Else Block">
# MAGIC <img class="img-step5" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_course_project_overview/course_project_step_5_for_each_task.png" alt="Step 5 For Each Task">
# MAGIC <img class="img-step6" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_course_project_overview/course_project_step_6_transforming_data.png" alt="Step 6 Transforming Data">
# MAGIC <img class="img-step7" src="https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/lecture_course_project_overview/course_project_step_7_dashboard_creation.png" alt="Step 7 Dashboard Creation">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>