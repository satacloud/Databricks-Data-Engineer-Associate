# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - DevOps Fundamentals
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, we will explore the concept of DevOps and discuss how it supports and enhances Lakeflow Jobs in Data Engineering and Machine Learning.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. Explain how DevOps and DataOps principles integrate development, operations, and data workflows for smoother collaboration and faster delivery.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. What is DevOps?
# MAGIC
# MAGIC <div class="devops-intro-section">
# MAGIC <style>
# MAGIC .devops-intro-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .devops-intro-section *{box-sizing:border-box;}
# MAGIC .devops-main-grid{display:grid;grid-template-columns:.95fr 1.05fr;gap:22px;align-items:stretch;}
# MAGIC .devops-venn-card,.devops-benefits-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:22px;}
# MAGIC .devops-venn-wrap{position:relative;min-height:285px;display:flex;align-items:center;justify-content:center;}
# MAGIC .devops-circle{width:230px;height:230px;border-radius:50%;display:flex;align-items:center;justify-content:center;text-align:center;padding:24px;font-size:18px;line-height:1.25;font-weight:800;}
# MAGIC .devops-circle.left{background:#FFF1EE;border:2px solid #FF5F46;color:#0B2026;margin-right:0px;}
# MAGIC .devops-circle.right{background:#EAF7F2;border:2px solid #00A972;color:#0B2026;margin-left:0px;}
# MAGIC .devops-center{position:absolute;left:50%;top:28%;transform:translate(-50%,-50%);width:128px;height:128px;border-radius:50%;background:#FF5F46;color:#fff;display:flex;align-items:center;justify-content:center;text-align:center;font-size:22px;font-weight:900;box-shadow:0 8px 20px rgba(27,49,57,.12);}
# MAGIC .devops-callout{margin-top:18px;background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 18px;font-size:17px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC .devops-benefits-title{font-size:20px;font-weight:900;margin:0 0 16px 0;color:#0B2026;text-align:center;}
# MAGIC .devops-benefit-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;}
# MAGIC .devops-benefit{background:#F9F7F4;border:1px solid #DCE0E2;border-top:5px solid #FF5F46;border-radius:12px;padding:18px;min-height:106px;display:flex;align-items:center;justify-content:center;text-align:center;font-size:18px;line-height:1.3;font-weight:800;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .devops-benefit:nth-child(2){border-top-color:#2272B4;}
# MAGIC .devops-benefit:nth-child(3){border-top-color:#00A972;}
# MAGIC .devops-benefit:nth-child(4){border-top-color:#98102A;}
# MAGIC @media screen and (max-width:900px){.devops-main-grid,.devops-benefit-grid{grid-template-columns:1fr;}.devops-venn-wrap{min-height:320px;}.devops-circle{width:210px;height:210px;}}
# MAGIC </style>
# MAGIC <div class="devops-main-grid">
# MAGIC <div class="devops-venn-card">
# MAGIC <div class="devops-venn-wrap">
# MAGIC <div class="devops-circle left">Software Engineering Best Practices</div>
# MAGIC <div class="devops-circle right">IT Operations</div>
# MAGIC <div class="devops-center">DevOps</div>
# MAGIC </div>
# MAGIC <div class="devops-callout">Fosters <strong>collaboration</strong> between development and operations teams to <strong>automate Lakeflow Jobs</strong> and <strong>streamline processes</strong>.</div>
# MAGIC </div>
# MAGIC <div class="devops-benefits-card">
# MAGIC <div class="devops-benefits-title">Key benefits include</div>
# MAGIC <div class="devops-benefit-grid">
# MAGIC <div class="devops-benefit">Faster deployments</div>
# MAGIC <div class="devops-benefit">Improved collaboration</div>
# MAGIC <div class="devops-benefit">Enhanced reliability</div>
# MAGIC <div class="devops-benefit">Better scalability</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>So, what exactly is DevOps?</p>
# MAGIC <p>DevOps is a culture and set of practices that combines Software Engineering Best Practices with IT Operations to deliver software more rapidly, efficiently, and with higher quality.</p>
# MAGIC <p>It’s all about fostering collaboration between development and operations teams to automate your Lakeflow Jobs, streamline the processes, and ensure continuous delivery of applications.</p>
# MAGIC <p>Key benefits of DevOps include:</p>
# MAGIC <ul>
# MAGIC <li>Faster deployment cycles</li>
# MAGIC <li>Improved collaboration between teams</li>
# MAGIC <li>Enhanced system reliability</li>
# MAGIC <li>Better scalability and efficiency</li>
# MAGIC </ul>
# MAGIC <p>In short, DevOps is a way to build and deliver software quickly and reliably by bridging the gap between development and operations.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. DevOps Lifecycle
# MAGIC
# MAGIC <div class="devops-lifecycle-click">
# MAGIC <style>
# MAGIC .devops-lifecycle-click{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .devops-lifecycle-click *{box-sizing:border-box;}
# MAGIC .devops-lifecycle-click input[type="radio"]{display:none;}
# MAGIC .dls-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:22px;margin-bottom:18px;}
# MAGIC .dls-row{display:flex;align-items:center;justify-content:center;gap:10px;flex-wrap:wrap;}
# MAGIC .dls-pill{background:#F9F7F4;border:1px solid #DCE0E2;border-radius:999px;padding:10px 16px;font-size:16px;line-height:1.2;font-weight:900;color:#0B2026;cursor:pointer;transition:all .2s ease;}
# MAGIC .dls-pill.orange{border-color:#FFD2C9;background:#FFF1EE;color:#FF5F46;}
# MAGIC .dls-pill.blue{border-color:#BFDDF3;background:#F0F7FD;color:#2272B4;}
# MAGIC .dls-pill.green{border-color:#BFE8D9;background:#EAF7F2;color:#00A972;}
# MAGIC .dls-pill.maroon{border-color:#E8C5CD;background:#F8EEF1;color:#98102A;}
# MAGIC .dls-pill:hover{filter:brightness(.96);}
# MAGIC .dls-arrow{color:#FF5F46;font-size:24px;font-weight:900;}
# MAGIC #dls-plan:checked ~ .dls-card label[for="dls-plan"],
# MAGIC #dls-code:checked ~ .dls-card label[for="dls-code"],
# MAGIC #dls-build:checked ~ .dls-card label[for="dls-build"],
# MAGIC #dls-test:checked ~ .dls-card label[for="dls-test"],
# MAGIC #dls-release:checked ~ .dls-card label[for="dls-release"],
# MAGIC #dls-deploy:checked ~ .dls-card label[for="dls-deploy"],
# MAGIC #dls-operate:checked ~ .dls-card label[for="dls-operate"],
# MAGIC #dls-monitor:checked ~ .dls-card label[for="dls-monitor"]{background:#FF5F46;border-color:#FF5F46;color:#fff;}
# MAGIC .dls-image-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:16px;overflow:hidden;}
# MAGIC .dls-image-panel{display:none;animation:dlsFade .25s ease both;}
# MAGIC .dls-image-wrap{background:#ffffff;border:1px solid #DCE0E2;border-radius:12px;padding:12px;display:flex;align-items:center;justify-content:center;min-height:430px;}
# MAGIC .dls-image-wrap img{display:block;width:100%;max-height:520px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC #dls-plan:checked ~ .dls-image-card .img-plan,
# MAGIC #dls-code:checked ~ .dls-image-card .img-code,
# MAGIC #dls-build:checked ~ .dls-image-card .img-build,
# MAGIC #dls-test:checked ~ .dls-image-card .img-test,
# MAGIC #dls-release:checked ~ .dls-image-card .img-release,
# MAGIC #dls-deploy:checked ~ .dls-image-card .img-deploy,
# MAGIC #dls-operate:checked ~ .dls-image-card .img-operate,
# MAGIC #dls-monitor:checked ~ .dls-image-card .img-monitor{display:block;}
# MAGIC @keyframes dlsFade{0%{opacity:0;transform:translateY(8px);}100%{opacity:1;transform:translateY(0);}}
# MAGIC @media screen and (max-width:900px){.devops-lifecycle-click{overflow-x:auto;}.dls-card,.dls-image-card{min-width:900px;}.dls-image-wrap{min-height:360px;}}
# MAGIC </style>
# MAGIC
# MAGIC <input type="radio" id="dls-plan" name="dls-step" checked>
# MAGIC <input type="radio" id="dls-code" name="dls-step">
# MAGIC <input type="radio" id="dls-build" name="dls-step">
# MAGIC <input type="radio" id="dls-test" name="dls-step">
# MAGIC <input type="radio" id="dls-release" name="dls-step">
# MAGIC <input type="radio" id="dls-deploy" name="dls-step">
# MAGIC <input type="radio" id="dls-operate" name="dls-step">
# MAGIC <input type="radio" id="dls-monitor" name="dls-step">
# MAGIC
# MAGIC <div class="dls-card">
# MAGIC <div class="dls-row">
# MAGIC <label class="dls-pill orange" for="dls-plan">PLAN</label><div class="dls-arrow">→</div>
# MAGIC <label class="dls-pill blue" for="dls-code">CODE</label><div class="dls-arrow">→</div>
# MAGIC <label class="dls-pill green" for="dls-build">BUILD</label><div class="dls-arrow">→</div>
# MAGIC <label class="dls-pill maroon" for="dls-test">TEST</label><div class="dls-arrow">→</div>
# MAGIC <label class="dls-pill orange" for="dls-release">RELEASE</label><div class="dls-arrow">→</div>
# MAGIC <label class="dls-pill blue" for="dls-deploy">DEPLOY</label><div class="dls-arrow">→</div>
# MAGIC <label class="dls-pill green" for="dls-operate">OPERATE</label><div class="dls-arrow">→</div>
# MAGIC <label class="dls-pill maroon" for="dls-monitor">MONITOR</label>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dls-image-card">
# MAGIC <div class="dls-image-panel img-plan">
# MAGIC <div class="dls-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_lifecycle_plan.png" alt="Plan the project's features and requirements">
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dls-image-panel img-code">
# MAGIC <div class="dls-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_lifecycle_code.png" alt="Code the project">
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dls-image-panel img-build">
# MAGIC <div class="dls-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_lifecycle_build.png" alt="Build the code into executable files">
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dls-image-panel img-test">
# MAGIC <div class="dls-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_lifecycle_test.png" alt="Test the code by running automated tests">
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dls-image-panel img-release">
# MAGIC <div class="dls-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_lifecycle_release.png" alt="Release the packaged application">
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dls-image-panel img-deploy">
# MAGIC <div class="dls-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_lifecycle_deploy.png" alt="Deploy the project to production environments">
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dls-image-panel img-operate">
# MAGIC <div class="dls-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_lifecycle_operate.png" alt="Operate the released application">
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dls-image-panel img-monitor">
# MAGIC <div class="dls-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_lifecycle_monitor.png" alt="Monitor performance and gather feedback">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>Let’s dive into the 8 key steps of the DevOps lifecycle, breaking each stage down into simple, easy-to-understand points.</p>
# MAGIC <p>First up is Planning. This is where we define project goals, gather requirements, and make sure the whole team is aligned on what needs to be delivered.</p>
# MAGIC <p>Next, we move on to Coding. Here, developers write the application’s source code, building the features and functionality we need.</p>
# MAGIC <p>After that comes Building. This is where we compile the code into executable files, making sure all dependencies are correctly integrated.</p>
# MAGIC <p>Then, we hit Testing. In this stage, we run automated tests to ensure the code works as expected, catching any bugs before we release. Testing is extremely important within DevOps.</p>
# MAGIC <p>Now it’s time for Release. We want to package the application, ensuring it’s production-ready, and prepare for a controlled rollout.</p>
# MAGIC <p>Once the release is ready, we move to Deploying. This is when we push the application to the production environment and make it available to users.</p>
# MAGIC <p>After deployment, we need to Operate the application. This means monitoring the performance, managing its resources, and quickly addressing any issues that come up.</p>
# MAGIC <p>Lastly, we get to Monitoring. Here, we track the app’s performance, gather feedback, and continuously work on improvements to keep things running smoothly.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. DevOps as a Continuous Practice
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div class="devops-overview-simple">
# MAGIC <style>
# MAGIC .devops-overview-simple{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .devops-overview-simple *{box-sizing:border-box;}
# MAGIC .devops-callout{background:#ffffff;border:1.5px solid #9AA6AC;border-radius:2px;padding:18px 26px;text-align:center;font-size:30px;line-height:1.22;font-weight:400;color:#000000;margin-bottom:18px;}
# MAGIC .devops-callout strong{font-weight:900;}
# MAGIC .devops-image-wrap{background:#ffffff;border:none;border-radius:10px;padding:0;display:flex;align-items:center;justify-content:center;}
# MAGIC .devops-image-wrap img{width:100%;max-width:980px;height:auto;display:block;background:transparent;border-radius:8px;}
# MAGIC @media screen and (max-width:900px){.devops-callout{font-size:22px;padding:16px 18px;}.devops-image-wrap{overflow-x:auto;justify-content:flex-start;}.devops-image-wrap img{min-width:850px;}}
# MAGIC </style>
# MAGIC <div class="devops-callout">
# MAGIC DevOps is a process for <strong>continuously</strong> integrating, testing and deploying your code.
# MAGIC </div>
# MAGIC <div class="devops-image-wrap">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_lifecycle.png" alt="DevOps lifecycle diagram" style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>The DevOps lifecycle is all about seamless collaboration between development and operations teams to deliver high-quality software.</p>
# MAGIC <p>Continuously iterating throughout the DevOps lifecycle as the project needs fixes, updates or features.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. DevOps for Data Engineering and Machine Learning
# MAGIC
# MAGIC <div class="devops-data-ml-section">
# MAGIC <style>
# MAGIC .devops-data-ml-section{
# MAGIC   width:1100px;
# MAGIC   max-width:100%;
# MAGIC   margin:16px auto 28px auto;
# MAGIC   font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
# MAGIC   color:#0B2026;
# MAGIC }
# MAGIC .devops-data-ml-section *{
# MAGIC   box-sizing:border-box;
# MAGIC }
# MAGIC .ddm-grid{
# MAGIC   gap:22px;
# MAGIC   align-items:center;
# MAGIC }
# MAGIC .ddm-image-box{
# MAGIC   background:#ffffff;
# MAGIC   border:1px solid #DCE0E2;
# MAGIC   border-radius:14px;
# MAGIC   padding:16px;
# MAGIC   box-shadow:0 3px 12px rgba(27,49,57,0.07);
# MAGIC   display:flex;
# MAGIC   align-items:center;
# MAGIC   justify-content:center;
# MAGIC }
# MAGIC .ddm-image-box img{
# MAGIC   width:100%;
# MAGIC   max-height:430px;
# MAGIC   object-fit:contain;
# MAGIC   display:block;
# MAGIC   background:transparent;
# MAGIC   border-radius:8px;
# MAGIC }
# MAGIC @media screen and (max-width:900px){
# MAGIC   .ddm-grid{
# MAGIC     grid-template-columns:1fr;
# MAGIC   }
# MAGIC   .ddm-image-box img{
# MAGIC     max-height:360px;
# MAGIC   }
# MAGIC }
# MAGIC </style>
# MAGIC <div class="ddm-grid">
# MAGIC   <div class="ddm-image-box">
# MAGIC     <img
# MAGIC       src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/devops_data_engg_machine_learning.png"
# MAGIC       alt="DevOps for data engineering and machine learning">
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>We can apply DevOps principles to Data Engineering and Machine Learning. Remember, DevOps is all about automating processes, improving collaboration, testing, and speeding up delivery.</p>
# MAGIC <p>DataOps is a subset of DevOps and applies DevOps to data engineering. It automates the management of data pipelines, ensuring smooth, reliable data flows from collection to processing. This means fewer bottlenecks and faster insights.</p>
# MAGIC <p>MLOps is about applying DevOps to machine learning. It streamlines the process of deploying and managing ML models, ensuring that models move from development to production quickly and are monitored for performance.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. DevOps, DataOps and MLOps
# MAGIC
# MAGIC <div class="devops-compare-section">
# MAGIC <style>
# MAGIC .devops-compare-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .devops-compare-section *{box-sizing:border-box;}
# MAGIC .dcmp-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px;align-items:stretch;}
# MAGIC .dcmp-card{background:#fff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .dcmp-head{padding:16px 18px;color:#fff;text-align:center;font-size:24px;line-height:1.2;font-weight:900;}
# MAGIC .dcmp-head.devops{background:#FF5F46;}.dcmp-head.dataops{background:#00A972;}.dcmp-head.mlops{background:#2272B4;}
# MAGIC .dcmp-sub{background:#F9F7F4;border-bottom:1px solid #DCE0E2;padding:12px 16px;text-align:center;font-size:16px;line-height:1.35;font-weight:800;color:#0B2026;min-height:66px;display:flex;align-items:center;justify-content:center;}
# MAGIC .dcmp-body{padding:18px;}
# MAGIC .dcmp-body ul{margin:0;padding-left:20px;}
# MAGIC .dcmp-body li{font-size:16px;line-height:1.45;margin-bottom:10px;color:#0B2026;}
# MAGIC .dcmp-scope{margin-top:14px;background:#FFF1EE;border:1px solid #FFD2C9;border-left:5px solid #FF5F46;border-radius:10px;padding:10px 12px;font-size:15px;font-weight:800;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.dcmp-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="dcmp-grid">
# MAGIC <div class="dcmp-card"><div class="dcmp-head devops">DevOps</div><div class="dcmp-sub">Software development and IT operations</div><div class="dcmp-body"><ul><li>Automate CI/CD</li><li>Enable continuous code testing</li><li>Version control</li><li>Establish Production-grade Lakeflow Jobs</li><li>Orchestration &amp; Automation</li><li>Monitor system performance</li></ul></div></div>
# MAGIC <div class="dcmp-card"><div class="dcmp-head dataops">DataOps</div><div class="dcmp-sub">Building quality data pipeline processes</div><div class="dcmp-body"><ul><li>A set of practices, processes, and technologies</li><li>Optimize data processing</li><li>Centralize data discovery, management, and governance</li><li>Establish traceable data lineage and monitoring</li><li>Enhance collaboration across teams</li><li>Monitor data quality</li></ul></div></div>
# MAGIC <div class="dcmp-card"><div class="dcmp-head mlops">MLOps</div><div class="dcmp-sub">ML model development and deployment</div><div class="dcmp-body"><ul><li>Treating model code as software</li><li>Treating models as data</li><li>Manage the model lifecycle</li><li>Monitor Model Performance</li></ul><div class="dcmp-scope">Outside the scope of this course.</div></div></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>DevOps, DataOps and ModelOps are a set of practices, processes, and technologies. Let's compare the three approaches that streamline Lakeflow Jobs in different tech areas.</p>
# MAGIC <p>Let’s break them down.</p>
# MAGIC <p>With DevOps, it's all about bridging the gap between software development and IT operations. The main goal is to automate CI/CD, or continuous integration and deployment, making software deployment faster and more reliable.</p>
# MAGIC <p>With DevOps, we also focus on automated testing and version control to ensure code quality. The aim is to establish smooth, production-grade Lakeflow Jobs and automate orchestration to reduce manual work.</p>
# MAGIC <p>Lastly, system performance monitoring helps catch issues early, keeping everything running smoothly.</p>
# MAGIC <p>Next is DataOps, which is all about Building quality data pipeline processes. It optimizes data processing and centralizes data discovery, management, and governance with Unity Catalog.</p>
# MAGIC <p>DataOps also emphasizes traceable data lineage, so you can track data at every stage. Monitoring data throughout the pipeline ensures that it stays accurate and accessible, while promoting team collaboration to improve data flow and quality.</p>
# MAGIC <p>Lastly, we have ModelOps, which focuses on the lifecycle of machine learning models. It treats model code like software, ensuring it’s versioned, tested, and deployed efficiently.</p>
# MAGIC <p>ModelOps also focuses on managing the model lifecycle and monitoring model performance after deployment to ensure models stay accurate and effective over time. MLOps is outside the scope of this course.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. DataOps = DevOps for Data Engineering
# MAGIC
# MAGIC <div class="dataops-close-section">
# MAGIC <style>
# MAGIC .dataops-close-section{witext-align:center;font-size:30px;line-height:1.15;font-weight:900;}
# MAGIC .dataops-text{background:#fff;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:22px;font-size:22px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC .dataops-text strong{color:#FF5F46;}
# MAGIC @media screen and (max-width:800px){.dataops-hero{grid-template-columns:1fr;}.dataops-text{font-size:18px;}}
# MAGIC </style>
# MAGIC <div class="dataops-hero">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_devops_fundamentals/dataops_devops.png" alt="DataOps DevOps for Data Engineering">
# MAGIC <div class="dataops-text">You want to think about how <strong>DevOps</strong> principles and culture can be applied to your <strong>Data Engineering pipelines</strong>.</div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <p>DataOps is essentially DevOps for data engineering—it applies those same principles of automation, collaboration, and continuous improvement to data Lakeflow Jobs. Just as DevOps streamlines software delivery, DataOps focuses on optimizing the flow, quality, and management of data pipelines.</p>
# MAGIC <p>In the end, you want to think about how DevOps principles and culture can be applied to your Data Engineering pipelines.</p>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## G. Conclusion
# MAGIC
# MAGIC <ul>
# MAGIC <li>DevOps combines software engineering practices with IT operations to automate Lakeflow Jobs and streamline delivery.</li>
# MAGIC <li>The DevOps lifecycle continuously plans, codes, builds, tests, releases, deploys, operates, and monitors code.</li>
# MAGIC <li>DataOps applies DevOps principles and culture to Data Engineering pipelines.</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC