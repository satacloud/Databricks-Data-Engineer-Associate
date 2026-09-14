# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Version Control with Git Overview
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Git version control is important because it provides a structured way to manage, track, and collaborate on code changes in software projects. Basically, it is an essential tool for modern software development and DevOps. 
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. Describe organizational challenges with version control
# MAGIC 2. Explain how Git-based repositories work on Databricks
# MAGIC 3. Integrate GitHub repositories with Databricks
# MAGIC 4. Identify supported file types in Databricks repos

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Complications with Version Control
# MAGIC
# MAGIC <div class="vcgo-section vcgo-challenges">
# MAGIC <style>
# MAGIC .vcgo-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .vcgo-section *{box-sizing:border-box;}
# MAGIC .vcgo-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .vcgo-bar{height:8px;background:#FF5F46;}
# MAGIC .vcgo-bar.green{background:#00A972;}
# MAGIC .vcgo-bar.blue{background:#2272B4;}
# MAGIC .vcgo-bar.maroon{background:#98102A;}
# MAGIC .vcgo-body{padding:20px;}
# MAGIC .vcgo-body h3{font-size:18px;line-height:1.25;font-weight:800;margin:0 0 12px 0;color:#0B2026;}
# MAGIC .vcgo-body p,.vcgo-body li{font-size:16px;line-height:1.5;color:#0B2026;}
# MAGIC .vcgo-body p{margin:0 0 12px 0;}
# MAGIC .vcgo-body ul{margin:0;padding-left:22px;}
# MAGIC .vcgo-body li{margin-bottom:8px;}
# MAGIC .vcgo-img-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:12px;display:flex;align-items:center;justify-content:center;}
# MAGIC .vcgo-img-card img{width:100%;max-height:520px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC .vcgo-callout{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 20px;font-size:18px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.vcgo-grid-2,.vcgo-grid-3{grid-template-columns:1fr!important;}.vcgo-img-card{overflow-x:auto;justify-content:flex-start;}.vcgo-img-card img{min-width:820px;}}
# MAGIC </style>
# MAGIC <style>
# MAGIC .vcgo-challenge-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;}
# MAGIC .vcgo-challenge-card{min-height:170px;background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:18px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .vcgo-challenge-card:nth-child(2){border-left-color:#98102A;}
# MAGIC .vcgo-challenge-card:nth-child(3){border-left-color:#2272B4;}
# MAGIC .vcgo-challenge-card:nth-child(4){border-left-color:#00A972;}
# MAGIC .vcgo-challenge-card:nth-child(5){border-left-color:#FF5F46;}
# MAGIC .vcgo-challenge-card h3{font-size:18px;line-height:1.25;margin:0 0 10px 0;font-weight:800;}
# MAGIC .vcgo-challenge-card p{font-size:16px;line-height:1.45;margin:0;}
# MAGIC @media screen and (max-width:900px){.vcgo-challenge-grid{grid-template-columns:1fr;}}
# MAGIC </style>
# MAGIC <div class="vcgo-callout" style="margin-bottom:18px;">Organizational Challenges</div>
# MAGIC <div class="vcgo-challenge-grid">
# MAGIC <div class="vcgo-challenge-card"><h3>Development Silos</h3><p>Silos form over time that isolate development and team operations.</p></div>
# MAGIC <div class="vcgo-challenge-card"><h3>Lower Software Quality</h3><p>Independent development leads to lower quality due to duplicate code, inconsistent standards, code reviewing, and more.</p></div>
# MAGIC <div class="vcgo-challenge-card"><h3>Unstable Versioning</h3><p>It can become difficult to track, revert, and audit changes.</p></div>
# MAGIC <div class="vcgo-challenge-card"><h3>Frequent Updates</h3><p>Without proper version control, managing frequent updates increases risk.</p></div>
# MAGIC <div class="vcgo-challenge-card"><h3>Scaling Development</h3><p>Branching, merging, and CI/CD integration can become difficult, diminishing the ability to scale development.</p></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>A lack of centralized version control leads to development silos, resulting in duplicate code, inconsistent standards, and lower software quality. Unstable versioning makes tracking, reverting, and auditing changes difficult, increasing risks when managing frequent updates. Additionally, branching, merging, and CI/CD integration become challenging, ultimately limiting scalability.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Secure Code Changes Through Branching
# MAGIC
# MAGIC <div class="vcgo-section vcgo-branching">
# MAGIC <style>
# MAGIC .vcgo-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .vcgo-section *{box-sizing:border-box;}
# MAGIC .vcgo-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .vcgo-bar{height:8px;background:#FF5F46;}
# MAGIC .vcgo-bar.green{background:#00A972;}
# MAGIC .vcgo-bar.blue{background:#2272B4;}
# MAGIC .vcgo-bar.maroon{background:#98102A;}
# MAGIC .vcgo-body{padding:20px;}
# MAGIC .vcgo-body h3{font-size:18px;line-height:1.25;font-weight:800;margin:0 0 12px 0;color:#0B2026;}
# MAGIC .vcgo-body p,.vcgo-body li{font-size:16px;line-height:1.5;color:#0B2026;}
# MAGIC .vcgo-body p{margin:0 0 12px 0;}
# MAGIC .vcgo-body ul{margin:0;padding-left:22px;}
# MAGIC .vcgo-body li{margin-bottom:8px;}
# MAGIC .vcgo-img-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:12px;display:flex;align-items:center;justify-content:center;}
# MAGIC .vcgo-img-card img{width:100%;max-height:520px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC .vcgo-callout{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 20px;font-size:18px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.vcgo-grid-2,.vcgo-grid-3{grid-template-columns:1fr!important;}.vcgo-img-card{overflow-x:auto;justify-content:flex-start;}.vcgo-img-card img{min-width:820px;}}
# MAGIC </style>
# MAGIC <style>
# MAGIC .vcgo-grid-2{display:grid;grid-template-columns:.75fr 1.25fr;gap:20px;align-items:stretch;}
# MAGIC .vcgo-mini-flow{display:flex;align-items:center;justify-content:center;gap:10px;flex-wrap:wrap;margin-top:16px;}
# MAGIC .vcgo-pill{background:#F9F7F4;border:1px solid #DCE0E2;border-radius:999px;padding:8px 14px;font-size:15px;font-weight:800;color:#0B2026;}
# MAGIC .vcgo-arrow{color:#FF5F46;font-size:24px;font-weight:900;}
# MAGIC </style>
# MAGIC <div class="vcgo-grid-2">
# MAGIC <div class="vcgo-card">
# MAGIC <div class="vcgo-bar"></div>
# MAGIC <div class="vcgo-body">
# MAGIC <ul>
# MAGIC <li>Complementary concept to CI/CD → Enables effective CI</li>
# MAGIC <li>Version control changes and run through quality control before merging to main branch and deploying</li>
# MAGIC <li>Example: Gitflow</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="vcgo-img-card"><img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/gitflow_example.png" alt="Secure code changes through branching"></div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>This is an example of a version control and code management over time with Gitflow, which is a common branching strategy that organizes branches for features, releases, and hotfixes.</p>
# MAGIC <p>Here you will see different versions of a feature (v0.1, v0.2, and v1.0).</p>
# MAGIC <p>Starting with the first version v0.1 on the main branch, branching occurs to the feature branch where testing and be properly performed in isolation.</p>
# MAGIC <p>Now that we know how complicated version control can be and we’ve seen an example of what securing code changes looks like, let’s take a look at how Git can be used with Databricks.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Overview of Git with Databricks
# MAGIC
# MAGIC <div class="vcgo-section vcgo-git-overview">
# MAGIC <style>
# MAGIC .vcgo-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .vcgo-section *{box-sizing:border-box;}
# MAGIC .vcgo-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;display:flex;flex-direction:column;}
# MAGIC .vcgo-bar{height:8px;background:#FF5F46;}
# MAGIC .vcgo-bar.green{background:#00A972;}
# MAGIC .vcgo-bar.blue{background:#2272B4;}
# MAGIC .vcgo-body{padding:20px;display:flex;flex-direction:column;flex:1;}
# MAGIC .vcgo-body h3{font-size:18px;line-height:1.25;font-weight:800;margin:0 0 12px 0;color:#0B2026;text-align:center;}
# MAGIC .vcgo-body p,.vcgo-body li{font-size:16px;line-height:1.5;color:#0B2026;}
# MAGIC .vcgo-body p{margin:0 0 12px 0;}
# MAGIC .vcgo-body ul{margin:0 0 16px 0;padding-left:22px;}
# MAGIC .vcgo-body li{margin-bottom:8px;}
# MAGIC .vcgo-img-box{margin-top:auto;width:100%;min-height:130px;background:#ffffff;border:1px solid #DCE0E2;border-radius:12px;padding:12px;display:flex;align-items:center;justify-content:center;text-align:center;}
# MAGIC .vcgo-img-box img{display:block;width:100%;max-width:220px;max-height:115px;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;margin:0 auto;}
# MAGIC .vcgo-img-box.wide{flex:1;min-height:260px;}
# MAGIC .vcgo-img-box.wide img{max-width:310px;max-height:235px;}
# MAGIC .vcgo-callout{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 20px;font-size:18px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC .vcgo-grid-3{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;align-items:stretch;}
# MAGIC @media screen and (max-width:900px){
# MAGIC   .vcgo-grid-3{grid-template-columns:1fr!important;}
# MAGIC   .vcgo-img-box img{max-width:260px;}
# MAGIC   .vcgo-img-box.wide{min-height:220px;}
# MAGIC   .vcgo-img-box.wide img{max-width:300px;max-height:200px;}
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="vcgo-callout" style="margin-bottom:18px;">
# MAGIC Git is a free and open-source software framework designed to track changes in source code during software development.
# MAGIC </div>
# MAGIC
# MAGIC <div class="vcgo-grid-3">
# MAGIC   <div class="vcgo-card">
# MAGIC     <div class="vcgo-bar"></div>
# MAGIC     <div class="vcgo-body">
# MAGIC       <h3>Common Git Tools and Git-Based Services</h3>
# MAGIC       <div class="vcgo-img-box wide">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/tools_git-based_services.png" alt="Common Git Tools and Git-Based Services">
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="vcgo-card">
# MAGIC     <div class="vcgo-bar green"></div>
# MAGIC     <div class="vcgo-body">
# MAGIC       <h3>Benefits of Git</h3>
# MAGIC       <ul>
# MAGIC         <li>Collaboration</li>
# MAGIC         <li>Version Control</li>
# MAGIC         <li>Branching and Merging</li>
# MAGIC         <li>Distributed Nature</li>
# MAGIC         <li>Open Source and Widely Adopted</li>
# MAGIC       </ul>
# MAGIC       <div class="vcgo-img-box">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/benefits_of_git.png" alt="Benefits of Git">
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="vcgo-card">
# MAGIC     <div class="vcgo-bar blue"></div>
# MAGIC     <div class="vcgo-body">
# MAGIC       <h3>Git Integration on Databricks</h3>
# MAGIC       <ul>
# MAGIC         <li>Visual Git client and API</li>
# MAGIC         <li>Supports Common Operations</li>
# MAGIC         <li>Supports common Git providers</li>
# MAGIC         <li>Visual diff comparison</li>
# MAGIC         <li>Designed for authoring and collaborative Lakeflow Jobs</li>
# MAGIC       </ul>
# MAGIC       <div class="vcgo-img-box">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/integration_databricks.png" alt="Git Integration on Databricks">
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <div class="vcgo-callout" style="margin-bottom:18px;">
# MAGIC Integrate 3rd party tools into your Lakeflow Jobs that make sense for your organizational needs.
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Git Benefits:</p>
# MAGIC <p>Version control enables tracking code changes, facilitating rollback and collaboration. Branching and merging allow multiple developers to work in parallel and integrate changes efficiently. A distributed job ensures each developer has a full local repository, enhancing flexibility and reliability. Git is optimized for high-performance handling of large projects, and its security features use cryptographic integrity checks to prevent data corruption.</p>
# MAGIC <p>Git Tools &amp; Services</p>
# MAGIC <p>GitHub, GitLab, Bitbucket, Azure DevOps</p>
# MAGIC <p>– Cloud-based repositories with CI/CD, issue tracking, and team collaboration.</p>
# MAGIC <p>Git CLI &amp; GUI Clients (e.g., SourceTree, GitKraken, VS Code Git Integration)</p>
# MAGIC <p>– Provides different interfaces for managing repositories.</p>
# MAGIC <p>CI/CD Integration</p>
# MAGIC <p>– Automated testing and deployment pipelines.</p>
# MAGIC <p>Code Review &amp; Collaboration</p>
# MAGIC <p>– Features like pull requests and merge approvals streamline teamwork.</p>
# MAGIC <p>Security &amp; Access Control</p>
# MAGIC <p>– Role-based permissions and audit logs enhance repository security.</p>
# MAGIC <p>Visual Git Client -</p>
# MAGIC <p>Databricks provides a user-friendly interface for common Git operations, which we will discuss about on the next slide.</p>
# MAGIC <p>Seamless Integration -</p>
# MAGIC <p>Users can leverage remote Git repos while developing code inside Databricks notebooks</p>
# MAGIC <p>CI/CD Capabilities -</p>
# MAGIC <p>The repos REST API enables integration of data and AI projects into CI/CD pipelines, allowing users to automate Git Lakeflow Jobs</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Generating GitHub Personal Access Token(PAT)
# MAGIC
# MAGIC ##### Click the red highlighted box in each step to move to the next step.
# MAGIC
# MAGIC <div class="vcgo-pat-flow">
# MAGIC <style>
# MAGIC .vcgo-pat-flow{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .vcgo-pat-flow *{box-sizing:border-box;}
# MAGIC .vcgo-pat-flow input[type="radio"]{display:none;}
# MAGIC .vcgo-stepbar{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-start;border-bottom:2px solid #EEEDE9;margin-bottom:16px;}
# MAGIC .vcgo-stepbar label{padding:10px 14px;border-bottom:3px solid transparent;background:none;font-size:15px;font-weight:800;color:#888;cursor:pointer;margin-bottom:-2px;}
# MAGIC #vcgo-pat-1:checked ~ .vcgo-stepbar label[for="vcgo-pat-1"],#vcgo-pat-2:checked ~ .vcgo-stepbar label[for="vcgo-pat-2"],#vcgo-pat-3:checked ~ .vcgo-stepbar label[for="vcgo-pat-3"],#vcgo-pat-4:checked ~ .vcgo-stepbar label[for="vcgo-pat-4"],#vcgo-pat-5:checked ~ .vcgo-stepbar label[for="vcgo-pat-5"]{color:#FF5F46;border-bottom-color:#FF5F46;}
# MAGIC .vcgo-stage{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:16px;overflow:hidden;}
# MAGIC .vcgo-panel{display:none;}
# MAGIC #vcgo-pat-1:checked ~ .vcgo-stage .panel-1,#vcgo-pat-2:checked ~ .vcgo-stage .panel-2,#vcgo-pat-3:checked ~ .vcgo-stage .panel-3,#vcgo-pat-4:checked ~ .vcgo-stage .panel-4,#vcgo-pat-5:checked ~ .vcgo-stage .panel-5{display:grid;grid-template-columns:.32fr .68fr;gap:18px;align-items:center;animation:vcgoFade .25s ease both;}
# MAGIC .vcgo-step-note{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 20px;font-size:18px;line-height:1.45;font-weight:800;color:#0B2026;}
# MAGIC .vcgo-image-wrap{position:relative;background:#ffffff;border:1px solid #DCE0E2;border-radius:12px;padding:12px;display:flex;align-items:center;justify-content:center;min-height:410px;}
# MAGIC .vcgo-image-box{position:relative;display:inline-block;max-width:100%;}
# MAGIC .vcgo-image-box img{display:block;max-width:100%;max-height:520px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC .vcgo-hotspot{position:absolute;border:4px solid #FF5F46;border-radius:4px;background:rgba(255,95,70,.06);box-shadow:0 0 0 5px rgba(255,95,70,.13);cursor:pointer;animation:vcgoPulse 1.25s ease-in-out infinite;}
# MAGIC .vcgo-hotspot:hover{background:rgba(255,95,70,.14);}
# MAGIC .hot-settings{left:1.4%;top:61%;width:38%;height:5.6%;}
# MAGIC .hot-dev{left:4.6%;top:94.6%;width:58%;height:3.9%;}
# MAGIC .hot-pat{left:8%;top:34%;width:86%;height:54%;}
# MAGIC .hot-fine{left:54%;top:73%;width:14%;height:14%;}
# MAGIC .hot-classic{left:71%;top:43%;width:25%;height:30%;}
# MAGIC .vcgo-reset{display:inline-flex;margin-top:14px;background:#F9F7F4;border:1px solid #DCE0E2;border-radius:999px;padding:8px 13px;font-size:14px;font-weight:800;color:#0B2026;cursor:pointer;}
# MAGIC @keyframes vcgoPulse{0%,100%{border-color:#FF5F46;box-shadow:0 0 0 5px rgba(255,95,70,.13);}50%{border-color:#98102A;box-shadow:0 0 0 8px rgba(255,95,70,.08);}}
# MAGIC @keyframes vcgoFade{0%{opacity:0;transform:translateY(6px);}100%{opacity:1;transform:translateY(0);}}
# MAGIC @media screen and (max-width:900px){#vcgo-pat-1:checked ~ .vcgo-stage .panel-1,#vcgo-pat-2:checked ~ .vcgo-stage .panel-2,#vcgo-pat-3:checked ~ .vcgo-stage .panel-3,#vcgo-pat-4:checked ~ .vcgo-stage .panel-4,#vcgo-pat-5:checked ~ .vcgo-stage .panel-5{grid-template-columns:1fr;}.vcgo-image-wrap{overflow-x:auto;justify-content:flex-start;}.vcgo-stepbar{flex-wrap:wrap;}.vcgo-image-box.wide{min-width:850px;}}
# MAGIC </style>
# MAGIC
# MAGIC <input type="radio" id="vcgo-pat-1" name="vcgo-pat-step" checked>
# MAGIC <input type="radio" id="vcgo-pat-2" name="vcgo-pat-step">
# MAGIC <input type="radio" id="vcgo-pat-3" name="vcgo-pat-step">
# MAGIC <input type="radio" id="vcgo-pat-4" name="vcgo-pat-step">
# MAGIC <input type="radio" id="vcgo-pat-5" name="vcgo-pat-step">
# MAGIC
# MAGIC <div class="vcgo-stepbar">
# MAGIC <label for="vcgo-pat-1">1. Settings</label>
# MAGIC <label for="vcgo-pat-2">2. Developer settings</label>
# MAGIC <label for="vcgo-pat-3">3. Personal access tokens</label>
# MAGIC <label for="vcgo-pat-4">4. Fine-grained token</label>
# MAGIC <label for="vcgo-pat-5">5. Classic token</label>
# MAGIC </div>
# MAGIC
# MAGIC <div class="vcgo-stage">
# MAGIC <div class="vcgo-panel panel-1">
# MAGIC <div class="vcgo-step-note">Open your GitHub profile menu and select <strong>Settings</strong>.</div>
# MAGIC <div class="vcgo-image-wrap">
# MAGIC <div class="vcgo-image-box">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/github_settings_menu.png" alt="GitHub profile menu settings">
# MAGIC <label class="vcgo-hotspot hot-settings" for="vcgo-pat-2" aria-label="Go to Developer settings"></label>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="vcgo-panel panel-2">
# MAGIC <div class="vcgo-step-note">From the settings page, select <strong>Developer settings</strong>.</div>
# MAGIC <div class="vcgo-image-wrap">
# MAGIC <div class="vcgo-image-box">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/github_developer_settings.png" alt="GitHub developer settings">
# MAGIC <label class="vcgo-hotspot hot-dev" for="vcgo-pat-3" aria-label="Go to Personal access tokens"></label>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="vcgo-panel panel-3">
# MAGIC <div class="vcgo-step-note">Open <strong>Personal access tokens</strong> and choose a token type.</div>
# MAGIC <div class="vcgo-image-wrap">
# MAGIC <div class="vcgo-image-box">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/github_pat_menu.png" alt="GitHub personal access token menu">
# MAGIC <label class="vcgo-hotspot hot-pat" for="vcgo-pat-4" aria-label="Go to fine grained token screen"></label>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="vcgo-panel panel-4">
# MAGIC <div class="vcgo-step-note">For fine-grained tokens, click <strong>Generate new token</strong>.</div>
# MAGIC <div class="vcgo-image-wrap">
# MAGIC <div class="vcgo-image-box wide">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/github_fine_grained_tokens.png" alt="GitHub fine-grained personal access tokens">
# MAGIC <label class="vcgo-hotspot hot-fine" for="vcgo-pat-5" aria-label="Go to classic token screen"></label>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="vcgo-panel panel-5">
# MAGIC <div class="vcgo-step-note">
# MAGIC For classic tokens, open <strong>Generate new token</strong> and choose <strong>Generate new token (classic)</strong>.
# MAGIC <br>
# MAGIC <label class="vcgo-reset" for="vcgo-pat-1">Start again</label>
# MAGIC </div>
# MAGIC <div class="vcgo-image-wrap">
# MAGIC <div class="vcgo-image-box wide">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/github_classic_tokens.png" alt="GitHub classic personal access token page">
# MAGIC <label class="vcgo-hotspot hot-classic" for="vcgo-pat-1" aria-label="Start again"></label>
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
# MAGIC <p>To generate a Personal Access Token (PAT) in GitHub, go to <b>Settings → Developer</b> settings, choose either fine-grained or classic tokens, and click Generate new token. Configure the required repository permissions, copy the token, and use it in Databricks for integration.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Connecting to Databricks with GitHub PAT
# MAGIC
# MAGIC ##### Click the red highlighted box in each step to move to the next step.
# MAGIC
# MAGIC <div class="vcgo-db-pat-flow">
# MAGIC <style>
# MAGIC .vcgo-db-pat-flow{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .vcgo-db-pat-flow *{box-sizing:border-box;}
# MAGIC .vcgo-db-pat-flow input[type="radio"]{display:none;}
# MAGIC .dbpat-stepbar{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-start;border-bottom:2px solid #EEEDE9;margin-bottom:16px;}
# MAGIC .dbpat-stepbar label{padding:10px 14px;border-bottom:3px solid transparent;background:none;font-size:15px;font-weight:800;color:#888;cursor:pointer;margin-bottom:-2px;}
# MAGIC #dbpat-step-1:checked ~ .dbpat-stepbar label[for="dbpat-step-1"],#dbpat-step-2:checked ~ .dbpat-stepbar label[for="dbpat-step-2"],#dbpat-step-3:checked ~ .dbpat-stepbar label[for="dbpat-step-3"],#dbpat-step-4:checked ~ .dbpat-stepbar label[for="dbpat-step-4"],#dbpat-step-5:checked ~ .dbpat-stepbar label[for="dbpat-step-5"]{color:#FF5F46;border-bottom-color:#FF5F46;}
# MAGIC .dbpat-stage{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:16px;overflow:hidden;}
# MAGIC .dbpat-panel{display:none;}
# MAGIC #dbpat-step-1:checked ~ .dbpat-stage .panel-1,#dbpat-step-2:checked ~ .dbpat-stage .panel-2,#dbpat-step-3:checked ~ .dbpat-stage .panel-3,#dbpat-step-4:checked ~ .dbpat-stage .panel-4,#dbpat-step-5:checked ~ .dbpat-stage .panel-5{display:grid;grid-template-columns:.3fr .7fr;gap:18px;align-items:center;animation:dbpatFade .25s ease both;}
# MAGIC .dbpat-note{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 20px;font-size:18px;line-height:1.45;font-weight:800;color:#0B2026;}
# MAGIC .dbpat-image-wrap{position:relative;background:#ffffff;border:1px solid #DCE0E2;border-radius:12px;padding:12px;display:flex;align-items:center;justify-content:center;min-height:410px;}
# MAGIC .dbpat-image-box{position:relative;display:inline-block;max-width:100%;}
# MAGIC .dbpat-image-box img{display:block;max-width:100%;max-height:520px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC .dbpat-hotspot{position:absolute;border:4px solid #FF5F46;border-radius:4px;background:rgba(255,95,70,.06);box-shadow:0 0 0 5px rgba(255,95,70,.13);cursor:pointer;animation:dbpatPulse 1.25s ease-in-out infinite;}
# MAGIC .dbpat-hotspot:hover{background:rgba(255,95,70,.14);}
# MAGIC .hot-profile{left:15%;top:37%;width:2.2%;height:4.9%;}
# MAGIC .hot-settings{left:2.1%;top:53.8%;width:5.8%;height:4.5%;}
# MAGIC .hot-developer{left:25%;top:56.7%;width:5.4%;height:5%;}
# MAGIC .hot-token-icon{left:39%;top:74%;width:5.7%;height:11%;}
# MAGIC .hot-token-field{left:50%;top:80%;width:45%;height:17%;}
# MAGIC .dbpat-reset{display:inline-flex;margin-top:14px;background:#F9F7F4;border:1px solid #DCE0E2;border-radius:999px;padding:8px 13px;font-size:14px;font-weight:800;color:#0B2026;cursor:pointer;}
# MAGIC @keyframes dbpatPulse{0%,100%{border-color:#FF5F46;box-shadow:0 0 0 5px rgba(255,95,70,.13);}50%{border-color:#98102A;box-shadow:0 0 0 8px rgba(255,95,70,.08);}}
# MAGIC @keyframes dbpatFade{0%{opacity:0;transform:translateY(6px);}100%{opacity:1;transform:translateY(0);}}
# MAGIC @media screen and (max-width:900px){#dbpat-step-1:checked ~ .dbpat-stage .panel-1,#dbpat-step-2:checked ~ .dbpat-stage .panel-2,#dbpat-step-3:checked ~ .dbpat-stage .panel-3,#dbpat-step-4:checked ~ .dbpat-stage .panel-4,#dbpat-step-5:checked ~ .dbpat-stage .panel-5{grid-template-columns:1fr;}.dbpat-image-wrap{overflow-x:auto;justify-content:flex-start;}.dbpat-image-box{min-width:850px;}.dbpat-stepbar{flex-wrap:wrap;}}
# MAGIC </style>
# MAGIC
# MAGIC <input type="radio" id="dbpat-step-1" name="dbpat-step" checked>
# MAGIC <input type="radio" id="dbpat-step-2" name="dbpat-step">
# MAGIC <input type="radio" id="dbpat-step-3" name="dbpat-step">
# MAGIC <input type="radio" id="dbpat-step-4" name="dbpat-step">
# MAGIC <input type="radio" id="dbpat-step-5" name="dbpat-step">
# MAGIC
# MAGIC <div class="dbpat-stepbar">
# MAGIC <label for="dbpat-step-1">1. Profile Menu</label>
# MAGIC <label for="dbpat-step-2">2. Settings</label>
# MAGIC <label for="dbpat-step-3">3. Developer</label>
# MAGIC <label for="dbpat-step-4">4. Token</label>
# MAGIC <label for="dbpat-step-5">5. Linked Accounts</label>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbpat-stage">
# MAGIC <div class="dbpat-panel panel-1">
# MAGIC <div class="dbpat-note">Open the Databricks user profile menu.</div>
# MAGIC <div class="dbpat-image-wrap">
# MAGIC <div class="dbpat-image-box">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/seamless_integration.png" alt="Connecting Databricks to GitHub PAT flow">
# MAGIC <label class="dbpat-hotspot hot-profile" for="dbpat-step-2" aria-label="Go to Settings step"></label>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbpat-panel panel-2">
# MAGIC <div class="dbpat-note">Select <strong>Settings</strong> from the profile menu.</div>
# MAGIC <div class="dbpat-image-wrap">
# MAGIC <div class="dbpat-image-box">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/seamless_integration.png" alt="Connecting Databricks to GitHub PAT flow">
# MAGIC <label class="dbpat-hotspot hot-settings" for="dbpat-step-3" aria-label="Go to Developer step"></label>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbpat-panel panel-3">
# MAGIC <div class="dbpat-note">In Settings, select <strong>Developer</strong>.</div>
# MAGIC <div class="dbpat-image-wrap">
# MAGIC <div class="dbpat-image-box">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/seamless_integration.png" alt="Connecting Databricks to GitHub PAT flow">
# MAGIC <label class="dbpat-hotspot hot-developer" for="dbpat-step-4" aria-label="Go to Token step"></label>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbpat-panel panel-4">
# MAGIC <div class="dbpat-note">Use the GitHub <strong>Token</strong> to connect Databricks with GitHub.</div>
# MAGIC <div class="dbpat-image-wrap">
# MAGIC <div class="dbpat-image-box">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/seamless_integration.png" alt="Connecting Databricks to GitHub PAT flow">
# MAGIC <label class="dbpat-hotspot hot-token-icon" for="dbpat-step-5" aria-label="Go to Linked accounts step"></label>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbpat-panel panel-5">
# MAGIC <div class="dbpat-note">
# MAGIC In <strong>Linked accounts</strong>, select GitHub and enter the personal access token.
# MAGIC <br>
# MAGIC <label class="dbpat-reset" for="dbpat-step-1">Start again</label>
# MAGIC </div>
# MAGIC <div class="dbpat-image-wrap">
# MAGIC <div class="dbpat-image-box">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/seamless_integration.png" alt="Connecting Databricks to GitHub PAT flow">
# MAGIC <label class="dbpat-hotspot hot-token-field" for="dbpat-step-1" aria-label="Start again"></label>
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
# MAGIC <p>Setting up a Personal Access Token (PAT) and connecting to your repositories is simple—click your user icon, go to <b>Settings → Developer settings → Personal access tokens</b>, then copy the token into the provided text box. Once saved, your account is linked and ready to use with your repository.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. Databricks Git Folders
# MAGIC
# MAGIC A **Databricks Git folder** is a folder in your workspace that is linked to a remote Git repository. It lets you run common Git operations — clone, commit, push, pull, and branch — directly from the Databricks UI, without leaving the workspace.
# MAGIC
# MAGIC ##### Select each tab below to explore what you can do inside a Databricks Git folder.
# MAGIC
# MAGIC <div class="dbgf-flow">
# MAGIC <style>
# MAGIC .dbgf-flow{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .dbgf-flow *{box-sizing:border-box;}
# MAGIC .dbgf-flow input[type="radio"]{display:none;}
# MAGIC .dbgf-tabs{display:flex;gap:8px;flex-wrap:wrap;border-bottom:2px solid #EEEDE9;margin-bottom:16px;}
# MAGIC .dbgf-tabs label{padding:10px 14px;border-bottom:3px solid transparent;font-size:15px;font-weight:800;color:#888;cursor:pointer;margin-bottom:-2px;}
# MAGIC #dbgf-1:checked ~ .dbgf-tabs label[for="dbgf-1"],#dbgf-2:checked ~ .dbgf-tabs label[for="dbgf-2"],#dbgf-3:checked ~ .dbgf-tabs label[for="dbgf-3"],#dbgf-4:checked ~ .dbgf-tabs label[for="dbgf-4"],#dbgf-5:checked ~ .dbgf-tabs label[for="dbgf-5"]{color:#FF5F46;border-bottom-color:#FF5F46;}
# MAGIC .dbgf-stage{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:18px;}
# MAGIC .dbgf-panel{display:none;}
# MAGIC #dbgf-1:checked ~ .dbgf-stage .p1,#dbgf-2:checked ~ .dbgf-stage .p2,#dbgf-3:checked ~ .dbgf-stage .p3,#dbgf-4:checked ~ .dbgf-stage .p4,#dbgf-5:checked ~ .dbgf-stage .p5{display:grid;grid-template-columns:.42fr .58fr;gap:20px;align-items:center;animation:dbgfFade .25s ease both;}
# MAGIC .dbgf-note{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 20px;}
# MAGIC .dbgf-note h3{margin:0 0 8px 0;font-size:18px;font-weight:900;color:#0B2026;}
# MAGIC .dbgf-note p{margin:0;font-size:16px;line-height:1.5;font-weight:600;color:#0B2026;}
# MAGIC .dbgf-win{width:100%;background:#fff;border:1px solid #DCE0E2;border-radius:12px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .dbgf-winbar{display:flex;align-items:center;gap:6px;padding:10px 14px;background:#F9F7F4;border-bottom:1px solid #DCE0E2;}
# MAGIC .dbgf-dot{width:10px;height:10px;border-radius:50%;background:#DCE0E2;}
# MAGIC .dbgf-wintitle{margin-left:8px;font-size:14px;font-weight:800;color:#0B2026;}
# MAGIC .dbgf-winbody{padding:16px 18px;}
# MAGIC .dbgf-lbl{font-size:13px;font-weight:800;color:#5A6B70;margin:2px 0 5px 0;}
# MAGIC .dbgf-inp{border:2px solid #DCE0E2;border-radius:8px;padding:10px 12px;font-size:13px;color:#0B2026;background:#fff;font-family:Consolas,Monaco,monospace;overflow-x:auto;white-space:nowrap;}
# MAGIC .dbgf-inp.hl{border-color:#FF5F46;box-shadow:0 0 0 4px rgba(255,95,70,.13);animation:dbgfPulse 1.3s ease-in-out infinite;}
# MAGIC .dbgf-btn{display:inline-block;margin-top:12px;padding:9px 18px;border-radius:8px;font-size:14px;font-weight:800;color:#fff;}
# MAGIC .dbgf-btn.blue{background:#2272B4;}
# MAGIC .dbgf-btn.orange{background:#FF5F46;}
# MAGIC .dbgf-btn.hl{animation:dbgfPulse 1.3s ease-in-out infinite;box-shadow:0 0 0 4px rgba(34,114,180,.18);}
# MAGIC .dbgf-chip{display:inline-flex;align-items:center;gap:6px;background:#EAF1F6;border:1px solid #C4D8E6;border-radius:999px;padding:4px 12px;font-size:13px;font-weight:800;color:#2272B4;margin-bottom:10px;}
# MAGIC .dbgf-file{display:flex;align-items:center;gap:8px;font-family:Consolas,Monaco,monospace;font-size:13px;padding:7px 10px;border:1px solid #EEEDE9;border-radius:8px;margin-top:8px;}
# MAGIC .dbgf-tag{font-weight:900;font-size:12px;padding:1px 7px;border-radius:5px;}
# MAGIC .dbgf-tag.add{background:#E6F4EA;color:#1E7B34;}
# MAGIC .dbgf-branch{border:2px solid #DCE0E2;border-radius:8px;overflow:hidden;max-width:300px;}
# MAGIC .dbgf-branch div{padding:9px 12px;font-size:14px;font-weight:700;border-bottom:1px solid #EEEDE9;}
# MAGIC .dbgf-branch div:last-child{border-bottom:none;}
# MAGIC .dbgf-branch .sel{background:#EAF1F6;color:#2272B4;font-weight:900;}
# MAGIC .dbgf-diff{font-family:Consolas,Monaco,monospace;font-size:13px;border:1px solid #DCE0E2;border-radius:8px;overflow:hidden;}
# MAGIC .dbgf-diff div{padding:5px 10px;}
# MAGIC .dbgf-add{background:#E6F4EA;color:#1E7B34;}
# MAGIC .dbgf-del{background:#FCE8E6;color:#98102A;}
# MAGIC .dbgf-ctx{background:#fff;color:#5A6B70;}
# MAGIC @keyframes dbgfPulse{0%,100%{box-shadow:0 0 0 4px rgba(255,95,70,.13);}50%{box-shadow:0 0 0 7px rgba(255,95,70,.07);}}
# MAGIC @keyframes dbgfFade{0%{opacity:0;transform:translateY(6px);}100%{opacity:1;transform:translateY(0);}}
# MAGIC @media screen and (max-width:900px){#dbgf-1:checked ~ .dbgf-stage .p1,#dbgf-2:checked ~ .dbgf-stage .p2,#dbgf-3:checked ~ .dbgf-stage .p3,#dbgf-4:checked ~ .dbgf-stage .p4,#dbgf-5:checked ~ .dbgf-stage .p5{grid-template-columns:1fr;}.dbgf-tabs{flex-wrap:wrap;}}
# MAGIC </style>
# MAGIC
# MAGIC <input type="radio" id="dbgf-1" name="dbgf" checked>
# MAGIC <input type="radio" id="dbgf-2" name="dbgf">
# MAGIC <input type="radio" id="dbgf-3" name="dbgf">
# MAGIC <input type="radio" id="dbgf-4" name="dbgf">
# MAGIC <input type="radio" id="dbgf-5" name="dbgf">
# MAGIC
# MAGIC <div class="dbgf-tabs">
# MAGIC <label for="dbgf-1">1. Clone a repo</label>
# MAGIC <label for="dbgf-2">2. Commit &amp; push</label>
# MAGIC <label for="dbgf-3">3. Pull updates</label>
# MAGIC <label for="dbgf-4">4. Manage branches</label>
# MAGIC <label for="dbgf-5">5. Visual validation</label>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbgf-stage">
# MAGIC
# MAGIC <div class="dbgf-panel p1">
# MAGIC <div class="dbgf-note"><h3>Clone a remote repository</h3><p>Create a Git folder by pasting your repository's <strong>HTTPS URL</strong> into the <em>Create Git folder</em> dialog. Databricks detects the provider automatically and clones the repo into your workspace.</p></div>
# MAGIC <div class="dbgf-win">
# MAGIC <div class="dbgf-winbar"><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-wintitle">Create Git folder</span></div>
# MAGIC <div class="dbgf-winbody">
# MAGIC <div class="dbgf-lbl">Git repository URL</div>
# MAGIC <div class="dbgf-inp hl">https://github.com/&lt;your-username&gt;/databricks_devops.git</div>
# MAGIC <div class="dbgf-lbl" style="margin-top:12px;">Git provider</div>
# MAGIC <div class="dbgf-inp">GitHub&nbsp;&nbsp;(detected automatically)</div>
# MAGIC <div class="dbgf-btn blue">Create Git folder</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbgf-panel p2">
# MAGIC <div class="dbgf-note"><h3>Commit and push changes using the UI</h3><p>Stage your changes, write a commit message, and push to the remote — all from the Databricks UI, without a terminal.</p></div>
# MAGIC <div class="dbgf-win">
# MAGIC <div class="dbgf-winbar"><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-wintitle">databricks_devops</span></div>
# MAGIC <div class="dbgf-winbody">
# MAGIC <span class="dbgf-chip">&#9095; main</span>
# MAGIC <div class="dbgf-lbl">Changes</div>
# MAGIC <div class="dbgf-file"><span class="dbgf-tag add">A</span>README.md</div>
# MAGIC <div class="dbgf-lbl" style="margin-top:12px;">Commit message</div>
# MAGIC <div class="dbgf-inp">First commit</div>
# MAGIC <div class="dbgf-btn orange hl">Commit &amp; Push</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbgf-panel p3">
# MAGIC <div class="dbgf-note"><h3>Pull branch updates</h3><p>Pull the latest commits from the remote branch so your workspace stays in sync with changes made by the rest of your team.</p></div>
# MAGIC <div class="dbgf-win">
# MAGIC <div class="dbgf-winbar"><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-wintitle">databricks_devops</span></div>
# MAGIC <div class="dbgf-winbody">
# MAGIC <span class="dbgf-chip">&#9095; main</span>
# MAGIC <div class="dbgf-file"><span class="dbgf-tag add">&#8595;</span>2 new commits on origin/main</div>
# MAGIC <div class="dbgf-btn blue hl">Pull</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbgf-panel p4">
# MAGIC <div class="dbgf-note"><h3>Manage branches</h3><p>Create and switch branches right inside the Git folder to isolate feature work from <strong>main</strong> — the foundation of a safe branching workflow.</p></div>
# MAGIC <div class="dbgf-win">
# MAGIC <div class="dbgf-winbar"><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-wintitle">Switch branch</span></div>
# MAGIC <div class="dbgf-winbody">
# MAGIC <div class="dbgf-branch"><div class="sel">&#9095; main&nbsp;&nbsp;&#10003;</div><div>&#9095; dev</div></div>
# MAGIC <div class="dbgf-btn orange">Create Branch</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div class="dbgf-panel p5">
# MAGIC <div class="dbgf-note"><h3>Visual validation when committing</h3><p>Review a visual diff of exactly what changed before you commit, so you push only what you intend.</p></div>
# MAGIC <div class="dbgf-win">
# MAGIC <div class="dbgf-winbar"><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-dot"></span><span class="dbgf-wintitle">README.md &nbsp;&mdash;&nbsp; diff</span></div>
# MAGIC <div class="dbgf-winbody">
# MAGIC <div class="dbgf-diff">
# MAGIC <div class="dbgf-ctx">&nbsp;# Databricks DevOps Training</div>
# MAGIC <div class="dbgf-del">- My first push.</div>
# MAGIC <div class="dbgf-add">+ My first push and commit.</div>
# MAGIC <div class="dbgf-add">+ Added project overview.</div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Databricks Git folders streamline development by tightly integrating version control systems into the Databricks ecosystem, making it easier to manage code collaboratively while adhering to best practices.</p>
# MAGIC <p>Use-cases include:</p>
# MAGIC
# MAGIC - Collaborative development of machine learning models and ETL pipelines.
# MAGIC - Source-controlling SQL queries for analytics workloads.
# MAGIC - Automating deployments through CI/CD pipelines.
# MAGIC
# MAGIC <p>Walk through the tabs above to see the core Git-folder operations: cloning a repo by pasting its GitHub URL into the <strong>Create Git folder</strong> dialog, committing and pushing changes from the UI, pulling updates, managing branches, and reviewing a visual diff before you commit.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## G. Git-Based Repos in Databricks
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/repos_API_automate_CI-CD.png" alt="Repos API to automate CI/CD"
# MAGIC     style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>1. Aha! Feature:</p>
# MAGIC <p>DB-3749 W2.0: Projects API private preview with tag and commit based checkouts | Aha!</p>
# MAGIC <p>2. Description of the features:</p>
# MAGIC <p>The Repos API provides programmatic access to git-based Re[ps that are part of the Workspace 2.0 effort. With the API, customers can integrate Databricks Repos with their CI/CD workflow. They can programmatically create/update/delete Repos, perform git operations, and specify Git versions when running Jobs based on notebooks in Repos.</p>
# MAGIC <p>3. Value of the feature (aka what databricks was before this feature, and what this feature will do for databricks)</p>
# MAGIC <p>Right now, many customers have built workarounds using the Databricks CLI to pull notebooks from Databricks, check them into Git, pull them from Git, and push them back to Databricks. This is not a very robust solution. With Repos and the Repos API we provide a native feature to pull code from a Git repository, check updates back into Git, and programmatically update this Repos using the Repos API</p>
# MAGIC <p>4. Associated summary slide bullet points: Repos API for CI/CD integration</p>
# MAGIC <p>5. Cloud: All</p>
# MAGIC <p>6. Deployment (MT, ST, Azure): GA</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## H. Arbitrary Files Support in Repos
# MAGIC
# MAGIC <div class="vcgo-section vcgo-files">
# MAGIC <style>
# MAGIC .vcgo-section{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .vcgo-section *{box-sizing:border-box;}
# MAGIC .vcgo-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .vcgo-bar{height:8px;background:#FF5F46;}
# MAGIC .vcgo-bar.green{background:#00A972;}
# MAGIC .vcgo-bar.blue{background:#2272B4;}
# MAGIC .vcgo-bar.maroon{background:#98102A;}
# MAGIC .vcgo-body{padding:20px;}
# MAGIC .vcgo-body h3{font-size:18px;line-height:1.25;font-weight:800;margin:0 0 12px 0;color:#0B2026;}
# MAGIC .vcgo-body p,.vcgo-body li{font-size:16px;line-height:1.5;color:#0B2026;}
# MAGIC .vcgo-body p{margin:0 0 12px 0;}
# MAGIC .vcgo-body ul{margin:0;padding-left:22px;}
# MAGIC .vcgo-body li{margin-bottom:8px;}
# MAGIC .vcgo-img-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);padding:12px;display:flex;align-items:center;justify-content:center;}
# MAGIC .vcgo-img-card img{width:100%;max-height:520px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC .vcgo-callout{background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 20px;font-size:18px;line-height:1.45;font-weight:700;color:#0B2026;}
# MAGIC @media screen and (max-width:900px){.vcgo-grid-2,.vcgo-grid-3{grid-template-columns:1fr!important;}.vcgo-img-card{overflow-x:auto;justify-content:flex-start;}.vcgo-img-card img{min-width:820px;}}
# MAGIC </style>
# MAGIC <style>
# MAGIC .vcgo-files-grid{display:grid;grid-template-columns:.9fr 1.1fr;gap:20px;align-items:stretch;}
# MAGIC .vcgo-file-cards{display:grid;gap:14px;}
# MAGIC .vcgo-file-card{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:18px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .vcgo-file-card:nth-child(2){border-left-color:#00A972;}
# MAGIC .vcgo-file-card:nth-child(3){border-left-color:#2272B4;}
# MAGIC .vcgo-file-card:nth-child(4){border-left-color:#98102A;}
# MAGIC .vcgo-file-card h3{font-size:18px;line-height:1.25;margin:0 0 8px 0;font-weight:850;}
# MAGIC .vcgo-file-card p{font-size:16px;line-height:1.45;margin:0;}
# MAGIC </style>
# MAGIC <div class="vcgo-files-grid">
# MAGIC <div class="vcgo-file-cards">
# MAGIC <div class="vcgo-file-card">Portability of code<p>Library files - use Python/R files as packages</p></div>
# MAGIC <div class="vcgo-file-card">Environment specification portability<p>Build packages from the same repo</p></div>
# MAGIC <div class="vcgo-file-card">Small data ease of use<p>Relative imports</p></div>
# MAGIC <div class="vcgo-file-card">Whatever you can do with files “just works”</div>
# MAGIC </div>
# MAGIC <div class="vcgo-img-card"><img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_version_control/arbitrary_files_support_in_repos.png" alt="Arbitrary files support in Repos"></div>
# MAGIC </div>
# MAGIC <br>
# MAGIC <div class="vcgo-file-card"><p>.txt &nbsp; .yml &nbsp; .py &nbsp; .csv &nbsp; ...</p></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Arbitrary files are supported in repos, enabling portability of code for library files like Python and R. This makes it possible to build packages from the same repository, use relative imports, and structure your project in a way that best suits your needs.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC