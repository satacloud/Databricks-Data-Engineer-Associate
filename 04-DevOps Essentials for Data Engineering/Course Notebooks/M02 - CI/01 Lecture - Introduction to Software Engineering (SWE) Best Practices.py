# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Introduction to Software Engineering (SWE) Best Practices
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, we’ll introduce some key software development best practices and explore how they can be applied to building reliable data pipelines.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to: 
# MAGIC 1. Identify key software engineering practices like version control, testing, and code reviews.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Introduction to SWE Best Practices
# MAGIC
# MAGIC <div class="swe-intro-practices">
# MAGIC <style>
# MAGIC .swe-intro-practices{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .swe-intro-practices *{box-sizing:border-box;}
# MAGIC .swe-intro-grid{display:grid;grid-template-columns:.9fr 1.1fr;gap:22px;align-items:stretch;}
# MAGIC .swe-intro-left{background:#F9F7F4;border:1px solid #DCE0E2;border-radius:14px;padding:24px;display:flex;flex-direction:column;justify-content:center;box-shadow:0 3px 12px rgba(27,49,57,.07);}
# MAGIC .swe-intro-left h3{font-size:22px;line-height:1.25;margin:0 0 14px 0;color:#0B2026;}
# MAGIC .swe-intro-left p{font-size:18px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC .swe-intro-right{display:grid;gap:14px;}
# MAGIC .swe-intro-card{background:#fff;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:18px 20px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .swe-intro-card.green{border-left-color:#00A972;}
# MAGIC .swe-intro-card h3{font-size:18px;line-height:1.25;margin:0 0 8px 0;color:#0B2026;}
# MAGIC .swe-intro-card p{font-size:16px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC .swe-intro-card img{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;padding:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);display:flex;align-items:center;justify-content:center;}
# MAGIC .swe-intro-card-img img{width:100%;max-height:560px;object-fit:contain;border-radius:8px;background:transparent;}
# MAGIC @media screen and (max-width:900px){.swe-intro-grid{grid-template-columns:1fr;}.swe-intro-img{overflow-x:auto;}.swe-intro-img img{min-width:850px;}}
# MAGIC </style>
# MAGIC <div class="swe-intro-grid">
# MAGIC <div class="swe-intro-left">
# MAGIC <p>To build reliable data pipelines, we can learn from software engineering best practices.</p>
# MAGIC </div>
# MAGIC <div class="swe-intro-right">
# MAGIC <div class="swe-intro-card-img">
# MAGIC <img src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_introduction_software_engineering_best_practices/introduction_SWE_best_practices.png" alt="Introduction to Best Practices"></div>
# MAGIC <div class="swe-intro-card"><h3>Following best practices in development ensures that your data pipelines are efficient, scalable, and maintainable
# MAGIC </p></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>In this lesson, we’ll introduce some key software development best practices and explore how they can be applied to building reliable data pipelines. 
# MAGIC
# MAGIC Incorporating these best practices in data pipeline development helps ensure your pipelines are efficient, scalable, and easy to maintain. 
# MAGIC
# MAGIC Let’s take a high-level look at some of the most important best practices.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Best Practices
# MAGIC
# MAGIC Select each tab below to explore software engineering best practices.
# MAGIC
# MAGIC <div class="swe-carousel">
# MAGIC <style>
# MAGIC .swe-carousel{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .swe-carousel *{box-sizing:border-box;}
# MAGIC .swe-carousel input[type="radio"]{display:none;}
# MAGIC .swe-tabbar{display:flex;justify-content:flex-start;align-items:center;gap:0;border-bottom:2px solid #EEEDE9;margin-bottom:18px;flex-wrap:wrap;}
# MAGIC .swe-tabbar label{padding:10px 16px;border-bottom:3px solid transparent;background:none;font-size:16px;font-weight:800;color:#888;cursor:pointer;margin-bottom:-2px;}
# MAGIC #swe-tab-1:checked ~ .swe-tabbar label[for="swe-tab-1"]{color:#FF5F46;border-bottom-color:#FF5F46;}
# MAGIC #swe-tab-2:checked ~ .swe-tabbar label[for="swe-tab-2"]{color:#2272B4;border-bottom-color:#2272B4;}
# MAGIC #swe-tab-3:checked ~ .swe-tabbar label[for="swe-tab-3"]{color:#00A972;border-bottom-color:#00A972;}
# MAGIC #swe-tab-4:checked ~ .swe-tabbar label[for="swe-tab-4"]{color:#98102A;border-bottom-color:#98102A;}
# MAGIC #swe-tab-5:checked ~ .swe-tabbar label[for="swe-tab-5"]{color:#FF5F46;border-bottom-color:#FF5F46;}
# MAGIC #swe-tab-6:checked ~ .swe-tabbar label[for="swe-tab-6"]{color:#2272B4;border-bottom-color:#2272B4;}
# MAGIC .swe-panel{display:none;}
# MAGIC #swe-tab-1:checked ~ .swe-panels .swe-panel-1,#swe-tab-2:checked ~ .swe-panels .swe-panel-2,#swe-tab-3:checked ~ .swe-panels .swe-panel-3,#swe-tab-4:checked ~ .swe-panels .swe-panel-4,#swe-tab-5:checked ~ .swe-panels .swe-panel-5,#swe-tab-6:checked ~ .swe-panels .swe-panel-6{display:block;animation:sweFade .25s ease both;}
# MAGIC .swe-card{background:#ffffff;border:1px solid #DCE0E2;border-radius:14px;box-shadow:0 3px 12px rgba(27,49,57,.07);overflow:hidden;}
# MAGIC .swe-topbar{height:8px;background:#FF5F46;}
# MAGIC .swe-topbar.blue{background:#2272B4;}
# MAGIC .swe-topbar.green{background:#00A972;}
# MAGIC .swe-topbar.maroon{background:#98102A;}
# MAGIC .swe-body{padding:22px;}
# MAGIC .swe-ribbon{background:#FF5F46;color:#ffffff;border-radius:10px;padding:14px 20px;text-align:center;font-size:24px;line-height:1.2;font-weight:850;}
# MAGIC .swe-ribbon.blue{background:#2272B4;}
# MAGIC .swe-ribbon.green{background:#00A972;}
# MAGIC .swe-ribbon.maroon{background:#98102A;}
# MAGIC .swe-grid-3{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;align-items:stretch;}
# MAGIC .swe-grid-2{display:grid;grid-template-columns:repeat(2,1fr);gap:18px;align-items:stretch;}
# MAGIC .swe-block{background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:18px;box-shadow:0 2px 8px rgba(27,49,57,.06);}
# MAGIC .swe-block.blue{border-left-color:#2272B4;}
# MAGIC .swe-block.green{border-left-color:#00A972;}
# MAGIC .swe-block.maroon{border-left-color:#98102A;}
# MAGIC .swe-block h3{font-size:18px;line-height:1.25;margin:0 0 10px 0;color:#0B2026;font-weight:850;}
# MAGIC .swe-block p{font-size:16px;line-height:1.5;margin:0;color:#0B2026;}
# MAGIC .swe-vr-context{margin-top:18px;background:#F9F7F4;border:1px solid #DCE0E2;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 20px;font-size:17px;line-height:1.5;color:#0B2026;font-weight:700;}
# MAGIC .swe-cicd-wrap{display:grid;grid-template-columns:1fr 70px 1fr;gap:16px;align-items:stretch;}
# MAGIC .swe-cicd-arrow{width:70px;height:70px;border-radius:50%;background:#FFF1EE;color:#FF5F46;display:flex;align-items:center;justify-content:center;font-size:34px;font-weight:900;margin:auto;}
# MAGIC .swe-env-wrap{display:grid;grid-template-columns:1fr 1fr;gap:18px;align-items:stretch;}
# MAGIC .swe-bp-callout{margin-top:18px;background:#FFF1EE;border:1px solid #FFD2C9;border-left:6px solid #FF5F46;border-radius:12px;padding:16px 20px;font-size:18px;line-height:1.45;font-weight:700;text-align:center;}
# MAGIC @keyframes sweFade{0%{opacity:0;transform:translateY(6px);}100%{opacity:1;transform:translateY(0);}}
# MAGIC @media screen and (max-width:900px){.swe-grid-3,.swe-grid-2,.swe-cicd-wrap,.swe-env-wrap{grid-template-columns:1fr;}.swe-cicd-arrow{transform:rotate(90deg);}}
# MAGIC </style>
# MAGIC <input type="radio" id="swe-tab-1" name="swe-carousel-tab" checked>
# MAGIC <input type="radio" id="swe-tab-2" name="swe-carousel-tab">
# MAGIC <input type="radio" id="swe-tab-3" name="swe-carousel-tab">
# MAGIC <input type="radio" id="swe-tab-4" name="swe-carousel-tab">
# MAGIC <input type="radio" id="swe-tab-5" name="swe-carousel-tab">
# MAGIC <input type="radio" id="swe-tab-6" name="swe-carousel-tab">
# MAGIC <div class="swe-tabbar">
# MAGIC <label for="swe-tab-1">Coding Practices</label>
# MAGIC <label for="swe-tab-2">Document Code</label>
# MAGIC <label for="swe-tab-3">Automated Testing</label>
# MAGIC <label for="swe-tab-4">Version Control &amp; Code Review</label>
# MAGIC <label for="swe-tab-5">CI/CD</label>
# MAGIC <label for="swe-tab-6">Isolated Environments</label>
# MAGIC </div>
# MAGIC <div class="swe-panels">
# MAGIC <div class="swe-panel swe-panel-1">
# MAGIC <div class="swe-card">
# MAGIC <div class="swe-topbar"></div>
# MAGIC <div class="swe-body">
# MAGIC <div class="swe-ribbon">Coding Practices</div>
# MAGIC <div class="swe-vr-context">Version Control and Code Review are part of software engineering best practices for coding, documenting code, and automated testing.</div>
# MAGIC <br>
# MAGIC <div class="swe-grid-3">
# MAGIC <div class="swe-block">
# MAGIC <h3>Code Readability</h3>
# MAGIC <p>Write code that is easy to understand, navigate, and maintain.</p>
# MAGIC </div>
# MAGIC <div class="swe-block blue">
# MAGIC <h3>Naming Conventions</h3>
# MAGIC <p>Use descriptive, consistent names for variables, functions, and classes.</p>
# MAGIC </div>
# MAGIC <div class="swe-block green">
# MAGIC <h3>Modular Design</h3>
# MAGIC <p>Break down software into smaller, reusable components (functions).</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="swe-panel swe-panel-2">
# MAGIC <div class="swe-card">
# MAGIC <div class="swe-topbar blue"></div>
# MAGIC <div class="swe-body">
# MAGIC <div class="swe-ribbon blue">Document Code</div>
# MAGIC <br>
# MAGIC <div class="swe-grid-3">
# MAGIC <div class="swe-block blue"><h3>Improves Code Maintainability</h3></div>
# MAGIC <div class="swe-block blue"><h3>Enhances Collaboration</h3></div>
# MAGIC <div class="swe-block blue"><h3>Facilitates Knowledge Transfer</h3></div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="swe-panel swe-panel-3">
# MAGIC <div class="swe-card">
# MAGIC <div class="swe-topbar green"></div>
# MAGIC <div class="swe-body">
# MAGIC <div class="swe-ribbon green">Automated Testing</div>
# MAGIC <br>
# MAGIC <div class="swe-grid-2">
# MAGIC <div class="swe-block green">
# MAGIC <h3>Unit Tests</h3>
# MAGIC <p>Verifies the functionality of a single unit in isolation.</p>
# MAGIC </div>
# MAGIC <div class="swe-block green">
# MAGIC <h3>2. Integration Tests</h3>
# MAGIC <p>Tests how different components or systems work together.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="swe-panel swe-panel-4">
# MAGIC <div class="swe-card">
# MAGIC <div class="swe-topbar maroon"></div>
# MAGIC <div class="swe-body">
# MAGIC <div class="swe-ribbon maroon">Version Control and Code Review</div>
# MAGIC <br>
# MAGIC <div class="swe-grid-2">
# MAGIC <div class="swe-block maroon">
# MAGIC <h3>Version Control</h3>
# MAGIC <p>Tools like Git are essential for tracking changes, collaborating with team members, and maintaining a history of the codebase.</p>
# MAGIC </div>
# MAGIC <div class="swe-block maroon">
# MAGIC <h3>Code Review</h3>
# MAGIC <p>Help catch bugs early and ensure adherence to coding standards.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="swe-panel swe-panel-5">
# MAGIC <div class="swe-card">
# MAGIC <div class="swe-topbar"></div>
# MAGIC <div class="swe-body">
# MAGIC <div class="swe-ribbon">CI/CD</div>
# MAGIC <br>
# MAGIC <div class="swe-cicd-wrap">
# MAGIC <div class="swe-block">
# MAGIC <h3>Continuous Integration (CI)</h3>
# MAGIC <p>A practice where developers frequently commit, build, test and release code to a shared repository.</p>
# MAGIC </div>
# MAGIC <div class="swe-cicd-arrow">→</div>
# MAGIC <div class="swe-block green">
# MAGIC <h3>Continuous Deployment (CD)</h3>
# MAGIC <p>Automating the release of code to production after passing automated tests.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="swe-block blue" style="margin-top:16px;text-align:center;">
# MAGIC <h3>Main focus of this course</h3>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="swe-panel swe-panel-6">
# MAGIC <div class="swe-card">
# MAGIC <div class="swe-topbar blue"></div>
# MAGIC <div class="swe-body">
# MAGIC <div class="swe-ribbon blue">Isolated Environments</div>
# MAGIC <br>
# MAGIC <div class="swe-env-wrap">
# MAGIC <div class="swe-block blue">
# MAGIC <h3>Workspaces</h3>
# MAGIC <p>Utilizing multiple workspaces, one for each environment.</p>
# MAGIC </div>
# MAGIC <div class="swe-block green">
# MAGIC <h3>Catalogs</h3>
# MAGIC <p>Utilizing multiple catalogs, one for each environment.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC <div class="swe-bp-callout">The focus is on writing high-quality code, testing it, and ensuring scalability and maintainability.</div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Coding Practices
# MAGIC
# MAGIC - We will start with some key coding practices that promote better code development.
# MAGIC - First, Code Readability. Write code that’s easy to understand and maintain. Clear, readable code reduces confusion and minimizes the chances of errors when updates or changes are needed.
# MAGIC - Next, utilizing consistent naming conventions. Using descriptive, consistent names for variables, functions, and classes makes your code self-explanatory and enhances collaboration among team members.
# MAGIC - Finally, incorporating modular design in your code base. This means breaking your project down into smaller, reusable components (like functions). This not only makes your code easier to maintain but also allows for smoother scaling as your project grows.
# MAGIC - You can also use code linting tools to help enforce these practices. Linting tools automatically analyze your code for potential errors, inconsistencies, and style violations. While linting tools are outside the scope of this course, they are an excellent resource for improving code quality and maintaining readability.
# MAGIC
# MAGIC Document Code
# MAGIC
# MAGIC - Another important best practice is documenting your code. 
# MAGIC - Good documentation improves the following:
# MAGIC - First the ability to maintain your code. Clear docs help developers understand the purpose and functionality of the code, making updates and bug fixes quicker and easier.
# MAGIC - Next good documentation improves collaboration. Well-documented code lets team members get up to speed fast, reducing misunderstandings and errors.
# MAGIC - Lastly documentation improves knowledge transfer. By documenting your code it preserves key info about the code design and structure, ensuring smooth transitions when team members change.
# MAGIC
# MAGIC Automated Testing
# MAGIC
# MAGIC - Testing is an extremely critical component of software development best practices. 
# MAGIC - Writing both unit tests and integration tests is essential for verifying that both individual components and their interactions function correctly.
# MAGIC - A unit test verifies the functionality of a single unit or component of code, typically in isolation to ensure it behaves as expected.
# MAGIC - Integration tests, on the other hand, check how different components or systems work together to ensure they function correctly as a whole.
# MAGIC - We will talk more about these later.
# MAGIC
# MAGIC Version Control and Code Review
# MAGIC
# MAGIC - Another essential best practice is using version control and code reviews on your project.
# MAGIC - With version control, tools like Git are essential for tracking changes, collaborating with your team, and keeping a history of your codebase. They allow you to roll back changes, manage multiple versions, and avoid conflicts, all while keeping your work organized and secure.
# MAGIC - Next is Code Reviews. When combined with version control, code reviews are an effective way to catch bugs early, improve code quality, and ensure consistency in coding standards. Code reviews foster collaboration, encourage knowledge sharing within the team, and ultimately lead to more maintainable and reliable code.
# MAGIC - In short, use version control to manage your codebase effectively, and conducting code reviews help to improve the quality of your code through collaboration.
# MAGIC
# MAGIC CI/CD
# MAGIC
# MAGIC - Next is CI/CD, or continuous integration and continuous deployment/delivery.
# MAGIC - At a high level,continuous Integration (CI) is when developers regularly commit code, build, test and release code to a shared repository. The goal is to catch issues early through continuous integration and testing.
# MAGIC - Next is Continuous Deployment (CD). This automates the release of code to production after passing your automated tests (unit and integration tests). The goal is to deliver features and fixes quickly and consistently avoiding errors.
# MAGIC - This course mainly focuses on Continuous Integration within the CI/CD pipeline, with a high level overview of Continuous deployment and delivery.
# MAGIC
# MAGIC Isolated Environments
# MAGIC
# MAGIC - Lastly, you do not want to be modifying code directly on the production codebase. 
# MAGIC - Organizations often use different environments for each stage. A typical setup includes “Development & Stage” and “Production,” but this can vary based your organization's processes.
# MAGIC - Separate environments help isolate changes and ensure thorough testing before deployment, preventing issues from mixing development and production.
# MAGIC - In Databricks, you can isolate environments in a few ways:
# MAGIC - You can use multiple Workspaces, one for each environment.
# MAGIC - Or, use a single Workspace with multiple catalogs.
# MAGIC - One major advantage of Databricks is Unity Catalog, which provides built-in features like lineage, security, and monitoring, all without needing third-party tools.
# MAGIC
# MAGIC This was a quick high level overview of some key software engineering best practices. There are many more that we do not cover here in this overview.
# MAGIC
# MAGIC These practices aim to create high-quality, maintainable software that can evolve over time. The focus is on writing efficient, readable, and defect-free code.
# MAGIC
# MAGIC As we move forward, keep in mind how these best practices can help you build better, more efficient data pipelines..
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Software Engineering with Databricks
# MAGIC
# MAGIC <div class="swe-databricks-tools">
# MAGIC <style>
# MAGIC .swe-databricks-tools{width:1100px;max-width:100%;margin:16px auto 28px auto;font-family:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#0B2026;}
# MAGIC .swe-databricks-tools *{box-sizing:border-box;}
# MAGIC .swe-tools-title{background:#0B2026;color:#fff;border-radius:12px 12px 0 0;padding:16px 22px;text-align:center;font-size:24px;font-weight:850;}
# MAGIC .swe-tools-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:0;border:1px solid #DCE0E2;border-top:0;border-radius:0 0 12px 12px;overflow:hidden;box-shadow:0 3px 12px rgba(27,49,57,.07);}
# MAGIC .swe-tool{position:relative;background:#F9F7F4;border-right:1px solid #DCE0E2;padding:22px 18px 92px 18px;min-height:285px;}
# MAGIC .swe-tool:last-child{border-right:0;}
# MAGIC .swe-tool h3{font-size:18px;line-height:1.25;margin:0 0 14px 0;color:#FF5F46;font-weight:850;}
# MAGIC .swe-tool p{font-size:16px;line-height:1.45;margin:0;color:#0B2026;}
# MAGIC .swe-tool:nth-child(2) h3{color:#00A972;}
# MAGIC .swe-tool:nth-child(3) h3{color:#2272B4;}
# MAGIC .swe-tool:nth-child(4) h3{color:#98102A;}
# MAGIC .swe-tool-icon{position:absolute;right:18px;bottom:18px;width:64px;height:64px;object-fit:contain;background:transparent;mix-blend-mode:multiply;filter:contrast(1.15) brightness(1);border-radius:4px;}
# MAGIC @media screen and (max-width:900px){.swe-tools-grid{grid-template-columns:1fr;}.swe-tool{border-right:0;border-bottom:1px solid #DCE0E2;min-height:220px;}.swe-tool:last-child{border-bottom:0;}}
# MAGIC </style>
# MAGIC <div class="swe-tools-title">Tools Overview</div>
# MAGIC <div class="swe-tools-grid">
# MAGIC <div class="swe-tool">
# MAGIC <h3>Databricks Workspaces</h3>
# MAGIC <p>Develop code and run unit tests in a Databricks Workspaces or locally using Notebooks or Files (SQL, Python, Scala, etc.).</p>
# MAGIC <img class="swe-tool-icon" src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_introduction_software_engineering_best_practices/databricks_workspaces_icon.png" alt="Databricks Workspaces icon">
# MAGIC </div>
# MAGIC <div class="swe-tool">
# MAGIC <h3>Databricks Git folders</h3>
# MAGIC <p>Utilize Databricks Git folders to provide version control and significantly improve the workflow.</p>
# MAGIC <img class="swe-tool-icon" src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_introduction_software_engineering_best_practices/databricks_git_folders_icon.png" alt="Databricks Git folders icon">
# MAGIC </div>
# MAGIC <div class="swe-tool">
# MAGIC <h3>Unity Catalog</h3>
# MAGIC <p>Focus on using Unity Catalog within a single Workspace or multiple Workspaces to isolate your environments securely, providing the necessary data access.</p>
# MAGIC <img class="swe-tool-icon" src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_introduction_software_engineering_best_practices/unity_catalog_icon.png" alt="Unity Catalog icon">
# MAGIC </div>
# MAGIC <div class="swe-tool">
# MAGIC <h3>Databricks deployment tools</h3>
# MAGIC <p>Get code tested &amp; deployed via CI/CD pipelines using Databricks deployment tools to deploy to your desired environment automatically.</p>
# MAGIC <img class="swe-tool-icon" src="https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/lecture_introduction_software_engineering_best_practices/databricks_deployment_tools_icon.png" alt="Databricks deployment tools icon">
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <p>Let’s talk about some tools within the Databricks Data Intelligence Platform that you can use to implement software engineering best practices within Databricks. First, you can develop code and run unit tests in Databricks Workspaces (or locally) using Notebooks or Files such as SQL, Python, and Scala. Next, use Databricks Git folders for version control to streamline your workflow and enhance collaboration. You can also leverage Unity Catalog within a single Workspace or across multiple Workspaces to securely isolate environments and manage data access. Finally, automating the process, from compile, test, and deploying your code through CI/CD pipelines using Databricks deployment tools.</p>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Conclusion
# MAGIC
# MAGIC <ul>
# MAGIC <li>Software engineering best practices help create efficient, scalable, maintainable, and reliable data pipelines.</li>
# MAGIC <li>Coding practices, documentation, automated testing, version control, code review, CI/CD, and isolated environments support high-quality data pipeline development.</li>
# MAGIC <li>Databricks Workspaces, Git folders, Unity Catalog, and deployment tools help implement software engineering best practices on the Databricks Data Intelligence Platform.</li>
# MAGIC </ul>
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next lecture, discuss how to modularize PySpark code and explore the benefits of doing so.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC