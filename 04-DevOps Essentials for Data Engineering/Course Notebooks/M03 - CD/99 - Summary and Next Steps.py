# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/devops-essentials-for-data-engineering-en_us-2.2.1/images/20260819T031035Z/DevOps Essentials for Data Engineering/Course Notebooks/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary and Next Steps

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. What You Accomplished
# MAGIC
# MAGIC Across this course you took a single data engineering project — an end-to-end medallion pipeline (bronze → silver → gold) — and hardened it with software engineering and DevOps best practices, one discipline at a time.
# MAGIC
# MAGIC ```
# MAGIC   Modular Code  →  Unit Tests  →  Integration Tests  →  Version Control (Git)  →  Deploy (DABs)
# MAGIC   ───────────────────────── Continuous Integration ──────────────────────────    ──── CD ────
# MAGIC ```
# MAGIC
# MAGIC | Step | Module | What You Did |
# MAGIC |------|--------|--------------|
# MAGIC | 1 | M02 | Reviewed **software engineering best practices** — readability, naming, documentation, testing, version control, and code review — and where CI/CD fits |
# MAGIC | 2 | M02 | **Modularized PySpark code**, turning inline pipeline logic into reusable functions |
# MAGIC | 3 | M02 | Learned **DevOps and DataOps fundamentals** and the lifecycle as a continuous practice |
# MAGIC | 4 | M02 | Explored **the role of CI/CD**, the testing pyramid, and branching strategy |
# MAGIC | 5 | M02 | **Planned the project** and isolated dev, stage, and prod using Workspace and Unity Catalog isolation |
# MAGIC | 6 | M02 | Wrote and ran **unit tests** for PySpark with `pytest` and `pyspark.testing.utils` |
# MAGIC | 7 | M02 | Performed **integration tests** with SDP expectations and Lakeflow Jobs tasks |
# MAGIC | 8 | M02 | Applied **version control** with Databricks Git Folders and GitHub |
# MAGIC | 9 | M03 | Compared deployment options and **deployed Databricks assets** with Declarative Automation Bundles (DABs) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Key Takeaways
# MAGIC
# MAGIC - **Modularity is the enabler.** You cannot cleanly unit-test or reuse code until it is broken into functions — so modularization comes first, and everything else builds on it.
# MAGIC - **The testing pyramid guides where to invest:** many cheap, fast **unit tests** at the base; fewer, slower **integration tests** in the middle; costly **system tests** at the top.
# MAGIC - **On Databricks, integration testing has two idiomatic paths:** SDP **expectations** (declarative data-quality rules inside the pipeline) and **Lakeflow Jobs** tasks (orchestrated validation steps).
# MAGIC - **Environment isolation** — separate workspaces and Unity Catalog catalogs/schemas for dev, stage, and prod — is what makes safe CI/CD possible.
# MAGIC - **Declarative Automation Bundles (DABs) are the recommended deployment vehicle** because they package code and configuration declaratively and slot directly into a CI/CD workflow. The REST API, CLI, and SDK are lower-level alternatives for specific needs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Additional Resources
# MAGIC
# MAGIC Explore the following resources to deepen your understanding of DevOps and CI/CD on Databricks.
# MAGIC
# MAGIC ### C1. Documentation
# MAGIC
# MAGIC - **CI/CD on Databricks** - the end-to-end workflow and available tools:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/ci-cd/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/ci-cd/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/ci-cd/)
# MAGIC
# MAGIC - **Best practices and recommended CI/CD workflows** - version control, testing, and environment isolation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/ci-cd/best-practices) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/ci-cd/best-practices) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/ci-cd/best-practices)
# MAGIC
# MAGIC - **What are Declarative Automation Bundles (DABs)?** - infrastructure-as-code for Databricks projects:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/)
# MAGIC
# MAGIC - **Run CI/CD with Declarative Automation Bundles** - automate validate, deploy, and run:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/ci-cd) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/ci-cd) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/ci-cd)
# MAGIC
# MAGIC - **Unit testing for Databricks notebooks** - `pytest` and language-specific test frameworks:
# MAGIC [AWS](https://docs.databricks.com/aws/en/notebooks/testing) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/notebooks/testing) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/notebooks/testing)
# MAGIC
# MAGIC - **Use Databricks Git Folders** - version control with GitHub and other Git providers:
# MAGIC [AWS](https://docs.databricks.com/aws/en/repos/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/repos/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/repos/)
# MAGIC
# MAGIC ### C2. Blogs and Announcements
# MAGIC
# MAGIC - [Announcing the General Availability of Databricks Asset Bundles](https://www.databricks.com/blog/announcing-general-availability-databricks-asset-bundles) - How DABs let you version, test, deploy, and collaborate on jobs, pipelines, and notebooks as a single unit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Next Steps
# MAGIC
# MAGIC ### D1. Apply What You Learned
# MAGIC
# MAGIC - Re-run the labs on your own project data: **modularize** your PySpark, add **unit tests**, then wire up an **integration test** using both an SDP expectation and a Jobs task so you have felt both methods.
# MAGIC - Take version control further: create a **feature branch**, open a **pull request**, and practice a **code review** — the one DevOps discipline the course describes but does not force you to perform.
# MAGIC - Build a real **Declarative Automation Bundle** (`databricks.yml`) for your project, deploy it to a **dev** target, then promote it to **stage** and **prod**. From there, automate `bundle validate` and `bundle deploy` in a CI tool such as GitHub Actions to close the full CI/CD loop.
# MAGIC
# MAGIC ### D2. Continue Your Learning
# MAGIC
# MAGIC This course is part of the **Data Engineer Learning Path**. Continue building your data and AI skills through Databricks self-paced and instructor-led training.
# MAGIC
# MAGIC Visit [Databricks Training and Certification](https://www.databricks.com/learn/training/home).
# MAGIC
# MAGIC ### D3. Earn a Certification
# MAGIC
# MAGIC Validate your expertise with the **Databricks Certified Data Engineer Associate** and **Professional** credentials, which cover software engineering and production best practices on the platform.
# MAGIC
# MAGIC Visit [Databricks Certification and Badging](https://www.databricks.com/learn/training/certification).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC