# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/deploy-workloads-with-lakeflow-jobs-en_us-3.3.1/images/20260819T120710Z/Deploy Workloads with Lakeflow Jobs/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary and Next Steps

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Course Recap
# MAGIC
# MAGIC Across this course you learned how to orchestrate and automate data, analytics, and AI workloads using **Lakeflow Jobs** — from the core building blocks through to production-ready, fault-tolerant pipelines.
# MAGIC
# MAGIC | Step | What You Did | Key Takeaway |
# MAGIC |:---:|---|---|
# MAGIC | 1 | Reviewed data engineering in Databricks and where **Lakeflow Jobs** fits as the orchestration layer | Lakeflow Jobs provides unified orchestration for data, analytics, and AI on the Data Intelligence Platform |
# MAGIC | 2 | Explored the **building blocks** of a job — jobs, tasks, task types, compute, and **DAGs** | Tasks connected in a DAG define the control flow and dependencies of a workload |
# MAGIC | 3 | Created and ran multi-task jobs using the **Lakeflow Jobs UI** (demo + lab) | The UI lets you compose notebook and SQL tasks into a single orchestrated job |
# MAGIC | 4 | Configured **task parameters, dynamic values, notifications, and retry policies**, and automated jobs with **schedules and triggers** | Match the trigger to the workload — scheduled, file arrival, table update, or continuous |
# MAGIC | 5 | Built dynamic workflows with **conditional (run-if, If/Else)** and **iterative (For Each)** tasks | Conditional and iterative tasks make pipelines adaptive and resilient to runtime conditions |
# MAGIC | 6 | Recovered from failures with **Repair and Rerun** and monitored performance with **system tables** and the **Spark UI** | Repair reruns only failed tasks; system tables and the Spark UI surface bottlenecks and SLAs |
# MAGIC | 7 | Applied **production best practices** — compute selection, modular orchestration, Git integration, and maintainable design | Production jobs use the right compute, modular design, and version control for reliability at scale |

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Course Learning Objectives Recap
# MAGIC
# MAGIC By completing this course, you are now able to:
# MAGIC
# MAGIC 1. **Understand** the role of Lakeflow Jobs within the Databricks ecosystem as a unified orchestration platform for data, analytics, and AI workloads.
# MAGIC 2. **Design and implement** data workloads using Directed Acyclic Graphs (DAGs), demonstrating the relationship between jobs, tasks, and dependencies.
# MAGIC 3. **Configure** various job scheduling options including manual, scheduled, file arrival, and continuous triggers to automate workflow execution.
# MAGIC 4. **Implement** advanced workflow features such as conditional task execution, run-if dependencies, and repair runs to create robust, fault-tolerant data pipelines.
# MAGIC 5. **Apply** best practices for production workloads including selecting appropriate compute options, implementing modular orchestration, and utilizing proper error handling techniques.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Additional Resources
# MAGIC
# MAGIC Explore the following resources to learn more about Lakeflow Jobs and stay up to date with the latest platform updates.
# MAGIC
# MAGIC ### C1. Documentation
# MAGIC
# MAGIC - What is Lakeflow Jobs — orchestration for the Databricks Data Intelligence Platform:
# MAGIC [AWS](https://docs.databricks.com/aws/en/jobs/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/jobs/)
# MAGIC
# MAGIC - Trigger types for Lakeflow Jobs (scheduled, file arrival, table update, continuous):
# MAGIC [AWS](https://docs.databricks.com/aws/en/jobs/triggers) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/triggers) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/jobs/triggers)
# MAGIC
# MAGIC - Conditional tasks (run-if and If/Else) and For Each tasks:
# MAGIC [AWS](https://docs.databricks.com/aws/en/jobs/conditional-tasks) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/conditional-tasks) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/jobs/conditional-tasks)
# MAGIC
# MAGIC - Repair and rerun a failed job run:
# MAGIC [AWS](https://docs.databricks.com/aws/en/jobs/repair-job-failures) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/repair-job-failures) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/jobs/repair-job-failures)
# MAGIC
# MAGIC - Monitor jobs with system tables (`system.lakeflow`):
# MAGIC [AWS](https://docs.databricks.com/aws/en/admin/system-tables/jobs) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/jobs) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/admin/system-tables/jobs)
# MAGIC
# MAGIC ### C2. Blog and Announcements
# MAGIC
# MAGIC - [Announcing the General Availability of Databricks Lakeflow](https://www.databricks.com/blog/announcing-general-availability-databricks-lakeflow) — Lakeflow Connect, Declarative Pipelines, and Jobs are now GA, with a new IDE for data engineering.
# MAGIC
# MAGIC ### C3. Additional Features Outside the Scope of this Course
# MAGIC
# MAGIC - Apache Spark Declarative Pipelines — declarative, end-to-end streaming and batch pipelines:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dlt/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dlt/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dlt/)
# MAGIC
# MAGIC - Databricks Asset Bundles (DABs) — deploy jobs and pipelines as version-controlled, environment-aware bundles:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Next Steps
# MAGIC
# MAGIC Continue building your Databricks skills with additional training and certification resources.
# MAGIC
# MAGIC ### D1. Continue Your Learning
# MAGIC
# MAGIC Expand your data engineering knowledge through Databricks self-paced and instructor-led training. These courses offer hands-on instruction across the Databricks Data Intelligence Platform, Databricks SQL, task orchestration, and Unity Catalog.
# MAGIC
# MAGIC Visit [Databricks Training and Certification](https://www.databricks.com/learn/training/home)
# MAGIC
# MAGIC - [Data Ingestion with Lakeflow Connect](https://www.databricks.com/training/catalog/data-ingestion-with-lakeflow-connect-2968) — Ingest data into Databricks from files, cloud storage, and enterprise systems.
# MAGIC - [Build Data Pipelines with Apache Spark Declarative Pipelines](https://www.databricks.com/training/catalog/build-data-pipelines-with-apache-spark-declarative-pipelines-1686) — Author end-to-end pipelines with the declarative framework.
# MAGIC - [Data Engineering with Databricks](https://www.databricks.com/training/catalog/data-engineering-with-databricks-911) — Deepen your skills across ELT, incremental processing, and production pipelines.
# MAGIC
# MAGIC ### D2. Earn a Certification
# MAGIC
# MAGIC Validate your Databricks expertise by earning an official credential. Certifications demonstrate your ability to apply Databricks technologies in real-world data and AI workloads.
# MAGIC
# MAGIC Visit [Databricks Certification and Badging](https://www.databricks.com/learn/training/certification)
# MAGIC
# MAGIC - **Databricks Certified Data Engineer Associate** — the natural next credential after this course. The exam covers the Data Intelligence Platform, ELT with Spark SQL and Python, incremental data processing, production pipelines, and data governance.
# MAGIC - **Databricks Certified Data Engineer Professional** — for advanced data engineering on the Lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>