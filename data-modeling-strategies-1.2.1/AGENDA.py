# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Modeling Strategies
# MAGIC
# MAGIC This course offers a deep dive into designing data models within the Databricks Lakehouse environment, and understanding the data products lifecycle. Participants will learn to align business requirements with data organization and model design leveraging Delta Lake and Unity Catalog for defining data architectures, and techniques for data integration and sharing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC The content was developed for participants with these skills/knowledge/abilities:
# MAGIC - Basic knowledge of data warehousing concepts, including schemas, ETL processes, and business intelligence workflows.
# MAGIC - Familiarity with SQL, including table creation, joins, constraints, and data manipulation using queries.
# MAGIC - Basic experience with Databricks or similar cloud-based data platforms.
# MAGIC - Fundamental knowledge of Lakehouse architecture and Delta Lake operations.
# MAGIC - Exposure to Python and PySpark for data processing and transformation tasks (recommended but not mandatory).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Course Agenda
# MAGIC
# MAGIC The following modules are part of the **Data Modeling Strategies** course by Databricks Academy.
# MAGIC
# MAGIC | Module Name                                                                                    | Content                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
# MAGIC | ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
# MAGIC | Data Warehouse Data Modeling | **Lecture:** Lakehouse Architecture Recap <br> **Lecture:** DWH Modeling Overview <br> **Lecture:** Inmon’s Corporate Information Factory <br/> [Demo: 01 - Entity Relationship Modeling and Working with Constraints]($./1 - Entity Relationship Modeling and Working with Constraints) <br> **Lecture:** Kimball’s Dimensional Modeling <br/> [Demo: 02 - Dimensional Modeling and ETL]($./2 - Dimensional Modeling and ETL) <br> **Lecture:** Data Vault 2.0 <br/> [Demo: 03 - Data Vault 2.0]($./3 - Data Vault 2.0) |
# MAGIC | Modern Data Architecture Use Cases | **Lecture:**  Modern Case Study: Feature Store <br/> [Demo: 04 - Modern Use Case: Feature Store on Databricks]($./4 - Modern Use Case: Feature Store on Databricks) <br/> [Demo: 05 - Lab Teardown]($./5 - Lab Teardown) <br> **Lecture:** Combining Approaches |
# MAGIC | Data Products | **Lecture:** Defining Data Products |
# MAGIC | Comprehensive Lab | [Lab: 01 - Data Warehousing Modeling with ERM and Dimensional Modeling in Databricks]($./6L - Data Warehousing Modeling with ERM and Dimensional Modeling in Databricks)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use one of the following Databricks runtimes: **`15.4.x-scala2.12`**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
