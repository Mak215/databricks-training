-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 1 - Setup and Bronze Table
-- MAGIC This Notebook is used for task 1 in the job from the directions in notebook: **DEWD00 - 04-Creating a Simple Databricks Job**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this course.
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-04

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configure Your Environment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Set the default catalog to **getstarted** and the schema to your specific schema. Then, view the available tables to confirm that no tables currently exist in your schema.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set the catalog and schema
-- MAGIC spark.sql(f'USE CATALOG {DA.catalog_name}')
-- MAGIC spark.sql(f'USE SCHEMA {DA.schema_name}')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### BRONZE
-- MAGIC **Objective:** Create a table using all of the CSV files in the **myfiles** volume.

-- COMMAND ----------

-- Create an empty table and columns
CREATE TABLE IF NOT EXISTS current_employees_bronze_job (
  ID INT,
  FirstName STRING,
  Country STRING,
  Role STRING
  );

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create the bronze raw ingestion table and include the file name for the rows
-- MAGIC spark.sql(f'''
-- MAGIC   COPY INTO current_employees_bronze_job
-- MAGIC   FROM '/Volumes/dbacademy/{DA.schema_name}/myfiles/'
-- MAGIC   FILEFORMAT = CSV
-- MAGIC   FORMAT_OPTIONS (
-- MAGIC     'header' = 'true', 
-- MAGIC     'inferSchema' = 'true'
-- MAGIC )
-- MAGIC ''').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
