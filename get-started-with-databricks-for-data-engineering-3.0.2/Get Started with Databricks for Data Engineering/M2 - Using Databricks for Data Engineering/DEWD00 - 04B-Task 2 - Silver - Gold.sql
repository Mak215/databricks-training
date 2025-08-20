-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Task 2 - Silver - Gold Table
-- MAGIC This Notebook is used for task 2 in the job from the directions in notebook: **DEWD00 - 04-Creating a Simple Databricks Job**

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
-- MAGIC ### SILVER
-- MAGIC **Objective**: Transform the bronze table and create the silver table.
-- MAGIC
-- MAGIC 1. Create a table named **current_employees_silver_job** from the **current_employees_bronze_job** table. 
-- MAGIC
-- MAGIC     The table will:
-- MAGIC     - Select the columns **ID**, **FirstName**, **Country**.
-- MAGIC     - Convert the **Role** column to uppercase.
-- MAGIC     - Add two new columns: **CurrentTimeStamp** and **CurrentDate**.

-- COMMAND ----------

-- Create a temporary view to use to merge the data into the final silver table
CREATE OR REPLACE TABLE current_employees_silver_job AS 
SELECT 
  ID,
  FirstName,
  Country,
  upper(Role) as Role,                 -- Upcase the Role column
  current_timestamp() as CurrentTimeStamp,    -- Get the current datetime
  date(CurrentTimeStamp) as CurrentDate       -- Get the date
FROM current_employees_bronze_job;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GOLD
-- MAGIC **Objective:** Aggregate the silver table to create the final gold table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create a temporary view named **temp_view_total_roles_job** that aggregates the total number of employees by role. Then, display the results of the view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW temp_view_total_roles_job AS 
SELECT
  Role, 
  count(*) as TotalEmployees
FROM current_employees_silver_job
GROUP BY Role;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Create the final gold table named **total_roles_gold_job** with the specified columns.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS total_roles_gold_job (
  Role STRING,
  TotalEmployees INT
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Insert all rows from the aggregated temporary view **temp_view_total_roles_job** into the **total_roles_gold_job** table, overwriting the existing data in the table. This overwrites the data in a table but keeps the existing schema and table definition and properties.
-- MAGIC
-- MAGIC     Confirm the following:
-- MAGIC     - **num_affected_rows** is *4*
-- MAGIC     - **num_inserted_rows** is *4*

-- COMMAND ----------

INSERT OVERWRITE TABLE total_roles_gold_job
SELECT * 
FROM temp_view_total_roles_job;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
