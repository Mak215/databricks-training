# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating and Working with a Delta Table 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC    - Click **More** in the drop-down.
# MAGIC
# MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course.
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-01

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Explore your Catalog
# MAGIC
# MAGIC **The Unity Catalog object model**
# MAGIC
# MAGIC In Unity Catalog, all metadata is registered in a metastore. The hierarchy of database objects in any Unity Catalog metastore is divided into three levels, represented as a three-level namespace (catalog.schema.table-etc) when you reference tables, views, volumes, models, and functions.
# MAGIC
# MAGIC ![unity_catalog_object_model](../Includes/images/unity_catalog_object_model.png)
# MAGIC
# MAGIC
# MAGIC For more information check out [What is Unity Catalog?](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Viewing the Catalog and Schema
# MAGIC Complete the following to manually view the course catalog **dbacademy** and your schema:
# MAGIC
# MAGIC - Select the Catalog icon ![catalog_icon](../Includes/images/catalog_icon.png) in the left navigation bar. 
# MAGIC
# MAGIC - You should see your catalog name(**dbacademy**)
# MAGIC
# MAGIC - Expand the **dbacademy** catalog. Within the catalog, you should see a variety of schemas (databases).
# MAGIC
# MAGIC - Find your specific schema (begins with **labuser**). You can locate your schema in the classroom setup notes in the first cell.
# MAGIC
# MAGIC - Expand your schema. Notice that your schema only contains a volume named **myfiles**.
# MAGIC
# MAGIC - Expand your volume. The volume should contain a single CSV file named **employees.csv**.

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Defaulting Catalog and Schema
# MAGIC Execute the cells to set the current catalog to **dbacademy** and the current schema to your specific schema. This configuration avoids the need to use a two-level naming convention (catalog.schema) in your queries. The SELECT statement will display the name of your current catalog and schema. 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DA.schema_name -- Precreated SQL variable pointing to your unique schema

# COMMAND ----------

# MAGIC %md
# MAGIC Let's modify our default catalog and schema using the `USE CATALOG` and `USE SCHEMA` statements. This eliminates the need to specify the three-level name for objects in your **labuser** schema (i.e., catalog.schema.object).
# MAGIC
# MAGIC   - `USE CATALOG` – Sets the current catalog.
# MAGIC
# MAGIC   - `USE SCHEMA` – Sets the current schema.
# MAGIC
# MAGIC     **NOTE:** Since our dynamic schema name is stored in the SQL variable `DA.schema_name` as a string, we will need to use the `IDENTIFIER` clause to interpret the constant string in our variable as a schema name. The `IDENTIFIER` clause can interpret a constant string as any of the following:
# MAGIC     - Relation (table or view) name
# MAGIC     - Function name
# MAGIC     - Column name
# MAGIC     - Field name
# MAGIC     - Schema name
# MAGIC     - Catalog name
# MAGIC
# MAGIC     [IDENTIFIER clause documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names-identifier-clause?language=SQL)
# MAGIC
# MAGIC Run the following cell to set and view your default catalog and schema. Confirm that your default catalog is **dbacademy** and your schema is **labuser** (this uses the `DA.schema_name` variable created in the classroom setup script).
# MAGIC
# MAGIC **NOTE:** Alternatively, you can simply add your schema name without using the `IDENTIFIER` clause.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);
# MAGIC
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Describing the Schema 
# MAGIC Use the [DESCRIBE SCHEMA EXTENDED](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-schema.html) statement to display the metadata and properties of your schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA EXTENDED IDENTIFIER(DA.schema_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Show Table
# MAGIC Use the [SHOW TABLES](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-show-tables.html) statement to display the available tables in your schema. You will notice that there are currently no tables available in your schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Show Volumes
# MAGIC Use the [SHOW VOLUMES](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-show-volumes.html) statement to list all of the volumes available in your schema. Notice that you have a single volume available, **myfiles**.
# MAGIC
# MAGIC
# MAGIC **NOTE**: Volumes are Unity Catalog objects representing a logical volume of storage in a cloud object storage location. Volumes provide capabilities for accessing, storing, governing, and organizing files. While tables provide governance over tabular datasets, volumes add governance over non-tabular datasets. 
# MAGIC
# MAGIC You can use volumes to store and access files in any format, including structured, semi-structured, and unstructured data.
# MAGIC
# MAGIC [What are Unity Catalog volumes?](https://docs.databricks.com/en/volumes/index.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. Listing files/data in Volumes
# MAGIC Use the LIST statement to list all of the files in the **myfiles** volume. Notice that the volume only has the **employees.csv** file. It contains information about current employees. 
# MAGIC
# MAGIC When interacting with data in volumes, you use the path provided by Unity Catalog, which always has the following format: `/Volumes/catalog_name/schema_name/volume_name/`.
# MAGIC
# MAGIC For more information on exploring directories and data files managed with Unity Catalog volumes, check out the [Explore storage and find data files](https://docs.databricks.com/en/discover/files.html)  documentation.
# MAGIC

# COMMAND ----------

spark.sql(f"LIST '/Volumes/dbacademy/{DA.schema_name}/myfiles'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Create a Delta Table from a CSV File
# MAGIC
# MAGIC All saved tables on Databricks are Delta tables by default. Whether you’re using Apache Spark DataFrames or SQL, you get all the benefits of Delta Lake just by saving your data to the lakehouse with default settings.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Query the **employees.csv** file directly using SQL to view the file. Notice that the query returns a view of the CSV file with the headers as the first row of data.
# MAGIC
# MAGIC **NOTE**: This syntax is specific to Spark SQL and allows you to read files directly without explicitly loading them into a table first. You specify the file format and enclose the file path in **backticks**. This method works for various file types.
# MAGIC
# MAGIC *SELECT * FROM \<file_format>.\`/path/to/file`;*

# COMMAND ----------

spark.sql(f'''
SELECT *
FROM csv.`/Volumes/dbacademy/{DA.schema_name}/myfiles/` 
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. You can use the *text* file format to view files as strings in a column. This enables you to view the contents of the file. Notice that the first row contains the column names and the fields are separated by commas.

# COMMAND ----------

spark.sql(f'''
SELECT * 
FROM text.`/Volumes/dbacademy/{DA.schema_name}/myfiles/`
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. With SQL you can use the [read_files](https://docs.databricks.com/en/sql/language-manual/functions/read_files.html) table-valued function to read the CSV file into tabular form and apply specific options to modify how the file is read. 
# MAGIC
# MAGIC     Execute the query and confirm that the results show 4 employees with valid column names.
# MAGIC
# MAGIC **NOTE**: A rescued data column is provided by default to rescue any data that doesn’t match the schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   '/Volumes/dbacademy/' || DA.schema_name || '/myfiles/',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Use the CREATE TABLE AS (CTAS) statement with the query from above to create a Delta table in the Lakehouse named **current_employees** using the **employees.csv** file. The table will be created in your schema.
# MAGIC
# MAGIC **NOTE:** The CREATE TABLE statement will create a Delta table by default.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the table if it already exists for demonstration purposes
# MAGIC DROP TABLE IF EXISTS current_employees;
# MAGIC
# MAGIC -- Create a Delta table using the CSV file
# MAGIC
# MAGIC CREATE TABLE current_employees AS
# MAGIC SELECT 
# MAGIC   ID, 
# MAGIC   FirstName, 
# MAGIC   Country, 
# MAGIC   Role 
# MAGIC FROM read_files(
# MAGIC   '/Volumes/dbacademy/' || DA.schema_name || '/myfiles/',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC   );
# MAGIC
# MAGIC
# MAGIC -- Display table
# MAGIC SELECT *
# MAGIC FROM current_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Complete the following steps to manually view the table in your schema:
# MAGIC
# MAGIC    a. Select the Catalog icon ![catalog_icon](../Includes/images/catalog_icon.png) in the left navigation bar. 
# MAGIC
# MAGIC    b. Find the catalog name **dbacademy**.
# MAGIC
# MAGIC    c. Select the refresh icon ![refresh_icon](../Includes/images/refresh_icon.png) to refresh the catalog.
# MAGIC
# MAGIC    d. Expand the **dbacademy** catalog. Within the catalog you should see a variety of schemas (databases).
# MAGIC
# MAGIC    e. Find your schema. You can locate your schema in the classroom setup notes in the first cell. 
# MAGIC
# MAGIC    f. Expand your schema. Notice that your schema contains **Volumes** and **Tables**.
# MAGIC
# MAGIC    g. Expand **Tables**. Confirm that the **current_employees** Delta table is available.

# COMMAND ----------

# MAGIC %md
# MAGIC (optional) To create a Delta table from a CSV file using Python, you can use the code below:

# COMMAND ----------


#
# Read the CSV file and create a Spark DataFrame
#
sdf = (spark
       .read
       .format("csv")
       .option('header', 'true')
       .option('inferSchema','true')
       .load(f'/Volumes/dbacademy/{DA.schema_name}/myfiles/')
    )


#
# Create a Delta table from the Spark DataFrame
#

(sdf
 .write
 .mode("overwrite")
 .format("delta")
 .saveAsTable(f"dbacademy.{DA.schema_name}.current_employees_py")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Read the Delta table using Python.

# COMMAND ----------

(spark
 .read
 .table(f"dbacademy.{DA.schema_name}.current_employees_py")
 .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Show the tables in your schema using Python. You should see a table named **current_employees** and **current_employees_py**.
# MAGIC

# COMMAND ----------

spark.catalog.listTables(f"dbacademy.{DA.schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Query the **current_employees** table to view the data. Confirm that it contains 4 rows of data with a list of current employees.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Use the DESCRIBE DETAIL statement to view detailed information about the **current_employees** table. View the results. 
# MAGIC
# MAGIC     Notice the following:
# MAGIC     - The **format** column indicates that the **current_employees** table has been created as a Delta table.
# MAGIC     - The **location** column displays the cloud location of the table in the following format for AWS: 
# MAGIC         - *s3://\<bucket-name>/\<metastore id>/tables/\<table id>* 
# MAGIC
# MAGIC **NOTES:** 
# MAGIC     - The results of the DESCRIBE DETAIL statement includes additional information about the Delta table. For more details, refer to the [Detail schema documentation](https://docs.databricks.com/en/delta/table-details.html#detail-schema).
# MAGIC     - For more information on how paths work for data managed by Unity Catalog check out the [How do paths work for data managed by Unity Catalog?](https://docs.databricks.com/en/data-governance/unity-catalog/paths.html) documentation.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL current_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Use the [DESCRIBE EXTENDED](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html) statement to display detailed information about the specified columns and additional metadata for the Delta table.
# MAGIC
# MAGIC     Notice the following:
# MAGIC     - The top portion of the results displays column metadata of the table.
# MAGIC     - Scroll down the **col_name** column to find the *Type* value. Notice that the **data_type** specifies *Managed*. This indicates that Databricks manages the lifecycle and file layout for the table. Managed tables are the default table creation method.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED current_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 10. Execute the [DESCRIBE HISTORY](https://docs.databricks.com/en/sql/language-manual/delta-describe-history.html) statement on the **current_employees** table to retrieve Delta table history.
# MAGIC
# MAGIC     Notice the following:
# MAGIC     - The **version** column indicates the table is on version 0
# MAGIC     - The **timestamp** column displays when the table was created
# MAGIC     - The **operation** column shows what operation was performed.
# MAGIC     - The **operationsMetrics** column display information about the number of files, number of output rows, and number of output bytes.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY current_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Insert, Update and Delete Records in the Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC 1. View the current **current_employees** table. 
# MAGIC
# MAGIC     Notice the following in the Delta table:
# MAGIC     - There are 4 columns and 4 rows
# MAGIC     - The **ID** values *1111*, *2222*, *3333*, *4444* are in the table
# MAGIC     - The  **Role** of **ID** *1111* is *Manager*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Perform the following operations on the **current_employees** Delta table:
# MAGIC
# MAGIC    a. Insert two new employees named *Alex* and *Sanjay*. 
# MAGIC
# MAGIC    b. Update the **Role** of employee **ID** *1111* to *Senior Manager*.
# MAGIC
# MAGIC    c. Delete the record of the employee with the **ID** *3333*.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Insert two employees into the table
# MAGIC INSERT INTO current_employees 
# MAGIC VALUES
# MAGIC     (5555, 'Alex','USA', 'Instructor'),
# MAGIC     (6666, 'Sanjay','India', 'Instructor');
# MAGIC
# MAGIC -- 2. Update a record in the table
# MAGIC UPDATE current_employees
# MAGIC   SET Role = 'Senior Manager'
# MAGIC   WHERE ID = 1111;
# MAGIC
# MAGIC -- 3. Delete a record in the table
# MAGIC DELETE FROM current_employees
# MAGIC   WHERE ID = 3333;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. View the data in the **current_employees** table. Notice that the table has been modified from the original version.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees
# MAGIC ORDER BY ID;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Each operation that modifies a Delta Lake table creates a new table version. View the history of the table.
# MAGIC
# MAGIC     Notice the following:
# MAGIC     - The table has versions 0 through 4. 
# MAGIC     - **Version 0** is the original table that was created.
# MAGIC     - **Version 1** contains the WRITE operation that inserted two new employees.
# MAGIC     - **Version 2** contains the UPDATE operation that modified the job role.
# MAGIC     - **Version 3** contains the DELETE operation that removed an employee.
# MAGIC     - **Version 4** contains the OPTIMIZE operation on the table. Predictive optimization is a feature in Delta Lake on Databricks that automatically optimizes Delta tables (optional feature). For more information, view the [Predictive optimization for Delta Lake
# MAGIC     ](https://docs.databricks.com/en/optimizations/predictive-optimization.html) documentation.
# MAGIC
# MAGIC **NOTE:** The `OPTIMIZE` operation(s) might be in a different order.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY current_employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Use Time Travel to Read Previous Versions of the Delta Table
# MAGIC You can use history information to audit operations, rollback a table, or query a table at a specific point in time using time travel.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. View the current version of the **current_employees** table. By default, the most recent version of the table will be used.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees
# MAGIC ORDER BY ID;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Use time travel to view the table prior to the DELETE operation. Notice that the results show 6 employees before the record was deleted.
# MAGIC
# MAGIC **NOTE**: Time travel takes advantage of the power of the Delta Lake transaction log to access data that is no longer in the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees VERSION AS OF 2
# MAGIC ORDER BY ID;
# MAGIC
# MAGIC -- Alternate syntax
# MAGIC -- SELECT *
# MAGIC -- FROM current_employees@v2
# MAGIC -- ORDER BY ID;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. View the original table using version 0. Notice that the original 4 employees are displayed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees VERSION AS OF 0
# MAGIC ORDER BY ID;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Drop the Tables
# MAGIC 1. Drop the Delta tables created in this demonstration.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_employees;
# MAGIC DROP TABLE IF EXISTS current_employees_py;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC
