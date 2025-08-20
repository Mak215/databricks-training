# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Transforming Data Using the Medallion Architecture
# MAGIC ![medallion_architecture](../Includes/images/medallion_architecture.png)

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

# MAGIC %run ../Includes/Classroom-Setup-03

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Configure and Explore Your Environment

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Set the default catalog to **dbacademy** and the schema to your specific schema. Then, view the current catalog and schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);
# MAGIC
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. View the available files in your schema's **myfiles** volume. Confirm that the volume contains two CSV files, **employees.csv** and **employees2.csv**.

# COMMAND ----------

spark.sql(f"LIST '/Volumes/dbacademy/{DA.schema_name}/myfiles/' ").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Simple Example of the Medallion Architecture
# MAGIC
# MAGIC **Objective**: Create a job that can be scheduled to run on a schedule. The pipeline will:
# MAGIC
# MAGIC 1. Ingest all CSV files from the **myfiles** volume and create a bronze table.
# MAGIC 2. Prepare the bronze table by adding new columns and create a silver table.
# MAGIC 3. Create a gold aggregated table for consumers.

# COMMAND ----------

# MAGIC %md
# MAGIC ### BRONZE
# MAGIC **Objective:** Create a table using all of the CSV files in the **myfiles** volume.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Execute the cell to perform the following steps:
# MAGIC
# MAGIC     - The `DROP TABLE IF EXISTS` statement drops the **current_employees_bronze** table if it already exists (for demonstration purposes).
# MAGIC     
# MAGIC     - The `CREATE TABLE IF NOT EXISTS` statement creates the Delta table **current_employees_bronze** if it doesn't already exist and defines its columns.
# MAGIC     
# MAGIC     - The `COPY INTO` statement:
# MAGIC         - Loads all the CSV files from the **myfiles** volume in your schema into the **current_employees_bronze** table.
# MAGIC         - Uses the first row as headers and infers the schema from the CSV files.
# MAGIC     
# MAGIC     - The final `SELECT` query displays all rows from the **current_employees_bronze** table.
# MAGIC
# MAGIC
# MAGIC View the results and confirm that the table contains **6 rows** and **4 columns**.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the table if it exists for demonstration purposes
# MAGIC DROP TABLE IF EXISTS current_employees_bronze;
# MAGIC
# MAGIC -- Create an empty table and columns
# MAGIC CREATE TABLE IF NOT EXISTS current_employees_bronze (
# MAGIC   ID INT,
# MAGIC   FirstName STRING,
# MAGIC   Country STRING,
# MAGIC   Role STRING
# MAGIC   );

# COMMAND ----------

## Create the bronze raw ingestion table and include the CSV file name for the rows
spark.sql(f'''
  COPY INTO current_employees_bronze
  FROM '/Volumes/dbacademy/{DA.schema_name}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
    'header' = 'true', 
    'inferSchema' = 'true'
)
''').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### SILVER
# MAGIC **Objective**: Transform the bronze table and create the silver table.
# MAGIC
# MAGIC 1. Create a table named **current_employees_silver** from the **current_employees_bronze** table. 
# MAGIC
# MAGIC     The table will:
# MAGIC     - Select the columns **ID**, **FirstName**, **Country**.
# MAGIC     - Convert the **Role** column to uppercase.
# MAGIC     - Add two new columns: **CurrentTimeStamp** and **CurrentDate**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view to use to merge the data into the final silver table
# MAGIC CREATE OR REPLACE TABLE current_employees_silver AS 
# MAGIC SELECT 
# MAGIC   ID,
# MAGIC   FirstName,
# MAGIC   Country,
# MAGIC   upper(Role) as Role,                 -- Upcase the Role column
# MAGIC   current_timestamp() as CurrentTimeStamp,    -- Get the current datetime
# MAGIC   date(CurrentTimeStamp) as CurrentDate       -- Get the date
# MAGIC FROM current_employees_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. View the **current_employees_silver** table. Confirm that the table contains 6 rows and 6 columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD
# MAGIC **Objective:** Aggregate the silver table to create the final gold table.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create a temporary view named **temp_view_total_roles** that aggregates the total number of employees by role. Then, display the results of the view.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view_total_roles AS 
# MAGIC SELECT
# MAGIC   Role, 
# MAGIC   count(*) as TotalEmployees
# MAGIC FROM current_employees_silver
# MAGIC GROUP BY Role;
# MAGIC
# MAGIC
# MAGIC SELECT *
# MAGIC FROM temp_view_total_roles;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Create the final gold table named **total_roles_gold** with the specified columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS total_roles_gold (
# MAGIC   Role STRING,
# MAGIC   TotalEmployees INT
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Insert all rows from the aggregated temporary view **temp_view_total_rows** into the **total_roles_gold** table, overwriting the existing data in the table. This overwrites the data in a table but keeps the existing schema and table definition and properties.
# MAGIC
# MAGIC     Confirm the following:
# MAGIC     - **num_affected_rows** is *4*
# MAGIC     - **num_inserted_rows** is *4*

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE total_roles_gold
# MAGIC SELECT * 
# MAGIC FROM temp_view_total_roles;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Query the **total_roles_gold** table to view the total number of employees by role.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM total_roles_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC 5. View the history of the **total_roles_gold** table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY total_roles_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Data Governance and Security
# MAGIC **Objectives:** View the lineage of the **total_roles_gold** table and learn how to set its permissions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Complete the following to open your schema in the **Catalog Explorer**.
# MAGIC
# MAGIC - a. Select the Catalog icon ![catalog_icon](../Includes/images/catalog_icon.png) in the left navigation bar. 
# MAGIC
# MAGIC - b. Type the module's catalog name in the search bar (**dbacademy**).
# MAGIC
# MAGIC - c. Select the refresh icon ![refresh_icon](../Includes/images/refresh_icon.png) to refresh the **dbacademy** catalog.
# MAGIC
# MAGIC - d. Expand the **dbacademy** catalog. Within the catalog, you should see a variety of schemas (databases).
# MAGIC
# MAGIC - e. Find and select your schema. You can locate your schema in the setup notes in the first cell. 
# MAGIC
# MAGIC - f.  Click the options icon ![options_icon](../Includes/images/options_icon.png) to the right of your schema and choose **Open in Catalog Explorer**.
# MAGIC
# MAGIC - g. Notice that the four tables we created in the demo: **current_employees_bronze**, **current_employees_silver**, **total_roles_gold** and **wine_quality_table** are shown in the **Catalog Explorer** for your schema:
# MAGIC   - **current_employees_bronze**
# MAGIC   - **current_employees_silver**
# MAGIC   - **total_roles_gold**
# MAGIC   - **wine_quality_table**
# MAGIC
# MAGIC - h. In the **Catalog Explorer** select the **total_roles_gold** table.
# MAGIC
# MAGIC Leave the **Catalog Explorer** tab open.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Complete the following to view the **total_roles_gold** table's permissions, history, lineage and insights in Catalog Explorer: 
# MAGIC
# MAGIC    a. **Permissions**. 
# MAGIC
# MAGIC   - Select the **Permissions** tab. This will display all permissions on the table. Currently the table does not have any permissions set.
# MAGIC
# MAGIC   - Select **Grant**. This allows you to add multiple principals and assign privileges to them. Users must have access to the Catalog and Schema of the table.
# MAGIC   
# MAGIC   - Select **Cancel**. 
# MAGIC
# MAGIC    b. **History**
# MAGIC
# MAGIC   - Select the **History** tab. This will display the table's history. The **total_roles_gold** table currently has two versions.
# MAGIC
# MAGIC    c. **Lineage**
# MAGIC
# MAGIC   - Select the **Lineage** tab. This displays the table's lineage. Confirm that the **current_employees_silver** table is shown.
# MAGIC
# MAGIC   - Select the **See lineage graph** button ![see_lineage_graph_button](../Includes/images/see_lineage_graph_button.png). This displays the table's lineage visually. You can select the ![plus_button](../Includes/images/plus_button.png) icon to view additional information.
# MAGIC
# MAGIC   - Close out of the lineage graph.
# MAGIC
# MAGIC    d. **Insights**
# MAGIC
# MAGIC   - Select the **Insights** tab. You can use the Insights tab in **Catalog Explorer** to view the most frequent recent queries and users of any table registered in Unity Catalog. The Insights tab reports on frequent queries and user access for the past 30 days.
# MAGIC   
# MAGIC    e. Close the **Catalog Explorer** browser tab.

# COMMAND ----------

# MAGIC %md
# MAGIC ##D. Cleanup
# MAGIC 1. Drop views and tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the tables
# MAGIC DROP TABLE IF EXISTS current_employees_bronze;
# MAGIC DROP TABLE IF EXISTS current_employees_silver;
# MAGIC DROP TABLE IF EXISTS total_roles_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC
