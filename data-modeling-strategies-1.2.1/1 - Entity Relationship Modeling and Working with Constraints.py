# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Entity Relationship Modeling and Working with Constraints
# MAGIC In this demo, we will explore how to model data using Entity Relationship Modeling (ERM) within Databricks. You will learn how to create tables with primary key (PK) and foreign key (FK) constraints, load data from TPC-H datasets, identify constraint violations, and visualize table relationships using Entity Relationship Diagrams (ERDs). Additionally, you will practice managing data consistency and performing clean-up operations without disrupting the environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Apply primary and foreign key constraints to model relational table relationships in Databricks.
# MAGIC - Demonstrate how to load data into tables using SQL commands from the bronze schema.
# MAGIC - Evaluate the impact of unenforced constraints and simulate data integrity violations.
# MAGIC - Analyze and troubleshoot constraint violations using SQL operations.
# MAGIC - Visualize Entity Relationship Diagrams to interpret table relationships effectively.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC * Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**. <br>
# MAGIC
# MAGIC ##### **📌**If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC   - In the drop-down, select **More**.
# MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the demo:
# MAGIC
# MAGIC * To run this notebook, you need to use one of the following Databricks runtime(s): **15.4.x-scala2.12**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Run the lab setup (prerequisite)
# MAGIC
# MAGIC Before proceeding with the contents of this and other lab activities, please ensure that you have run the lab setup script.
# MAGIC As a result of running the script, you will create:
# MAGIC 1. A dedicated catalog named after your lab user account.  
# MAGIC 2. Schemas named `bronze`, `silver`, and `gold` inside the catalog.  
# MAGIC 3. TPC-H tables copied from Samples into your `bronze` schema.

# COMMAND ----------

# DBTITLE 1,%run ./Includes/setup/lab_setup
# MAGIC %run ./Includes/setup/lab_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Choose your working catalog and schema
# MAGIC
# MAGIC Throughout this lab, you can create new tables in `silver` (or another schema of your choice). 
# MAGIC For demonstration, we will use the `silver` schema in the user-specific catalog.  
# MAGIC

# COMMAND ----------

# DBTITLE 1,Working Catalog and Schema
import re

# Get the current user and extract the catalog name by splitting the email at '@' and taking the first part
user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]

# Replace all special characters in the `user_id` with an underscore '_' to create the catalog name
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id) # New Code

# Define the schema name to be used
schema_name = "silver"

# COMMAND ----------

# Create a widget to capture catalog name and all schema names
dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("schema_name", schema_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current catalog to the extracted catalog name
# MAGIC USE CATALOG IDENTIFIER(:catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current schema to the defined schema name
# MAGIC USE SCHEMA IDENTIFIER(:schema_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the default catalog and schema names
# MAGIC SELECT current_catalog() AS Catalog_Name, current_schema() AS Schema_Name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create tables with constraints
# MAGIC
# MAGIC We will create two tables to demonstrate **Primary Key (PK)** and **Foreign Key (FK)** constraints:
# MAGIC
# MAGIC 1. `lab_customer` with a primary key on `c_custkey`.  
# MAGIC 2. `lab_orders` with:  
# MAGIC    - A primary key on `o_orderkey`.  
# MAGIC    - A foreign key on `o_custkey` referencing `lab_customer(c_custkey)`.
# MAGIC
# MAGIC
# MAGIC While Databricks **does not** enforce primary key (PK) and foreign key (FK) constraints by default, it **does** use them to *explain* the relationships between tables.
# MAGIC
# MAGIC These constraints are informational only and serve to encode relationships between fields in tables, without enforcing data integrity.
# MAGIC
# MAGIC This approach allows Databricks to provide useful features such as: Entity Relationship Diagrams (ERDs) in Catalog Explorer, which visually display the primary key and foreign key relationships between tables as a graph.
# MAGIC
# MAGIC `NOT NULL` and `CHECK` constraints on the table are enforced.

# COMMAND ----------

# DBTITLE 1,CREATE silver.lab_customer
# MAGIC %sql
# MAGIC -- Create the lab_customer table with a PRIMARY KEY constraint on c_custkey
# MAGIC CREATE TABLE IF NOT EXISTS lab_customer 
# MAGIC (
# MAGIC   c_custkey INT,
# MAGIC   c_name STRING,
# MAGIC   c_address STRING,
# MAGIC   c_nationkey INT,
# MAGIC   c_phone STRING,
# MAGIC   c_acctbal DECIMAL(12,2),
# MAGIC   c_mktsegment STRING,
# MAGIC   c_comment STRING,
# MAGIC   CONSTRAINT pk_custkey PRIMARY KEY (c_custkey)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Current Catalog
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Current Schema
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Replace `<default_catalog_name>` and `<default_schema_name>` values in the below code with the actual default catalog and schema names from the query output in the previous cells:
# MAGIC
# MAGIC **Target Code Line:**```dbsql
# MAGIC CONSTRAINT fk_custkey FOREIGN KEY (o_custkey) REFERENCES <default_catalog_name>.<default_schema_name>.lab_customer
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,CREATE silver.lab_orders
# MAGIC %sql
# MAGIC -- Create the lab_orders table with PRIMARY KEY on o_orderkey
# MAGIC -- and a FOREIGN KEY referencing lab_customer(c_custkey)
# MAGIC -- Note: Provide REFERENCES with three level namespace
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lab_orders
# MAGIC (
# MAGIC   o_orderkey INT,
# MAGIC   o_custkey INT,
# MAGIC   o_orderstatus STRING,
# MAGIC   o_totalprice DECIMAL(12,2),
# MAGIC   o_orderdate DATE,
# MAGIC   o_orderpriority STRING,
# MAGIC   o_clerk STRING,
# MAGIC   o_shippriority INT,
# MAGIC   o_comment STRING,
# MAGIC   CONSTRAINT pk_orderkey PRIMARY KEY (o_orderkey),
# MAGIC   CONSTRAINT fk_custkey FOREIGN KEY (o_custkey) REFERENCES <default_catalog_name>.<default_schema_name>.lab_customer(c_custkey)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: Insert data from the TPC-H bronze tables
# MAGIC
# MAGIC We’ll populate the newly created `lab_customer` and `lab_orders` tables from the TPC-H data located in the `bronze` schema.

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.lab_customer FROM bronze.customer
# MAGIC %sql
# MAGIC -- Insert data into lab_customer from bronze.customer
# MAGIC INSERT INTO lab_customer
# MAGIC SELECT
# MAGIC   c_custkey,
# MAGIC   c_name,
# MAGIC   c_address,
# MAGIC   c_nationkey,
# MAGIC   c_phone,
# MAGIC   c_acctbal,
# MAGIC   c_mktsegment,
# MAGIC   c_comment
# MAGIC FROM bronze.customer;
# MAGIC

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.lab_orders FROM bronze.orders
# MAGIC %sql
# MAGIC -- Insert data into lab_orders from bronze.orders
# MAGIC INSERT INTO lab_orders
# MAGIC SELECT
# MAGIC   o_orderkey,
# MAGIC   o_custkey,
# MAGIC   o_orderstatus,
# MAGIC   o_totalprice,
# MAGIC   o_orderdate,
# MAGIC   o_orderpriority,
# MAGIC   o_clerk,
# MAGIC   o_shippriority,
# MAGIC   o_comment
# MAGIC FROM bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5: Demonstrate `CONSTRAINT` violations
# MAGIC
# MAGIC As previously mentioned, Databricks does not enforce the constraints we define for primary and foreign relationships. Let's see the implications of this in practice.
# MAGIC
# MAGIC 1. **Foreign Key Violation**: Insert a row into `lab_orders` referencing a nonexistent `o_custkey`.  
# MAGIC 2. **Primary Key Violation**: Insert a duplicate row in `lab_customer`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Foreign Key Violation
# MAGIC
# MAGIC We'll insert an order referencing a nonexistent `o_custkey`. In the current implementation, Databricks does not enforce this relationship by default, so this insert may succeed without error.

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.lab_orders
# MAGIC %sql
# MAGIC INSERT INTO lab_orders 
# MAGIC VALUES
# MAGIC (
# MAGIC   9999999,         -- o_orderkey
# MAGIC   9999999,         -- o_custkey (nonexistent in lab_customer)
# MAGIC   'F',
# MAGIC   1000.00,
# MAGIC   current_date(),
# MAGIC   '3-LOW',
# MAGIC   'Clerk#000000001',
# MAGIC   0,
# MAGIC   'Testing invalid customer key'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### Primary Key Violation
# MAGIC
# MAGIC Next, we’ll insert a row into `lab_customer` using a value for `c_custkey` that already exists in the table. Again, this should not raise an error, due to the default behavior of our unenforced constraints.

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.lab_customer
# MAGIC %sql
# MAGIC -- Assuming c_custkey = 1 exists in lab_customer
# MAGIC INSERT INTO lab_customer
# MAGIC VALUES
# MAGIC (
# MAGIC   1,
# MAGIC   'Duplicate Customer',
# MAGIC   'Duplicate Address',
# MAGIC   9999,
# MAGIC   '999-999-9999',
# MAGIC   9999.99,
# MAGIC   'DUPLICATE_SEGMENT',
# MAGIC   'Inserting a duplicate primary key for demonstration'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Revert to a clean state
# MAGIC
# MAGIC To remove problematic rows, we can either delete them directly, truncate the tables, or use Delta Time Travel to revert. Below is an example of deleting the recently added violating rows (be sure to adjust the keys, if you inserted different values).

# COMMAND ----------

# DBTITLE 1,DELETE FROM silver.lab_orders
# MAGIC %sql
# MAGIC -- Remove the foreign key violation (orderkey=9999999)
# MAGIC DELETE FROM lab_orders
# MAGIC WHERE o_orderkey = 9999999;
# MAGIC

# COMMAND ----------

# DBTITLE 1,DELETE FROM silver.lab_customer
# MAGIC %sql
# MAGIC -- Remove the duplicate primary key row
# MAGIC DELETE FROM lab_customer
# MAGIC WHERE c_custkey = 1 
# MAGIC   AND c_name = 'Duplicate Customer';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Visualizing the ER Diagram in Databricks
# MAGIC
# MAGIC Based on our definition of the constraints, we can use Databricks to see these relationships in a diagram:
# MAGIC 1. In Databricks, navigate to the **Catalog** explorer on the left.  
# MAGIC 2. Select your catalog (based on your lab user identity) and then the schema (e.g., `silver`).  
# MAGIC 3. Locate the `lab_orders` table, which has a foreign key relationship with `lab_customer`.
# MAGIC 4. Click on the **View Relationships** button to view the diagram of your tables and their relationships.  
# MAGIC
# MAGIC You should see that `lab_orders` has a foreign key referencing `lab_customer`.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 8: Lab Teardown for Created Tables
# MAGIC
# MAGIC Drop only the silver lab tables you created, leaving your main TPC-H tables in bronze, and the overall environment intact.

# COMMAND ----------

# DBTITLE 1,DROP silver.lab_orders, DROP silver.lab_customer
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lab_orders;
# MAGIC DROP TABLE IF EXISTS lab_customer;

# COMMAND ----------

# MAGIC %md
# MAGIC Remove all widgets created during the demo to clean up the notebook environment.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC Throughout this demo, we applied data warehousing modeling techniques to create and manage tables in Databricks. We established primary and foreign key relationships using constraints, inserted data, and explored how Databricks handles constraint violations. By leveraging Entity Relationship Diagrams, we visualized table relationships and understood their structural significance. By the end of this demo, you will have developed practical skills in relational data modeling, schema management, and error troubleshooting in a modern data platform, empowering you to build and manage scalable data solutions.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
