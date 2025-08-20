# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Warehousing Modeling with ERM and Dimensional Modeling in Databricks
# MAGIC In this lab, you will explore modern data warehousing techniques using Databricks, starting with Entity Relationship Modeling \(ERM\) and Dimensional Modeling. You will also dive into advanced approaches such as Data Vault 2.0 and machine learning feature engineering with Databricks Feature Store. From defining relational constraints to tracking historical changes with SCD Type 2, and from designing vault models to executing batch inference, this lab equips you to build scalable, analytics-ready data architectures and ML workflows on a unified platform.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC * Apply primary and foreign key constraints to maintain relational integrity in Delta tables
# MAGIC * Build dimensional models using Slowly Changing Dimension (SCD) Type 2 techniques
# MAGIC * Implement Data Vault 2.0 components (Hubs, Links, Satellites) using hash keys
# MAGIC * Design scalable ETL pipelines to populate and manage relational, dimensional, and vault schemas
# MAGIC * Create and manage feature tables using the Databricks Feature Store
# MAGIC * Train machine learning models using registered features for reproducible pipelines
# MAGIC * Perform batch inference by joining feature data and applying trained models at scale

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
# MAGIC * Alternatively, on a non-ML runtime cluster, manually install required libraries in a similar way. (For this demo, we have non-ML cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Libraries and Run the Setup Script
# MAGIC
# MAGIC **Task 1:** Install the Databricks Feature Engineering library to enable table definitions, training set creation, and feature publishing.

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

# MAGIC %md
# MAGIC Once the library is installed, we restart the Python kernel so it’s fully available.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC **Task 2:** Before proceeding with the contents of this and other lab activities, please ensure that you have run the lab setup script.
# MAGIC As a result of running the script, you will create:
# MAGIC 1. A dedicated catalog named after your lab user account.  
# MAGIC 2. Schemas named `bronze`, `silver`, and `gold` inside the catalog.  
# MAGIC 3. TPC-H tables copied from Samples into your `bronze` schema.

# COMMAND ----------

# MAGIC %run ./Includes/setup/lab_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Choose your working catalog and schema
# MAGIC
# MAGIC Throughout this lab, you can create new tables in `silver` (or another schema of your choice). 
# MAGIC For demonstration, we will use the `silver` schema in the user-specific catalog.

# COMMAND ----------

import re
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

# Get the current user and extract the catalog name by splitting the email at '@' and taking the first part
user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]

# Replace all special characters in the `user_id` with an underscore '_' to create the catalog name
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id) # New Code

# Define the silver schema name to be used
silver_schema = "silver"
gold_schema = "gold"

print("Catalog and schemas set for feature development.")

# COMMAND ----------

# Create a widget to capture catalog name and all schema names
dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("silver_schema", silver_schema)
dbutils.widgets.text("gold_schema", gold_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current catalog to the extracted catalog name
# MAGIC USE CATALOG IDENTIFIER(:catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current schema to the defined schema name
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the default catalog and schema names
# MAGIC SELECT current_catalog() AS Catalog_Name, current_schema() AS Schema_Name;

# COMMAND ----------

# Set the default catalog and schema in spark sql
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {silver_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Step 3: Create Tables with Constraints**  
# MAGIC
# MAGIC **Task 1:**  
# MAGIC Create two tables with **Primary Key (PK)** and **Foreign Key (FK)** constraints:  
# MAGIC
# MAGIC 1. **Create `lab_customer`** with a primary key on `c_custkey`.  
# MAGIC 2. **Create `lab_orders`** with:  
# MAGIC    - A primary key on `o_orderkey`.  
# MAGIC    - A foreign key on `o_custkey` referencing `lab_customer(c_custkey)`.  
# MAGIC
# MAGIC **Note:**  
# MAGIC - Databricks does not enforce PK/FK constraints but uses them for relationships in **Entity Relationship Diagrams (ERDs)** in Catalog Explorer.  
# MAGIC - `NOT NULL` and `CHECK` constraints **are** enforced.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the lab_customer table with a PRIMARY KEY constraint on c_custkey
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

# MAGIC %md
# MAGIC Check the current catalog and schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Check Current Catalog
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Check Current Schema
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC **Task 2:**  
# MAGIC Create the `lab_orders` table with the following constraints:  
# MAGIC    - A **Primary Key (PK)** on `o_orderkey`.  
# MAGIC    - A **Foreign Key (FK)** on `o_custkey` referencing `lab_customer(c_custkey)`.
# MAGIC
# MAGIC **Note:**  
# MAGIC    - Use the actual default catalog name for `REFERENCES` keyword.
# MAGIC
# MAGIC **Example Target Code Line:**  
# MAGIC ```dbsql
# MAGIC CONSTRAINT fk_custkey FOREIGN KEY (o_custkey) REFERENCES <default_catalog_name>.silver.lab_customer
# MAGIC ```
# MAGIC
# MAGIC In this example, replace the `<default_catalog_name>` value with the actual value from previous query output.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the lab_orders table with PRIMARY KEY on o_orderkey
# MAGIC ---- and a FOREIGN KEY referencing lab_customer(c_custkey)
# MAGIC ---- Note: Provide REFERENCES with three level namespace
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
# MAGIC   CONSTRAINT fk_custkey FOREIGN KEY (o_custkey) REFERENCES <default_catalog_name>.silver.lab_customer(c_custkey)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: Insert data from the TPC-H bronze tables
# MAGIC
# MAGIC **Task:**  
# MAGIC Populate the newly created `lab_customer` and `lab_orders` tables with data from the TPC-H tables located in the `bronze` schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Insert data into lab_customer from bronze.customer
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

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Insert data into lab_orders from bronze.orders
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
# MAGIC **Task:** Validate Constraint Behavior  
# MAGIC
# MAGIC Since Databricks does not enforce primary and foreign key constraints, perform the following steps to observe their behavior:  
# MAGIC
# MAGIC 1. **Foreign Key Test:** Insert a row into `lab_orders` with an `o_custkey` that does not exist in `lab_customer`.  
# MAGIC 2. **Primary Key Test:** Insert a duplicate row into `lab_customer` using an already existing `c_custkey`.  
# MAGIC
# MAGIC Analyze the results to confirm that these operations succeed without constraint enforcement.

# COMMAND ----------

# MAGIC %md
# MAGIC **Task 1:** Test Foreign Key Constraint  
# MAGIC
# MAGIC Insert a row into `lab_orders` with an `o_custkey` that does not exist in `lab_customer`. Since Databricks does not enforce foreign key constraints, verify whether the insertion succeeds without an error.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Test Foreign Key Constraint
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
# MAGIC **Task 2:** Test Primary Key Constraint  
# MAGIC
# MAGIC Insert a duplicate row into `lab_customer` using a `c_custkey` value that already exists in the table. Since Databricks does not enforce primary key constraints, check if the insertion is allowed without any errors.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Test Primary Key Constraint
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
# MAGIC **Task:** Revert to a Clean State  
# MAGIC
# MAGIC Remove the violating rows from `lab_customer` and `lab_orders` to restore a clean state. Choose one of the following approaches:  
# MAGIC
# MAGIC - **Delete specific rows**: Manually remove the recently added violating rows by specifying their keys.  
# MAGIC - **Truncate tables**: Clear all data while keeping the table structure intact.  
# MAGIC - **Use Delta Time Travel**: Roll back the table to a previous version before the constraint violations occurred.  
# MAGIC
# MAGIC Ensure you adjust the keys accordingly if different values were inserted.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Remove the foreign key violation (orderkey=9999999)
# MAGIC DELETE FROM lab_orders
# MAGIC WHERE o_orderkey = 9999999;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Remove the duplicate primary key row
# MAGIC DELETE FROM lab_customer
# MAGIC WHERE c_custkey = 1 
# MAGIC   AND c_name = 'Duplicate Customer';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Visualizing the ER Diagram in Databricks
# MAGIC
# MAGIC **Task:** Visualize the ER Diagram in Databricks  
# MAGIC
# MAGIC Use Databricks to explore the relationships between your tables:  
# MAGIC
# MAGIC 1. Open **Databricks** and navigate to the **Catalog Explorer** from the left panel.  
# MAGIC 2. Select your assigned **catalog** and then open the **schema** (e.g., `silver`).  
# MAGIC 3. Find the `lab_orders` table, which contains a foreign key referencing `lab_customer`.  
# MAGIC 4. Click **View Relationships** to visualize the **Entity Relationship (ER) Diagram** showing the connection between `lab_orders` and `lab_customer`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Table Definitions
# MAGIC
# MAGIC **Task 1:** Define Silver Tables
# MAGIC
# MAGIC Create **refined tables** in the **silver schema** to standardize and clean the data.  
# MAGIC
# MAGIC 1. **Define the `refined_customer` table** based on the TPC-H `customer` table.  
# MAGIC 2. **Define the `refined_orders` table** based on the TPC-H `orders` table.  
# MAGIC 3. **Standardize column names** to maintain consistency.  
# MAGIC
# MAGIC These tables will be used in the next step for data loading.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Set the current schema to the extracted silver_schema name in DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the refined_customer table if it does not already exist
# MAGIC CREATE TABLE IF NOT EXISTS refined_customer (
# MAGIC   customer_id INT,            -- Unique identifier for the customer
# MAGIC   name STRING,                -- Name of the customer
# MAGIC   address STRING,             -- Address of the customer
# MAGIC   nation_key INT,             -- Foreign key linking to the nation table
# MAGIC   phone STRING,               -- Phone number of the customer
# MAGIC   acct_bal DECIMAL(12, 2),    -- Account balance of the customer
# MAGIC   market_segment STRING,      -- Market segment of the customer
# MAGIC   comment STRING              -- Additional comments about the customer
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the refined_orders table if it does not already exist
# MAGIC CREATE TABLE IF NOT EXISTS refined_orders (
# MAGIC   order_id INT,                -- Unique identifier for the order
# MAGIC   customer_id INT,             -- Foreign key linking to the customer table
# MAGIC   order_status STRING,         -- Status of the order (e.g., pending, shipped)
# MAGIC   total_price DECIMAL(12, 2),  -- Total price of the order
# MAGIC   order_date DATE,             -- Date when the order was placed
# MAGIC   order_priority STRING,       -- Priority level of the order
# MAGIC   clerk STRING,                -- Clerk who handled the order
# MAGIC   ship_priority INT,           -- Shipping priority of the order
# MAGIC   comment STRING               -- Additional comments about the order
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Task 2: Create Gold Tables (Star Schema)**  
# MAGIC
# MAGIC Define the **gold tables** using a **star schema** for analytics and reporting.  
# MAGIC
# MAGIC 1. **Create `DimCustomer`** with Slowly Changing Dimension (SCD) Type 2 attributes, including:  
# MAGIC    - `start_date`, `end_date`, and `is_current` for historical tracking.  
# MAGIC    - `GENERATED ALWAYS AS IDENTITY` for surrogate keys.  
# MAGIC
# MAGIC 2. **Create `DimDate`** to store date-related attributes for analysis.  
# MAGIC
# MAGIC 3. **Create `FactOrders`** as the central fact table, linking to dimension tables.  
# MAGIC
# MAGIC These tables will be used to optimize query performance and support historical tracking.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Set the current schema to the extracted gold_schema name in DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:gold_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the DimCustomer table to store customer details with Slowly Changing Dimension (SCD) Type 2 attributes for historical tracking
# MAGIC CREATE TABLE IF NOT EXISTS DimCustomer
# MAGIC (
# MAGIC   dim_customer_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key for the dimension table
# MAGIC   customer_id INT,                                       -- Unique identifier for the customer
# MAGIC   name STRING,                                           -- Name of the customer
# MAGIC   address STRING,                                        -- Address of the customer
# MAGIC   nation_key INT,                                        -- Foreign key linking to the nation table
# MAGIC   phone STRING,                                          -- Phone number of the customer
# MAGIC   acct_bal DECIMAL(12,2),                                -- Account balance of the customer
# MAGIC   market_segment STRING,                                 -- Market segment of the customer
# MAGIC   comment STRING,                                        -- Additional comments about the customer
# MAGIC   start_date DATE,                                       -- SCD2 start date indicating the beginning of the record's validity
# MAGIC   end_date DATE,                                         -- SCD2 end date indicating the end of the record's validity
# MAGIC   is_current BOOLEAN,                                    -- Flag to indicate if the record is the current version
# MAGIC   CONSTRAINT pk_dim_customer PRIMARY KEY (dim_customer_key)  -- Primary key constraint on the surrogate key
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the Simple DimDate table to store date-related information
# MAGIC CREATE TABLE IF NOT EXISTS DimDate (
# MAGIC   dim_date_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key for the DimDate table
# MAGIC   full_date DATE,                                    -- Full date value
# MAGIC   day INT,                                           -- Day of the month
# MAGIC   month INT,                                         -- Month of the year
# MAGIC   year INT,                                          -- Year value
# MAGIC   CONSTRAINT pk_dim_date PRIMARY KEY (dim_date_key) RELY  -- Primary key constraint on dim_date_key
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Check Current Catalog
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Check Current Schema
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC **Task 3:**  
# MAGIC Create the `FactOrders` table referencing `DimCustomer` and `DimDate` tables.
# MAGIC
# MAGIC **Note:**  
# MAGIC    - Use the actual default catalog name for `REFERENCES` keyword.
# MAGIC
# MAGIC **Example Target Code Line:**  
# MAGIC ```dbsql
# MAGIC CONSTRAINT fk_customer FOREIGN KEY (dim_customer_key) REFERENCES <default_catalog_name>.gold.DimCustomer(dim_customer_key),  -- Foreign key constraint linking to DimCustomer
# MAGIC
# MAGIC CONSTRAINT fk_date FOREIGN KEY (dim_date_key) REFERENCES <default_catalog_name>.gold.DimDate(dim_date_key)  -- Foreign key constraint linking to DimDate
# MAGIC ```
# MAGIC
# MAGIC In this example, replace the `<default_catalog_name>` value with the actual value from previous query output.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the FactOrders table referencing DimCustomer and DimDate tables
# MAGIC CREATE TABLE IF NOT EXISTS FactOrders (
# MAGIC   fact_orders_key BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key for the FactOrders table
# MAGIC   order_id INT,                                        -- Unique identifier for the order
# MAGIC   dim_customer_key BIGINT,                             -- Foreign key linking to the DimCustomer table
# MAGIC   dim_date_key BIGINT,                                 -- Foreign key linking to the DimDate table
# MAGIC   total_price DECIMAL(12, 2),                          -- Total price of the order
# MAGIC   order_status STRING,                                 -- Status of the order (e.g., pending, shipped)
# MAGIC   order_priority STRING,                               -- Priority level of the order
# MAGIC   clerk STRING,                                        -- Clerk who handled the order
# MAGIC   ship_priority INT,                                   -- Shipping priority of the order
# MAGIC   comment STRING,                                      -- Additional comments about the order
# MAGIC   CONSTRAINT pk_fact_orders PRIMARY KEY (fact_orders_key),  -- Primary key constraint on fact_orders_key
# MAGIC   CONSTRAINT fk_customer FOREIGN KEY (dim_customer_key) REFERENCES <default_catalog_name>.gold.DimCustomer(dim_customer_key),  -- Foreign key constraint linking to DimCustomer
# MAGIC   CONSTRAINT fk_date FOREIGN KEY (dim_date_key) REFERENCES <default_catalog_name>.gold.DimDate(dim_date_key)  -- Foreign key constraint linking to DimDate
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notes on `GENERATED ALWAYS AS IDENTITY`
# MAGIC - Each table automatically generates unique numbers for the surrogate key column.  
# MAGIC - You do not insert a value for those columns; Delta handles it seamlessly.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9: Load Data into Silver
# MAGIC
# MAGIC **Task:** Load data from the TPC-H `bronze` tables (`bronze.customer` and `bronze.orders`) into the newly created `refined_customer` and `refined_orders` tables in the **silver** schema.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Use the `SELECT` statement to extract data from `bronze.customer` and `bronze.orders`.
# MAGIC 2. Insert the data into the respective **silver** tables: `refined_customer` and `refined_orders`.
# MAGIC
# MAGIC Ensure data is cleansed and transformed as needed before loading into the silver tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Switch to the catalog using the extracted catalog name
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Switch to the silver schema
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Insert transformed data from the bronze.customer table into the refined_customer table
# MAGIC INSERT INTO
# MAGIC   refined_customer
# MAGIC SELECT
# MAGIC   c_custkey AS customer_id,                      -- Unique identifier for the customer
# MAGIC   TRIM(c_name) AS name,                          -- Name of the customer
# MAGIC   TRIM(c_address) AS address,                    -- Address of the customer
# MAGIC   c_nationkey AS nation_key,                     -- Foreign key linking to the nation table
# MAGIC   TRIM(c_phone) AS phone,                        -- Phone number of the customer
# MAGIC   CAST(c_acctbal AS DECIMAL(12, 2)) AS acct_bal, -- Account balance of the customer
# MAGIC   TRIM(c_mktsegment) AS market_segment,          -- Market segment of the customer
# MAGIC   TRIM(c_comment) AS comment                     -- Additional comments about the customer
# MAGIC FROM
# MAGIC   bronze.customer;                               -- Source table in the bronze layer

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Insert transformed data from the bronze.orders table into the refined_orders table
# MAGIC INSERT INTO
# MAGIC   refined_orders
# MAGIC SELECT
# MAGIC   o_orderkey AS order_id,                      -- Unique identifier for the order
# MAGIC   o_custkey AS customer_id,                    -- Foreign key linking to the customer table
# MAGIC   TRIM(o_orderstatus) AS order_status,         -- Status of the order (e.g., pending, shipped)
# MAGIC   CAST(o_totalprice AS DECIMAL(12, 2)) AS total_price,  -- Total price of the order
# MAGIC   o_orderdate AS order_date,                   -- Date when the order was placed
# MAGIC   TRIM(o_orderpriority) AS order_priority,     -- Priority level of the order
# MAGIC   TRIM(o_clerk) AS clerk,                      -- Clerk who handled the order
# MAGIC   o_shippriority AS ship_priority,             -- Shipping priority of the order
# MAGIC   TRIM(o_comment) AS comment                   -- Additional comments about the order
# MAGIC FROM
# MAGIC   bronze.orders;                               -- Source table in the bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10: Validate the Table Records
# MAGIC **Task:** Validate if records were successfully loaded into the `refined_customer` and `refined_orders` tables

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the count of records in the refined_customer table
# MAGIC SELECT COUNT(*) AS refined_customer_count FROM refined_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the count of records in the refined_orders table
# MAGIC SELECT COUNT(*) AS refined_orders_count FROM refined_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the count of records in the refined_customer table
# MAGIC SELECT COUNT(*) AS refined_customer_count FROM refined_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the count of records in the refined_orders table
# MAGIC SELECT COUNT(*) AS refined_orders_count FROM refined_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 11: Initial Load into Gold (Dimensional Model)
# MAGIC
# MAGIC **Task:** Perform the initial load of `DimCustomer`, `DimDate`, and `FactOrders` into the **gold** schema.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Load all customer data into the `DimCustomer` table as *current* entries.
# MAGIC    - Use a `SELECT` statement to extract customer data from the refined source and insert into `DimCustomer` as current customers.
# MAGIC    
# MAGIC 2. Create and insert date entries into the `DimDate` table from the `refined_orders` data.
# MAGIC    - If the `DimDate` table is pre-loaded with daily dates for multiple years, ensure all relevant dates in `refined_orders` are populated in `DimDate`.
# MAGIC
# MAGIC 3. Populate the `FactOrders` table by linking each order to the correct dimension keys (e.g., customer, date).
# MAGIC    - Use the appropriate `JOIN` to link the `refined_orders` table with `DimCustomer` and `DimDate` and insert the data into `FactOrders`.
# MAGIC
# MAGIC Ensure all dimension keys are correctly linked and any required transformations are applied before loading into the gold tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Switch to the gold schema using the USE SCHEMA SQL command
# MAGIC USE SCHEMA IDENTIFIER(:gold_schema);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 DimCustomer (SCD Type 2) Initial Load
# MAGIC
# MAGIC **Task:** Perform the initial load of `DimCustomer` with SCD Type 2 logic.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Update every row in the `DimCustomer` table with `start_date = CURRENT_DATE()`, `end_date = NULL`, and `is_current = TRUE` for all customers.
# MAGIC    - Use an `UPDATE` statement to set these values for every customer row in `DimCustomer`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Insert data into the DimCustomer dimension table
# MAGIC INSERT INTO DimCustomer
# MAGIC (
# MAGIC   customer_id,    -- Unique identifier for the customer
# MAGIC   name,           -- Name of the customer
# MAGIC   address,        -- Address of the customer
# MAGIC   nation_key,     -- Key representing the nation of the customer
# MAGIC   phone,          -- Phone number of the customer
# MAGIC   acct_bal,       -- Account balance of the customer
# MAGIC   market_segment, -- Market segment of the customer
# MAGIC   comment,        -- Additional comments about the customer
# MAGIC   start_date,     -- Start date of the record
# MAGIC   end_date,       -- End date of the record (NULL for current records)
# MAGIC   is_current      -- Flag indicating if the record is current
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,    -- Select customer_id from the source table
# MAGIC   name,           -- Select name from the source table
# MAGIC   address,        -- Select address from the source table
# MAGIC   nation_key,     -- Select nation_key from the source table
# MAGIC   phone,          -- Select phone from the source table
# MAGIC   acct_bal,       -- Select acct_bal from the source table
# MAGIC   market_segment, -- Select market_segment from the source table
# MAGIC   `comment`,      -- Select comment from the source table
# MAGIC   CURRENT_DATE(), -- Set start_date to the current date
# MAGIC   NULL,           -- Set end_date to NULL for current records
# MAGIC   TRUE            -- Set is_current to TRUE for current records
# MAGIC FROM IDENTIFIER(:silver_schema || '.' || 'refined_customer')   -- Source table in the silver schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 DimDate
# MAGIC
# MAGIC **Task:** Populate the `DimDate` table with unique `order_date` values from `refined_orders`.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Extract unique `order_date` values from `refined_orders`.
# MAGIC    - Use a `SELECT DISTINCT order_date` query to retrieve the unique dates.
# MAGIC
# MAGIC 2. Use built-in functions to split the `order_date` values into `day`, `month`, and `year` components.
# MAGIC    - Apply date functions to extract `day`, `month`, and `year` from `order_date`.
# MAGIC
# MAGIC 3. Insert the split date values into the `DimDate` table.
# MAGIC    - Use an `INSERT INTO DimDate` query with the transformed date components.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Insert distinct dates into the DimDate dimension table
# MAGIC INSERT INTO DimDate
# MAGIC (
# MAGIC   full_date,  -- Full date value
# MAGIC   day,        -- Day part of the date
# MAGIC   month,      -- Month part of the date
# MAGIC   year        -- Year part of the date
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC   order_date,          -- Full date value from refined_orders
# MAGIC   DAY(order_date),     -- Extracted day part of the date
# MAGIC   MONTH(order_date),   -- Extracted month part of the date
# MAGIC   YEAR(order_date)     -- Extracted year part of the date
# MAGIC FROM IDENTIFIER(:silver_schema || '.' || 'refined_orders')
# MAGIC WHERE order_date IS NOT NULL  -- Ensure the date is not null

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.3 FactOrders
# MAGIC
# MAGIC **Task:** Populate the `FactOrders` table by linking each order to `DimCustomer` and `DimDate`.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Join `refined_orders` with `DimCustomer` using `(customer_id = dc.customer_id AND is_current = TRUE)` to ensure only active customers are linked.
# MAGIC    - Use a `JOIN` between `refined_orders` and `DimCustomer` on `customer_id` and filter with `is_current = TRUE`.
# MAGIC
# MAGIC 2. Link each order to `DimDate` based on `order_date`.
# MAGIC    - Use a `JOIN` between `refined_orders` and `DimDate` on `order_date`.
# MAGIC
# MAGIC 3. Insert the joined data into the `FactOrders` table.
# MAGIC    - Use an `INSERT INTO FactOrders` statement to load the data.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Insert data into the FactOrders table
# MAGIC INSERT INTO FactOrders
# MAGIC (
# MAGIC   order_id,          -- Unique identifier for the order
# MAGIC   dim_customer_key,  -- Foreign key referencing the customer dimension
# MAGIC   dim_date_key,      -- Foreign key referencing the date dimension
# MAGIC   total_price,       -- Total price of the order
# MAGIC   order_status,      -- Status of the order
# MAGIC   order_priority,    -- Priority of the order
# MAGIC   clerk,             -- Clerk handling the order
# MAGIC   ship_priority,     -- Shipping priority of the order
# MAGIC   comment            -- Additional comments about the order
# MAGIC )
# MAGIC SELECT
# MAGIC   ro.order_id,       -- Select order_id from refined_orders
# MAGIC   dc.dim_customer_key, -- Select dim_customer_key from DimCustomer
# MAGIC   dd.dim_date_key,   -- Select dim_date_key from DimDate
# MAGIC   ro.total_price,    -- Select total_price from refined_orders
# MAGIC   ro.order_status,   -- Select order_status from refined_orders
# MAGIC   ro.order_priority, -- Select order_priority from refined_orders
# MAGIC   ro.clerk,          -- Select clerk from refined_orders
# MAGIC   ro.ship_priority,  -- Select ship_priority from refined_orders
# MAGIC   ro.comment         -- Select comment from refined_orders
# MAGIC FROM IDENTIFIER(:silver_schema || '.' || 'refined_orders') ro
# MAGIC JOIN DimCustomer dc
# MAGIC   ON ro.customer_id = dc.customer_id
# MAGIC   AND dc.is_current = TRUE -- Join on customer_id and ensure the customer record is current
# MAGIC JOIN DimDate dd
# MAGIC   ON ro.order_date = dd.full_date -- Join on order_date and full_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation
# MAGIC
# MAGIC **Task:** Validate the record counts in each **gold** table.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Check the record count in the `DimCustomer` table.
# MAGIC    - Use `SELECT COUNT(*) FROM DimCustomer` to check the number of records.
# MAGIC
# MAGIC 2. Check the record count in the `DimDate` table.
# MAGIC    - Use `SELECT COUNT(*) FROM DimDate` to check the number of records.
# MAGIC
# MAGIC 3. Check the record count in the `FactOrders` table.
# MAGIC    - Use `SELECT COUNT(*) FROM FactOrders` to check the number of records.
# MAGIC
# MAGIC Ensure the expected number of records are present in each table after the initial load.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the record count for the DimCustomer table
# MAGIC SELECT 'DimCustomer' AS table_name, COUNT(*) AS record_count FROM DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the record count for the DimDate table
# MAGIC SELECT 'DimDate' AS table_name, COUNT(*) AS record_count FROM DimDate

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the record count for the FactOrders table
# MAGIC SELECT 'FactOrders' AS table_name, COUNT(*) AS record_count FROM FactOrders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12: Incremental Updates (SCD Type 2 MERGE)
# MAGIC
# MAGIC **Task:** Perform incremental updates on `DimCustomer` for changes in customer data.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Detect changes in `refined_customer` (e.g., changed address, new customer).
# MAGIC 2. Use a **MERGE** statement to close the old record and insert a new record in a single operation.
# MAGIC    - Update the existing record in `DimCustomer` by setting `end_date = CURRENT_DATE()` and `is_current = FALSE`.
# MAGIC    - Insert a new record with updated attributes: `start_date = CURRENT_DATE()`, `end_date = NULL`, and `is_current = TRUE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.1 Example: Create Dummy Incremental Changes
# MAGIC
# MAGIC **Task:** Simulate incremental changes for customer data.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Create a dummy change for an existing customer (e.g., `customer_id = 101`) changing their address.
# MAGIC 2. Create a dummy entry for a brand-new customer (e.g., `customer_id = 99999`).

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a temporary view simulating incremental updates, including an existing customer with updated information and a new customer with initial details
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW incremental_customer_updates AS
# MAGIC ---- Existing customer with updated information
# MAGIC SELECT 101    AS customer_id,
# MAGIC        'CHANGED Name'   AS name,
# MAGIC        'Updated Address 500' AS address,
# MAGIC        77     AS nation_key,
# MAGIC        '555-NEW-8888'   AS phone,
# MAGIC        CAST(999.99 AS DECIMAL(12,2)) AS acct_bal,
# MAGIC        'NEW_SEGMENT'    AS market_segment,
# MAGIC        'Existing row changed' AS comment
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC ---- New customer with initial information
# MAGIC SELECT 99999,
# MAGIC        'Completely New',
# MAGIC        '123 New Street',
# MAGIC        99,
# MAGIC        '999-999-1234',
# MAGIC        CAST(500.00 AS DECIMAL(12,2)),
# MAGIC        'MARKET_NEW',
# MAGIC        'Newly added customer';

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the contents of the temporary view to verify the data
# MAGIC SELECT * FROM incremental_customer_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.2 Single MERGE for SCD Type 2
# MAGIC
# MAGIC **Task:** Execute a **MERGE** statement to handle both updates and new customer insertions.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Create a `MERGE` statement that:
# MAGIC    - Identifies the old record (if the customer already exists) and marks it as closed (`is_current = FALSE`, `end_date = CURRENT_DATE()`).
# MAGIC    - Inserts a new row for any changed customer (with updated attributes, `is_current = TRUE`, `start_date = CURRENT_DATE()`, and `end_date = NULL`).
# MAGIC    - For new customers, only insert the new row with `is_current = TRUE`, `start_date = CURRENT_DATE()`, and `end_date = NULL`.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC
# MAGIC **Task:** Understand how the **MERGE** statement works.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. If the old record is found, update it to close out the current record by setting `is_current = FALSE` and `end_date = CURRENT_DATE()`.
# MAGIC 2. For any new or changed customer, insert a new record with `is_current = TRUE`, a fresh `start_date`, and no `end_date`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Perform an incremental update using a MERGE statement to update existing records and insert new records in the DimCustomer table based on changes in customer data
# MAGIC WITH staged_changes AS (
# MAGIC   ---- "OLD" row: used to find and update the existing active dimension record
# MAGIC   SELECT
# MAGIC     i.customer_id,
# MAGIC     i.name,
# MAGIC     i.address,
# MAGIC     i.nation_key,
# MAGIC     i.phone,
# MAGIC     i.acct_bal,
# MAGIC     i.market_segment,
# MAGIC     i.comment,
# MAGIC     'OLD' AS row_type
# MAGIC   FROM incremental_customer_updates i
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   ---- "NEW" row: used to insert a brand-new dimension record
# MAGIC   SELECT
# MAGIC     i.customer_id,
# MAGIC     i.name,
# MAGIC     i.address,
# MAGIC     i.nation_key,
# MAGIC     i.phone,
# MAGIC     i.acct_bal,
# MAGIC     i.market_segment,
# MAGIC     i.comment,
# MAGIC     'NEW' AS row_type
# MAGIC   FROM incremental_customer_updates i
# MAGIC )
# MAGIC
# MAGIC ---- Perform the merge operation on the DimCustomer table
# MAGIC MERGE INTO DimCustomer t
# MAGIC USING staged_changes s
# MAGIC   ON t.customer_id = s.customer_id
# MAGIC      AND t.is_current = TRUE
# MAGIC      AND s.row_type = 'OLD'
# MAGIC
# MAGIC ---- When a match is found, update the existing record to close it out
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     t.is_current = FALSE,
# MAGIC     t.end_date   = CURRENT_DATE()
# MAGIC
# MAGIC ---- When no match is found, insert the new record as the current version
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     customer_id,
# MAGIC     name,
# MAGIC     address,
# MAGIC     nation_key,
# MAGIC     phone,
# MAGIC     acct_bal,
# MAGIC     market_segment,
# MAGIC     comment,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     is_current
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.customer_id,
# MAGIC     s.name,
# MAGIC     s.address,
# MAGIC     s.nation_key,
# MAGIC     s.phone,
# MAGIC     s.acct_bal,
# MAGIC     s.market_segment,
# MAGIC     s.comment,
# MAGIC     CURRENT_DATE(),
# MAGIC     NULL,
# MAGIC     TRUE
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.3 Validate the Updated Rows
# MAGIC
# MAGIC **Task:** Validate the updates to customer data.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Query and display the details of the existing customer with `customer_id = 101`.
# MAGIC    - Use `SELECT * FROM DimCustomer WHERE customer_id = 101` to view the old and new versions.
# MAGIC 2. Query and display the details of the new customer with `customer_id = 99999`.
# MAGIC    - Use `SELECT * FROM DimCustomer WHERE customer_id = 99999` to view the newly inserted record.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query to display details of an existing customer with customer_id 101
# MAGIC SELECT 
# MAGIC   dim_customer_key,  -- Unique key for the customer in the dimension table
# MAGIC   customer_id,       -- Customer ID
# MAGIC   name,              -- Customer name
# MAGIC   address,           -- Customer address
# MAGIC   is_current,        -- Flag indicating if the record is current
# MAGIC   start_date,        -- Start date of the record
# MAGIC   end_date           -- End date of the record
# MAGIC FROM 
# MAGIC   DimCustomer        -- Dimension table containing customer data
# MAGIC WHERE 
# MAGIC   customer_id = 101  -- Filter condition to select the customer with ID 101
# MAGIC ORDER BY 
# MAGIC   dim_customer_key   -- Order the results by the unique customer key

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query to display details of a new customer with customer_id 99999
# MAGIC SELECT 
# MAGIC   dim_customer_key,  -- Unique key for the customer in the dimension table
# MAGIC   customer_id,       -- Customer ID
# MAGIC   name,              -- Customer name
# MAGIC   address,           -- Customer address
# MAGIC   is_current,        -- Flag indicating if the record is current
# MAGIC   start_date,        -- Start date of the record
# MAGIC   end_date           -- End date of the record
# MAGIC FROM 
# MAGIC   DimCustomer        -- Dimension table containing customer data
# MAGIC WHERE 
# MAGIC   customer_id = 99999 -- Filter condition to select the customer with ID 99999
# MAGIC ORDER BY 
# MAGIC   dim_customer_key   -- Order the results by the unique customer key

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected Result:**  
# MAGIC - For `customer_id = 101`, the old record will have `is_current = FALSE`, and a new version will be inserted with `is_current = TRUE`.
# MAGIC - For `customer_id = 99999`, only one record will exist, with `is_current = TRUE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13: Sample Queries on the Star Schema
# MAGIC
# MAGIC **Task:** Run a few sample queries to analyze the data in the `FactOrders`, `DimCustomer`, and `DimDate` tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.1 Row Counts
# MAGIC
# MAGIC **Task:** Display the count of records in the `DimCustomer`, `DimDate`, and `FactOrders` tables.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Query to display the count of records in the `DimCustomer` table.
# MAGIC    - Use `SELECT COUNT(*) FROM DimCustomer`.
# MAGIC
# MAGIC 2. Query to display the count of records in the `DimDate` table.
# MAGIC    - Use `SELECT COUNT(*) FROM DimDate`.
# MAGIC
# MAGIC 3. Query to display the count of records in the `FactOrders` table.
# MAGIC    - Use `SELECT COUNT(*) FROM FactOrders`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the count of records in the DimCustomer table with the table name
# MAGIC SELECT 'DimCustomer' AS table_name, COUNT(*) AS record_count FROM DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the count of records in the DimDate table with the table name
# MAGIC SELECT 'DimDate' AS table_name, COUNT(*) AS record_count FROM DimDate

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the count of records in the FactOrders table with the table name
# MAGIC SELECT 'FactOrders' AS table_name, COUNT(*) AS record_count FROM FactOrders

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.2 Example Query: Top Market Segments
# MAGIC
# MAGIC **Task:** Display the total amount spent by customers in each market segment, limited to the top 10 segments.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Query to calculate the total amount spent by customers in each market segment.
# MAGIC    - Use `SELECT market_segment, SUM(order_amount) AS total_spent FROM FactOrders f JOIN DimCustomer dc ON f.customer_id = dc.customer_id GROUP BY market_segment ORDER BY total_spent DESC LIMIT 10`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Display the total amount spent by customers in each market segment, limited to the top 10 segments
# MAGIC SELECT 
# MAGIC   dc.market_segment,                -- Select the market segment from the DimCustomer dimension table
# MAGIC   SUM(f.total_price) AS total_spent -- Calculate the total amount spent by summing the total_price from the FactOrders fact table
# MAGIC FROM 
# MAGIC   FactOrders f                      -- Fact table containing order data
# MAGIC JOIN 
# MAGIC   DimCustomer dc                    -- Dimension table containing customer data
# MAGIC   ON f.dim_customer_key = dc.dim_customer_key -- Join condition on customer key
# MAGIC GROUP BY 
# MAGIC   dc.market_segment                 -- Group the results by market segment
# MAGIC ORDER BY 
# MAGIC   total_spent DESC                  -- Order the results by total amount spent in descending order
# MAGIC LIMIT 10                            -- Limit the results to the top 10 market segments

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13.3 Example Query: Order Counts by Year
# MAGIC
# MAGIC **Task:** Count the number of orders for each year by joining the `FactOrders` table with the `DimDate` table.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Query to count the number of orders for each year.
# MAGIC    - Use `SELECT dd.year, COUNT(*) AS orders_count FROM FactOrders f JOIN DimDate dd ON f.dim_date_key = dd.dim_date_key GROUP BY dd.year ORDER BY dd.year`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Count the number of orders for each year by joining the FactOrders table with the DimDate table
# MAGIC SELECT 
# MAGIC   dd.year,            -- Select the year from the DimDate dimension table
# MAGIC   COUNT(*) AS orders_count -- Count the number of orders for each year
# MAGIC FROM 
# MAGIC   FactOrders f        -- Fact table containing order data
# MAGIC JOIN 
# MAGIC   DimDate dd          -- Dimension table containing date data
# MAGIC   ON f.dim_date_key = dd.dim_date_key -- Join condition on date key
# MAGIC GROUP BY 
# MAGIC   dd.year             -- Group the results by year
# MAGIC ORDER BY 
# MAGIC   dd.year             -- Order the results by year

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14: Creating Data Vault 2.0 Tables
# MAGIC
# MAGIC **Task:** Create core Data Vault 2.0 components—Hubs, Links, and Satellites to model business entities and their relationships.

# COMMAND ----------

# MAGIC %md
# MAGIC Start by setting the silver schema as the default schema in DBSQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Switch to the silver schema using the USE SCHEMA SQL command
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.1 Create Hub Tables
# MAGIC
# MAGIC **Task:** Create hub tables to store unique business keys and metadata.
# MAGIC
# MAGIC Steps to perform:
# MAGIC
# MAGIC 1. Create the `HubCustomer` table.
# MAGIC    - This table stores unique customer business keys.
# MAGIC
# MAGIC 2. Create the `HubOrder` table.
# MAGIC    - This table stores unique order business keys.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the Hub table for Customer
# MAGIC CREATE TABLE IF NOT EXISTS H_Customer
# MAGIC (
# MAGIC   customer_hk STRING NOT NULL COMMENT 'MD5(customer_id)',
# MAGIC   customer_id INT NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_h_customer PRIMARY KEY (customer_hk)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the Hub table for Orders
# MAGIC CREATE TABLE IF NOT EXISTS H_Order
# MAGIC (
# MAGIC   order_hk STRING NOT NULL COMMENT 'MD5(order_id)',
# MAGIC   order_id INT NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_h_order PRIMARY KEY (order_hk)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.2 Create Link Tables
# MAGIC
# MAGIC **Task:** Create link tables to represent relationships between business entities.
# MAGIC
# MAGIC Steps to perform:
# MAGIC
# MAGIC 1. Create the `L_Customer_Order` link table.
# MAGIC    - This table maps customers to their orders using hashed composite keys.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Creates a link table to map customers to their orders with a hashed primary key
# MAGIC CREATE TABLE IF NOT EXISTS L_Customer_Order
# MAGIC (
# MAGIC   customer_order_hk STRING NOT NULL COMMENT 'MD5(customer_hk||order_hk)',
# MAGIC   customer_hk STRING NOT NULL,
# MAGIC   order_hk STRING NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_l_customer_order PRIMARY KEY (customer_order_hk)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.3 Create Satellite Tables
# MAGIC
# MAGIC **Task 1:** Create satellite tables to store descriptive attributes and track historical changes.
# MAGIC
# MAGIC Steps to perform:
# MAGIC
# MAGIC 1. Create the `Sat_Customer_Info` table.
# MAGIC    - This table holds descriptive information about customers.
# MAGIC
# MAGIC 2. Create the `Sat_Order_Info` table.
# MAGIC    - This table holds descriptive information about orders.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the satellite for storing the Customer Descriptive Info
# MAGIC CREATE TABLE IF NOT EXISTS S_Customer
# MAGIC (
# MAGIC   customer_hk STRING NOT NULL,
# MAGIC   hash_diff STRING NOT NULL COMMENT 'MD5 of all descriptive columns',
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   nation_key INT,
# MAGIC   phone STRING,
# MAGIC   acct_bal DECIMAL(12,2),
# MAGIC   market_segment STRING,
# MAGIC   comment STRING,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_s_customer PRIMARY KEY (customer_hk, load_timestamp)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the satellite table for storing the Order Descriptive Info
# MAGIC CREATE TABLE IF NOT EXISTS S_Order
# MAGIC (
# MAGIC   order_hk STRING NOT NULL,
# MAGIC   hash_diff STRING NOT NULL COMMENT 'MD5 of all descriptive columns',
# MAGIC   order_status STRING,
# MAGIC   total_price DECIMAL(12,2),
# MAGIC   order_date DATE,
# MAGIC   order_priority STRING,
# MAGIC   clerk STRING,
# MAGIC   ship_priority INT,
# MAGIC   comment STRING,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_s_order PRIMARY KEY (order_hk, load_timestamp)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Task 2:** Set the default catalog and schema for Spark SQL operations.
# MAGIC
# MAGIC Steps to perform:
# MAGIC
# MAGIC 1. Set the default catalog to your main catalog.
# MAGIC 2. Set the default schema to silver schema.
# MAGIC

# COMMAND ----------

# Set the default catalog to your main catalog and schema to the silver schema in spark sql
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {silver_schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current catalog to the extracted catalog name
# MAGIC USE CATALOG IDENTIFIER(:catalog_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current schema to the defined schema name
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15: ETL Process
# MAGIC
# MAGIC **Task:** Load refined data into the Data Vault 2.0 tables following a structured ETL approach.
# MAGIC
# MAGIC Sub-steps include:
# MAGIC 1. Loading Hubs  
# MAGIC 2. Loading Links  
# MAGIC 3. Loading Satellites
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.1 Define Helper Functions
# MAGIC
# MAGIC **Task:** Define Python functions to generate hash keys and hash diff columns for customers, orders, and their relationships.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Define a function to generate customer hash keys.
# MAGIC 2. Define a function to generate order hash keys.
# MAGIC 3. Define a function to generate customer-order link hash keys.
# MAGIC 4. Define a function to generate a hash_diff column for change tracking.
# MAGIC

# COMMAND ----------

# Generate hash keys and hash diff columns for Data Vault entities, including customer, order, and their relationships, using MD5
from pyspark.sql.functions import md5, concat_ws, col

# Define a custom function to generate a hash key for customer_id
def generate_customer_hash_keys(df):
    return df.withColumn(
        "customer_hk", 
        md5(col("customer_id").cast("string"))
    )

# Define a custom function to generate a hash key for order_id
def generate_order_hash_keys(df):
    return df.withColumn(
        "order_hk", 
        md5(col("order_id").cast("string"))
    )

# Define a custom function to generate a composite hash key for customer and order
def generate_customer_order_hash_key(df):
    return df.withColumn(
        "customer_order_hk",
        md5(concat_ws("||", col("customer_hk"), col("order_hk")))
    )

# Define a custom function to generate a hash difference for change detection
def generate_hash_diff(df, columns):
    return df.withColumn("hash_diff", md5(concat_ws("||", *[col(c) for c in columns])))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.2 Load Refined Tables to Silver Layer
# MAGIC
# MAGIC **Task 1:** Create refined dimension and fact tables by renaming columns and casting data types.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Create the refined customer dimension table in the silver layer.
# MAGIC 2. Create the refined orders fact table in the silver layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Creates refined customer dimension table in the silver layer
# MAGIC CREATE TABLE IF NOT EXISTS silver.refined_customer (
# MAGIC   customer_id INT NOT NULL,
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   nation_key INT,
# MAGIC   phone STRING,
# MAGIC   acct_bal DECIMAL(12, 2),
# MAGIC   market_segment STRING,
# MAGIC   comment STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Creates refined orders fact table in the silver layer
# MAGIC CREATE TABLE IF NOT EXISTS silver.refined_orders (
# MAGIC   order_id INT NOT NULL,
# MAGIC   customer_id INT NOT NULL,
# MAGIC   order_status STRING,
# MAGIC   total_price DECIMAL(12, 2),
# MAGIC   order_date DATE,
# MAGIC   order_priority STRING,
# MAGIC   clerk STRING,
# MAGIC   ship_priority INT,
# MAGIC   comment STRING
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC **Task 2:** Define and execute ETL load functions.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Define ETL load functions for customers and orders.
# MAGIC 2. Run the ETL load functions to prepare data for Data Vault tables.

# COMMAND ----------

# Defining ETL Load Functions
from pyspark.sql.functions import col, to_date, current_timestamp

def etl_refined_customer():
    bronze_customer = spark.table("bronze.customer")
    refined_customer = bronze_customer.select(
        col("c_custkey").cast("int").alias("customer_id"),
        col("c_name").alias("name"),
        col("c_address").alias("address"),
        col("c_nationkey").cast("int").alias("nation_key"),
        col("c_phone").alias("phone"),
        col("c_acctbal").cast("decimal(12,2)").alias("acct_bal"),
        col("c_mktsegment").alias("market_segment"),
        col("c_comment").alias("comment")
    )
    refined_customer.write.mode("overwrite").saveAsTable("silver.refined_customer")

def etl_refined_orders():
    bronze_orders = spark.table("bronze.orders")
    refined_orders = bronze_orders.select(
        col("o_orderkey").cast("int").alias("order_id"),
        col("o_custkey").cast("int").alias("customer_id"),
        col("o_orderstatus").alias("order_status"),
        col("o_totalprice").cast("decimal(12,2)").alias("total_price"),
        to_date(col("o_orderdate"), "yyyy-MM-dd").alias("order_date"),
        col("o_orderpriority").alias("order_priority"),
        col("o_clerk").alias("clerk"),
        col("o_shippriority").cast("int").alias("ship_priority"),
        col("o_comment").alias("comment")
    )
    refined_orders.write.mode("overwrite").saveAsTable("silver.refined_orders")

# COMMAND ----------

# Run ETL Load
etl_refined_customer()
etl_refined_orders()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.3 Load Customer Hub
# MAGIC
# MAGIC **Task:** Load and merge customer data into the H_Customer hub table.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Apply hash key generation to the refined customer data.
# MAGIC 2. Merge new customer records with metadata into the hub table.
# MAGIC 3. Preview the first 10 records in the H_Customer hub.

# COMMAND ----------

# Load and merge new customer records into the H_Customer hub table with hash key and metadata
from pyspark.sql.functions import current_timestamp, lit
silver_customer_df = spark.sql("SELECT * FROM silver.refined_customer")

customer_hub_data = (
    generate_customer_hash_keys(silver_customer_df)
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("TPC-H"))
)

customer_hub_data.createOrReplaceTempView("customer_hub_stage")

spark.sql("""
MERGE INTO H_Customer AS target
USING customer_hub_stage AS source
ON target.customer_hk = source.customer_hk
WHEN NOT MATCHED THEN
  INSERT (customer_hk, customer_id, load_timestamp, record_source)
  VALUES (source.customer_hk, source.customer_id, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Preview first 10 records from the H_Customer hub table
# MAGIC SELECT * FROM H_Customer LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.4 Load Customer Satellite
# MAGIC
# MAGIC **Task:** Load and merge descriptive customer data into the S_Customer satellite table.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Generate hash_diff from customer descriptive attributes.
# MAGIC 2. Merge records into the satellite table with metadata.

# COMMAND ----------

# Merge new customer descriptive records into the S_Customer satellite table with hash diff and metadata
customer_sat_columns = ["name", "address", "nation_key", "phone", "acct_bal", "market_segment", "comment"]

customer_sat_data = generate_hash_diff(customer_hub_data, customer_sat_columns)
customer_sat_data.createOrReplaceTempView("customer_sat_stage")

spark.sql(f"""
MERGE INTO S_Customer AS target
USING customer_sat_stage AS source
ON target.customer_hk = source.customer_hk AND target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_hk, hash_diff, {', '.join(customer_sat_columns)}, load_timestamp, record_source)
  VALUES (source.customer_hk, source.hash_diff, {', '.join([f'source.{col}' for col in customer_sat_columns])}, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.5 Load Order Hub and Satellite
# MAGIC
# MAGIC **Task:** Load and merge order data into the H_Order hub and S_Order satellite tables.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Generate hash keys for orders and hash_diff for descriptive fields.
# MAGIC 2. Merge order records into the hub and satellite tables with metadata.

# COMMAND ----------

# Load and merge new order records into hub and satellite tables with hash keys, hash diff, and metadata
silver_orders_df = spark.sql("SELECT * FROM silver.refined_orders")

order_hub_data = (
    generate_order_hash_keys(silver_orders_df)
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("TPC-H"))
)

order_hub_data.createOrReplaceTempView("order_hub_stage")

spark.sql("""
MERGE INTO H_Order AS target
USING order_hub_stage AS source
ON target.order_hk = source.order_hk
WHEN NOT MATCHED THEN
  INSERT (order_hk, order_id, load_timestamp, record_source)
  VALUES (source.order_hk, source.order_id, source.load_timestamp, source.record_source)
""")

order_sat_columns = ["order_status", "total_price", "order_date", "order_priority", "clerk", "ship_priority", "comment"]

order_sat_data = generate_hash_diff(order_hub_data, order_sat_columns)
order_sat_data.createOrReplaceTempView("order_sat_stage")

spark.sql(f"""
MERGE INTO S_Order AS target
USING order_sat_stage AS source
ON target.order_hk = source.order_hk AND target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN
  INSERT (order_hk, hash_diff, {', '.join(order_sat_columns)}, load_timestamp, record_source)
  VALUES (source.order_hk, source.hash_diff, {', '.join([f'source.{col}' for col in order_sat_columns])}, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15.6 Load Customer-Order Link
# MAGIC
# MAGIC **Task:** Load and merge customer-order relationship data into the L_Customer_Order link table.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Generate composite hash key from customer and order hash keys.
# MAGIC 2. Merge link records into the link table with metadata.

# COMMAND ----------

# Create and merge customer-order link records with combined hash key and metadata into the link table
from pyspark.sql.functions import concat_ws

link_data = (
    # Join silver_orders_df to H_Customer and H_Order using the natural keys
    silver_orders_df.alias("orders")
    .join(spark.table("H_Customer").alias("hc"), on=[col("orders.customer_id") == col("hc.customer_id")], how="inner")
    .join(spark.table("H_Order").alias("ho"), on=[col("orders.order_id") == col("ho.order_id")], how="inner")
    # Select the already established hash keys for both hubs
    .select(
        col("hc.customer_hk").alias("customer_hk"),
        col("ho.order_hk").alias("order_hk")
    )
    # Create a combined hash key
    .withColumn("customer_order_hk", md5(concat_ws("||", col("customer_hk"), col("order_hk"))))
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("TPC-H"))
)

link_data.createOrReplaceTempView("link_stage")


spark.sql("""
MERGE INTO L_Customer_Order AS target
USING link_stage AS source
ON target.customer_order_hk = source.customer_order_hk
WHEN NOT MATCHED THEN
  INSERT (customer_order_hk, customer_hk, order_hk, load_timestamp, record_source)
  VALUES (source.customer_order_hk, source.customer_hk, source.order_hk, source.load_timestamp, source.record_source)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16: Creating Business Views
# MAGIC
# MAGIC **Task:** Create business views that join Hubs, Links, and Satellites to simplify querying for end users.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Join Customer Hub, Customer Satellite, Order Hub, and Order Satellite using the Link table.
# MAGIC 2. Create a business view combining customer and order details.
# MAGIC    - This view can later be materialized in the Gold layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create a Business View combining Customer and Order details (This can be materialized in gold layer, but for this lab it is presented as a view)
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold.BV_Customer_Order AS
# MAGIC SELECT 
# MAGIC     hc.customer_id,
# MAGIC     sc.name AS customer_name,
# MAGIC     sc.address AS customer_address,
# MAGIC     ho.order_id,
# MAGIC     so.order_date,
# MAGIC     so.total_price,
# MAGIC     so.order_status
# MAGIC FROM 
# MAGIC     H_Customer hc
# MAGIC JOIN 
# MAGIC     S_Customer sc ON hc.customer_hk = sc.customer_hk
# MAGIC JOIN 
# MAGIC     L_Customer_Order lco ON hc.customer_hk = lco.customer_hk
# MAGIC JOIN 
# MAGIC     H_Order ho ON lco.order_hk = ho.order_hk
# MAGIC JOIN 
# MAGIC     S_Order so ON ho.order_hk = so.order_hk;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 17: Sample Query
# MAGIC
# MAGIC **Task:** Run a sample query to demonstrate how to use the business view created from the Data Vault model.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Query the full contents of the business view created in the previous step.
# MAGIC 2. Calculate total sales by customer and display results in descending order.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Query the full contents of the business view created in the previous step
# MAGIC select * from gold.BV_Customer_Order

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Calculate total sales by customer and display results in descending order
# MAGIC SELECT 
# MAGIC     customer_name,
# MAGIC     SUM(total_price) AS total_sales
# MAGIC FROM 
# MAGIC     gold.BV_Customer_Order
# MAGIC GROUP BY 
# MAGIC     customer_name
# MAGIC ORDER BY 
# MAGIC     total_sales DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 18: Verification Steps
# MAGIC
# MAGIC **Task:** Perform basic verification to ensure the integrity and correctness of the Data Vault model.
# MAGIC
# MAGIC Steps to perform:
# MAGIC
# MAGIC 1. Check the number of records in each Data Vault table (Hubs, Links, and Satellites).
# MAGIC 2. Verify that each order is associated with exactly one customer.
# MAGIC    - Count orders with one and multiple associated customers to ensure correct link integrity.

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Check record counts
# MAGIC SELECT 'H_Customer' AS table_name, COUNT(*) AS record_count FROM H_Customer
# MAGIC UNION ALL
# MAGIC SELECT 'H_Order' AS table_name, COUNT(*) AS record_count FROM H_Order
# MAGIC UNION ALL
# MAGIC SELECT 'L_Customer_Order' AS table_name, COUNT(*) AS record_count FROM L_Customer_Order
# MAGIC UNION ALL
# MAGIC SELECT 'S_Customer' AS table_name, COUNT(*) AS record_count FROM S_Customer
# MAGIC UNION ALL
# MAGIC SELECT 'S_Order' AS table_name, COUNT(*) AS record_count FROM S_Order;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Verify that each order is associated with exactly one customer
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_orders,
# MAGIC   SUM(CASE WHEN customer_count = 1 THEN 1 ELSE 0 END) AS orders_with_one_customer,
# MAGIC   SUM(CASE WHEN customer_count != 1 THEN 1 ELSE 0 END) AS orders_with_multiple_customers
# MAGIC FROM (
# MAGIC   SELECT order_hk, COUNT(DISTINCT customer_hk) AS customer_count
# MAGIC   FROM L_Customer_Order
# MAGIC   GROUP BY order_hk
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 19: Creating or Updating Features
# MAGIC
# MAGIC **Task:** Build customer-level features using the `refined_orders` and `refined_customer` tables.
# MAGIC
# MAGIC Features to create:
# MAGIC - `total_orders`: total number of orders per customer  
# MAGIC - `avg_order_value`: average price per order  
# MAGIC - `total_spending`: total amount spent overall  
# MAGIC - `market_segment`: customer’s market segment

# COMMAND ----------

# Generate base customer-level features by aggregating order data and joining with customer info
from pyspark.sql.functions import col, count, avg, sum, current_timestamp

orders_df = spark.sql("SELECT * FROM refined_orders")
customers_df = spark.sql("SELECT * FROM refined_customer")

base_features_df = (
    orders_df.groupBy("customer_id")
    .agg(
        count("*").alias("total_orders"),
        avg("total_price").alias("avg_order_value"),
        sum("total_price").alias("total_spending")
    )
    .join(
        customers_df.select("customer_id", "market_segment"),
        on="customer_id",
        how="inner"
    )
    .withColumn("feature_update_ts", current_timestamp())
)

display(base_features_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 19.1: Register or Merge-Update Feature Table
# MAGIC
# MAGIC **Task:** Create a feature table named `customer_features` in the gold schema. If the table already exists, merge in the new data.

# COMMAND ----------

# Create or merge-update the 'customer_features' table with aggregated customer-level features
feature_table_name = f"{catalog_name}.{gold_schema}.customer_features"

try:
    fs.create_table(
        name=feature_table_name,
        primary_keys=["customer_id"],
        schema=base_features_df.schema,
        description="Customer-level features derived from refined tables."
    )
    print(f"Feature table '{feature_table_name}' created.")
except Exception as e:
    print(f"Feature table might already exist: {e}")

fs.write_table(
    name=feature_table_name,
    df=base_features_df,
    mode="merge"  
)

print(f"Feature table '{feature_table_name}' updated with new features.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 19.2: Scheduling Updates (Optional)
# MAGIC
# MAGIC **Task:** Create a subset of this notebook and schedule it as a Databricks Job to periodically refresh the features with fresh data.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 20: Model Training with Feature Store
# MAGIC
# MAGIC **Task:** Train a binary classification model using features stored in the Feature Store and refined customer data.
# MAGIC
# MAGIC Perform the following:
# MAGIC 1. Create a simple label: customers who spent more than a certain threshold are considered “high spender.”
# MAGIC 2. Look up the same features via `FeatureLookup` for training.
# MAGIC 3. Train a basic Logistic Regression model.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20.1 Create Binary Label for Classification
# MAGIC
# MAGIC **Task:** Add a binary label to the customer dataset where high spenders (total_spending > threshold) are labeled as 1, others as 0.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Use the `when` and `otherwise` functions from PySpark to define a label column.
# MAGIC 2. Select relevant feature and label columns for model training.

# COMMAND ----------

# Add binary label to customers based on whether their total spending exceeds a threshold
from pyspark.sql.functions import when

threshold = 20000.0

labeled_df = (
    base_features_df
    .withColumn("label", when(col("total_spending") > threshold, 1).otherwise(0))
    .select("customer_id", "total_orders", "avg_order_value", "total_spending", "market_segment", "label")
)

display(labeled_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20.2 Build a Training Set with FeatureLookups
# MAGIC
# MAGIC **Task:** Use the Feature Store to look up precomputed features and join them with the labeled customer data.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Define a `FeatureLookup` object with required feature names and join key.
# MAGIC 2. Drop duplicate columns from the labeled DataFrame to avoid conflicts.
# MAGIC 3. Create a training set using `fs.create_training_set()` and load it as a Spark DataFrame.

# COMMAND ----------

# Create a training set by looking up features from the feature store and joining them with labeled data
from databricks.feature_store import FeatureLookup

feature_lookup = FeatureLookup(
    table_name=feature_table_name,
    feature_names=[
        "total_orders",
        "avg_order_value",
        "total_spending",
        "market_segment",
    ],
    lookup_key="customer_id",
)
labeled_df_clean = labeled_df.drop("total_orders", "avg_order_value", "total_spending", "market_segment")

training_set = fs.create_training_set(
    df=labeled_df_clean,
    feature_lookups=[feature_lookup],
    label="label",
    exclude_columns=["feature_update_ts"]
)

training_df = training_set.load_df()
display(training_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20.3 Train a Simple Logistic Regression Model
# MAGIC
# MAGIC **Task:** Train a simple logistic regression model using a pipeline that includes feature transformations and classification.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Index and one-hot encode the categorical `market_segment` column.
# MAGIC 2. Use `VectorAssembler` to combine numerical and encoded features into a single vector.
# MAGIC 3. Define a `LogisticRegression` model.
# MAGIC 4. Create a `Pipeline` with the transformation stages and fit the model on the training dataset.

# COMMAND ----------

# Build and train a logistic regression model using a pipeline with feature transformation steps
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

# String indexer for the categorical column
indexer = StringIndexer(inputCol="market_segment", outputCol="market_segment_idx", handleInvalid="keep")
encoder = OneHotEncoder(inputCols=["market_segment_idx"], outputCols=["market_segment_vec"])

assembler = VectorAssembler(
    inputCols=["total_orders", "avg_order_value", "total_spending", "market_segment_vec"],
    outputCol="features"
)

lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])
model = pipeline.fit(training_df)

print("Model training complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 20.4 Log Model to MLflow (Optional)
# MAGIC
# MAGIC **Task:** (Optional) Log the trained model to MLflow for versioning, tracking, and deployment.
# MAGIC
# MAGIC **Note:** This step is optional and not implemented in this lab notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 21: Batch Inference Using the Feature Store
# MAGIC
# MAGIC **Scenario**: We have some new or existing customer IDs, and we want to predict which ones might be high spenders. We’ll:
# MAGIC 1. Demonstrate creating a DataFrame of customer IDs.
# MAGIC 2. Look up the same features via `FeatureLookup`.
# MAGIC 3. Generate predictions with our trained pipeline.
# MAGIC
# MAGIC **Task:** Predict high spender customers by using their feature data and a trained ML model.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 21.1 Create Sample Customer List
# MAGIC
# MAGIC **Task:** Select a small set of customer IDs for batch inference.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Select a few customer IDs (e.g., 5 rows) from the training DataFrame.
# MAGIC 2. Add a flag column called `batch_inference_example` to indicate this subset is for demo purposes.

# COMMAND ----------

# Select 5 sample customers and add a flag column for batch inference demo
from pyspark.sql.functions import lit

sample_customers = (
    training_df.select("customer_id")
    .limit(5)
    .withColumn("batch_inference_example", lit(True))
)

display(sample_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 21.2 Retrieve Features and Score
# MAGIC
# MAGIC **Task:** Lookup features from the Feature Store for the sample customers and generate predictions using the trained model.
# MAGIC
# MAGIC Steps to perform:
# MAGIC 1. Use `FeatureLookup` to retrieve necessary features from the feature table based on `customer_id`.
# MAGIC 2. Create an inference set by combining sample customers with the looked-up features.
# MAGIC 3. Load the inference DataFrame and apply the trained model pipeline.
# MAGIC 4. Display the predicted labels and probabilities for each customer.

# COMMAND ----------

# Perform batch inference by retrieving features for sample customers and applying the trained model
inference_lookup = FeatureLookup(
    table_name=feature_table_name,
    feature_names=[
        "total_orders",
        "avg_order_value",
        "total_spending",
        "market_segment"
    ],
    lookup_key="customer_id"
)

inference_set = fs.create_training_set(
    df=sample_customers,
    feature_lookups=[inference_lookup],
    label=None 
)

inference_df = inference_set.load_df()
predictions = model.transform(inference_df)
display(predictions.select("customer_id", "prediction", "probability"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Now, clean up your work.
# MAGIC
# MAGIC Run the below script to delete the `catalog_name` catalog and all its objects if it exists.

# COMMAND ----------

# Drop the catalog along with all objects (schemas, tables) inside
spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC Remove all widgets created during the demo to clean up the notebook environment.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this lab, you practiced a wide range of modern data warehousing and ML feature engineering techniques in Databricks. You modeled relationships using ERM, handled historical data using SCD Type 2, and applied the Data Vault 2.0 framework to build scalable, flexible data models. You also created and managed features with the Databricks Feature Store, trained ML models, and performed batch inference. Together, these exercises provided practical experience in designing integrated data and ML pipelines for real-world analytical use cases.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
