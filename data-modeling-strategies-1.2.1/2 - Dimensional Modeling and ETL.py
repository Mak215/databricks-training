# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Dimensional Modeling and ETL
# MAGIC In this demo, we will explore how to implement Slowly Changing Dimensions (SCD) Type 2 using Databricks within a star schema. You will work through the process of creating dimension and fact tables, applying data transformations, and maintaining historical records using SCD Type 2. This hands-on exercise will strengthen your data modeling and ETL skills using Spark SQL and Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Apply SCD Type 2 concepts to track historical changes in dimension tables using Databricks.
# MAGIC - Design a star schema for efficient querying and analysis in a data warehouse environment.
# MAGIC - Construct ETL pipelines using Spark SQL and Delta Lake to transform and load data.
# MAGIC - Evaluate the accuracy and completeness of data using data validation techniques.
# MAGIC - Develop scalable and automated data workflows using Databricks notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks to Perform
# MAGIC
# MAGIC In this notebook, we will explore how to build a *dimensional model* (specifically, a star schema) from our TPC-H data, using **silver** (refined) tables and **gold** (dimension and fact) tables.
# MAGIC
# MAGIC In doing so, we will walk through the following steps:
# MAGIC
# MAGIC 1. **Define Our Table Structures**: Understand how `GENERATED ALWAYS AS IDENTITY` works and create all necessary tables (silver and gold).  
# MAGIC 2. **Load the Silver Layer**: Move data from `bronze` (raw) TPC-H tables to the refined tables, applying any necessary transformations (renaming columns, trimming strings, normalizing data types).  
# MAGIC 3. **Create an SCD Type 2 Dimension**: Explore how to build a _Slowly Changing Dimension_ that preserves history for changed records.  
# MAGIC 4. **Load the Gold Layer**: Fill the dimension and fact tables with both **initial** and **incremental** data. Demonstrate how to use a single `MERGE` statement for SCD Type 2 updates.  
# MAGIC 5. **Validate and Explore Sample Queries**: Verify our data loads and query our new star schema for insights.  
# MAGIC
# MAGIC By the end, we’ll have a **gold** star schema with an **SCD Type 2 dimension** (`DimCustomer`) that tracks historical changes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC As a reminder, you should have already run a **setup script** in the preceding notebook that created:  
# MAGIC    - A user-specific catalog.  
# MAGIC    - Schemas: `bronze`, `silver`, and `gold`.  
# MAGIC    - TPC-H tables copied into `bronze`.
# MAGIC
# MAGIC If, for whatever reason you have not already performed this setup, please return now to **1 - Entity Relationship Modeling and Working with Constraints** and execute the `%run` command in Cell 8 of that notebook.

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
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Table Definitions
# MAGIC
# MAGIC To explore dimensional modeling, we will define:  
# MAGIC - **Silver** tables (sometimes called the *refined* or *integration* layer).  
# MAGIC - **Gold** tables that implement a **star schema** with an SCD Type 2 dimension.  
# MAGIC
# MAGIC A key feature we’ll use is `GENERATED ALWAYS AS IDENTITY` in Delta, which automatically assigns incrementing numeric values for surrogate keys.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Set Your Catalog

# COMMAND ----------

# DBTITLE 1,USE CATALOG
import re

# Get the current user's email and extract the catalog name by splitting at '@' and taking the first part
user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]

# Replace all special characters in the `user_id` with an underscore '_' to create the catalog name
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id)

# COMMAND ----------

# Define the schema name for the silver and gold layers
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

# Create a widget to capture catalog name and all schema names
dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("silver_schema", silver_schema)
dbutils.widgets.text("gold_schema", gold_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current catalog to the extracted catalog name in DBSQL
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Create Silver Tables
# MAGIC
# MAGIC The **silver** schema is typically a place for refined data. We will define two example refined tables:  
# MAGIC - `refined_customer` (based on TPC-H `customer`)  
# MAGIC - `refined_orders` (based on TPC-H `orders`)
# MAGIC
# MAGIC Each table will rename and standardize columns. We only define the schema here; we’ll load data next.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current schema to the extracted silver_schema name in DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# DBTITLE 1,CREATE TABLE IF NOT EXISTS refined_customer
# MAGIC %sql
# MAGIC -- Creating the refined_customer table if it does not already exist
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

# DBTITLE 1,CREATE TABLE IF NOT EXISTS refined_orders
# MAGIC %sql
# MAGIC -- Creating the refined_orders table if it does not already exist
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
# MAGIC ### 1.3 Create Gold Tables (Star Schema)
# MAGIC
# MAGIC #### We define:
# MAGIC - **`DimCustomer`** (with SCD Type 2 attributes)  
# MAGIC - **`DimDate`**  
# MAGIC - **`FactOrders`**  
# MAGIC
# MAGIC #### Key Steps
# MAGIC 1. `GENERATED ALWAYS AS IDENTITY` for surrogate keys.  
# MAGIC 2. Additional columns in `DimCustomer` to manage SCD Type 2 (e.g., `start_date`, `end_date`, and `is_current`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current schema to the extracted gold_schema name in DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:gold_schema);

# COMMAND ----------

# DBTITLE 1,CREATE TABLE IF NOT EXISTS DimCustomer
# MAGIC %sql
# MAGIC -- Create the DimCustomer table to store customer details with Slowly Changing Dimension (SCD) Type 2 attributes for historical tracking
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

# DBTITLE 1,CREATE TABLE IF NOT EXISTS DimDate
# MAGIC %sql
# MAGIC -- Simple DimDate table to store date-related information
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
# MAGIC -- Check Current Catalog
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Current Schema
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Replace `<default_catalog_name>` and `<default_schema_name>` values in both the lines in the below code with the actual default catalog and schema names from the query output in the previous cells:
# MAGIC
# MAGIC **Target Code Lines:**
# MAGIC ```dbsql
# MAGIC CONSTRAINT fk_customer FOREIGN KEY (dim_customer_key) REFERENCES <default_catalog_name>.<default_schema_name>.DimCustomer(dim_customer_key),  -- Foreign key constraint linking to DimCustomer
# MAGIC
# MAGIC CONSTRAINT fk_date FOREIGN KEY (dim_date_key) REFERENCES <default_catalog_name>.<default_schema_name>.DimDate(dim_date_key)  -- Foreign key constraint linking to DimDate
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,CREATE TABLE IF NOT EXISTS FactOrders
# MAGIC %sql
# MAGIC -- FactOrders table creation referencing DimCustomer and DimDate
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
# MAGIC   CONSTRAINT fk_customer FOREIGN KEY (dim_customer_key) REFERENCES <default_catalog_name>.<default_schema_name>.DimCustomer(dim_customer_key),  -- Foreign key constraint linking to DimCustomer
# MAGIC   CONSTRAINT fk_date FOREIGN KEY (dim_date_key) REFERENCES <default_catalog_name>.<default_schema_name>.DimDate(dim_date_key)  -- Foreign key constraint linking to DimDate
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notes on `GENERATED ALWAYS AS IDENTITY`
# MAGIC - Each table automatically generates unique numbers for the surrogate key column.  
# MAGIC - You do not insert a value for those columns; Delta handles it seamlessly.

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Loading Data into Silver
# MAGIC
# MAGIC We will load data from the TPC-H `bronze` tables into `refined_customer` and `refined_orders`. This step assumes your TPC-H dataset is in `bronze.customer` and `bronze.orders`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch to the catalog using the extracted catalog name in DBSQL
# MAGIC USE CATALOG IDENTIFIER(:catalog_name);
# MAGIC
# MAGIC -- Switch to the silver schema in DBSQL
# MAGIC USE SCHEMA IDENTIFIER(:silver_schema);

# COMMAND ----------

# DBTITLE 1,INSERT INTO silver.refined_customer FROM bronze.customer
# MAGIC %sql
# MAGIC -- Insert transformed data from the bronze.customer table into the refined_customer table
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

# DBTITLE 1,INSERT INTO silver.refined_orders FROM bronze.orders
# MAGIC %sql
# MAGIC -- Insert transformed data from the bronze.orders table into the refined_orders table
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
# MAGIC #### Validation
# MAGIC Check if records loaded into `refined_customer` and `refined_orders`:

# COMMAND ----------

# DBTITLE 1,display(refined_customer), display(refined_orders)
# MAGIC %sql
# MAGIC -- Display the count of records in the refined_customer table
# MAGIC SELECT COUNT(*) AS refined_customer_count FROM refined_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the count of records in the refined_orders table
# MAGIC SELECT COUNT(*) AS refined_orders_count FROM refined_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the count of records in the refined_customer table
# MAGIC SELECT COUNT(*) AS refined_customer_count FROM refined_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the count of records in the refined_orders table
# MAGIC SELECT COUNT(*) AS refined_orders_count FROM refined_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Initial Load into Gold (Dimensional Model)
# MAGIC
# MAGIC #### Key Steps:
# MAGIC 1. Perform an **initial load** of `DimCustomer` (all customers as *current*).  
# MAGIC 2. Create date entries in `DimDate` from `refined_orders`. (This can be a pre loaded table for daily dates for multiple years)  
# MAGIC 3. Populate `FactOrders`, linking each order to the correct dimension keys.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch to the gold schema using the USE SCHEMA SQL command
# MAGIC USE SCHEMA IDENTIFIER(:gold_schema);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 DimCustomer (SCD Type 2) Initial Load
# MAGIC - Mark every row with `start_date = CURRENT_DATE()`, `end_date = NULL`, and `is_current = TRUE`.

# COMMAND ----------

# DBTITLE 1,INSERT INTO DimCustomer from silver.refined_customer
# MAGIC %sql
# MAGIC -- Insert data into the DimCustomer dimension table
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
# MAGIC ### 3.2 DimDate
# MAGIC - Collect unique `order_date` values from `refined_orders`.  
# MAGIC - Use built-in functions to split them into `day, month, year`.

# COMMAND ----------

# DBTITLE 1,INSERT INTO DimDate FROM silver.refined_orders
# MAGIC %sql
# MAGIC -- Insert distinct dates into the DimDate dimension table
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
# MAGIC ### 3.3 FactOrders
# MAGIC - Link each order to `DimCustomer` and `DimDate`.  
# MAGIC - We join on `(customer_id = dc.customer_id AND is_current = TRUE)` for SCD Type 2, ensuring we match only an active dimension record.

# COMMAND ----------

# DBTITLE 1,INSERT INTO FactOrders FROM silver.refined_orders JOIN DimCustomer JOIN DimDate
# MAGIC %sql
# MAGIC -- Insert data into the FactOrders table
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
# MAGIC Check record counts in each **gold** table:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the record count for the DimCustomer table
# MAGIC SELECT 'DimCustomer' AS table_name, COUNT(*) AS record_count FROM DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the record count for the DimDate table
# MAGIC SELECT 'DimDate' AS table_name, COUNT(*) AS record_count FROM DimDate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the record count for the FactOrders table
# MAGIC SELECT 'FactOrders' AS table_name, COUNT(*) AS record_count FROM FactOrders

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Incremental Updates (SCD Type 2 MERGE)
# MAGIC
# MAGIC In real life, you would detect changes in `refined_customer` over time (e.g., a changed address or a new customer). When you see a difference:
# MAGIC 1. **Close** the old record in DimCustomer (`end_date = <current date>`, `is_current = FALSE`).  
# MAGIC 2. **Insert** a new record (with the updated attributes, `start_date = <current date>`, `end_date = NULL`, `is_current = TRUE`).  
# MAGIC
# MAGIC Below is a single MERGE statement that can update the old record and insert the new version in one pass.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Example: Create Dummy Incremental Changes
# MAGIC
# MAGIC This simulates:  
# MAGIC - An *existing* customer (ID=101) changing their address.  
# MAGIC - A *brand-new* customer (ID=99999).

# COMMAND ----------

# DBTITLE 1,CREATE OR REPLACE TEMP VIEW incremental_customer_updates
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW incremental_customer_updates AS
# MAGIC -- Existing customer with updated information
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
# MAGIC -- New customer with initial information
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
# MAGIC -- Display the contents of the temporary view to verify the data
# MAGIC SELECT * FROM incremental_customer_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Single MERGE for SCD Type 2
# MAGIC 1. We produce two rows for *any changed* customer: one labeled `"OLD"` to close out the current record, and one labeled `"NEW"` to insert an updated version.  
# MAGIC 2. For a truly new customer, we only produce a `"NEW"` row.

# COMMAND ----------

# DBTITLE 1,spark.sql(merge_sql)
# MAGIC %sql
# MAGIC WITH staged_changes AS (
# MAGIC   -- "OLD" row: used to find and update the existing active dimension record
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
# MAGIC   -- "NEW" row: used to insert a brand-new dimension record
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
# MAGIC -- Perform the merge operation on the DimCustomer table
# MAGIC MERGE INTO DimCustomer t
# MAGIC USING staged_changes s
# MAGIC   ON t.customer_id = s.customer_id
# MAGIC      AND t.is_current = TRUE
# MAGIC      AND s.row_type = 'OLD'
# MAGIC
# MAGIC -- When a match is found, update the existing record to close it out
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     t.is_current = FALSE,
# MAGIC     t.end_date   = CURRENT_DATE()
# MAGIC
# MAGIC -- When no match is found, insert the new record as the current version
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
# MAGIC #### Explanation:
# MAGIC - If a row is found matching the `"OLD"` record (meaning that customer currently is active in `DimCustomer`), we **update** that record to close it out (`is_current=FALSE` and `end_date=TODAY`).  
# MAGIC - Every `"NEW"` row won’t match, so it triggers the “INSERT” path. We insert a new row with `is_current=TRUE`, a fresh `start_date`, and no `end_date`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Validate the Updated Rows

# COMMAND ----------

# DBTITLE 1,SELECT FROM DimCustomer
# MAGIC %sql
# MAGIC -- Query to display details of an existing customer with customer_id 101
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
# MAGIC -- Query to display details of a new customer with customer_id 99999
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
# MAGIC You should see that the old version of `customer_id = 101` now has `is_current=FALSE`, and a new version was inserted. A brand-new `customer_id=99999` has only one record (`is_current=TRUE`).

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Sample Queries on the Star Schema
# MAGIC
# MAGIC Now that we have `FactOrders` linking to `DimCustomer` and `DimDate`, let’s run a few checks to see how we can analyze the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Row Counts

# COMMAND ----------

# DBTITLE 1,display(DimCustomer), display(DimState), display(FactOrders)
# MAGIC %sql
# MAGIC -- Display the count of records in the DimCustomer table with the table name
# MAGIC SELECT 'DimCustomer' AS table_name, COUNT(*) AS record_count FROM DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the count of records in the DimDate table with the table name
# MAGIC SELECT 'DimDate' AS table_name, COUNT(*) AS record_count FROM DimDate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display the count of records in the FactOrders table with the table name
# MAGIC SELECT 'FactOrders' AS table_name, COUNT(*) AS record_count FROM FactOrders

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Example Query: Top Market Segments

# COMMAND ----------

# DBTITLE 1,SELECT FROM FactOrders JOIN  DimCustomer
# MAGIC %sql
# MAGIC -- Display the total amount spent by customers in each market segment, limited to the top 10 segments
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
# MAGIC ### 5.3 Example Query: Order Counts by Year

# COMMAND ----------

# DBTITLE 1,SELECT FROM FactOrders JOIN DimDate
# MAGIC %sql
# MAGIC -- Count the number of orders for each year by joining the FactOrders table with the DimDate table
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
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Summary & Next Steps
# MAGIC
# MAGIC 1. **Defined Table Structures**:  
# MAGIC    - **Silver** (refined/integration) tables (`refined_customer`, `refined_orders`).  
# MAGIC    - **Gold** dimension/fact tables (`DimCustomer`, `DimDate`, `FactOrders`).  
# MAGIC    - Used `GENERATED ALWAYS AS IDENTITY` for surrogate keys.  
# MAGIC    - Managed SCD Type 2 in `DimCustomer` using `start_date`, `end_date`, and `is_current`.  
# MAGIC
# MAGIC 2. **Loaded Initial Data**:  
# MAGIC    - Moved from `bronze` TPC-H to **silver**.  
# MAGIC    - Populated **gold** dimension and fact tables.  
# MAGIC
# MAGIC 3. **Incremental Updates**:  
# MAGIC    - Demonstrated a single MERGE for SCD Type 2.  
# MAGIC    - Closed out old dimension records and inserted new ones simultaneously.  
# MAGIC
# MAGIC 4. **Validation**:  
# MAGIC    - Showed row counts, queries to confirm star schema functionality.  
# MAGIC
# MAGIC **Where to Go Next**:  
# MAGIC - Incorporate automated checks for changes in `refined_customer` to keep `DimCustomer` synchronized over time.  
# MAGIC - Extend Type 2 logic to other dimensions, like part, supplier, or date-based attributes.  
# MAGIC - Create additional fact tables (e.g., FactLineItems) to make your star schema bigger.  
# MAGIC - Explore advanced BI or analytics queries on your star schema, taking full advantage of Databricks performance optimizations.

# COMMAND ----------

# MAGIC %md
# MAGIC #### You have completed:
# MAGIC 1. Building and structuring your **silver** tables.  
# MAGIC 2. Designing a **gold** star schema with a Type 2 dimension.  
# MAGIC 3. Loading your dimension and fact tables with both **initial** and **incremental** data updates.  
# MAGIC 4. Running queries to validate and explore the star schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Before proceeding, execute the SQL block in the cell below to clean up your work.

# COMMAND ----------

# DBTITLE 1,DROP silver and gold tables
# MAGIC %sql
# MAGIC -- Drop Silver tables
# MAGIC DROP TABLE IF EXISTS silver.refined_customer;
# MAGIC DROP TABLE IF EXISTS silver.refined_orders;
# MAGIC
# MAGIC -- Drop Gold tables
# MAGIC DROP TABLE IF EXISTS gold.DimCustomer;
# MAGIC DROP TABLE IF EXISTS gold.DimDate;
# MAGIC DROP TABLE IF EXISTS gold.FactOrders;

# COMMAND ----------

# MAGIC %md
# MAGIC Remove all widgets created during the demo to clean up the notebook environment.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this demo, we successfully implemented a star schema design for a data warehouse using Databricks. We applied the SCD Type 2 methodology to manage historical data in dimension tables while ensuring data consistency and traceability. Through step-by-step data transformation and ETL pipeline development, we gained practical experience in creating and managing data models for analytics. By the end of this demo, you will have developed proficiency in building scalable and efficient ETL pipelines, maintaining data lineage, and applying dimensional modeling concepts to real-world datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
