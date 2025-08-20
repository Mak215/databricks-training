# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Vault 2.0
# MAGIC In this demo, you will dive into the concepts of Data Vault 2.0 and learn how to implement its model within Databricks. Data Vault 2.0 provides a scalable and flexible approach to data warehousing, focusing on creating Hubs, Links, and Satellites for efficient storage and tracking of core business data. You will learn how to build these structures using hash keys, set up the ETL pipeline to load the data, and create business views for end-user querying. By the end of this demo, you will be equipped with the skills to implement a Data Vault model and optimize it for performance and scalability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC * Apply Data Vault 2.0 concepts to create Hubs, Links, and Satellites in Databricks.
# MAGIC * Design a scalable and flexible data model using hash keys for performance optimization.
# MAGIC * Implement an ETL pipeline to load data into Data Vault components.
# MAGIC * Develop business views that simplify querying and analysis for end-users.
# MAGIC * Verify the integrity and accuracy of the Data Vault model with sample queries and checks.

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
# MAGIC #### Key Concepts
# MAGIC
# MAGIC 1. **Hub**: Contains a unique list of business keys and related metadata
# MAGIC 2. **Link**: Connects hubs to represent relationships
# MAGIC 3. **Satellite**: Stores the descriptive attributes for a hub or link, allowing historical tracking
# MAGIC
# MAGIC Data Vault 2.0 emphasizes the use of *hash keys* for performance and scalability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setting Up the Environment
# MAGIC
# MAGIC First, let's set up our Databricks environment:

# COMMAND ----------

# DBTITLE 1,USE CATALOG, USE silver schema
import re

# Get the current user and extract the catalog name by splitting the email at '@' and taking the first part
user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]

# Replace all special characters in the `user_id` with an underscore '_' to create the catalog name
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id)

# Define the schema name to be used
silver_schema = "silver"

# COMMAND ----------

# Create a widget to capture catalog name and all schema names
dbutils.widgets.text("catalog_name", catalog_name)
dbutils.widgets.text("silver_schema", silver_schema)

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
# MAGIC ## 2. Creating Data Vault 2.0 Tables
# MAGIC
# MAGIC ### 2.1 Hubs
# MAGIC
# MAGIC Hubs store the core business entities. Each hub contains a unique list of business keys and related metadata.

# COMMAND ----------

# DBTITLE 1,CREATE TABLE H_Customer
# MAGIC %sql
# MAGIC -- Hub for Customer
# MAGIC CREATE TABLE IF NOT EXISTS H_Customer
# MAGIC (
# MAGIC   customer_hk STRING NOT NULL COMMENT 'MD5(customer_id)',
# MAGIC   customer_id INT NOT NULL,
# MAGIC   load_timestamp TIMESTAMP NOT NULL,
# MAGIC   record_source STRING,
# MAGIC   CONSTRAINT pk_h_customer PRIMARY KEY (customer_hk)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,CREATE TABLE H_Order
# MAGIC %sql
# MAGIC -- Hub for Orders
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
# MAGIC ### 2.2 Links
# MAGIC
# MAGIC Links represent relationships between hubs. They connect different business entities.

# COMMAND ----------

# DBTITLE 1,CREATE TABLE L_Customer_Order
# MAGIC %sql
# MAGIC -- Creates a link table to map customers to their orders with a hashed primary key
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
# MAGIC ### 2.3 Satellites
# MAGIC
# MAGIC Satellites store descriptive attributes and track changes over time. They are linked to hubs or links via hash keys.

# COMMAND ----------

# DBTITLE 1,CREATE TABLE S_Customer
# MAGIC %sql
# MAGIC -- Satellite for Customer Descriptive Info
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
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Table S_Order
# MAGIC %sql
# MAGIC -- Satellite for Order Descriptive Info
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
# MAGIC Set the default catalog and schema in spark sql.

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {silver_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ETL Process
# MAGIC
# MAGIC Now that we have our Data Vault structure in place, let's load some data. We'll use a simplified ETL process:
# MAGIC
# MAGIC 1. Load Hubs  
# MAGIC 2. Load Links  
# MAGIC 3. Load Satellites

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Helper Functions

# COMMAND ----------

# DBTITLE 1,Hashing helper function defs
# Generate hash keys and hash diff columns for Data Vault entities, including customer, order, and their relationships, using MD5.
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
# MAGIC ### 3.2 Loading Refined Table (Column Name Change, Data typecasting)

# COMMAND ----------

# DBTITLE 1,CREATE TABLE silver.refined_customer
# MAGIC %sql
# MAGIC -- Creates refined customer dimension table in the silver layer
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

# DBTITLE 1,CREATE TABLE silver.refined_orders
# MAGIC %sql
# MAGIC -- Creates refined orders fact table in the silver layer
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

# DBTITLE 1,ETL load function defs
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

# DBTITLE 1,etl_refined_customer(), etl_refined_orders()
# Run ETL Load
etl_refined_customer()
etl_refined_orders()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Loading the Customer Hub

# COMMAND ----------

# DBTITLE 1,MERGE INTO H_Customer USING customer_hub_stage
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

# DBTITLE 1,SELECT FROM H_Customer
# MAGIC %sql
# MAGIC -- Preview first 10 records from the H_Customer hub table
# MAGIC SELECT * FROM H_Customer LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Loading the Customer Satellite

# COMMAND ----------

# DBTITLE 1,MERGE INTO S_Customer USING customer_sat_stage
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
# MAGIC ### 3.5 Loading the Order Hub and Satellite

# COMMAND ----------

# DBTITLE 1,MERGE INTO H_Order USING order_hub_stage, MERGE INTO S_Order USING order_sat_stage
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
# MAGIC ### 3.6 Loading the Customer-Order Link

# COMMAND ----------

# DBTITLE 1,MERGE INTO L_Customer_Order USING link_stage
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
# MAGIC ## 4. Creating Business Views
# MAGIC
# MAGIC To make querying easier for end-users, we can create views that join the various vault components.

# COMMAND ----------

# DBTITLE 1,CREATE OR REPLACE VIEW gold.BV_Customer_Order
# MAGIC %sql
# MAGIC -- Business View combining Customer and Order details (This can be materialized in gold layer, but for this lab it is presented as a view)
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
# MAGIC ## 5. Sample Query
# MAGIC
# MAGIC Let's run a query to demonstrate how to use our Data Vault model:

# COMMAND ----------

# DBTITLE 1,SELECT * FROM gold.BV_Customer_Order
# MAGIC %sql
# MAGIC select * from gold.BV_Customer_Order

# COMMAND ----------

# DBTITLE 1,SELECT customer_name, total_sales FROM gold.BV_Customer_Order
# MAGIC %sql
# MAGIC -- Total Sales by customer
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
# MAGIC ## 6. Verification Steps
# MAGIC
# MAGIC Let's perform some basic verification to ensure our Data Vault is working correctly:

# COMMAND ----------

# DBTITLE 1,Check record counts
# MAGIC %sql
# MAGIC -- Check record counts
# MAGIC SELECT 'H_Customer' AS table_name, COUNT(*) AS record_count FROM H_Customer
# MAGIC UNION ALL
# MAGIC SELECT 'H_Order' AS table_name, COUNT(*) AS record_count FROM H_Order
# MAGIC UNION ALL
# MAGIC SELECT 'L_Customer_Order' AS table_name, COUNT(*) AS record_count FROM L_Customer_Order
# MAGIC UNION ALL
# MAGIC SELECT 'S_Customer' AS table_name, COUNT(*) AS record_count FROM S_Customer
# MAGIC UNION ALL
# MAGIC SELECT 'S_Order' AS table_name, COUNT(*) AS record_count FROM S_Order;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Verify order to customer
# MAGIC %sql
# MAGIC -- Verify that each order is associated with exactly one customer
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
# MAGIC ## Cleanup 
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,DROP Hubs, Link, Satellites, gold.BV_Customer_Order
# MAGIC %sql
# MAGIC -- We are not cleaning up the refined table created here as it will be needed in the feature store lab
# MAGIC -- Drop Satellites first
# MAGIC DROP TABLE IF EXISTS S_Customer;
# MAGIC DROP TABLE IF EXISTS S_Order;
# MAGIC
# MAGIC -- Drop the Link
# MAGIC DROP TABLE IF EXISTS L_Customer_Order;
# MAGIC
# MAGIC -- Drop Hubs
# MAGIC DROP TABLE IF EXISTS H_Customer;
# MAGIC DROP TABLE IF EXISTS H_Order;
# MAGIC
# MAGIC -- Drop gold business views 
# MAGIC DROP VIEW IF EXISTS gold.BV_Customer_Order

# COMMAND ----------

# MAGIC %md
# MAGIC Remove all widgets created during the demo to clean up the notebook environment.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this demo, we successfully implemented a basic Data Vault 2.0 model in Databricks. We created Hubs, Links, and Satellites using hash keys, loaded data into them, and set up business views for easier querying. You have learned how Data Vault 2.0 provides a structured yet flexible method for managing large-scale data. With this knowledge, you can now build and optimize your own Data Vault models, supporting scalable and efficient data analytics. Future enhancements could involve implementing incremental loading strategies or expanding the vault with more entities.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
