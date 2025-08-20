# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Modern Use Case: Feature Store on Databricks
# MAGIC In this demo, we will walk through the process of using Databricks Feature Store to build, train, and deploy machine learning models using features stored in a centralized repository. We will cover the installation of necessary libraries, creating and updating features, training a model with these features, and performing batch inference to make predictions. By the end of this demo, you will gain hands-on experience in leveraging the power of Databricks Feature Store to streamline machine learning workflows and ensure consistency in feature management.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives 
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Apply the process of installing and setting up Databricks Feature Store libraries.
# MAGIC - Create and manage feature tables in Databricks to store and update features.
# MAGIC - Train a machine learning model using features retrieved from the Feature Store.
# MAGIC - Perform batch inference to generate predictions based on feature data.
# MAGIC - Evaluate the effectiveness of periodic feature updates for maintaining model accuracy.

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
# MAGIC ### Notebook Tasks:
# MAGIC This notebook walks through the process of:
# MAGIC 1. **Installing Libraries** for the Databricks Feature Store.
# MAGIC 2. **Building Features** and saving them to Feature Store.
# MAGIC 3. **Training a Model** using these features.
# MAGIC 4. **Performing Batch Inference** with the same features.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Required Libraries
# MAGIC
# MAGIC We’ll install the Databricks Feature Engineering library (a.k.a Feature Store) so we can create table definitions, load training sets, and publish features.

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

# MAGIC %md
# MAGIC Once the library is installed, we restart the Python kernel so it’s fully available.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Environment Setup
# MAGIC
# MAGIC - We’ll work in a catalog and schema that you have access to.
# MAGIC - Adjust the catalog/schema names to match your environment or naming conventions.

# COMMAND ----------

import re
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()


user_id = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0]
catalog_name = re.sub(r'[^a-zA-Z0-9]', '_', user_id)
silver_schema = "silver"
gold_schema = "gold"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE {silver_schema}")

print("Catalog and schemas set for feature development.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creating or Updating Features
# MAGIC
# MAGIC **Goal**: Build customer-level features from example refined tables (`refined_orders` and `refined_customer`):
# MAGIC - `total_orders`: total number of orders per customer
# MAGIC - `avg_order_value`: average price per order
# MAGIC - `total_spending`: total amount spent overall
# MAGIC - `market_segment`: a categorical column from the customer dimension

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
# MAGIC ### 3.1 Register or Merge-Update Feature Table
# MAGIC
# MAGIC We’ll create a feature table called `customer_features` in the `gold` schema. If it already exists, we’ll merge in the new data.

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
# MAGIC ### 3.2 Scheduling Updates
# MAGIC
# MAGIC This notebook (or a subset of it) can be scheduled as a Databricks Job to periodically refresh the features with fresh data. This ensures your features remain up to date for downstream ML tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model Training with Feature Store
# MAGIC
# MAGIC We’ll:
# MAGIC 1. Create a simple label: customers who spent more than a certain threshold are considered “high spender.”
# MAGIC 2. Look up the same features via `FeatureLookup` for training.
# MAGIC 3. Train a basic Logistic Regression model.

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
# MAGIC ### 4.1 Build a Training Set with FeatureLookups
# MAGIC
# MAGIC Use a lookup to retrieve columns from the feature table and join them with our labeled dataset.

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
# MAGIC ### 4.2 Train a Simple Logistic Regression Model
# MAGIC
# MAGIC We’ll convert the categorical column `market_segment` into numeric form, vectorize all features, and train a binary classifier.

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
# MAGIC ### 4.3 (Optional) Log Model to MLflow
# MAGIC
# MAGIC If you’d like to track versions, metrics, and deploy the model easily, log it with MLflow. For brevity, we’ll omit those details here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Batch Inference Using the Feature Store
# MAGIC
# MAGIC **Scenario**: We have some new or existing customer IDs, and we want to predict which ones might be high spenders. We’ll:
# MAGIC 1. Demonstrate creating a DataFrame of customer IDs.
# MAGIC 2. Look up the same features via `FeatureLookup`.
# MAGIC 3. Generate predictions with our trained pipeline.

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
# MAGIC ### 5.1 Retrieve Features and Score
# MAGIC
# MAGIC Merging the prepared customers with our feature table results in a final DataFrame that can be run through the model.

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
# MAGIC ### Key Concepts Covered:
# MAGIC - **Periodic Feature Updates** keep your model’s input features current.
# MAGIC - **Single Source of Truth** for features via Feature Store leads to consistent training & inference.
# MAGIC - **Batch or Real-Time Inference** can be implemented using the same feature definitions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this demo, we successfully implemented an end-to-end pipeline using Databricks Feature Store to manage, train, and predict with machine learning features. We created and updated feature tables, trained a model using these features, and performed batch inference to make predictions. By following this workflow, you can ensure consistency and efficiency in feature management, enabling you to build and deploy more reliable machine learning models. Future steps could include implementing real-time inference or automating feature updates for more dynamic data workflows.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
