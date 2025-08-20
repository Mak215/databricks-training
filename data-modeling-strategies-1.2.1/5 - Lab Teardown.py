# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Teardown

# COMMAND ----------

# MAGIC %md
# MAGIC Congratulations! We hope that you have enjoyed this set of lab activities, designed to give you a sense of how easily you can support a variety of interwoven data modeling methodologies for both data warehouse and modern use cases in Databricks.
# MAGIC
# MAGIC We encourage you to continue your learning journey with additional courses in the Data Architect Associate pathway. As an additional reminder, you may be interested in exploring other learning pathways, such as Data Engineer and ML Engineer, for deeper technical training in the disciplines of greatest interest to you.

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
# MAGIC #### Before you, go, we'd appreciate it if you use the lab teardown script to tidy up the artifacts from your lab activities, overall.

# COMMAND ----------

# DBTITLE 1,%run lab_teardown
# MAGIC %run ./Includes/teardown/lab_teardown

# COMMAND ----------

# MAGIC %md
# MAGIC ### Thank you for participating in this lab!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
