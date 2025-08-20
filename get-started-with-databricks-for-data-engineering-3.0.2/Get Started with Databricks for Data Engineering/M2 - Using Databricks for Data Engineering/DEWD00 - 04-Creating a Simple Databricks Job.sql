-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating a Simple Databricks Job
-- MAGIC
-- MAGIC Databricks Jobs (Lakeflow Jobs) provides a collection of tools that allow you to schedule and orchestrate all processing tasks on Databricks.
-- MAGIC
-- MAGIC **Objective:** Use the pipeline built in the previous demonstration to create two tasks in a job. The pipeline has been separated into two notebooks for demonstration purposes:
-- MAGIC - **DEWD00 - 04A-Task 1 - Setup - Bronze**
-- MAGIC - **DEWD00 - 04B-Task 2 - Silver - Gold**
-- MAGIC
-- MAGIC
-- MAGIC **NOTE:** You could have used a DLT pipeline for this data engineering task, but DLT is beyond the scope of this course. DLTs can be scheduled within a Workflow with additional tasks.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC
-- MAGIC    - Click **More** in the drop-down.
-- MAGIC
-- MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
-- MAGIC
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC
-- MAGIC 3. Wait a few minutes for the cluster to start.
-- MAGIC
-- MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

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
-- MAGIC
-- MAGIC ## 1. Generate Lakeflow Job Configuration
-- MAGIC
-- MAGIC Configuring this lakeflow job will require parameters unique to a given user.
-- MAGIC
-- MAGIC Run the cell below to print out values you'll use to configure your lakeflow job in subsequent steps.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.print_lakeflow_job_info()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2. Configure Job with a Notebook Task
-- MAGIC
-- MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by creating a job with a single task, and can add more if required.
-- MAGIC
-- MAGIC Complete the following to create a lakeflow job with two tasks using the notebooks from above (**DEWD00 - 04A-Task 1 - Setup - Bronze** and **DEWD00 - 04B-Task 2 - Silver - Gold**):
-- MAGIC
-- MAGIC 1. Right-click the **Jobs & Pipelines** button on the sidebar, and open the link in a new tab. This way, you can refer to these instructions, as needed.
-- MAGIC
-- MAGIC 2. Confirm you are in the **Jobs & Pipelines** tab.
-- MAGIC
-- MAGIC 3. On the right side, select **Create -> Job**.
-- MAGIC
-- MAGIC 4. In the top-left of the screen, enter the **Job Name** provided above to add a name for the lakeflow job.
-- MAGIC
-- MAGIC 5. Configure the lakeflow job and task as specified below. You'll need the values provided in the cell output above for this step.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Create Task 1
-- MAGIC | Setting | Instructions |
-- MAGIC |--|--|
-- MAGIC | Task name | Enter **Setup-Bronze** |
-- MAGIC | Type | Choose **Notebook**. Note in the dropdown list the many different types of lakeflow jobs that can be scheduled |
-- MAGIC | Source | Choose **Workspace** |
-- MAGIC | Path | Use the navigator to specify the **DEWD00 - 04A-LakeFlow Task 1 - Setup - Bronze** notebook. Use the path from above to help find the notebook. |
-- MAGIC | Compute | From the dropdown menu, under **Existing All Purpose Clusters**, select your cluster |
-- MAGIC | Create | Select the **Create task** button to create the task |
-- MAGIC
-- MAGIC **NOTE**: When selecting your all-purpose cluster, you may get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Task 2
-- MAGIC | Setting | Instructions |
-- MAGIC |--|--|
-- MAGIC | New task | Select **Add task** within your job. Then select **Notebook**|
-- MAGIC | Task name | Enter **Silver-Gold** |
-- MAGIC | Type | Choose **Notebook**. Note in the dropdown list the many different types of lakeflow jobs that can be scheduled |
-- MAGIC | Source | Choose **Workspace** |
-- MAGIC | Path | Use the navigator to specify the **DEWD00 - 04B-LakeFlow Task 2 - Silver - Gold** notebook. Use the path from above to help find the notebook. |
-- MAGIC | Compute | From the dropdown menu, under **Existing All Purpose Clusters**, select your cluster |
-- MAGIC | Create | Select the **Create task** button to create the task |
-- MAGIC
-- MAGIC **NOTE**: When selecting your all-purpose cluster, you may get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## 3. Explore Scheduling Options
-- MAGIC Complete the following steps to explore the scheduling options:
-- MAGIC
-- MAGIC 1. On the right hand side of the Jobs UI, locate the **Schedules & Triggers** section.
-- MAGIC
-- MAGIC 2. Select the **Add trigger** button to explore scheduling options.
-- MAGIC
-- MAGIC 3. Changing the **Trigger type** from **None (Manual)** to **Scheduled** will bring up a cron scheduling UI.
-- MAGIC
-- MAGIC    - This UI provides extensive options for setting up chronological scheduling of your LakeFlow Jobs. Settings configured with the UI can also be output in cron syntax, which can be edited if custom configuration is not available when the UI is needed.
-- MAGIC    
-- MAGIC 4. Select **Cancel** to return to Job details.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Run Job
-- MAGIC Select **Run now** above  **Job details** to execute the job.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Review Job Run
-- MAGIC
-- MAGIC To review the job run:
-- MAGIC
-- MAGIC 1. On the Job details page, select the **Runs** tab in the top-left of the screen (you should currently be on the **Tasks** tab)
-- MAGIC
-- MAGIC 2. Find your job.
-- MAGIC
-- MAGIC 3. Open the output details by clicking on the timestamp field under the **Start time** column
-- MAGIC
-- MAGIC     - If **the job is still running**, you will see the active state of the notebook with a **Status** of **`Pending`** or **`Running`** in the right side panel.
-- MAGIC
-- MAGIC     - If **the job has completed**, you will see the full execution of the notebook with a **Status** of **`Succeeded`** or **`Failed`** in the right side panel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
-- MAGIC
