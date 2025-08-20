# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

DA.create_volume()

# COMMAND ----------

DA.create_employees_csv()

# COMMAND ----------

DA.create_taxi_files()

# COMMAND ----------

DA.display_config_values([('Course Catalog',DA.catalog_name),('Your Schema',DA.schema_name)])
