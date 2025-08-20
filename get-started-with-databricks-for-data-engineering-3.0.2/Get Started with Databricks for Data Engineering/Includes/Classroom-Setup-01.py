# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

DA.drop_employee_csv2()

# COMMAND ----------

DA.drop_taxi_volume()

# COMMAND ----------

DA.create_volume()

# COMMAND ----------

DA.create_employees_csv()

# COMMAND ----------

DA.display_config_values([('Course Catalog',DA.catalog_name),('Your Schema',DA.schema_name)])
