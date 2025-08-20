# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

from pyspark.sql.types import StructType

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE DA VARIABLE USING SQL FOR USER INFORMATION FROM THE META TABLE
# MAGIC
# MAGIC -- Create a temp view storing information from the obs table.
# MAGIC CREATE OR REPLACE TEMP VIEW user_info AS
# MAGIC SELECT map_from_arrays(collect_list(replace(key,'.','_')), collect_list(value))
# MAGIC FROM dbacademy.ops.meta;
# MAGIC
# MAGIC -- Create SQL dictionary var (map)
# MAGIC DECLARE OR REPLACE DA MAP<STRING,STRING>;
# MAGIC
# MAGIC -- Set the temp view in the DA variable
# MAGIC SET VAR DA = (SELECT * FROM user_info);
# MAGIC
# MAGIC DROP VIEW IF EXISTS user_info;

# COMMAND ----------

import pandas as pd

# COMMAND ----------

volume_name = "myfiles"

# COMMAND ----------

@DBAcademyHelper.add_method
def create_volume(self):
    """
    Create a volume in the module's catalog using the specified volume_name.
    """
    ## Specify the volume name
    create_volume_location = f'{DA.catalog_name}.{DA.schema_name}.{volume_name}'

    ## Create the volume
    createVolume = f'CREATE VOLUME IF NOT EXISTS {create_volume_location}'
    spark.sql(createVolume)
    print(f'Your volume {self.__bold}{volume_name}{self.__reset} in {self.__bold}{self.catalog_name}.{self.schema_name}{self.__reset} is available.')

# COMMAND ----------

@DBAcademyHelper.add_method
def create_employees_csv(self):
    """Creates the employees.csv file in the specified catalog.Schema.Volume."""
    data = [
        ["1111", "Kristi", "USA", "Manager"],
        ["2222", "Sophia", "Greece", "Developer"],
        ["3333", "Peter", "USA", "Developer"],
        ["4444", "Zebi", "Pakistan", "Administrator"]
    ]
    columns = ["ID", "Firstname", "Country", "Role"]

    df = pd.DataFrame(data, columns=columns)
    file_path = f"/Volumes/{self.catalog_name}/{self.schema_name}/{volume_name}/employees.csv"
    df.to_csv(file_path, index=False)

    print(f"The employees.csv is created in your schema's {volume_name} volume")

# COMMAND ----------

@DBAcademyHelper.add_method
def create_employees_csv2(self):
    """
    Creates the employees2.csv file in the specified catalog.Schema.Volume.
    """
    # Create data for the CSV file
    data = [
        [5555, 'Alex','USA', 'Instructor'],
        [6666, 'Sanjay','India', 'Instructor']
    ]
    columns = ["ID","Firstname", "Country", "Role"]

    ## Create the DataFrame
    df = pd.DataFrame(data, columns=columns)

    ## Create the CSV file in the course Catalog.Schema.Volume
    df.to_csv(f"/Volumes/{self.catalog_name}/{self.schema_name}/{volume_name}/employees2.csv", index=False)

    print(f"The employees2.csv is created in your schema's {volume_name} volume")

# COMMAND ----------

@DBAcademyHelper.add_method
def print_lakeflow_job_info(self):
    path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    newpath = path.replace('DEWD00 - 04-Creating a Simple Databricks LakeFlow','')
    task1path = newpath + '/DEWD00 - 04A-Workflow Task 1 - Setup - Bronze'
    task2path = newpath + '/DEWD00 - 04B-Workflow Task 2 - Silver - Gold'
    
    print(f'Name your LakeFlow Job: {self.__bold}{DA.schema_name}_Example{self.__reset}')
    print(' ')
    print(f'{self.__bold}NOTEBOOK PATHS FOR TASKS{self.__reset}')
    print(f'- Task 1 notebook path: \n{self.__bold}{task1path}{self.__reset}')
    print(f'- Task 2 notebook path: \n{self.__bold}{task2path}{self.__reset}')

# COMMAND ----------

@DBAcademyHelper.add_method
def create_taxi_files(self):
    """
    Create the samples.nyctaxi.trips Delta as a csv file in the user's volume.
    """
    spark.sql(f'CREATE VOLUME IF NOT EXISTS {DA.catalog_name}.{DA.schema_name}.taxi_files')
    output_volume = f'/Volumes/{DA.catalog_name}/{DA.schema_name}/taxi_files'
    

    sdf = spark.table("samples.nyctaxi.trips")
    
    (sdf
        .write
        .mode("overwrite")
        .csv(output_volume, header=True)
        )
    
    print(f'The taxi data is available in the {self.__bold}taxi_files{self.__reset} volume within the {self.__bold}{DA.catalog_name}{self.__reset} Catalog in your schema {self.__bold}{DA.schema_name}{self.__reset}.')

# COMMAND ----------

@DBAcademyHelper.add_method
def drop_employee_csv2(self):
    """
    Drop the employee_csv2 file if it exists.
    """
    file_path = f"/Volumes/{DA.catalog_name}/{DA.schema_name}/{volume_name}/employees2.csv"

    try:
        dbutils.fs.ls(file_path)  # Check if the file exists
        dbutils.fs.rm(file_path)  # Delete the file if found
    except Exception as e:
        return

# COMMAND ----------

@DBAcademyHelper.add_method
def drop_taxi_volume(self):
    """
    Drop the taxidata volume if exists.
    """
    spark.sql('DROP volume if exists taxi_files')
