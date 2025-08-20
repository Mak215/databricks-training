# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

import numpy as np
np.set_printoptions(precision=2)

import logging
logging.getLogger("tensorflow").setLevel(logging.ERROR)

# COMMAND ----------

import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
import pyspark.pandas as ps

import re

#from databricks.feature_engineering import FeatureEngineeringClient

#from sklearn.model_selection import train_test_split

# COMMAND ----------

# Initialize DBAcademyHelper
DA = DBAcademyHelper() 
DA.init()

# COMMAND ----------

@DBAcademyHelper.add_method
def create_demo_table(self):
    my_catalog = DA.catalog_name
    my_schema = DA.schema_name
    my_table = "wine_quality_table"
    full_table_name = f"{my_catalog}.{my_schema}.{my_table}"

    spark.sql(f"USE CATALOG {my_catalog}")
    spark.sql(f"USE SCHEMA {my_schema}")

    # Drop the table if it exists
    if spark.catalog.tableExists(full_table_name):
        spark.sql(f"DROP TABLE {full_table_name}")
        print(f"Delta table dropped at: {full_table_name}")

    data_path = f"/Volumes/dbacademy_wine_quality/v01/data"

    df = (
        spark.read.format("delta")
        .load(data_path)
        .withColumn("wine_id", F.monotonically_increasing_id())
        .select(
            "wine_id",
            "fixed_acidity",
            "volatile_acidity",
            "citric_acid",
            "residual_sugar",
            "chlorides",
            "free_sulfur_dioxide",
            "total_sulfur_dioxide",
            "density",
            "pH",
            "sulphates",
            "alcohol",
            "quality",
        )
    )

    df.write.format("delta").mode("overwrite").saveAsTable(my_table)

    print(f"Delta table created at: {full_table_name}")
