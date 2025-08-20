# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.appName("EmployeeVsManagerSalary").getOrCreate()

# Sample DataFrame creation (you can replace this with your actual DataFrame)
data = [
    (1, "Alice", 90000, "IT", None, "NY"),
    (2, "Bob", 60000, "IT", 1, "NY"),
    (3, "Charlie", 95000, "Finance", 1, "NY"),
    (4, "David", 85000, "IT", 2, "CA"),
    (5, "Eve", 91000, "Finance", 3, "CA"),
]
columns = ["EmpID", "name", "salary", "dept", "ManagerID", "location"]
employee_df = spark.createDataFrame(data, columns)

# Register as temporary view to use SQL
employee_df.createOrReplaceTempView("Employee")

# SQL to get employees earning more than their manager
query = """
SELECT 
    e.EmpID,
    e.name AS employee_name,
    e.salary AS employee_salary,
    m.EmpID AS manager_id,
    m.name AS manager_name,
    m.salary AS manager_salary
FROM 
    Employee e
JOIN 
    Employee m
ON 
    e.ManagerID = m.EmpID
WHERE 
    e.salary > m.salary
"""

result_df = spark.sql(query)

# Show results
result_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col

# Start Spark session
spark = SparkSession.builder.appName("EncryptEmpIDWithHash").getOrCreate()

# Sample DataFrame
data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie")
]
columns = ["EmpID", "name"]
df = spark.createDataFrame(data, columns)

# Encrypt/Hash EmpID using SHA-256
encrypted_df = df.withColumn("EncryptedEmpID", sha2(col("EmpID").cast("string"), 256))

# Show result
encrypted_df.show(truncate=False)


# COMMAND ----------

flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("dbfs:/FileStore/data/flight-data/csv/2015_summary.csv")

# COMMAND ----------

flightData2015 = spark.read \
  .option("delimiter", "|") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("skipRows", 4) \
  .csv("dbfs:/FileStore/data/flight-data/csv/2015_summary.csv")


# COMMAND ----------

#Modes PERMISSIVE,DROPMALFORMED,FAILFAST
df = spark.read.option("header","true").option("mode","PERMISSIVE").option("columnNameOfCorruptRecord","_corrupt").format("csv").load("dbfs:/FileStore/data/flight-data/csv/2015_summary.csv")
display(df)
print(df.schema)

# COMMAND ----------

df = spark.read.option("header","true").format("csv").load("dbfs:/FileStore/data/flight-data/csv/2015_summary.csv")
display(df)
print(df.schema)



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
expected = StructType([StructField('DEST_COUNTRY', StringType(), True), StructField('ORIGIN_COUNTRY_NAME', StringType(), True), StructField('count', StringType(), True)])
if df.schema != expected:
    print("Not Matched")
else:
    print("Matched")


# COMMAND ----------

from pyspark.sql.functions import count, col, when
null_count =df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
display(null_count)

# COMMAND ----------

from pyspark.sql.functions import trim, upper, col
col_to_format = ["DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME"]
df_format = df.select(*[trim(upper(col(c))).alias(c) for c in col_to_format])
display(df_format)

# COMMAND ----------

from pyspark.sql.functions import collect_list
df_grouped = df.groupBy("DEST_COUNTRY_NAME").agg(collect_list("ORIGIN_COUNTRY_NAME").alias("COUNTRY_LIST"))
display(df_grouped)

# COMMAND ----------

df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/FileStore/tables/by-day/*.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import count
df_skewedkey = df.groupBy("StockCode").count().orderBy("count", ascending = False).show()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/FileStore/delta/skewedkey")

# COMMAND ----------

# MAGIC %md
# MAGIC Derived Column

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth
df =df.withColumn("invoice year", year("InvoiceDate")).withColumn("Invoice Month", month("InvoiceDate")).withColumn("Invoice Day", dayofmonth("InvoiceDate"))
display(df)

# COMMAND ----------

from pyspark.sql.functions import when, lit

joined_df =df.join(df_dedup, df_dedup["StockCode"] == df["StockCode"],"outer").withColumn("match status", when(df["StockCode"].isNotNull() & df_dedup["StockCode"].isNotNull(), "M").when(df["StockCode"].isNotNull(), "L").otherwise("R"))
display(joined_df)

# COMMAND ----------

from pyspark.sql.functions import trim, col

s_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
for col_name in s_cols:
    df_trimmed = df.withColumn(col_name, trim(col(col_name)))

display(df_trimmed)


# COMMAND ----------

joined_df = df.join(df_pivot,df_pivot["StockCode"] == df["StockCode"],"inner")
display(joined_df)

# COMMAND ----------

df_pivot = df.groupBy("StockCode").pivot("invoice year").sum("Quantity")
display(df_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC De Duplication

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("InvoiceNo").orderBy("InvoiceDate")
df_dedup = df.withColumn("rn", row_number().over(windowSpec)).filter("rn=1").drop("rn")
display(df_dedup)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, row_number, col

windowSpec = Window.partitionBy("StockCode")
df_pc = df.withColumn("Total_Sales",sum("Quantity").over(windowSpec)).withColumn("sales_pct", col("Quantity")/col("Total_Sales"))
display(df_pc)
