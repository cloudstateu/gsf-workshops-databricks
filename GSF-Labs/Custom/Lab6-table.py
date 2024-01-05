# Databricks notebook source
# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import shutil
# MAGIC import pyspark.sql.functions as F
# MAGIC
# MAGIC
# MAGIC # Copy the file from DBFS to local file system
# MAGIC dbutils.fs.cp("dbfs:/mnt/parquet/mt_cars.parquet", "file:/tmp/mt_cars.parquet")
# MAGIC
# MAGIC # Read the Parquet file into a pandas DataFrame
# MAGIC pandas_df = pd.read_parquet("/tmp/mt_cars.parquet")
# MAGIC
# MAGIC display(pandas_df)
# MAGIC
# MAGIC
# MAGIC # Convert the pandas DataFrame to a PySpark DataFrame
# MAGIC df = spark.createDataFrame(pandas_df)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

def createSchema(schema, location):
    schema_name = schema
    schemas = spark.sql(f"SHOW DATABASES LIKE '{schema_name}'")

    if schemas.count() > 0:
        print(f"schema {schema_name} already exists")
    else:
        spark.sql(f"CREATE SCHEMA {schema_name} LOCATION '{location}'")

createSchema("car_workshops", "abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/parquet/car_workshops")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table car_workshops.m_cars

# COMMAND ----------

from pyspark.sql.functions import col


# Write the DataFrame to the table
print(df.columns)
df.write.format("delta").mode("overwrite").partitionBy("carb").saveAsTable("car_workshops.m_cars")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from car_workshops.m_cars;