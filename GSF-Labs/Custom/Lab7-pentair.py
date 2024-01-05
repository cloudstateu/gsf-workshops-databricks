# Databricks notebook source
# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import shutil
# MAGIC import pyspark.sql.functions as F
# MAGIC
# MAGIC
# MAGIC # Copy the file from DBFS to local file system
# MAGIC dbutils.fs.cp("dbfs:/mnt/parquet/Williamson.parquet", "file:/tmp/Williamson.parquet")
# MAGIC
# MAGIC # Read the Parquet file into a pandas DataFrame
# MAGIC pandas_df = pd.read_parquet("/tmp/Williamson.parquet")
# MAGIC display(pandas_df)
# MAGIC # Convert the pandas DataFrame to a PySpark DataFrame
# MAGIC df = spark.createDataFrame(pandas_df)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema hive_metastore.pentair_rest_workshops;

# COMMAND ----------

def createSchema(schema, location):
    schema_name = schema
    schemas = spark.sql(f"SHOW DATABASES LIKE '{schema_name}'")

    if schemas.count() > 0:
        print(f"schema {schema_name} already exists")
    else:
        spark.sql(f"CREATE SCHEMA {schema_name} LOCATION '{location}'")

createSchema("pentair_rest_workshops", "abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/parquet/pentair_rest_workshops")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_delta;
# MAGIC drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_parquet;
# MAGIC drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_json;
# MAGIC drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_csv;

# COMMAND ----------

from pyspark.sql.functions import col


# Write the DataFrame to the table
print(df.columns)


df.write.format("parquet").mode("overwrite").saveAsTable("pentair_rest_workshops.tbl_sensor_data_parquet")


# COMMAND ----------

from pyspark.sql.functions import col


# Write the DataFrame to the table
print(df.columns)
df = df.withColumn("turbidity", col("turbidity").cast("string"))
df = df.withColumn("chlorophyll", col("chlorophyll").cast("string"))

df.write.format("delta").mode("overwrite").saveAsTable("pentair_rest_workshops.tbl_sensor_data_delta")


# COMMAND ----------

from pyspark.sql.functions import col


# Write the DataFrame to the table
print(df.columns)
df = df.withColumn("turbidity", col("turbidity").cast("string"))
df = df.withColumn("chlorophyll", col("chlorophyll").cast("string"))

df.write.format("json").mode("overwrite").saveAsTable("pentair_rest_workshops.tbl_sensor_data_json")


# COMMAND ----------

from pyspark.sql.functions import col


# Write the DataFrame to the table
print(df.columns)
df = df.withColumn("turbidity", col("turbidity").cast("string"))
df = df.withColumn("chlorophyll", col("chlorophyll").cast("string"))

df.write.format("csv").mode("overwrite").saveAsTable("pentair_rest_workshops.tbl_sensor_data_csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select*
# MAGIC from hive_metastore.pentair_rest_workshops.tbl_sensor_data_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from hive_metastore.pentair_rest_workshops.tbl_sensor_data_parquet