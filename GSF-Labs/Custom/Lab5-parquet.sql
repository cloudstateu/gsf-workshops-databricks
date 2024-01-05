-- Databricks notebook source
-- MAGIC %python
-- MAGIC mount_point= "/mnt/parquet"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.unmount(mount_point)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "wasbs://sensor-data@gsfdatalaketest.blob.core.windows.net/workshops/parquet",
-- MAGIC   mount_point = mount_point,
-- MAGIC   extra_configs = {"fs.azure.account.key.gsfdatalaketest.blob.core.windows.net":dbutils.secrets.get(scope="blob", key="key")}
-- MAGIC )

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/parquet'

-- COMMAND ----------

SELECT * FROM parquet. `dbfs:/mnt/parquet/mt_cars.parquet`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read the Parquet file into a DataFrame
-- MAGIC df = spark.read.parquet("dbfs:/mnt/parquet/mt_cars.parquet")
-- MAGIC
-- MAGIC # Display the DataFrame
-- MAGIC display(df)

-- COMMAND ----------

SELECT * FROM parquet. `dbfs:/mnt/parquet/Williamson.parquet`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read the Parquet file into a DataFrame
-- MAGIC df = spark.read.parquet("dbfs:/mnt/parquet/Williamson.parquet")
-- MAGIC
-- MAGIC # Display the DataFrame
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import shutil
-- MAGIC
-- MAGIC # Copy the file from DBFS to local file system
-- MAGIC dbutils.fs.cp("dbfs:/mnt/parquet/Williamson.parquet", "file:/tmp/Williamson.parquet")
-- MAGIC
-- MAGIC # Read the Parquet file into a pandas DataFrame
-- MAGIC df = pd.read_parquet("/tmp/Williamson.parquet")
-- MAGIC
-- MAGIC # Display the DataFrame
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/parquet/csv-input'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # read from blob csv files 
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC # Create a SparkSession
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC # Define the file paths
-- MAGIC file_paths = [
-- MAGIC     "dbfs:/mnt/parquet/csv-input/research-and-development-survey-2022.csv"
-- MAGIC ]
-- MAGIC
-- MAGIC # Read the CSV files into a DataFrame
-- MAGIC df = spark.read.option("header", "true").csv(file_paths)
-- MAGIC
-- MAGIC # Show the DataFrame
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode('overwrite').parquet("dbfs:/mnt/parquet/parquet-output/research-and-development-survey-2022-parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Please note that using `coalesce(1)` can cause performance issues for large DataFrames because it forces all data to be processed on a single node. Use this method with caution and only when you're sure that the DataFrame can fit into the memory of a single node.
-- MAGIC
-- MAGIC Also, note that the `overwrite` mode will overwrite the existing Parquet file if it exists. If you don't want to overwrite the existing file, you can remove the `.mode('overwrite')` part from the code.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Convert the DataFrame to a Parquet file and save it into a specific folder
-- MAGIC df.coalesce(1).write.mode('overwrite').parquet("dbfs:/mnt/parquet/parquet-output/research-and-development-survey-2022-coalesce")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Convert the DataFrame to a Parquet file and save it into a specific folder
-- MAGIC df.write.partitionBy("Year").mode('overwrite').parquet("dbfs:/mnt/parquet/parquet-output/research-and-development-survey-2022-partitionBy")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC # Convert the Spark DataFrame to a pandas DataFrame
-- MAGIC pandas_df = df.toPandas()
-- MAGIC
-- MAGIC # Write the pandas DataFrame to a Parquet file
-- MAGIC pandas_df.to_parquet("/dbfs/mnt/parquet/parquet-output/research-and-development-survey-2022.parquet", engine='pyarrow')