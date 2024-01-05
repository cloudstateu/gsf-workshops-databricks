-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## CSV
-- MAGIC
-- MAGIC Display csv files from blob storage located in .../workshops/csv folder.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC # display folder /csv on blob storage
-- MAGIC files = dbutils.fs.ls(f"abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL
-- MAGIC Select data from csv files.

-- COMMAND ----------

SELECT * FROM csv. `abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/research-and-development-survey-2022.csv`;

-- COMMAND ----------

SELECT * FROM csv.`abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/overseas-trade-indexes-june-2023-quarter-provisional-*.csv`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pandas
-- MAGIC

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ### abfs - protocol
-- MAGIC
-- MAGIC ABFS stands for Azure Blob File System. It is a protocol used by Azure Data Lake Storage Gen2. The ABFS driver allows for data access and management of Azure Data Lake Storage Gen2. 
-- MAGIC
-- MAGIC There are two types of ABFS URIs:
-- MAGIC
-- MAGIC 1. `abfs://` - This is used when you have set up Azure Data Lake Storage Gen2 with a hierarchical namespace.
-- MAGIC 2. `abfss://` - This is the secure version of the `abfs://` URI scheme, and it is used when you want to access data over HTTPS.
-- MAGIC
-- MAGIC These URIs are used to read from and write to Azure Data Lake Storage Gen2 directly from Apache Spark, Apache Hadoop, and other Apache frameworks.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.pandas as pd
-- MAGIC
-- MAGIC file_paths = [
-- MAGIC     "abfs://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/overseas-trade-indexes-june-2023-quarter-provisional-01.csv",
-- MAGIC     "abfs://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/overseas-trade-indexes-june-2023-quarter-provisional-02.csv"
-- MAGIC ]
-- MAGIC
-- MAGIC # Read the CSV files into DataFrames
-- MAGIC dfs = [pd.read_csv(file_path) for file_path in file_paths]
-- MAGIC
-- MAGIC # Concatenate the DataFrames
-- MAGIC df = pd.concat(dfs, ignore_index=True)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ### Read data from external source.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC # Define the file path
-- MAGIC file_path = "https://api.fiskeridir.no/pub-aqua/api/v1/dump/new-legacy-csv-file"
-- MAGIC
-- MAGIC # Read the CSV file into a DataFrame
-- MAGIC df = pd.read_csv(file_path, sep=';')
-- MAGIC # Display the DataFrame
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Mnt files
-- MAGIC
-- MAGIC Pandas dosen't support abfs protocol, so we need to mnt storage to read from blob.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC mount_point= "/mnt/sensor-data/csv"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "wasbs://sensor-data@gsfdatalaketest.blob.core.windows.net/workshops/csv",
-- MAGIC   mount_point = mount_point,
-- MAGIC   extra_configs = {"fs.azure.account.key.gsfdatalaketest.blob.core.windows.net":dbutils.secrets.get(scope="blob", key="key")}
-- MAGIC )

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/sensor-data/csv'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC # Define the file path
-- MAGIC file_path = "/dbfs/mnt/sensor-data/csv/annual-enterprise-survey-2021-financial-year-provisional-csv.csv"
-- MAGIC # Read the CSV file into a DataFrame
-- MAGIC df = pd.read_csv(file_path)
-- MAGIC # Display the DataFrame
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Edit DataFrame
-- MAGIC df['Industry_name_NZSIOC'] = "all"
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write the DataFrame to the CSV file
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Write the DataFrame to a local CSV file
-- MAGIC df.to_csv("/dbfs/annual-enterprise-survey-2021-financial-year-provisional-csv-edit.csv", index=False)
-- MAGIC dbutils.fs.cp("dbfs:/annual-enterprise-survey-2021-financial-year-provisional-csv-edit.csv",
-- MAGIC                "dbfs:/mnt/sensor-data/csv/annual-enterprise-survey-2021-financial-year-provisional-csv-edit.csv")

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/'

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/annual-enterprise-survey-2021-financial-year-provisional-csv-edit.csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pyspark
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/full.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import csv
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC def convert_path(path):
-- MAGIC     """ Converts a file path from "/dbfs/{path}" to "dbfs:/{path}" """
-- MAGIC     if path.startswith("/dbfs/"):
-- MAGIC         return "dbfs:" + path[5:]
-- MAGIC     else:
-- MAGIC         return path
-- MAGIC
-- MAGIC def spark_to_csv(df, local_name, blob_path):
-- MAGIC     """ Converts spark dataframe to CSV file """
-- MAGIC     full_local_path = f"/dbfs/{local_name}"
-- MAGIC     convert_local_path = f"{convert_path(full_local_path)}"
-- MAGIC     full_blob_path = f"dbfs:/mnt/sensor-data/csv/{blob_path}"
-- MAGIC     
-- MAGIC     df = df.toPandas()
-- MAGIC     df.to_csv(full_local_path, index=False)
-- MAGIC     dbutils.fs.cp(convert_local_path, full_blob_path)
-- MAGIC     print(f"saved into: {full_blob_path}")  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/mnt/sensor-data/csv/separate/full.csv")
-- MAGIC dbutils.fs.rm("dbfs:/mnt/sensor-data/csv/separate/full-csv", recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # read from blob csv files 
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Create a SparkSession
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC
-- MAGIC # Define the file paths
-- MAGIC file_paths = [
-- MAGIC     "abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/overseas-trade-indexes-june-2023-quarter-provisional-01.csv",
-- MAGIC     "abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/overseas-trade-indexes-june-2023-quarter-provisional-02.csv"
-- MAGIC ]
-- MAGIC
-- MAGIC # Read the CSV files into a DataFrame
-- MAGIC df = spark.read.option("header", "true").csv(file_paths)
-- MAGIC
-- MAGIC # Show the DataFrame
-- MAGIC display(df)
-- MAGIC
-- MAGIC # # # Write the DataFrame to a CSV file in Azure Blob Storage
-- MAGIC df.write.option("header", "true").csv("abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/full-csv")
-- MAGIC
-- MAGIC # # # Write the DataFrame to a parquet file in Azure Blob Storage
-- MAGIC df.write.option("header", "true").save("abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/full-parquet")
-- MAGIC
-- MAGIC # save to single csv file
-- MAGIC spark_to_csv(df, "full.csv", "separate/full.csv")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # read from blob storage 
-- MAGIC df = spark.read.option("header", "true").csv("abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/full-csv")
-- MAGIC # display the DataFrame
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # read from blob storage 
-- MAGIC df = spark.read.option("header", "true").parquet("abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/full-parquet")
-- MAGIC # display the DataFrame
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # read from blob storage 
-- MAGIC df = spark.read.option("header", "true").csv("abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/csv/separate/full.csv")
-- MAGIC # display the DataFrame
-- MAGIC display(df)