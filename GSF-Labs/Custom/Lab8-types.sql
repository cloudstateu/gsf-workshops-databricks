-- Databricks notebook source
-- MAGIC %md
-- MAGIC When writing data to a distributed file system like Azure Blob Storage, Databricks (and Spark in general) creates several metadata files along with the actual data files. These files are used to handle the distributed nature of the write operation and to ensure data consistency.
-- MAGIC
-- MAGIC --_committed_<id>: This file is created by the driver node when a task is successfully completed. The <id> part is a unique identifier for the task. This file is used to indicate that the corresponding task has been committed.
-- MAGIC
-- MAGIC --_started_<id>: This file is created at the start of a task. The <id> part is a unique identifier for the task. This file is used to indicate that a task has started.
-- MAGIC
-- MAGIC --_SUCCESS: This file is created when the entire job is successfully completed. It's an empty file used just as an indicator of successful job completion.
-- MAGIC
-- MAGIC These files are not part of your data and are used internally by Spark/Databricks for handling distributed write operation

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM json. `abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/parquet/pentair_rest_workshops/tbl_sensor_data_json`;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM PARQUET. `abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/parquet/pentair_rest_workshops/tbl_sensor_data_parquet`;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM CSV. `abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/parquet/pentair_rest_workshops/tbl_sensor_data_csv`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When you save data in Delta format, Databricks creates a `_delta_log` directory. This directory is crucial for the functioning of Delta Lake, as it contains all the transaction logs for your Delta table.
-- MAGIC
-- MAGIC Delta Lake is a storage layer that brings ACID (Atomicity, Consistency, Isolation, Durability) transactions to Apache Spark and big data workloads. It's designed to handle large-scale, distributed data and it achieves this by maintaining a transaction log.
-- MAGIC
-- MAGIC The `_delta_log` directory contains a chronological set of JSON files (transaction logs) that keep track of every add, modify, or delete operation that has been performed on the data. This allows for versioning, rollback, and time travel features.
-- MAGIC
-- MAGIC In summary, the `_delta_log` directory is a critical component of Delta Lake's architecture, enabling powerful features like ACID transactions, schema enforcement, and data versioning.

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM DELTA. `abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/workshops/parquet/pentair_rest_workshops/tbl_sensor_data_delta`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The reason you see many Parquet files is likely due to the distributed nature of the write operations. In a distributed computing environment like Databricks, data is partitioned across multiple nodes for processing. Each node writes its own Parquet file, resulting in multiple files.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import shutil
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC
-- MAGIC # Copy the file from DBFS to local file system
-- MAGIC dbutils.fs.cp("dbfs:/mnt/parquet/Williamson.parquet", "file:/tmp/Williamson.parquet")
-- MAGIC
-- MAGIC # Read the Parquet file into a pandas DataFrame
-- MAGIC pandas_df = pd.read_parquet("/tmp/Williamson.parquet")
-- MAGIC # Convert the pandas DataFrame to a PySpark DataFrame
-- MAGIC df = spark.createDataFrame(pandas_df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC
-- MAGIC # Write the DataFrame to the table
-- MAGIC print(df.columns)
-- MAGIC df = df.withColumn("turbidity", col("turbidity").cast("string"))
-- MAGIC df = df.withColumn("chlorophyll", col("chlorophyll").cast("string"))
-- MAGIC
-- MAGIC df.coalesce(1).write.format("delta").mode("overwrite").saveAsTable("pentair_rest_workshops.tbl_sensor_data_delta_c")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC
-- MAGIC # Write the DataFrame to the table
-- MAGIC print(df.columns)
-- MAGIC df = df.withColumn("turbidity", col("turbidity").cast("string"))
-- MAGIC df = df.withColumn("chlorophyll", col("chlorophyll").cast("string"))
-- MAGIC
-- MAGIC df.coalesce(1).write.format("parquet").mode("overwrite").saveAsTable("pentair_rest_workshops.tbl_sensor_data_parquet_c")

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/parquet/pentair_rest_workshops/tbl_sensor_data_parquet_c/'

-- COMMAND ----------

SELECT * FROM parquet. `dbfs:/mnt/parquet/pentair_rest_workshops/tbl_sensor_data_parquet_c/part-00000-tid-5983145304764307180-33b1d83a-6715-46a5-a99b-95b5902cbbdc-10001-1-c000.snappy.parquet`;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/parquet/pentair_rest_workshops/tbl_sensor_data_delta_c/'

-- COMMAND ----------

select * from parquet. `dbfs:/mnt/parquet/pentair_rest_workshops/tbl_sensor_data_delta_c/part-00000-452d8555-3d02-48fe-b506-58c83922a020-c000.snappy.parquet`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC
-- MAGIC # Write the DataFrame to the table
-- MAGIC print(df.columns)
-- MAGIC df = df.withColumn("turbidity", col("turbidity").cast("string"))
-- MAGIC df = df.withColumn("chlorophyll", col("chlorophyll").cast("string"))
-- MAGIC
-- MAGIC df.coalesce(5).write.format("delta").mode("overwrite").saveAsTable("pentair_rest_workshops.tbl_sensor_data_delta_c5")

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/parquet/pentair_rest_workshops/tbl_sensor_data_delta_c5/'

-- COMMAND ----------

select count(*) from parquet. `dbfs:/mnt/parquet/pentair_rest_workshops/tbl_sensor_data_delta_c/part-00000-452d8555-3d02-48fe-b506-58c83922a020-c000.snappy.parquet`;


-- COMMAND ----------

select count(*) from parquet. `dbfs:/mnt/parquet/pentair_rest_workshops/tbl_sensor_data_delta_c5/part-00004-a25be5fc-249e-4cc2-aa17-5e90a8ace12a-c000.snappy.parquet`