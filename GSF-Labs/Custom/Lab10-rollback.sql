-- Databricks notebook source
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
-- MAGIC df = df.withColumn("year", F.date_format("measured_at", 'yyyy'))
-- MAGIC df = df.withColumn("month", F.date_format("measured_at", 'MM'))
-- MAGIC
-- MAGIC # Convert 'year' and 'month' to IntegerType
-- MAGIC df = df.withColumn("year", col("year").cast("int"))
-- MAGIC df = df.withColumn("month", col("month").cast("int"))
-- MAGIC
-- MAGIC # Write the DataFrame to the table
-- MAGIC df.write.format("delta").mode("overwrite").partitionBy("year", "month").saveAsTable("pentair_rest_workshops.tbl_sensor_data_rollback")

-- COMMAND ----------

DESCRIBE HISTORY pentair_rest_workshops.tbl_sensor_data_rollback

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import when, col
-- MAGIC
-- MAGIC
-- MAGIC display(df)
-- MAGIC df = df.withColumn("oxygen_saturation", 
-- MAGIC                    when(col("oxygen_saturation").isNotNull(), col("oxygen_saturation") + 10000.0)
-- MAGIC                    .otherwise(col("oxygen_saturation")))
-- MAGIC display(df)
-- MAGIC
-- MAGIC df.write.format("delta").mode("overwrite").partitionBy("year", "month").saveAsTable("pentair_rest_workshops.tbl_sensor_data_rollback")

-- COMMAND ----------

DESCRIBE HISTORY pentair_rest_workshops.tbl_sensor_data_rollback;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.table("pentair_rest_workshops.tbl_sensor_data_rollback@v0"))
-- MAGIC display(spark.read.table("pentair_rest_workshops.tbl_sensor_data_rollback@v1"))
-- MAGIC display(spark.read.table("pentair_rest_workshops.tbl_sensor_data_rollback@v4"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("parquet").mode("overwrite").partitionBy("year", "month").saveAsTable("pentair_rest_workshops.tbl_sensor_data_rollback_parquet")

-- COMMAND ----------

DESCRIBE HISTORY pentair_rest_workshops.tbl_sensor_data_rollback_parquet;

-- COMMAND ----------

drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_rollback;
drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_rollback_parquet;