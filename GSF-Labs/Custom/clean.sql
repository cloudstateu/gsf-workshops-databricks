-- Databricks notebook source
drop schema hive_metastore.pentair_rest_workshops

-- COMMAND ----------

drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_csv;
drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_delta;
drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_delta_c;
drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_delta_c5;
drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_json;
drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_p;
drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_parquet;
drop table hive_metastore.pentair_rest_workshops.tbl_sensor_data_parquet_c;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC mount_point= "/mnt/parquet"
-- MAGIC dbutils.fs.unmount(mount_point)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC mount_point= "/mnt/sensor-data/csv"
-- MAGIC dbutils.fs.unmount(mount_point)