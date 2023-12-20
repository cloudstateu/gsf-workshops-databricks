-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

CREATE TABLE workshops.managed_default
  (width INT, length INT, height INT);

INSERT INTO workshops.managed_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED workshops.managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Our Case

-- COMMAND ----------

DESCRIBE EXTENDED piscada.tbl_keyvalue;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## External Table

-- COMMAND ----------

CREATE TABLE workshops.external_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_default';
  
INSERT INTO workshops.external_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED workshops.external_default

-- COMMAND ----------

DROP TABLE workshops.managed_default;
DROP TABLE workshops.external_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Blob Storage

-- COMMAND ----------

CREATE SCHEMA custom
LOCATION 'abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/custom.db'

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED custom.managed_custom;

-- COMMAND ----------

CREATE TABLE custom.external_custom
  (width INT, length INT, height INT)
LOCATION 'abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/custom.db/external_custom';
  
INSERT INTO custom.external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED custom.external_custom;

-- COMMAND ----------

DROP TABLE custom.external_custom;

-- COMMAND ----------

DROP TABLE custom.managed_custom;


-- COMMAND ----------

-- MAGIC %fs ls 'abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/custom.db/external_custom'

-- COMMAND ----------

CREATE TABLE custom.external_custom
  (width INT, length INT, height INT)
LOCATION 'abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/custom.db/external_custom';

-- COMMAND ----------

select * from custom.external_custom;