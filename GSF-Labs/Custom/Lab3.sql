-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Views

-- COMMAND ----------

CREATE SCHEMA views
LOCATION 'abfss://sensor-data@gsfdatalaketest.dfs.core.windows.net/views'

-- COMMAND ----------

use views;
CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE TEMP VIEW temp_view_phones_brands
AS  SELECT DISTINCT brand
    FROM smartphones;

SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

SHOW TABLES;