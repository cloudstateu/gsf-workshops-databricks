-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Run other notebook

-- COMMAND ----------

-- MAGIC %run ../Includes/Setup
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(full_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Schema

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE SCHEMA workshops

-- COMMAND ----------

truncate table workshops.employees;
drop table workshops.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE workshops.employees (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT INTO workshops.employees
-- MAGIC VALUES 
-- MAGIC   (1, "Adam", 3500.0),
-- MAGIC   (2, "Sarah", 4020.5),
-- MAGIC   (3, "John", 2999.3),
-- MAGIC   (4, "Thomas", 4000.3),
-- MAGIC   (5, "Anna", 2500.0),
-- MAGIC   (6, "Kim", 6200.3);
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM workshops.employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Metadata

-- COMMAND ----------

DESCRIBE DETAIL workshops.employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/workshops.db/employees'

-- COMMAND ----------

DESCRIBE HISTORY workshops.employees

-- COMMAND ----------

UPDATE  workshops.employees 
SET salary = salary + 100
WHERE name LIKE "A%"