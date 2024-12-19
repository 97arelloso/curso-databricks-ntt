-- Databricks notebook source
-- MAGIC %md
-- MAGIC # VERSIONES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Cuando toca hablar de versiones toca ver el historial de cambios de la tabla.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/delta/history.html#retrieve-delta-table-history)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # TIME TRAVEL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Vamos a leer la primera inserción de la tabla.
-- MAGIC
-- MAGIC [Read Version](https://docs.databricks.com/en/delta/history.html#delta-time-travel-syntax)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Ahora vamos a leer la versión tras el primer delete, pero vamos a leer por fecha.
-- MAGIC
-- MAGIC [Read Version](https://docs.databricks.com/en/delta/history.html#delta-time-travel-syntax)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Por último vamos a restaurar la tabla a la versión de la primera inserción.
-- MAGIC
-- MAGIC [Restore Version](https://docs.databricks.com/en/delta/history.html#restore-a-delta-table-to-an-earlier-state)
