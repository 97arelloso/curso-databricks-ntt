-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1.- Crear la tabla "**conteos_tablas**" con formato delta sobre nuestro esquema con las siguientes columnas:
-- MAGIC - NOMBRE_TABLA (nombre de la tabla)
-- MAGIC - NUM_REGISTROS (conteo de la tabla, numérico)
-- MAGIC - COMENTARIO (comentario en caso de no haber insertado registros en la tabla)
-- MAGIC - FEC_INSERT (fecha en la que se ejecutará el insert, date)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS schema_alejandro.conteos_tablas 
(NOMBRE_TABLA STRING, NUM_REGISTROS INT, COMENTARIO STRING, FEC_INSERT TIMESTAMP DEFAULT SYSDATE)
USING delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Rescatar las variables del notebook "2.1.- Workflows".
-- MAGIC
-- MAGIC [Share information between tasks](https://docs.databricks.com/en/jobs/share-task-context.html)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val tabla = dbutils.jobs.taskValues.get(taskKey = "task1", key = "tabla", default = "error")
-- MAGIC val conteo = dbutils.jobs.taskValues.get(taskKey = "task1", key = "conteo", default = "0")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Insertar los datos obtenidos en la tabla "conteos_tablas".

-- COMMAND ----------

INSERT INTO schema_alejandro.conteos_tablas
--(NOMBRE_TABLA, NUM_REGISTROS) 
VALUES
  (tabla, conteo)
