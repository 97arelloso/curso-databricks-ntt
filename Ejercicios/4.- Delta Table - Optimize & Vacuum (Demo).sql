-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DEMO OPTIMIZE & VACUUM

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Creamos una tabla llamada **prueba_optimize** que tenga la columna ID numérica.
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Escribimos en la tabla con un repartition para que escriba más de un parquet.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Ejecutamos el comando optimize que nos comprime los archivos parquet.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Ahora vamos a eliminar los archivos sobrantes, que ya no se utilizan. Para ello ejecutaremos el comando vacuum.``

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Cambiamos las propiedades de la tabla para quitarle la propiedad de retención de archivos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- Ahora sí que podemos ejecutar el vacuum para eliminar los archivos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como alternativa, podemos ejecutar el vacuum expecificándole el periodo de retención.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Para comprobar qué hemos hecho, vamos a ver el historial de cambios de la tabla, además de comprobar el bucket de S3.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/delta/history.html#retrieve-delta-table-history)
