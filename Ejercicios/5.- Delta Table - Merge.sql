-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Merge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Vamos a crear una tabla análoga a _departamentos_delta_ llamada **departamentos_delta_new**, pero vacía.
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Ahora vamos a insertar los siguientes datos (mismo ID que en la tabla departamentos_delta para cada nombre):
-- MAGIC - Filas sin cambios:
-- MAGIC   - 1 | Finanzas | 4
-- MAGIC - Filas actualizadas:
-- MAGIC   - 2 | D&A | 13
-- MAGIC   - 3 | Recursos Humanos | 2
-- MAGIC - Filas nuevas:
-- MAGIC   - 11 | Utilities | 12
-- MAGIC   - 12 | Banking | 10
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Supongamos que estos datos son nuevos y hay que insertarlos en la tabla original. Se puede hacer un update, un insert y un delete por separado, pero hay una herramienta adecuada para esta tarea, y se trata del **merge**.
-- MAGIC
-- MAGIC [Databricks Merge](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Vamos a regresar la tabla _departamentos_delta_ a la versión anterior al merge, como hemos visto en el ejercicio _3.- Delta Table - Time Travel_.
-- MAGIC
-- MAGIC [Restore Version](https://docs.databricks.com/en/delta/history.html#restore-a-delta-table-to-an-earlier-state)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Ahora vamos a hacer el mismo merge pero utilizando scala o python.
-- MAGIC
-- MAGIC [Databricks Merge](https://docs.databricks.com/en/delta/merge.html)
