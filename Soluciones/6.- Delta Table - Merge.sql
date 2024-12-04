-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Merge

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Vamos a crear una tabla análoga a _departamentos_delta_ llamada **departamentos_delta_new**, pero vacía.
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

CREATE TABLE schema_alejandro.departamentos_delta_new
AS SELECT * FROM schema_alejandro.departamentos_delta
WHERE 1 = 2

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

INSERT INTO schema_alejandro.departamentos_delta
(ID, NAME, FLOOR) 
VALUES
  (1, "Finanzas", 4), (2, "D&A", 13), (3, "Recursos Humanos", 2), (11, "Utilities", 12), (12, "Banking", 10)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Supongamos que estos datos son nuevos y hay que insertarlos en la tabla original. Se puede hacer un update y un insert, pero hay una herramienta adecuada para esta tarea, y se trata del **merge**.
-- MAGIC
-- MAGIC [Databricks Merge](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html)

-- COMMAND ----------

MERGE INTO schema_alejandro.departamentos_delta old
USING schema_alejandro.departamentos_delta_new new
ON old.id = new.id
WHEN MATCHED THEN
  UPDATE SET
    id = new.id,
    name = new.name,
    floor = new.floor
WHEN NOT MATCHED
  THEN INSERT (
    id,
    name,
    floor
  )
  VALUES (
    new.id,
    new.name,
    new.floor
  )

-- COMMAND ----------

MERGE INTO schema_alejandro.departamentos_delta old
USING schema_alejandro.departamentos_delta_new new
ON old.id = new.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Vamos a regresar la tabla _departamentos_delta_ a la versión anterior al merge, como hemos visto en el ejercicio _3.- Delta Table - Time Travel_.
-- MAGIC
-- MAGIC [Restore Version](https://docs.databricks.com/en/delta/history.html#restore-a-delta-table-to-an-earlier-state)

-- COMMAND ----------

DESCRIBE HISTORY schema_alejandro.departamentos_delta

-- COMMAND ----------

RESTORE TABLE schema_alejandro.departamentos_delta TO VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Ahora vamos a hacer el mismo merge pero utilizando scala o python.
-- MAGIC
-- MAGIC [Databricks Merge](https://docs.databricks.com/en/delta/merge.html)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import io.delta.tables._
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC
-- MAGIC val deltaTableOld = DeltaTablee.forName(spark, "schema_alejandro.departamentos_delta")
-- MAGIC val deltaTableNew = DeltaTable.forPath(spark, "schema_alejandro.departamentos_delta_new")
-- MAGIC val dfNew = deltaTableNew.toDF()
-- MAGIC
-- MAGIC deltaTableOld
-- MAGIC   .as("old")
-- MAGIC   .merge(
-- MAGIC     dfUpdates.as("new"),
-- MAGIC     "old.id = new.id")
-- MAGIC   .whenMatched
-- MAGIC   .updateExpr(
-- MAGIC     Map(
-- MAGIC       "id" -> "new.id",
-- MAGIC       "name" -> "new.name",
-- MAGIC       "floor" -> "new.floor"
-- MAGIC     ))
-- MAGIC   .whenNotMatched
-- MAGIC   .insertExpr(
-- MAGIC     Map(
-- MAGIC       "id" -> "new.id",
-- MAGIC       "name" -> "new.name",
-- MAGIC       "floor" -> "new.floor"
-- MAGIC     ))
-- MAGIC   .execute()

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import io.delta.tables._
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC
-- MAGIC val deltaTableOld = DeltaTablee.forName(spark, "schema_alejandro.departamentos_delta")
-- MAGIC val deltaTableNew = DeltaTable.forPath(spark, "schema_alejandro.departamentos_delta_new")
-- MAGIC val dfNew = deltaTableNew.toDF()
-- MAGIC
-- MAGIC deltaTableOld
-- MAGIC   .as("old")
-- MAGIC   .merge(
-- MAGIC     dfUpdates.as("new"),
-- MAGIC     "old.id = new.id")
-- MAGIC   .whenMatched
-- MAGIC   .updateAll()
-- MAGIC   .whenNotMatched
-- MAGIC   .insertAll()
-- MAGIC   .execute()
