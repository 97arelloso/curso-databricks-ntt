-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SCHEMA EVOLUTION

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Para estos ejercicios vamos a tomar como referencia las tablas creadas en el ejercicio anterior (se va a ejecutar todo sobre ambas tablas). Veamos qué estructura tienen.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html)

-- COMMAND ----------

DESCRIBE schema_alejandro.departamentos_delta

-- COMMAND ----------

DESCRIBE schema_alejandro.departamentos_parquet

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Nos surge la necesidad de cambiar el nombre de la columna NAME a DEPT_NAME y FLOOR a DEPT_FLOOR.
-- MAGIC
-- MAGIC [Databricks Rename Column](https://docs.databricks.com/en/delta/column-mapping.html#rename-a-column)

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_delta RENAME COLUMN NAME TO DEPT_NAME;
ALTER TABLE schema_alejandro.departamentos_delta RENAME COLUMN FLOOR TO DEPT_FLOOR

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_parquet RENAME COLUMN NAME TO DEPT_NAME;
ALTER TABLE schema_alejandro.departamentos_parquet RENAME COLUMN FLOOR TO DEPT_FLOOR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Ahora nos piden que quieren eliminar la columna FLOOR, que carece de utilidad.
-- MAGIC
-- MAGIC [Databricks Drop Column](https://docs.databricks.com/en/delta/column-mapping.html#drop-columns)

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_delta DROP COLUMN DEPT_FLOOR

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_parquet DROP COLUMN FLOOR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Se han dado cuenta de que sí que necesitaban esa columna, así que vamos a crearla.
-- MAGIC
-- MAGIC [Databricks Alter Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table.html)

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_delta ADD COLUMN DEPT_FLOOR INT

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_parquet ADD COLUMN DEPT_FLOOR INT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC AL FINAL VER QUÉ TIENE CADA ARCHIVO PARQUET Y EL DELTA LOG

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SCHEMA ENFORCEMENT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Vamos a comprobar cómo funciona el schema enforcement en cada una de las tablas. Para ello primero vamos a ver qué estructura tienen.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html)

-- COMMAND ----------

DESCRIBE schema_alejandro.departamentos_delta

-- COMMAND ----------

DESCRIBE schema_alejandro.departamentos_parquet

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- En código Spark vamos a crear un DataFrame que tenga las siguientes columnas: nombre_persona, num_empleado. Y vamos a insertar 2 filas: (Juan, 0021), (Miguel, 0038). Por último, hacemos un append sobre ambas tablas.
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC columns = ["first_name", "favorite_color"]
-- MAGIC data = [("sal", "red"), ("cat", "pink")]
-- MAGIC rdd = spark.sparkContext.parallelize(data)
-- MAGIC df = rdd.toDF(columns)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.write.mode("append").insertInto("schema_alejandro.departamentos_delta")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.write.mode("append").insertInto("schema_alejandro.departamentos_parquet")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Comprobamos cómo se han escrito los datos en la tabla _departamentos_parquet_

-- COMMAND ----------

SELECT * FROM schema_alejandro.departamentos_parquet

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.read.option("mergeSchema", "true").format("parquet").load(
-- MAGIC     "schema_alejandro.departamentos_parquet"
-- MAGIC ).show()
-- MAGIC spark.read.option("mergeSchema", "true").table("schema_alejandro.departamentos_parquet").show()