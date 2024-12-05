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

DESCRIBE schema_alejandro.departamentos_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Nos surge la necesidad de cambiar el nombre de la columna NAME a DEPT_NAME y FLOOR a DEPT_FLOOR.
-- MAGIC
-- MAGIC [Databricks Rename Column](https://docs.databricks.com/en/delta/column-mapping.html#rename-a-column)

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_delta 
SET TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
);

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_delta RENAME COLUMN NAME TO DEPT_NAME;
ALTER TABLE schema_alejandro.departamentos_delta RENAME COLUMN FLOOR TO DEPT_FLOOR

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_external RENAME COLUMN NAME TO DEPT_NAME;
ALTER TABLE schema_alejandro.departamentos_external RENAME COLUMN FLOOR TO DEPT_FLOOR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Ahora nos piden que quieren eliminar la columna FLOOR, que carece de utilidad.
-- MAGIC
-- MAGIC [Databricks Drop Column](https://docs.databricks.com/en/delta/column-mapping.html#drop-columns)

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_delta DROP COLUMN DEPT_FLOOR

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_external DROP COLUMN FLOOR

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Se han dado cuenta de que sí que necesitaban esa columna, así que vamos a crearla.
-- MAGIC
-- MAGIC [Databricks Alter Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table.html)

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_delta ADD COLUMN DEPT_FLOOR INT

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_external ADD COLUMN DEPT_FLOOR INT

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

DESCRIBE schema_alejandro.departamentos_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- En código Spark vamos a crear un DataFrame que tenga las siguientes columnas: id, nombre_persona, cargo. Y vamos a insertar 2 filas: (21052, Juan, IT), (32534, Miguel, Secretaría). Por último, hacemos un append sobre ambas tablas.
-- MAGIC

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val columns = Seq("id_dept", "nombre_persona", "cargo")
-- MAGIC val data = Seq(("21052", "Juan", "IT"), ("32534", "Miguel", "Secretaría"))
-- MAGIC val rdd = spark.sparkContext.parallelize(data)
-- MAGIC val df = rdd.toDF(columns:_*)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.write.mode("append").insertInto("schema_alejandro.departamentos_delta")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val columns = Seq("id_dept", "nombre_persona", "cargo", "otro")
-- MAGIC val data = Seq(("21052", "Juan", "IT", "a"), ("32534", "Miguel", "Secretaría", "b"))
-- MAGIC val rdd = spark.sparkContext.parallelize(data)
-- MAGIC val df2 = rdd.toDF(columns:_*)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df2.write.mode("append").insertInto("schema_alejandro.departamentos_external")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Comprobamos cómo se han escrito los datos en la tabla _departamentos_parquet_
-- MAGIC
-- MAGIC [Databricks Merge Schema](https://docs.databricks.com/en/delta/update-schema.html#enable-schema-evolution)

-- COMMAND ----------

SELECT * FROM schema_alejandro.departamentos_external

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.read.option("mergeSchema", "true").format("parquet").load(
-- MAGIC     "schema_alejandro.departamentos_external"
-- MAGIC ).show()
-- MAGIC spark.read.option("mergeSchema", "true").table("schema_alejandro.departamentos_external").show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8.- Para poder escribir en la tabla _departamentos_delta_ necesitaremos especificarlo explícitamente, así que vamos a probar a hacerlo.
-- MAGIC
-- MAGIC [Databricks Merge Schema](https://docs.databricks.com/en/delta/update-schema.html#enable-schema-evolution)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.write.option("mergeSchema", "true").mode("append").format("delta").save(
-- MAGIC     "schema_alejandro.departamentos_delta"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CHECK CONSTRAINTS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9.- Nos surge ahora la necesidad de meter una condición a la tabla a la hora de insertar los datos. No queremos que el piso sea mayor que 30.
-- MAGIC
-- MAGIC [Databricks Check Constraints](https://docs.databricks.com/en/tables/constraints.html#set-a-check-constraint-in-databricks)

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_delta ADD CONSTRAINT pisoMenor31 CHECK (DEPT_FLOOR <= 30)

-- COMMAND ----------

ALTER TABLE schema_alejandro.departamentos_delta ADD CONSTRAINT pisoMenorOIgualQue30 CHECK (DEPT_FLOOR <= 30)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10.- Para probar esta funcionalidad, vamos a generar un par de filas: una que cumpla la condición y otra que no.
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

INSERT INTO schema_alejandro.departamentos_delta
(NAME, FLOOR) 
VALUES
  ("Formación", 5), ("Helipuerto", 31)
