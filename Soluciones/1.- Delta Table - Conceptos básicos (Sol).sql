-- Databricks notebook source
-- MAGIC %md
-- MAGIC # CONCEPTOS BÁSICOS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Para comenzar, vamos a crear un esquema propio (_schema_nombre_). Sobre este esquema vamos a realizar todos los ejercicios y, así, no se pisarán las tablas entre esquemas. Luego, vamos a decirle a databricks que use el esquema que acabamos de crear. De este modo, aunque se nos olvide escribir el esquema antes de la tabla, databricks sabrá que nos referimos a nuestro esquema.
-- MAGIC
-- MAGIC [Databricks Create Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-schema.html)
-- MAGIC
-- MAGIC [Databricks Use Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-use-schema.html)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS schema_alejandro

-- COMMAND ----------

USE SCHEMA schema_alejandro

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Crear la tabla "**departamentos_delta**" con formato delta sobre nuestro esquema con las siguientes columnas:
-- MAGIC - ID (identificador único, numérico, incremental)
-- MAGIC - NAME (nombre del departamento)
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, numérico)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

DROP TABLE IF EXISTS schema_alejandro.departamentos_delta

-- COMMAND ----------

CREATE TABLE schema_alejandro.departamentos_delta 
(ID LONG GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), NAME STRING, FLOOR INT)
USING delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Insertar los siguientes departamentos a la tabla generada en el paso anterior:
-- MAGIC - Finanzas | 4
-- MAGIC - D&A | 23
-- MAGIC - RRHH | 2
-- MAGIC - Cafetería | 18
-- MAGIC - Ciberseguridad | 31
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

INSERT INTO schema_alejandro.departamentos_delta
(NAME, FLOOR) 
VALUES
  ("Finanzas", 4), ("D&A", 23), ("RRHH", 2), ("Cafetería", 18), ("Ciberseguridad", 31)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Análogo al paso 2, crear la tabla "**departamentos_parquet**" con formato parquet/**, apuntando a "_/mnt/data/departamentos_parquet_" y**/ sobre nuestro esquema con las siguientes columnas:
-- MAGIC - ID (identificador único, numérico, incremental)
-- MAGIC - NAME (nombre del departamento)
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, numérico)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

CREATE TABLE schema_alejandro.departamentos_parquet
(ID LONG , NAME STRING, FLOOR INT)
USING parquet
--LOCATION "s3://databricks-workspace-stack-83c40-bucket/tablas_externas/departamentos_parquet"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Insertar los siguientes departamentos a la tabla generada en el paso anterior:
-- MAGIC - Finanzas | 4
-- MAGIC - D&A | 23
-- MAGIC - RRHH | 2
-- MAGIC - Cafetería | 18
-- MAGIC - Ciberseguridad | 31
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

INSERT INTO schema_alejandro.departamentos_parquet
(ID, NAME, FLOOR) 
VALUES
  (1, "Finanzas", 4), (2, "D&A", 23), (3, "RRHH", 2), (4, "Cafetería", 18), (5, "Ciberseguridad", 31)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- Actualizar la tabla departamentos_delta para que se modifique la fila con el ID 4.
-- MAGIC
-- MAGIC [Databricks Update Table](https://docs.databricks.com/en/sql/language-manual/delta-update.html)

-- COMMAND ----------

UPDATE schema_alejandro.departamentos_delta
SET NAME = 'Cafe', FLOOR = 17
WHERE ID = 4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Al igual que en el paso anterior, hacer lo mismo con la tabla departamentos_parquet para que actualice la fila con el ID 4.
-- MAGIC
-- MAGIC [Databricks Update Table](https://docs.databricks.com/en/sql/language-manual/delta-update.html)

-- COMMAND ----------

UPDATE schema_alejandro.departamentos_parquet
SET NAME = 'Cafe', FLOOR = 17
WHERE ID = 4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #¿Qué sucede?
-- MAGIC > La tabla _departamentos_delta_ está, como su nombre indica, en formato **delta**, mientras que _departamentos_parquet_ está, simplemente, en formato **parquet**. Un archivo parquet es inmutable, así que las tablas en formato parquet, no admiten operaciones del tipo UPDATE/DELETE, puesto que el/los archivo/s parquet afectado/s no pueden tener sus registros modificados. Para realizar esto, habría que volver a escribir la tabla entera.
-- MAGIC Y, ¿por qué funciona con el formato delta si los archivos también son de tipo parquet? Pues esto ocurre porque el formato delta no modifica un parquet ya creado, si no que genera otro parquet y realiza un borrado lógico del parquet antiguo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8.- Vamos a ver qué información nos proporcionan cada una de las tablas. Para este ejercicio necesitaremos las rutas donde se almacenan los datos.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html)

-- COMMAND ----------

DESCRIBE DETAIL schema_alejandro.departamentos_delta

-- COMMAND ----------

DESCRIBE DETAIL schema_alejandro.departamentos_parquet

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9.- Cogiendo la ruta de la tabla _departamentos_delta_ vamos a leer los parquet sueltos.

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.read.parquet("")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10.- Vamos a actualizar la tabla departamentos_delta para que elimine la fila con el ID 1.
-- MAGIC
-- MAGIC [Databricks Delete](https://docs.databricks.com/en/sql/language-manual/delta-delete-from.html)

-- COMMAND ----------

DELETE FROM schema_alejandro.departamentos_delta
WHERE ID = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11.- Por último, queremos ver qué cambios ha sufrido la tabla.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html)

-- COMMAND ----------

DESCRIBE HISTORY schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC También se puede revisar mediante **unity catalog**
