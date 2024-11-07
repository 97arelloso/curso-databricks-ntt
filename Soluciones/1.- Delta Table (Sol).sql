-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1.- Para comenzar, vamos a crear un esquema propio (_schema_nombre_). Sobre este esquema vamos a realizar todos los ejercicios y, así, no se pisarán las tablas entre esquemas. Luego, decirle a databricks que use el esquema que acabamos de crear. De este modo, aunque se nos olvide escribir el esquema antes de la tabla, databricks sabrá que nos referimos a nuestro esquema.
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
-- MAGIC 4.- Análogo al paso 2, crear la tabla "**departamentos_ext**" con formato parquet, apuntando a "_/mnt/data/departamentos_parquet_" y sobre nuestro esquema con las siguientes columnas:
-- MAGIC - ID (identificador único, numérico, incremental)
-- MAGIC - NAME (nombre del departamento)
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, numérico)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

CREATE EXTERNAL LOCATION departamentos_location
URL 's3://databricks-workspace-stack-83c40-bucket/tablas_externas/departamentos_parquet'
WITH (STORAGE CREDENTIAL ntt_laboratorio_databricks)

-- COMMAND ----------

CREATE TABLE schema_alejandro.departamentos_external
(ID LONG , NAME STRING, FLOOR INT)
USING parquet
LOCATION "s3://databricks-workspace-stack-83c40-bucket/tablas_externas/departamentos_parquet"

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

INSERT INTO schema_alejandro.departamentos_external
(ID, NAME, FLOOR) 
VALUES
  (1, "Finanzas", 4), (2, "D&A", 23), (3, "RRHH", 2), (4, "Cafetería", 18), (5, "Ciberseguridad", 31)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- Actualizar la tabla departamentos_ext para que elimine/actualice la fila de ID 4.

-- COMMAND ----------

UPDATE schema_alejandro.departamentos_external
SET NAME = 'Cafe'
WHERE ID = 4

-- COMMAND ----------

DELETE FROM schema_alejandro.departamentos_external
WHERE ID = 4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ¿Por qué sucede esto?
-- MAGIC > La tabla _departamentos_delta_ es **managed** (formato **delta**), mientras que _departamentos_ext_ es **unmanaged** (**externa**). Las tablas externas no aceptan las transacciones ACID, mientras que las managed sí.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos a ver cómo se comportan los archivos en las tablas externas. Para ello, hay que sacar el detalle de la tabla.
-- MAGIC
-- MAGIC [Databricks Describe Detail](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html#describe-detail)

-- COMMAND ----------

DESCRIBE DETAIL schema_alejandro.departamentos_ext

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ahora, con la ruta sacada en el paso anterior, se va a sacar un listado de los archivos que hay en esa ruta.
-- MAGIC

-- COMMAND ----------

LIST 's3://bucket-name/path/to/data'
%fs ls s3://bucket-name/path/to/data
dbutils.fs.ls("s3://bucket-name/path/to/data")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- Dejando de lado la tabla externa de momento, vamos a ver cómo se comportan los archivos en las tablas delta. Para ello, hay que sacar el detalle de la tabla.
-- MAGIC
-- MAGIC [Databricks Describe Detail](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html#describe-detail)

-- COMMAND ----------

DESCRIBE DETAIL schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Ahora, con la ruta sacada en el paso anterior, se va a sacar un listado de los archivos que hay en esa ruta.
-- MAGIC
-- MAGIC [Databricks dbutils.fs](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/databricks-utils#--file-system-utility-dbutilsfs)

-- COMMAND ----------

LIST 's3://bucket-name/path/to/data'
%fs ls s3://bucket-name/path/to/data
dbutils.fs.ls("s3://bucket-name/path/to/data")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8.- Actualizar las plantas de todos los departamentos inferiores a la planta 15, aumentando su piso en 1.
-- MAGIC
-- MAGIC [Databricks Update Table](https://docs.databricks.com/en/sql/language-manual/delta-update.html)

-- COMMAND ----------

UPDATE schema_alejandro.departamentos_delta
SET floor = floor + 1
WHERE floor < 15

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9.- Vamos a sacar de nuevo el detalle de la tabla y comparamos el número de archivos.

-- COMMAND ----------

DESCRIBE DETAIL schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10.- Tras esta actualización, vamos a volver a sacar un listado de los archivos que hay en la ruta, y comparamos el número de archivos con la ejecución anterior.

-- COMMAND ----------

-- MAGIC %fs ls ''

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ¿Por qué sucede esto?
-- MAGIC
-- MAGIC > Las tablas Delta, gracias al **Transaction Log** (también llamado Delta Log) saben qué archivos tienen que leer. Aunque haya más archivos en la ruta, solo se van a leer los correctos. Si se quiere más información sobre el Transaction Log dejo este artículo interesante: https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11.- Para comprobar esto, vamos a leer el Transaction Log. 

-- COMMAND ----------

-- MAGIC %fs head '.json'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12.- Ahora vamos a sacar el historial de cambios de la tabla.
-- MAGIC
-- MAGIC [Databricks Describe History](https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)

-- COMMAND ----------

DESCRIBE HISTORY schema_alejandro.departamentos_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13.- Ahora vamos a proceder a borrar ambas tablas.
-- MAGIC
-- MAGIC [Databricks Drop Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-drop-table.html)

-- COMMAND ----------

DROP TABLE IF EXISTS schema_alejandro.departamentos_delta

-- COMMAND ----------

DROP TABLE IF EXISTS schema_alejandro.departamentos_ext

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 14.- Y por último, vamos a revisar las rutas donde se alojaban los archivos de las tablas. También podemos comprobar que las tablas ya no están en el esquema.

-- COMMAND ----------

-- MAGIC %fs ls ''

-- COMMAND ----------

-- MAGIC %fs ls ''

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ¿Por qué pasa esto?
-- MAGIC
-- MAGIC > Las tablas externas apuntan a la _location_ que se ha asignado al crear la tabla, y al borrar la tabla no se borran los datos.
