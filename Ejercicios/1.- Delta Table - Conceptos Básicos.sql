-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1.- Para comenzar, vamos a crear un esquema propio (_schema_nombre_). Sobre este esquema vamos a realizar todos los ejercicios y, así, no se pisarán las tablas entre esquemas. Luego, decirle a databricks que use el esquema que acabamos de crear. De este modo, aunque se nos olvide escribir el esquema antes de la tabla, databricks sabrá que nos referimos a nuestro esquema.
-- MAGIC
-- MAGIC [Databricks Create Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-schema.html)
-- MAGIC
-- MAGIC [Databricks Use Schema](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-use-schema.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Crear la tabla "**departamentos_delta**" con formato delta sobre nuestro esquema con las siguientes columnas:
-- MAGIC - ID (identificador único, numérico)
-- MAGIC - NAME (nombre del departamento)
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, numérico)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Insertar los siguientes departamentos a la tabla generada en el paso anterior:
-- MAGIC - 1 | Finanzas | 4
-- MAGIC - 2 | D&A | 23
-- MAGIC - 3 | RRHH | 2
-- MAGIC - 4 | Cafetería | 18
-- MAGIC - 5 | Ciberseguridad | 31
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Análogo al paso 2, crear la tabla "**departamentos_external**" con formato parquet, apuntando a "s3://databricks-workspace-stack-be532-bucket/tablas_externas/departamentos_parquet_**nombre**" y sobre nuestro esquema con las siguientes columnas:
-- MAGIC - ID (identificador único, numérico)
-- MAGIC - NAME (nombre del departamento)
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, numérico)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

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

-- MAGIC %md
-- MAGIC 6.- Ahora vamos a actualizar la tabla departamentos_delta para que elimine/actualice la fila de ID 4.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Actualizar la tabla departamentos_external para que elimine/actualice la fila de ID 4.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Paramos aquí
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8.- Vamos a ver qué información nos proporcionan cada una de las tablas. Para este ejercicio necesitaremos las rutas donde se almacenan los datos.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9.- Ahora, con la ruta sacada en el paso anterior, se va a sacar un listado de los archivos que hay en esa ruta.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10.- Dejando de lado la tabla externa de momento, vamos a ver cómo se comportan los archivos en las tablas delta. Para ello, hay que sacar el detalle de la tabla.
-- MAGIC
-- MAGIC [Databricks Describe Detail](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html#describe-detail)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11.- Ahora, con la ruta sacada en el paso anterior, se va a sacar un listado de los archivos que hay en esa ruta.
-- MAGIC
-- MAGIC [Databricks dbutils.fs](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/databricks-utils#--file-system-utility-dbutilsfs)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12.- Actualizamos las plantas de todos los departamentos inferiores a la planta 15, aumentando su piso en 1.
-- MAGIC
-- MAGIC [Databricks Update Table](https://docs.databricks.com/en/sql/language-manual/delta-update.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13.- Ya que no podemos sacar un listado de archivos, podemos ver qué ha pasado con los archivos parquet si entramos en la ruta del s3.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 14.- Para comprobar esto, vamos a leer el Transaction Log. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 15.- Ahora vamos a sacar el historial de cambios de la tabla.
-- MAGIC
-- MAGIC [Databricks Describe History](https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)
