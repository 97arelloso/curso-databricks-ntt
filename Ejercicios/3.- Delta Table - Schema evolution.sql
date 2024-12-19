-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SCHEMA EVOLUTION

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1.- Para estos ejercicios vamos a tomar como referencia las tablas creadas en el ejercicio anterior (se va a ejecutar todo sobre ambas tablas). Veamos qué estructura tienen.
-- MAGIC
-- MAGIC [Databricks Describe Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2.- Nos surge la necesidad de cambiar el nombre de la columna NAME a DEPT_NAME y FLOOR a DEPT_FLOOR.
-- MAGIC
-- MAGIC [Databricks Rename Column](https://docs.databricks.com/en/delta/column-mapping.html#rename-a-column)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Ahora nos piden que quieren eliminar la columna FLOOR, que carece de utilidad.
-- MAGIC
-- MAGIC [Databricks Drop Column](https://docs.databricks.com/en/delta/column-mapping.html#drop-columns)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Se han dado cuenta de que sí que necesitaban esa columna, así que vamos a crearla.
-- MAGIC
-- MAGIC [Databricks Alter Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CHECK CONSTRAINTS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Nos surge ahora la necesidad de meter una condición a la tabla a la hora de insertar los datos. No queremos que el piso sea mayor que 30.
-- MAGIC
-- MAGIC [Databricks Check Constraints](https://docs.databricks.com/en/tables/constraints.html#set-a-check-constraint-in-databricks)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- El cliente nos está volviendo locos en este ejercicio... Ahora nos piden que el ID no puede ser mayor de 30.
-- MAGIC
-- MAGIC [Databricks Check Constraints](https://docs.databricks.com/en/tables/constraints.html#set-a-check-constraint-in-databricks)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7.- Vamos a ver las propiedades de la tabla para comprobar que esté metida la condición.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8.- Para probar esta funcionalidad, vamos a generar un par de filas: una que cumpla la condición y otra que no.
-- MAGIC
-- MAGIC [Databricks Insert Into](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-dml-insert-into.html#insert-into)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SCHEMA ENFORCEMENT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9.- En python/scala vamos a crear un DataFrame que tenga las siguientes columnas: nombre (string) y edad (int). Y vamos a insertar un par de filas sobre la ruta /tmp/parquet_table_**nombre**.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10.- Ahora vamos a insertar sobre esa misma tabla otro par de registros, pero las columnas van a ser: nombre (string) y edad (int).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11.- Leemos los datos a ver con qué nos encontramos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12.- Ahora vamos a generar el mismo DataFrame que antes, pero lo vamos a insertar en una tabla delta en la ruta /tmp/delta_table_**nombre**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13.- Ahora vamos a insertar sobre esa misma tabla otro par de registros, pero las columnas van a ser: nombre (string) y edad (int).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 14.- Para poder escribir en la tabla necesitaremos especificarlo explícitamente, así que vamos a probar a hacerlo.
-- MAGIC
-- MAGIC [Databricks Merge Schema](https://docs.databricks.com/en/delta/update-schema.html#enable-schema-evolution)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 15.- Leemos los datos a ver con qué nos encontramos.
