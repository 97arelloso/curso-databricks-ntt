-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1.- Crear un esquema propio (_schema_nombre_). Sobre este esquema se realizarán todos los ejercicios y no se pisarán las tablas entre esquemas. Luego, decirle a databricks que use el esquema que acabamos de crear. De este modo, aunque se nos olvide escribir el esquema antes de la tabla, databricks sabrá que nos referimos a nuestro esquema.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS schema_alejandro

-- COMMAND ----------

USE schema_alejandro

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
(ID INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), NAME STRING, FLOOR INT)
USING delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3.- Insertar los siguientes departamentos a la tabla generada en el paso anterior:
-- MAGIC - Finanzas | 4
-- MAGIC - D&A | 23
-- MAGIC - RRHH | 2-izq

-- COMMAND ----------

INSERT INTO schema_alejandro.departamentos_delta
--(NAME, FLOOR) 
VALUES
  ("Finanzas", 4), ("D&A", 23), ("RRHH", 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Análogo al paso 2, crear la tabla "**departamentos_ext**" con formato parquet sobre nuestro esquema con las siguientes columnas:
-- MAGIC - ID (identificador único, numérico, incremental)
-- MAGIC - NAME (nombre del departamento)
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, numérico)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

CREATE TABLE schema_alejandro.departamentos_ext
(ID INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), NAME STRING, FLOOR INT)
USING parquet
LOCATION "/mnt/data/departamentos_parquet"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5.- Insertar los siguientes departamentos a la tabla generada en el paso anterior:
-- MAGIC - Finanzas | 4
-- MAGIC - D&A | 23
-- MAGIC - RRHH | 2

-- COMMAND ----------

INSERT INTO schema_alejandro.departamentos_ext
--(NAME, FLOOR) 
VALUES
  ("Finanzas", 4), ("D&A", 23), ("RRHH", 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ¿Por qué sucede esto? (Diapo nueva, managed vs external)
-- MAGIC > La tabla _departamentos_delta_ es **managed** (formato **delta**), mientras que _departamentos_ext_ es **externa**. Las tablas externas no aceptan las transacciones ACID, mientras que las managed sí.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6.- Dejando de lado la tabla externa de momento, se van a actualizar las plantas de todos los departamentos inferiores a la planta 15, aumentando su piso en 1.
-- MAGIC
-- MAGIC [Databricks Update Table](https://docs.databricks.com/en/sql/language-manual/delta-update.html)

-- COMMAND ----------

UPDATE schema_alejandro.departamentos_delta
SET floor = floor + 1
WHERE floor < 15
