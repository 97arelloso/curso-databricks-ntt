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
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, que a veces contendrá texto)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)

-- COMMAND ----------

CREATE TABLE schema_alejandro.departamentos_delta 
(ID INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), NAME STRING, FLOOR STRING)
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
  ("Finanzas", "4"), ("D&A", "23"), ("RRHH", "2-izq")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4.- Análogo al paso 2, crear la tabla "**departamentos_ext**" con formato parquet sobre nuestro esquema con las siguientes columnas:
-- MAGIC - ID (identificador único, numérico, incremental)
-- MAGIC - NAME (nombre del departamento)
-- MAGIC - FLOOR (piso en el que se encuentra el departamento, que a veces contendrá texto)
-- MAGIC
-- MAGIC [Databricks Create Table](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)
