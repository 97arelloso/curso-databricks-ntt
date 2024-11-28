-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DEMO OPTIMIZE & VACUUM

-- COMMAND ----------

CREATE TABLE schema_alejandro.prueba_optimize (
  ID INT
)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = spark.range(0, 5)
-- MAGIC df.repartition(5).write.format("delta").insertInto("schema_alejandro.prueba_optimize")

-- COMMAND ----------

describe history schema_alejandro.prueba_optimize

-- COMMAND ----------

ALTER TABLE schema_alejandro.prueba_optimize SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '0 days');

-- COMMAND ----------

vacuum schema_alejandro.prueba_optimize 
