// Databricks notebook source
// MAGIC %md
// MAGIC 1.- Vamos a sacar y guardar en una variable el conteo de la tabla _samples.tpch.nation_ (dada por defecto por databricks).

// COMMAND ----------

val conteo = spark.sql("select count * from samples.tpch.nation")

// COMMAND ----------

// MAGIC %md
// MAGIC 2.- Ahora vamos a guardar ese resultado en una variable para pasársela a otra actividad/task. También vamos a pasar el nombre de la tabla.
// MAGIC
// MAGIC [Share information between tasks](https://docs.databricks.com/en/jobs/share-task-context.html)

// COMMAND ----------

dbutils.jobs.taskValues.set(key = "tabla", value = "samples.tpch.nation")
dbutils.jobs.taskValues.set(key = "conteo", value = conteo)
