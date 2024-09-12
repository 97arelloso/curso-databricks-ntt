// Databricks notebook source
// MAGIC %md
// MAGIC 1.- Vamos a sacar y guardar en una variable el conteo de la tabla que pasemos por parámetro a la actividad/task.
// MAGIC
// MAGIC [Get task parameter](https://docs.databricks.com/en/dev-tools/databricks-utils.html#widgets-utility-dbutilswidgets)

// COMMAND ----------

val tabla = dbutils.widgets.get("tabla")
val conteo = spark.sql(s"select count * from {tabla}")

// COMMAND ----------

// MAGIC %md
// MAGIC 2.- Ahora vamos a guardar ese resultado en una variable para pasársela a otra actividad/task. También vamos a pasar el nombre de la tabla.
// MAGIC
// MAGIC [Share information between tasks](https://docs.databricks.com/en/jobs/share-task-context.html)

// COMMAND ----------

dbutils.jobs.taskValues.set(key = "tabla", value = "samples.tpch.nation")
dbutils.jobs.taskValues.set(key = "conteo", value = conteo)
