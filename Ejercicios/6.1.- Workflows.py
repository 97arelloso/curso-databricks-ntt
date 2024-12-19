# Databricks notebook source
# MAGIC %md
# MAGIC 1.- Este notebook se ejecutará desde el Workflow. Desde el workflow pasaremos la variable "tabla", sobre la cual ejecutaremos un conteo.
# MAGIC
# MAGIC [Get task parameter](https://docs.databricks.com/en/dev-tools/databricks-utils.html#widgets-utility-dbutilswidgets)

# COMMAND ----------

# MAGIC %md
# MAGIC 2.- Ahora vamos a guardar ese resultado en una variable para pasársela a otra actividad/task. También vamos a pasar el nombre de la tabla.
# MAGIC
# MAGIC [Share information between tasks](https://docs.databricks.com/en/jobs/share-task-context.html)
