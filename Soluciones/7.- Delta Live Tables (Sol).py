# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 1.- Los datos que necesitamos se van a almacenar en S3 (_s3://my-bucket/squirrell_census_), desde donde los cogeremos. Para eso ya se ha creado previamente un volumen externo apuntando a esa ruta en el bucket. Luego vamos a ver qué archivos tenemos ahí.
# MAGIC
# MAGIC [Databricks Volumes](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS squirrell_census.squirrell_data
# MAGIC LOCATION 's3://databricks-workspace-stack-be532-bucket/squirrell_census'

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/nttdata_databricks_lab/squirrell_census/squirrell_data'

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora vamos a definir la capa "bronze". 

# COMMAND ----------

def bronze_layer(volume, tableName):
    path = f"{volume}/{tableName}"

    @dlt.table(
        name=f"{tableName}_bronze",
        comment=f"Bronze layer for {tableName}",
        table_properties={"layer": "bronze"}
        )
    def incremental_to_bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .load(path)
            .withColumn("bronze_timestamp", current_timestamp())
        )

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora, vamos a ejecutar el método de arriba para ingestar todos los archivos.

# COMMAND ----------

volume = "Volumes/nttdata_databricks_lab/squirrell_census/squirrell_data"

list_table = dbutils.fs.ls(volume)
for table in list_table:
    table_name = table[1].replace("/","")
    bronze_layer(volume, table_name)
