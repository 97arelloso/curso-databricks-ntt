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
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS squirrell_census_location
# MAGIC URL 's3://databricks-workspace-stack-be532-bucket/squirrell_census'
# MAGIC WITH (STORAGE CREDENTIAL nttdata_databricks_lab)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS squirrell_census.squirrell_data
# MAGIC LOCATION 's3://databricks-workspace-stack-be532-bucket/squirrell_census'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/nttdata_databricks_lab/squirrell_census/squirrell_data'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### BRONZE LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a definir la capa "bronze". Van a ser cargas en streaming mediante el Auto Loader de Databricks. Cada tabla va a tener el sufijo "__bronze_" para diferenciar cada capa.
# MAGIC
# MAGIC [Databricks Auto Loader](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html)
# MAGIC
# MAGIC [Databricks Auto Loader Options](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html#common-auto-loader-options)

# COMMAND ----------

def bronze_layer(volume, tableName):
    path = f"{volume}/{tableName}/*"
    tableNameUnderscore = tableName.replace("-", "_")

    @dlt.table(
        name=f"{tableNameUnderscore}_bronze",
        comment=f"Bronze layer for {tableNameUnderscore}",
        table_properties={"layer": "bronze"}
        )
    def incremental_to_bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .load(path)
            .withColumn("bronze_timestamp", F.current_timestamp())
        )

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora, vamos a ejecutar el método de arriba para ingestar todos los archivos.

# COMMAND ----------

volume = "/Volumes/nttdata_databricks_lab/squirrell_census/squirrell_data/raw_data"

list_table = dbutils.fs.ls(volume)
for table in list_table:
    table_name = table[1].replace("/","")
    bronze_layer(volume, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SILVER LAYER

# COMMAND ----------

def silver_layer(schema, tableName, pk, sequenceBy, expectations):
  dlt.create_streaming_table(
      name=f"{tableName}_silver",
      comment=f"Silver layer for {tableName}",
      table_properties={"layer": "silver", "primary key": pk},
      expect_all_or_drop=expectations
  )
  return dlt.apply_changes(
    target=f"{tableName}_silver",
    source=f"{tableName}_bronze",
    keys=pk,
    sequence_by=sequenceBy
  )

# COMMAND ----------

schema = "squirrell_census"

tables = {
    "park_data": {
        "pk": "park_id",
        "sequenceBy": "bronze_timestamp",
        "expectations": {
            "valid_pk": "park_id is NOT NULL"
        }
    },
    "squirrell_data": {
        "pk": "park_id",
        "sequenceBy": "bronze_timestamp",
        "expectations": {
            "valid_pk": "park_id is NOT NULL"
        }
    }
}

for table, config in tables.items():
    primaryKey = config["pk"]
    sequenceBy = config["sequenceBy"]
    expectations = config["expectations"]
    silver_layer(schema, table, primaryKey, sequenceBy, expectations)

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC Necesitamos crear una tabla que nos diga cuántas ardillas hay por parque.

# COMMAND ----------

@dlt.table(
  name="squirrell_count_gold",
  comment="Información con el número de ardillas por parque",
  table_properties={"layer": "gold", "tables_used": "park_data, squirrell_data"}
)

def squirrell_gold():
  dfSquirrellCount = (dlt.read("squirrell_data_silver")
                   .groupBy("park id")
                   .agg(F.count("squirrell id").alias("squirrell count"))
  )
  dfParkData = (dlt.read("park_data_silver")
              .select("park id", "park name")
  )
  return (dfSquirrellCount
          .join(dfParkData, "park id", "left")
          )

# COMMAND ----------

# MAGIC %md
# MAGIC También se requiee una sola tabla con toda la infomación disponible de las 3 tablas.

# COMMAND ----------

@dlt.table(
  name="squirrell_data_gold",
  comment="Información con toda la información sobre las ardillas, parques e historias",
  table_properties={"layer": "gold", "tables_used": "park_data, squirrell_data, stories"}
)

def squirrell_gold():
  dfSquirrellData = (dlt.read("squirrell_data_silver")
  )
  dfParkData = (dlt.read("park_data_silver")
              .select("park id", "park name")
  )
  dfStoriesData = (dlt.read("stories_silver")
               .select("park id", "Squirrels, Parks & The City Stories")
  )
  return (dfSquirrellData
          .join(dfParkData, "park id", "left")
          .join(dfStoriesData, "park id", "left")
          .select("squirrell id", "Primary Fur Color", "Squirrel Latitude (DD.DDDDDD)", "Squirrel Longitude (-DD.DDDDDD)", "park name", "Squirrels, Parks & The City Stories")
          )
