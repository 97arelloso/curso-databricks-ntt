# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion architecture

# COMMAND ----------

# MAGIC %md
# MAGIC El siguiente esquema muestra lo que se vamos a realizar en el siguiente notebook.
# MAGIC
# MAGIC ![my_test_image](files/images/Medallion_architecture_drawio.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Los datos que necesitamos se van a almacenar en S3 (_s3://my-bucket/squirrel_census_), desde donde los cogeremos. Para eso ya se ha creado previamente un volumen externo apuntando a esa ruta en el bucket. Luego vamos a ver qué archivos tenemos ahí.
# MAGIC
# MAGIC [Databricks Volumes](https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS squirrel_census_location
# MAGIC URL 's3://databricks-workspace-stack-be532-bucket/squirrel_census'
# MAGIC WITH (STORAGE CREDENTIAL nttdata_databricks_lab)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS squirrel_census.squirrel_data
# MAGIC LOCATION 's3://databricks-workspace-stack-be532-bucket/squirrel_census'

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/nttdata_databricks_lab/squirrel_census/squirrel_data'

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
# MAGIC 1.- El primer paso va a ser definir la capa "bronze". Van a ser cargas en streaming mediante el Auto Loader de Databricks. Cada tabla va a tener el sufijo "__bronze_" para diferenciar cada capa. Además vamos a añadirle el campo _bronze_timestamp_. Para ello necesitamos crear un método al que le pasemos como input el volumen y el nombre de la tabla. 
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
# MAGIC 2.- Una vez definido el método, vamos a llamarlo pasándole el volumen y las tres tablas.

# COMMAND ----------

volume = "/Volumes/nttdata_databricks_lab/squirrel_census/squirrel_data/raw_data"

list_table = dbutils.fs.ls(volume)
for table in list_table:
    table_name = table[1].replace("/","")
    bronze_layer(volume, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SILVER LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC 3.- Una vez que la capa bronze esté validada vamos a seguir con la silver. Para estas tablas también usaremos cargas en streaming. La primary key para park-data y stories será "park_id" y para squirrel-data será "squirrel_id". Además tenemos que comprobar que las pks no vengan nulas, en caso de que algún registro tenga la pk nula será eliminado.
# MAGIC
# MAGIC Igual que para la capa bronze, necesitamos crear un método al que, esta vez, le pasaremos como input la tabla, la pk, el campo de sequencia y las expectations.
# MAGIC
# MAGIC [Databricks Create Streaming Table](https://docs.databricks.com/en/delta-live-tables/python-ref.html#create-a-table-to-use-as-the-target-of-streaming-operations)

# COMMAND ----------

def silver_layer(tableName, pk, sequenceBy, expectations):
  dlt.create_streaming_table(
      name=f"{tableName}_silver",
      comment=f"Silver layer for {tableName}",
      table_properties={"layer": "silver", "primary key": pk},
      expect_all_or_drop=expectations
  )
  return dlt.apply_changes(
    target=f"{tableName}_silver",
    source=f"{tableName}_bronze",
    keys=[pk],
    sequence_by=F.col(sequenceBy)
  )

# COMMAND ----------

4.- Ahora creamos una variable con las tablas y sus respectivas propiedades, que pasaremos por parametro.

# COMMAND ----------

tables = {
    "park_data": {
        "pk": "park_id",
        "sequenceBy": "bronze_timestamp",
        "expectations": {
            "valid_pk": "park_id is NOT NULL"
        }
    },
    "squirrel_data": {
        "pk": "squirrel_id",
        "sequenceBy": "bronze_timestamp",
        "expectations": {
            "valid_pk": "squirrel_id is NOT NULL"
        }
    },
    "stories": {
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
    silver_layer(table, primaryKey, sequenceBy, expectations)

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC 5.- En esta capa vamos a crear dos tablas. Para la primera necesitamos sacar el número de ardillas que hay por parque con la fecha. (No vale utilizar el campo "number_of_squirrels" de la tabla park-data).

# COMMAND ----------

@dlt.table(
  name="squirrel_count_gold",
  comment="Información con el número de ardillas por parque",
  table_properties={"layer": "gold", "tables_used": "park_data, squirrel_data"}
)

def squirrel_gold():
  dfSquirrelCount = (dlt.read("squirrel_data_silver")
                   .groupBy("park_id")
                   .agg(F.count("squirrel_id").alias("squirrel_count"))
  )
  dfParkData = (dlt.read("park_data_silver")
              .select("park_id", "park_name", "date")
  )
  return (dfSquirrelCount
          .join(dfParkData, "park_id", "left")
          )

# COMMAND ----------

# MAGIC %md
# MAGIC 6.- La segunda tabla que se requiere debe contener la infomación disponible de las 3 tablas y también el conteo de la tabla anterior. Los campos que necesitamos obtener son: squirrel_id, primary_fur_color, squirrel_latitude, squirrel_longitude, park_name, stories y squirrel_count

# COMMAND ----------

@dlt.table(
  name="squirrel_data_gold",
  comment="Información con toda la información sobre las ardillas, parques e historias",
  table_properties={"layer": "gold", "tables_used": "park_data, squirrel_data, stories, squirrel_count_gold"}
)

def squirrel_gold():
  dfSquirrellData = (dlt.read("squirrel_data_silver")
  )
  dfParkData = (dlt.read("park_data_silver")
              .select("park_id", "park_name")
  )
  dfStoriesData = (dlt.read("stories_silver")
               .select("park_id", "stories")
  )
  dfSquirrelCount = (dlt.read("squirrel_count_gold")
               .select("park_id", "squirrel_count")
  )
  return (dfSquirrellData
          .join(dfParkData, "park_id", "left")
          .join(dfStoriesData, "park_id", "left")
          .join(dfSquirrelCount, "park_id", "left")
          .select("squirrel_id", "primary_fur_color", "squirrel_latitude", "squirrel_longitude", dfParkData.park_name, "stories", "squirrel_count")
          )
