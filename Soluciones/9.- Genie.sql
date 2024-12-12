-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Genie

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creamos un esquema para almacenar los datos.

-- COMMAND ----------

CREATE SCHEMA nttdata_databricks_lab.airbnb_euskadi

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos a tener un volumen en el que carguemos los datos de los airbnb de Euskadi.
-- MAGIC
-- MAGIC /Volumes/nttdata_databricks_lab/airbnb_euskadi/airbnb_euskadi

-- COMMAND ----------

CREATE VOLUME nttdata_databricks_lab.airbnb_euskadi.airbnb_euskadi

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creamos una carpeta dentro del volumen para almacenar los datos.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC mkdir /Volumes/nttdata_databricks_lab/airbnb_euskadi/airbnb_euskadi/listing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Descargamos los datos de muestra que nos ofrece airbnb en la carpeta que acabamos de crear.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd /Volumes/nttdata_databricks_lab/airbnb_euskadi/airbnb_euskadi/listing
-- MAGIC wget https://data.insideairbnb.com/spain/pv/euskadi/2024-09-29/data/listings.csv.gz

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Descomprimimos en archivo descargado.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC gzip -d /Volumes/nttdata_databricks_lab/airbnb_euskadi/airbnb_euskadi/listing/listings.csv.gz

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Comprobamos que esté ahí el archivo.

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd /Volumes/nttdata_databricks_lab/airbnb_euskadi/airbnb_euskadi/listing
-- MAGIC ls -sh 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Leemos el archivo y lo almacenamos en un DataFrame. Adicionalmente, transformamos la columna price para quitar el símbolo del dolar que nos molesta.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dfAirbnbEuskadi = spark.read.csv("/Volumes/nttdata_databricks_lab/airbnb_euskadi/airbnb_euskadi/listing/listings.csv", header=True, inferSchema=True, multiLine=True, escape='"')
-- MAGIC dfAirbnbEuskadi = dfAirbnbEuskadi.withColumn("price", regexp_replace(col("price"), "\\$", "").cast("decimal(10,2)"))
-- MAGIC display(dfAirbnbEuskadi)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Escribimos el DataFrame en el esquema que hemos generado para ello.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dfAirbnbEuskadi.write.format("delta").mode("overwrite").saveAsTable("nttdata_databricks_lab.airbnb_euskadi.listings")
