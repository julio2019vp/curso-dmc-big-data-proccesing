# Databricks notebook source
#importacion de librerias
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType

# COMMAND ----------

#variables
spark = SparkSession.builder.getOrCreate()

#Archivo en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_javp"
path_lakehouse=f"gs://{name_bucket}/produccion/dmc"
path_landing = f"{path_lakehouse}/landing/empresas/empresa.data"
path_bronze = f"{path_lakehouse}/bronze/empresas/"




# COMMAND ----------

#definicion de la columna
#todo debe estar en string
df_schema = StructType([
StructField("ID", StringType(),True),
StructField("EMPRESA_NAME", StringType(),True)
])

# COMMAND ----------

#Leer el archivo de origen
df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(path_landing)

display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").save(path_bronze)