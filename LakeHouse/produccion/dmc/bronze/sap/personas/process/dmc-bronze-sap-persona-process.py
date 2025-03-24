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
path_persona_landing = f"{path_lakehouse}/landing/personas/persona.data"
path_persona_bronze = f"{path_lakehouse}/bronze/personas/"




# COMMAND ----------

#definicion de la columna
#todo debe estar en string
df_schema = StructType([
StructField("ID", StringType(),True),
StructField("NOMBRE", StringType(),True),
StructField("TELEFONO", StringType(),True),
StructField("CORREO", StringType(),True),
StructField("FECHA_INGRESO", StringType(),True),
StructField("EDAD", StringType(),True),
StructField("SALARIO", StringType(),True),
StructField("ID_EMPRESA", StringType(),True),
])

# COMMAND ----------

#Leer el archivo de origen
df_personas = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(path_persona_landing)

display(df_personas)

# COMMAND ----------

df_personas.write.mode("overwrite").format("delta").save(path_persona_bronze)