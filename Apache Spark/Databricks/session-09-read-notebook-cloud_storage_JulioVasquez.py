# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType


spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

ruta = 'gs://dmc_datalake_dde_11_javp/archivos/persona.data'

# COMMAND ----------

df_schema = StructType([
StructField("ID", StringType(),True),
StructField("NOMBRE", StringType(),True),
StructField("TELEFONO", StringType(),True),
StructField("CORREO", StringType(),True),
StructField("FECHA_INGRESO", StringType(),True),
StructField("EDAD", IntegerType(),True),
StructField("SALARIO", DoubleType(),True),
StructField("ID_EMPRESA", StringType(),True),
])

df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(ruta)

# COMMAND ----------

display(df)

# COMMAND ----------

rutafifa = 'gs://dmc_datalake_dde_11_javp/archivos/fifa_ranking.csv'

df_fifa = spark.read.format("CSV").option("header","true").option("delimiter",",").load(rutafifa)

display(df_fifa)

df_fifa.printSchema()
