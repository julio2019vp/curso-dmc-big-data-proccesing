# Databricks notebook source
#importacion de librerias
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
from pyspark.sql.functions import regexp_replace,date_format,current_date,add_months,when,col, to_date, year, month, dayofmonth
from datetime import datetime
from dateutil.relativedelta import relativedelta
 

# COMMAND ----------

#variables
spark = SparkSession.builder.getOrCreate()

#Archivo en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_javp"
path_lakehouse=f"gs://{name_bucket}/produccion/dmc"
path_bronze = f"{path_lakehouse}/bronze/personas/"
path_silver = f"{path_lakehouse}/silver/personas/"

#captura del periodo de procesamiento
datetime_server = datetime.now()
periodo_actual = datetime_server.strftime("%Y%m")
datetime_previo_1m = datetime_server - relativedelta(months=1)
periodo_previo_1m_for = datetime_previo_1m.strftime("%Y%m")
print(periodo_actual)
print(periodo_previo_1m_for)



# COMMAND ----------


df = spark.read.format("delta").option("header","true").load(path_bronze)

display(df)

# COMMAND ----------

#Transformacion de datos
df_t = df.withColumn("telefono", regexp_replace('telefono','-',''))\
    .withColumn('PERIODO',date_format(add_months(current_date(),-1),"yyyyMM"))\
    .withColumn('SEGMENTO',when(col("salario")<3500,"Masivo").when((col("salario")>=3500) & (col("salario")<= 10000),"Premiun").otherwise("Beyond"))

#Casteo de datos
df_c = df_t.withColumn("ID",col("ID").cast(IntegerType()))\
    .withColumn("ID_EMPRESA",col("ID_EMPRESA").cast(IntegerType()))\
    .withColumn("EDAD",col("EDAD").cast(IntegerType()))\
    .withColumn("SALARIO",col("SALARIO").cast(DoubleType()))\
    .withColumn("FECHA_INGRESO",to_date(col("fecha_ingreso"), "yyyy-MM-dd"))\
    .withColumn("ANIO",year(col("fecha_ingreso")))\
    .withColumn("MES",month(col("fecha_ingreso")))\
    .withColumn("DIA",dayofmonth(col("fecha_ingreso")))

display(df_c)

# COMMAND ----------

df_c.write.mode("overwrite").partitionBy("periodo").format("delta").save(path_silver)