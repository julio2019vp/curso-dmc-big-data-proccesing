# Databricks notebook source
#importacion de librerias
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
from pyspark.sql.functions import regexp_replace,date_format,current_date,add_months,when,col, to_date, year, month, dayofmonth
 

# COMMAND ----------

#variables
spark = SparkSession.builder.getOrCreate()

#Archivo en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_javp"
path_lakehouse=f"gs://{name_bucket}/produccion/dmc"
path_bronze = f"{path_lakehouse}/bronze/transacciones/"
path_silver = f"{path_lakehouse}/silver/transacciones/"

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

#Casteo de datos
df_c = df.withColumn("ID_PERSONA",col("ID_PERSONA").cast(IntegerType()))\
    .withColumn("ID_EMPRESA",col("ID_EMPRESA").cast(IntegerType()))\
    .withColumn("MONTO",col("MONTO").cast(DoubleType()))\
    .withColumn("FECHA",to_date(col("FECHA"), "yyyy-MM-dd"))\
    .withColumn("ANIO",year(col("FECHA")))\
    .withColumn("MES",month(col("FECHA")))\
    .withColumn("DIA",dayofmonth(col("FECHA")))

display(df_c)

# COMMAND ----------

df_c.write.mode("overwrite").partitionBy("ANIO","MES","DIA").format("delta").save(path_silver)

# COMMAND ----------

