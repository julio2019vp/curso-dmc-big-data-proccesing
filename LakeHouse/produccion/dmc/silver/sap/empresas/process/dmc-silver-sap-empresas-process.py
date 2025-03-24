# Databricks notebook source
#importacion de librerias
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import regexp_replace,date_format,current_date,add_months,when,col, to_date, year, month, dayofmonth,upper
 

# COMMAND ----------

#variables
spark = SparkSession.builder.getOrCreate()

#Archivo en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_javp"
path_lakehouse=f"gs://{name_bucket}/produccion/dmc"
path_bronze = f"{path_lakehouse}/bronze/empresas/"
path_silver = f"{path_lakehouse}/silver/empresas/"

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
df_t = df.withColumn("EMPRESA_NAME",upper(col("EMPRESA_NAME")))\
    .withColumn("PERIODO",date_format(add_months(current_date(),-1),"yyyyMM"))

#Casteo de datos
df_c = df_t.withColumn("ID",col("ID").cast(IntegerType()))

display(df_c)

# COMMAND ----------

df_c.write.mode("overwrite").partitionBy("periodo").format("delta").save(path_silver)