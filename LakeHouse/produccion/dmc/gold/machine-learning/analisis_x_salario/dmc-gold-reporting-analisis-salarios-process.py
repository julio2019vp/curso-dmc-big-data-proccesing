# Databricks notebook source
from pyspark.sql import SparkSession
#from pyspark.sql.types import *
#from pyspark.sql.functions import *

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

#Archivos en Cloud Storage - Google Cloud Platform
name_bucket = "dmc_datalake_dde_11_javp"
path_lakehouse = f"gs://{name_bucket}/produccion/dmc"

#Tabla Input
path_silver_personas = f"{path_lakehouse}/silver/personas/"
path_silver_empresas = f"{path_lakehouse}/silver/empresas/"

#Tabla Output
path_gold = f"{path_lakehouse}/gold/machine-learning/analisis-x-salario"

# COMMAND ----------

df_personas = spark.read.format("delta").load(path_silver_personas)

df_empresas = spark.read.format("delta").load(path_silver_empresas)



# COMMAND ----------

display(df_personas)
display(df_empresas)

# COMMAND ----------

df_personas.createOrReplaceTempView("tb_personas")
df_empresas.createOrReplaceTempView("tb_empresas")

# COMMAND ----------

sql = """SELECT  p.periodo, e.empresa_name as nombre_empresa, p.salario, p.edad
        FROM tb_personas p inner join tb_empresas e on e.ID = p.ID_EMPRESA and e.periodo = p.periodo"""

df_result = spark.sql(sql)

# COMMAND ----------

display(df_result)

# COMMAND ----------

df_result.createOrReplaceTempView("tb_analisis_detalle")

# COMMAND ----------

sql_final = """ select periodo, nombre_empresa, sum(salario) as sum_salario, avg(salario) as avg_salario, avg(edad) as avg_edad 
from tb_analisis_detalle group by periodo, nombre_empresa """

 df_result_final = spark.sql(sql_final)
 display(df_result_final)

# COMMAND ----------

sql_final = """ select periodo
,nombre_empresa,sum(salario) as sum_salario,avg(salario) as avg_salario, avg(edad) as avg_edad from tb_analisis_detalle group by periodo
,nombre_empresa
"""
df_result_final = spark.sql(sql_final)
display(df_result_final)

# COMMAND ----------

df_result_final.write.mode("overwrite").format("delta").save(path_gold)

# COMMAND ----------

df = spark.read.format("delta").load(path_gold)
display(df)