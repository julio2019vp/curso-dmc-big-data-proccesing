-- Databricks notebook source
create external table bronze.personas(
  ID STRING,
  NOMBRE STRING,
  TELEFONO STRING,
  CORREO STRING,
  FECHA_INGRESO STRING,
  EDAD STRING,
  SALARIO STRING,
  ID_EMPRESA STRING
)
using delta
location "gs://dmc_datalake_dde_11_javp/produccion/dmc/bronze/personas/"

-- COMMAND ----------

select ID_EMPRESA,count(1) from bronze.personas group by ID_EMPRESA;