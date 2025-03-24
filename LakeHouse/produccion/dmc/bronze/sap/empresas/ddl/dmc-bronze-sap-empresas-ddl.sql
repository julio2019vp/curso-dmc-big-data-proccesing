-- Databricks notebook source
create external table bronze.empresas(
  ID STRING,
  EMPRESA_NAME STRING
)
using delta
location "gs://dmc_datalake_dde_11_javp/produccion/dmc/bronze/empresas/"

-- COMMAND ----------

select * from bronze.empresas