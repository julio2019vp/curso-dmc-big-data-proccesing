-- Databricks notebook source
create external table bronze.transacciones(
  ID_PERSONA STRING,
  ID_EMPRESA STRING,
  MONTO STRING,
  FECHA STRING
)

using delta
location "gs://dmc_datalake_dde_11_javp/produccion/dmc/bronze/transacciones/"

-- COMMAND ----------

select * from bronze.transacciones