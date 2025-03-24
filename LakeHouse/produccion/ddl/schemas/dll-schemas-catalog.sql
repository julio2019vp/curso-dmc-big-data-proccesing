-- Databricks notebook source
show catalogs;

-- COMMAND ----------

show databases;

-- COMMAND ----------

show tables in default;

-- COMMAND ----------

create schema bronze
location "gs://dmc_datalake_dde_11_javp/produccion/dmc/bronze"

-- COMMAND ----------

create schema silver
location "gs://dmc_datalake_dde_11_javp/produccion/dmc/silver"

-- COMMAND ----------

create schema if not exists gold
location "gs://dmc_datalake_dde_11_javp/produccion/dmc/gold"