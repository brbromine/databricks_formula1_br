-- Databricks notebook source
create schema if not exists f1_processed
location "/mnt/formula1dlbr/processed"

-- COMMAND ----------

desc schema f1_processed;

-- COMMAND ----------

select * from f1_processed.circuits;

-- COMMAND ----------

select * from f1_processed.races;

-- COMMAND ----------

select * from f1_processed.constructors;

-- COMMAND ----------

select * from f1_processed.drivers;

-- COMMAND ----------

select * from f1_processed.results;

-- COMMAND ----------

select * from f1_processed.pit_stops;

-- COMMAND ----------

select * from f1_processed.lap_times;

-- COMMAND ----------

select * from f1_processed.qualifying;

-- COMMAND ----------

show tables in f1_processed;
