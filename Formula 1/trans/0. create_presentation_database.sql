-- Databricks notebook source
create schema if not exists f1_presentation
location "/mnt/formula1dlbr/presentation"

-- COMMAND ----------

select race_id, count(1)
from f1_processed.pit_stops
group by 1
order by 1 desc;

-- COMMAND ----------

select race_id, count(1)
from f1_processed.lap_times
group by 1
order by 1 desc;

-- COMMAND ----------

select race_id, count(1)
from f1_processed.qualifying
group by 1
order by 1 desc;

-- COMMAND ----------

select circuit_id, count(1)
from f1_processed.circuits
group by 1
order by 1 desc;

-- COMMAND ----------

select race_id, count(1)
from f1_processed.races
group by 1
order by 1 desc;

-- COMMAND ----------

select constructor_id, count(1)
from f1_processed.constructors
group by 1
order by 1 desc;

-- COMMAND ----------

REFRESH TABLE f1_processed.circuits;

-- COMMAND ----------

select * from f1_processed.circuits;

-- COMMAND ----------

REFRESH TABLE f1_processed.races;

-- COMMAND ----------

select * from f1_processed.races;

-- COMMAND ----------

REFRESH TABLE f1_processed.constructors;

-- COMMAND ----------

select * from f1_processed.constructors;

-- COMMAND ----------

REFRESH TABLE f1_processed.drivers;

-- COMMAND ----------

select * from f1_processed.drivers;
