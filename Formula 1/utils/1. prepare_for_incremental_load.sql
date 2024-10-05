-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Drop all the tables

-- COMMAND ----------

drop schema if exists f1_processed cascade;

-- COMMAND ----------

create schema if not exists f1_processed
location "/mnt/formula1dlbr/processed"

-- COMMAND ----------

drop schema if exists f1_presentation cascade;

-- COMMAND ----------

create schema if not exists f1_presentation
location "/mnt/formula1dlbr/presentation"
