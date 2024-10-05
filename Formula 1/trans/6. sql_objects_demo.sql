-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

DESC SCHEMA demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

describe extended demo.race_results_python;

-- COMMAND ----------

select *
from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

create table if not exists demo.race_result_sql (
select *
from demo.race_results_python
where race_year = 2020
);


-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

desc extended demo.race_result_sql;

-- COMMAND ----------

drop table demo.race_result_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

desc extended demo.race_results_ext_py;

-- COMMAND ----------

create table if not exists demo.race_results_ext_sql (
race_year int,
race_name string,
race_date timestamp,
circuit_location	string,
driver_name	string,
driver_number	int,
driver_nationality	string,
team	string,
grid	int,
fastest_lap	int,
race_time	string,
points	int,
position	int,
created_at	timestamp
)
USING parquet
LOCATION "/mnt/formula1dlbr/presentation/race_results_ext_sql";



-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql;

-- COMMAND ----------

drop table demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC 1. Create temp view
-- MAGIC 2. Create Global temp view
-- MAGIC 3. Create Permanent view

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

create or replace temp view v_race_results as
select *
from demo.race_results_python
where race_year = 2018
;


-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

create or replace global temp view v_race_results as
select *
from demo.race_results_python
where race_year = 2018
;


-- COMMAND ----------

select * from global_temp.v_race_results;

-- COMMAND ----------

create or replace view demo.pv_race_results as
select *
from demo.race_results_python
where race_year = 2000
;
-- permanent view

-- COMMAND ----------



-- COMMAND ----------

show tables in demo;
