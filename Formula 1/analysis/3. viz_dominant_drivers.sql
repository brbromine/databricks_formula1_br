-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

show tables in f1_presentation;

-- COMMAND ----------

create or replace temp view v_dominant_drivers
as
select driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as driver_rank
from f1_presentation.calculated_race_results
group by 1
having total_races >= 50
order by avg_points desc;

-- COMMAND ----------

select 
cast(race_year as char(4)) as race_year,
driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in 
(select driver_name from v_dominant_drivers where driver_rank <= 10)
group by 1,2
order by race_year,avg_points desc;

-- COMMAND ----------

select 
cast(race_year as char(4)) as race_year,
driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in 
(select driver_name from v_dominant_drivers where driver_rank <= 10)
group by 1,2
order by race_year,avg_points desc;

-- COMMAND ----------

select 
cast(race_year as char(4)) as race_year,
driver_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in 
(select driver_name from v_dominant_drivers where driver_rank <= 10)
group by 1,2
order by race_year,avg_points desc;
