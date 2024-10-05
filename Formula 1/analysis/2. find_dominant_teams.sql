-- Databricks notebook source
show tables in f1_presentation;

-- COMMAND ----------

desc f1_presentation.calculated_race_results;

-- COMMAND ----------

select team_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by 1
having total_races >= 100
order by avg_points desc;

-- COMMAND ----------

select team_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
cast((avg(calculated_points)) as decimal(10,3)) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by 1
having total_races >= 100
order by avg_points desc;

-- COMMAND ----------

select team_name, 
count(1) as total_races,
sum(calculated_points) as total_points,
cast((avg(calculated_points)) as decimal(10,3)) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2001 and 2011
group by 1
having total_races >= 100
order by avg_points desc;
