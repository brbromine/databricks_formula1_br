-- Databricks notebook source
create schema if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for csv file

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits (
circuitid int, 
circuitref string, 
name string, 
location string, 
country string, 
lat double, 
lng double, 
alt int, 
url string
)
using csv
location "/mnt/formula1dlbr/raw/circuits.csv"
options (header true)

-- COMMAND ----------

select * 
from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races (
 raceId int, 
 year int, 
 round int, 
 circuitId int, 
 name string, 
 date date, 
 time string, 
 url string 
)
using csv
location "/mnt/formula1dlbr/raw/races.csv"
options (header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create table for JSON file

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors (
constructorId int, 
constructorRef string, 
name string, 
nationality string, 
url string 
)
using json
location "/mnt/formula1dlbr/raw/constructors.json"

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table
-- MAGIC - Single Line JSON
-- MAGIC - Complex structure

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers (
driverId int, 
driverRef string,
number int, 
code string,
name struct <forename: string, surname: string>,
dob date, 
nationality string, 
url string 
)
using json
location "/mnt/formula1dlbr/raw/drivers.json"

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results (
resultId int, 
raceId int, 
driverId int, 
constructorId int, 
number int, 
grid int, 
position int, 
positionText string, 
positionOrder int, 
points int, 
laps int, 
time string, 
milliseconds int, 
fastestLap int, 
rank int, 
fastestLapTime string, 
fastestLapSpeed float, 
statusId int 
)
using json
location "/mnt/formula1dlbr/raw/results.json"

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops (
 raceId int, 
 driverId int, 
 stop string, 
 lap int, 
 time string, 
 duration string, 
 milliseconds int 
)
using json
location "/mnt/formula1dlbr/raw/pit_stops.json"
options(multiline true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Lap Times table
-- MAGIC - CSV file
-- MAGIC - Multiple files

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times (
raceId int, 
driverId int, 
lap int, 
position int, 
time string, 
milliseconds int
)
using csv
location "/mnt/formula1dlbr/raw/lap_times"

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Qualifying table
-- MAGIC - JSON file
-- MAGIC - Multi line JSON
-- MAGIC - Multiple files

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying (
qualifyId int, 
raceId int, 
driverId int, 
constructorId int, 
number int, 
position int, 
q1 string, 
q2 string, 
q3 string 
)
using json
location "/mnt/formula1dlbr/raw/qualifying"
options(multiline true)

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

desc extended f1_raw.qualifying;
