# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
    create table if not exists f1_presentation.calculated_race_results
    (
        race_year int,
        team_name string,
        driver_id int,
        driver_name string,
        race_id int,
        position int,
        points int,
        calculated_points int,
        created_date timestamp,
        updated_date timestamp
    )
    using delta
""")

# COMMAND ----------

spark.sql(f"""
create or replace temp view race_result_updated
AS
select 
d.race_year
, c.name as team_name
, b.driver_id as driver_id
, b.name as driver_name
, d.race_id as race_id
, a.position
, a.points
, 11 - a.position as calculated_points
from f1_processed.results a
join f1_processed.drivers b on a.driver_id = b.driver_id
join f1_processed.constructors c on a.constructor_id = c.constructor_id
join f1_processed.races d on a.race_id = d.race_id 
where a.position <= 10 and a.file_date = '{v_file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_presentation.calculated_race_results as tgt
# MAGIC using race_result_updated as upd
# MAGIC on tgt.driver_id = upd.driver_id and tgt.race_id = upd.race_id
# MAGIC when matched then 
# MAGIC   update set 
# MAGIC   tgt.position = upd.position, 
# MAGIC   tgt.points = upd.points,
# MAGIC   tgt.calculated_points = upd.calculated_points,
# MAGIC   tgt.updated_date = current_timestamp()
# MAGIC when not matched then
# MAGIC   insert (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date)
# MAGIC   values (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.calculated_race_results  order by race_year, team_name, driver_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.calculated_race_results where race_year = 2021;
