# Databricks notebook source
# MAGIC %md
# MAGIC #### Access dataframes using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

display(spark.sql(f"SELECT count(1) FROM v_race_results WHERE race_year = {p_race_year};"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Temporary Views
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_result_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

display(spark.sql(f"SELECT * from global_temp.gv_race_results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by 1
# MAGIC order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.lap_times
# MAGIC group by 1
# MAGIC order by 1 desc;
