# Databricks notebook source
# MAGIC %md
# MAGIC #### Read all the data as required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

driver_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

circuit_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

constructor_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

race_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")
    

# COMMAND ----------

result_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .where(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join circuit to race

# COMMAND ----------

race_circuit_df = race_df.join(circuit_df, race_df.circuit_id == circuit_df.circuit_id, "inner") \
    .select(race_df.race_id, race_df.race_year, race_df.race_name, race_df.race_date, circuit_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join results to all other dataframes

# COMMAND ----------

race_result_df = result_df.join(race_circuit_df, result_df.result_race_id == race_circuit_df.race_id, "inner") \
    .join(driver_df, result_df.driver_id == driver_df.driver_id, "inner") \
    .join(constructor_df, result_df.constructor_id == constructor_df.constructor_id, "inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, desc, asc

# COMMAND ----------

final_df = race_result_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
    .withColumn("created_at", current_timestamp()) \
    .withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

#overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.driver_name = src.driver_name"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_presentation.race_results group by 1 order by 1 desc;
