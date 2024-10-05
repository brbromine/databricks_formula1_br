# Databricks notebook source
# MAGIC %md
# MAGIC #### Procuce driver standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, avg, max, min, desc, rank, when, col

from pyspark.sql.window import Window

# COMMAND ----------

race_result_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'") \
    .select("race_year").distinct().collect()

# COMMAND ----------

race_result_list

# COMMAND ----------

race_year_list = []
for race_year in race_result_list:
    race_year_list.append(race_year.race_year)
print(race_year_list)

# COMMAND ----------

race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .where(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

constructor_standing_df = race_result_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins")) \
    .orderBy("race_year")

display(constructor_standing_df.where("race_year = 2020"))

# COMMAND ----------

final_df = constructor_standing_df.withColumn("rank", rank().over(Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))))

display(final_df.where("race_year = 2020"))
                                         


# COMMAND ----------

#overwrite_partition(final_df, 'f1_presentation', 'constructors_standings', 'race_year')

merge_condition = "tgt.race_year = src.race_year and tgt.team = src.team"
merge_delta_data(final_df, 'f1_presentation', 'constructors_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year, count(1) from f1_presentation.constructors_standings group by 1 order by 1 desc;
