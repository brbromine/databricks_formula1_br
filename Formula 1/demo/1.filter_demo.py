# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

## races_filtered_df = races_df.filter((race_df["race_year"] == 2019) & (race_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------


