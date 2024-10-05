# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggegration functions demo

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Built-in Aggegrate functions

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

demo_df = race_result_df.where("race_year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, avg, max, min, desc, rank

from pyspark.sql.window import Window

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name").alias("circuits")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.where("driver_name like 'Lewis%'").select(sum("points")).show()

# COMMAND ----------

demo_df.where("driver_name like 'Lewis%'").select(sum("points"), countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .withColumnRenamed("count(DISTINCT race_name)", "total_races").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Group by

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points").alias("total points"), countDistinct("race_name").alias("total_races")).orderBy(desc("total points")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Window functions

# COMMAND ----------

demo_df = race_result_df.where("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year","driver_name") \
    .agg(sum("points").alias("total points"), countDistinct("race_name").alias("total_races")) \
    .orderBy("race_year", desc("total points")).withColumn("rank", rank().over(Window.partitionBy("race_year").orderBy(desc("total points"))))

display(demo_grouped_df)
