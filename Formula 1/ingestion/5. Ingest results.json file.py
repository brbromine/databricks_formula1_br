# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the .JSON file using the Spark DataFrame reader 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, FloatType

# COMMAND ----------

results_schema = StructType([StructField("resultId", IntegerType(), False),
                             StructField("raceId", IntegerType(), True),
                             StructField("driverId", IntegerType(), True),
                             StructField("constructorId", IntegerType(), True),
                             StructField("number", IntegerType(), True),
                             StructField("grid", IntegerType(), True),
                             StructField("position", IntegerType(), True),
                             StructField("positionText", StringType(), True),
                             StructField("positionOrder", IntegerType(), True),
                             StructField("points", IntegerType(), True),
                             StructField("laps", IntegerType(), True),
                             StructField("time", StringType(), True),
                             StructField("milliseconds", IntegerType(), True),
                             StructField("fastestLap", IntegerType(), True),
                             StructField("rank", IntegerType(), True),
                             StructField("fastestLapTime", StringType(), True),
                             StructField("fastestLapSpeed", FloatType(), True),
                             StructField("statusId", IntegerType(), True)])




# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlbr/raw/

# COMMAND ----------

result_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

result_df.printSchema()

# COMMAND ----------

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

result_dropped_df = result_df.drop("statusId")
## constructor_dropped_df = constructor_df.drop(constructor_df['url'])
## constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

result_final_df = result_dropped_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

result_final_df = add_ingestion_date(result_final_df)

# COMMAND ----------

result_final_df = result_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

display(result_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to Parquet file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1

# COMMAND ----------

# for race_id_list in result_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# result_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2

# COMMAND ----------

#overwrite_partition(result_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id"
merge_delta_data(result_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlbr/processed/results

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by 1
# MAGIC order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by 1,2
# MAGIC having count(1) > 1
# MAGIC order by 1,2 desc;
