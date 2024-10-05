# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step1 - Read the CSV file using the Spark DataFrame reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlbr/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                   StructField("year", IntegerType(), True),
                                   StructField("round", IntegerType(), True),
                                   StructField("circuitId", IntegerType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("date", DateType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("url", StringType(), True)])


# COMMAND ----------

races_df = spark.read \
.option("header",True).schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

## .schema(races_schema) after header

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.show()

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

races_selected_df =  races_df.select("raceId", "year", "round", "circuitId", "name", "date", "time")

# COMMAND ----------

races_selected_df =  races_df.select(races_df["raceId"], races_df["year"], races_df["round"], races_df["circuitId"], races_df["name"], races_df["date"], races_df["time"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df =  races_df.select(col("raceId"),  col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time")) 

## in case you want to rename the column

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3 - Rename the column as required

# COMMAND ----------

## 2 choices, first you can use an alias after the call function or the API's called withColumnRenamed

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add ingestion date to the DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

races_final_df = races_renamed_df \
.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_final_df = add_ingestion_date(races_final_df)

# COMMAND ----------

races_final_df = races_final_df.select("race_id", "race_year", "round", "circuit_id", "name", "race_timestamp", "ingestion_date", "data_source")

# COMMAND ----------

races_final_df.printSchema()

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlbr/processed/races

# COMMAND ----------

dbutils.notebook.exit("success")
