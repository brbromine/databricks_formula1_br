# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

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
# MAGIC ##### Step 1 - Read the .JSON file using the Spark DataFrame reader 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType(), True)
                          , StructField("surname", StringType(), True)
                          ])

# COMMAND ----------

drivers_schema = StructType([StructField("driverId", IntegerType(), False)
                             , StructField("driverRef", StringType(), True)
                             , StructField("number", IntegerType(), True)
                             , StructField("code", StringType(), True)
                             , StructField("name", name_schema)
                             , StructField("dob", DateType(), True)
                             , StructField("nationality", StringType(), True)
                             , StructField("url", StringType(), True)
                             ])

# COMMAND ----------

driver_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

driver_df.printSchema()

# COMMAND ----------

display(driver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

driver_dropped_df = driver_df.drop("url")
## constructor_dropped_df = constructor_df.drop(constructor_df['url'])
## constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

# COMMAND ----------

driver_final_df = driver_dropped_df.withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("driverRef","driver_ref") \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

driver_final_df = add_ingestion_date(driver_final_df)

# COMMAND ----------

display(driver_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to Parquet file

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlbr/processed/drivers

# COMMAND ----------

dbutils.notebook.exit("success")
