# Databricks notebook source
# MAGIC %md
# MAGIC #### Spark join transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .filter("circuit_id < 70") \
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

display(circuits_df)
display(races_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner").select(circuits_df["circuit_name"], circuits_df["location"], circuits_df["race_country"], races_df["race_name"], races_df["round"])

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

race_circuit_df.select("circuit_name").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer join

# COMMAND ----------

# Left outer join

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left").select(circuits_df["circuit_name"], circuits_df["location"], circuits_df["race_country"], races_df["race_name"], races_df["round"])

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# Right outer join

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right").select(circuits_df["circuit_name"], circuits_df["location"], circuits_df["race_country"], races_df["race_name"], races_df["round"])

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# Full outer join

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full").select(circuits_df["circuit_name"], circuits_df["location"], circuits_df["race_country"], races_df["race_name"], races_df["round"])

# COMMAND ----------

display(race_circuit_df)
