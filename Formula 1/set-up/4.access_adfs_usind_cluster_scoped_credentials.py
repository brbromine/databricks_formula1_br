# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake usinf cluster scoped credentials
# MAGIC 1. Set the Spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuit.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlbr.dfs.core.windows.net",
    "47bOhp6LrBlco7Ly0E/6+jvPQT21yi862hjvXu+tSgbWyo4VV3rxoznZwRnc/B+jGWcu/jL1sSPq+ASta1Ch8w=="
)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlbr.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlbr.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


