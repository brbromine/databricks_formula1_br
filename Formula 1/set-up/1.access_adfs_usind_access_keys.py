# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake usinf access keys
# MAGIC 1. Set the Spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuit.csv file

# COMMAND ----------

formular1dlbr_account_key = dbutils.secrets.get(scope='formula1-scope',key='formular1dlbr-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlbr.dfs.core.windows.net",
    formular1dlbr_account_key
)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlbr.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlbr.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


