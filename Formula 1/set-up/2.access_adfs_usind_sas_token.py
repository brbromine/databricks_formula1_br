# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake usinf SAS token
# MAGIC 1. Set the Spark config for SAS token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuit.csv file

# COMMAND ----------

formula1dlbr_demo_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formaula1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlbr.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlbr.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlbr.dfs.core.windows.net", formula1dlbr_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlbr.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlbr.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


