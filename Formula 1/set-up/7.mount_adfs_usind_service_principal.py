# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake usinf service principal
# MAGIC #### steps to follow
# MAGIC 1. Get client_id, tenant_id and client secret from key vault
# MAGIC 2. Set Spark Config with App/Client ID, Directory/ Tenant ID & Secret
# MAGIC 3. Call File system utility mount to the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

app_client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlbr-app-client-id')
app_tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlbr-app-tenant-id')
app_client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlbr-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": app_client_id,
          "fs.azure.account.oauth2.client.secret": app_client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{app_tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlbr.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlbr/demo",
  extra_configs = configs)

## source values filled with storage account name
## mount point values filled with conatainer name

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlbr/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlbr/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

##dbutils.fs.unmount('/mnt/formula1dlbr/demo')

## in case you wanted to unmount
