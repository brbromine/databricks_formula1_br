# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake usinf service principal
# MAGIC #### steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a secret / password for the application
# MAGIC 1. Set Sparf config with App/Client ID, Directory/ Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = "d1f9f01e-165d-4119-a1ad-e1d9a6b8e234"
tenant_id = "7a9177b3-67b9-4e07-bd3c-62fbe42372cb"
client_secret = "_as8Q~V20aGPp-p-cGUXw9UqBd.qvwhEaXaY4aac"

# COMMAND ----------

app_client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlbr-app-client-id')
app_tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlbr-app-tenant-id')
app_client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlbr-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlbr.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlbr.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlbr.dfs.core.windows.net", app_client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlbr.dfs.core.windows.net", app_client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlbr.dfs.core.windows.net", f"https://login.microsoftonline.com/{app_tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlbr.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlbr.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


