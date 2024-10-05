# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Containers for Project
# MAGIC

# COMMAND ----------

def mount_afls(storage_account_name, container_name):
        app_client_id = dbutils.secrets.get(scope = 'formula1-scope', key = f"{storage_account_name}-app-client-id")
        app_tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = f"{storage_account_name}-app-tenant-id")
        app_client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = f"{storage_account_name}-app-client-secret")

        ## Set Spark Configuration
        configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": app_client_id,
          "fs.azure.account.oauth2.client.secret": app_client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{app_tenant_id}/oauth2/token"}
        
        ## if the mount doesn't exist, simply ignore this step
        if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}"  for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

        ## Mount the storage account container 
        dbutils.fs.mount(
            source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storage_account_name}/{container_name}",
            extra_configs = configs)

        display(dbutils.fs.mounts())


# COMMAND ----------

mount_afls('formula1dlbr', 'raw')

# COMMAND ----------

mount_afls('formula1dlbr', 'processed')

# COMMAND ----------

mount_afls('formula1dlbr', 'presentation') 

# COMMAND ----------

mount_afls('formula1dlbr', 'demo')

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
