# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount Azure Data Lake Containers for the project

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    #Get secrets from key vault
    client_id = "c729de5f-b08c-45f1-a86c-a8905a314577"
    tenant_id = "32211bd0-67bc-433a-8e6f-a5108d37aa75"
    client_secret = "RGo8Q~BegPtoawG_IYUp~yfnFRdKhqEdITAvpb.S"
    
    
    #set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    

    #Unmount the point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")


    #Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

    

# COMMAND ----------

mount_adls('formuladl9032','raw')

# COMMAND ----------

mount_adls('formuladl9032','processed')

# COMMAND ----------

mount_adls('formuladl9032','presentation')
