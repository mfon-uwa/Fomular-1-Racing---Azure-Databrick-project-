# Databricks notebook source
storage_account_name = 'lsformularone'
client_app_id ='e8e67e66-abad-44a3-8541-13bac28e467e'
tenant_directory_id = 'd11185cc-5b35-406e-bb67-c12056098b1f'
client_secret = 'Enc8Q~ErS94~Fp8EzwI2XayjhX6EOrxOldPeEbgk'

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()



# COMMAND ----------

dbutils.secrets.list('formular1-secrets-scope')

# COMMAND ----------

storage_account_name = 'lsformularone'
#client_app_id ='e8e67e66-abad-44a3-8541-13bac28e467e'
#tenant_directory_id = 'd11185cc-5b35-406e-bb67-c12056098b1f'
#client_secret = 'Enc8Q~ErS94~Fp8EzwI2XayjhX6EOrxOldPeEbgk'

# COMMAND ----------

storage_account_name = 'lsformularone'
client_app_id =  dbutils.secrets.get(scope='formular1-secrets-scope', key ='client-app-id')
tenant_directory_id = dbutils.secrets.get(scope='formular1-secrets-scope', key ='tenant-directory-id')
client_secret = dbutils.secrets.get(scope='formular1-secrets-scope', key ='client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_app_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_directory_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://raw@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls(f'/mnt/{storage_account_name}/raw')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

container_name = "processed"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/lsformularone/processed")

# COMMAND ----------

def mounts_containers(container_name):
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mounts_containers("processed")

# COMMAND ----------


