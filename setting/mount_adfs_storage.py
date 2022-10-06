# Databricks notebook source
storage_account_name = 'lsformularone'
client_app_id ='xxxxxxx-abad-44a3-8541-11111111111111'
tenant_directory_id = 'xxxxxxx-5b35-406e-bb67-eeeeeeee'
client_secret = 'xxxxxxxx~Fp8EzwI2XayjhX6EOrrrrrrrrrrrr'

# COMMAND ----------

dbutils.secrets.list('formular1-secrets-scope')

# COMMAND ----------

storage_account_name = 'lsformularone'

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

def mounts_containers(container_name):
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mounts_containers("raw")

# COMMAND ----------

mounts_containers("processed")

# COMMAND ----------

mounts_containers("presentation")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

notebook_status = dbutils.notebook.exit("success")
notebook_status
