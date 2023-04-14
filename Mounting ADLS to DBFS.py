# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "18ee97dc-d974-46c4-a762-0d8965706e20",
          "fs.azure.account.oauth2.client.secret": "ZFI8Q~k45HEC89NEOgeNyxx9yiPEmcF~dlyikbgN",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/94d8fdba-0a19-4af8-bb5e-83b4d171d3f8/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@databrickssaudemy.dfs.core.windows.net/",
  mount_point = "/mnt/bronze>",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


