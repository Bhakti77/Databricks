# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.databricsksaudemy.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databricsksaudemy.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickssaudemy.dfs.core.windows.net", "sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-04-14T14:56:52Z&st=2023-04-14T06:56:52Z&spr=https&sig=JDbwqAeekGJpAdQENrQWgrBdk07BmZiHSlsZkzzNxx0%3D")

# COMMAND ----------

countries = spark.read.csv("abfss://bronze@databrickssaudemy.dfs.core.windows.net/countries.csv" ,header=True)

# COMMAND ----------


