# Databricks notebook source
Cl0QGM5Ku3r8XmQIAXTogiT0XoCm6xp+u60y7sd23n8NXlCs9xHFvwPqkMWqZUeS7GyFgH1L3m64+AStO3A+GA==

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databrickssaudemy.dfs.core.windows.net",
    "Cl0QGM5Ku3r8XmQIAXTogiT0XoCm6xp+u60y7sd23n8NXlCs9xHFvwPqkMWqZUeS7GyFgH1L3m64+AStO3A+GA==")

# COMMAND ----------

countries = spark.read.csv('abfs://bronze@databrickssaudemy.dfs.core.windows.net/countries.csv', header=True)

# COMMAND ----------

countries.display()

# COMMAND ----------

country_regions = spark.read.csv('abfs://bronze@databrickssaudemy.dfs.core.windows.net/country_regions.csv', header=True)

# COMMAND ----------

country_regions.display()

# COMMAND ----------


