# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/countries.txt")

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/countries_single_line.json")
dbutils.fs.rm("/FileStore/tables/countries_multi_line.json")

# COMMAND ----------

