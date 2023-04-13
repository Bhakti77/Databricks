# Databricks notebook source
countries_df = spark.read.csv("/FileStore/tables/countries.csv",inferSchema=True,header=True)

# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df.write.parquet("/FileStore/tables/output/countries_parquet")

# COMMAND ----------

display(spark.read.parquet("/FileStore/tables/output/countries_parquet"))

# COMMAND ----------

