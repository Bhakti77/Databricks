# Databricks notebook source
countries_df = spark.read.csv("/FileStore/tables/countries.csv", header= True)

# COMMAND ----------

countries_df.display()

# COMMAND ----------


countries_df2 = spark.read.options(header=True).csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df2.display()

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.schema

# COMMAND ----------

countries_df.describe()

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,DoubleType,StructField,StructType

countries_schema = StructType([StructField("COUNTRY_ID",IntegerType(),False),
StructField("NAME",StringType(),False),
StructField("Nationality",StringType(),False),
StructField("ISO_ALPHA2",StringType(),False),
StructField("CAPITAL",StringType(),False),
StructField("POPULATION",DoubleType(),False),
StructField("AREA_KM2",IntegerType(),False),
StructField("REGION_ID",IntegerType(),False),
StructField("SUB_REGION_ID",IntegerType(),False),
StructField("INTERMDIATE_REGION_ID",IntegerType(),False),
StructField("ORGANIZATIONAL_REGION_ID",IntegerType(),False),
])

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv",header=True,schema = countries_schema)

# COMMAND ----------

countries_df.schema

# COMMAND ----------

countries_sl_json_df = spark.read.json("/FileStore/tables/countries_single_line.json")

# COMMAND ----------

countries_sl_json_df.display()

# COMMAND ----------

countries_ml_json_df = spark.read.json("/FileStore/tables/countries_multi_line.json",multiLine=True)

# COMMAND ----------

countries_ml_json_df.display()

# COMMAND ----------

