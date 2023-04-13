# Databricks notebook source
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

countries_df.display()

# COMMAND ----------

countries_df.write.csv("/FileStore/tables/countries_out", header=True)

# COMMAND ----------

spark.read.csv("/FileStore/tables/countries_out").display()

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/countries_out", header=True)

# COMMAND ----------

df.write.csv("/FileStore/tables/output/countries_out",header=True,mode='overwrite')

# COMMAND ----------

df.write.mode('overwrite').partitionBy('REGION_ID').csv("/FileStore/tables/output/countries_out",header=True)

# COMMAND ----------

df2 = spark.read.csv("/FileStore/tables/output/countries_out/REGION_ID=102",header=True)

# COMMAND ----------

df2.display()

# COMMAND ----------

