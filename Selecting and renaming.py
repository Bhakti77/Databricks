# Databricks notebook source
 from pyspark.sql.types import IntegerType,StringType,DoubleType,StructField,StructType

path = '/FileStore/tables/countries.csv'

countries_schema = StructType([StructField("COUNTRY_ID",IntegerType(),False),
StructField("NAME",StringType(),False),
StructField("Nationality",StringType(),False),
StructField("COUNTRY_CODE",StringType(),False),
StructField("ISO_ALPHA2",StringType(),False),
StructField("CAPITAL",StringType(),False),
StructField("POPULATION",IntegerType(),False),
StructField("AREA_KM2",IntegerType(),False),
StructField("REGION_ID",IntegerType(),False),
StructField("SUB_REGION_ID",IntegerType(),False),
StructField("INTERMDIATE_REGION_ID",IntegerType(),False),
StructField("ORGANIZATIONAL_REGION_ID",IntegerType(),False),
])

countries_df = spark.read.csv(path, header= True,schema = countries_schema)

# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df.select('NAME','CAPITAL','POPULATION').display()

# COMMAND ----------

countries_df.select(countries_df['NAME']).display()

# COMMAND ----------

