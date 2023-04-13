# Databricks notebook source
from pyspark.sql.types import IntegerType,StringType,DoubleType,StructField,StructType

path = '/FileStore/tables/countries.csv'
countries_schema = StructType([StructField("COUNTRY_ID",IntegerType(),False),
StructField("NAME",StringType(),False),
StructField("Nationality",StringType(),False),
StructField("COUNTRY_CODE",StringType(),False),
StructField("ISO_ALPHA2",StringType(),False),
StructField("CAPITAL",StringType(),False),
StructField("POPULATION",DoubleType(),False),
StructField("AREA_KM2",IntegerType(),False),
StructField("REGION_ID",IntegerType(),False),
StructField("SUB_REGION_ID",IntegerType(),False),
StructField("INTERMDIATE_REGION_ID",IntegerType(),False),
StructField("ORGANIZATIONAL_REGION_ID",IntegerType(),False),
])

countries_df = spark.read.csv(path, header= True,schema=countries_schema)


# COMMAND ----------

from pyspark.sql.functions import sum,avg,count,min,max,col
countries_df.groupBy(col('REGION_ID')).agg(sum(col('POPULATION')).alias('pop')).filter(col('REGION_ID') == 20).display()

# COMMAND ----------

countries_df.groupBy(col('REGION_ID'),col('SUB_REGION_ID')) \
    .agg(min(col('POPULATION')).alias('min_pop'),max(col('POPULATION')).alias('max_pop')) \
        .sort(col('REGION_ID')).display()

# COMMAND ----------

