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

regions_path = '/FileStore/tables/country_regions.csv'

regions_schema = StructType([StructField("ID",StringType(),False),StructField("NAME",StringType(),False)
])

regions_df = spark.read.csv(regions_path,header=True, schema = regions_schema)

regions_df.display()

# COMMAND ----------

countries_df.join(regions_df,countries_df['REGION_ID']==regions_df['ID'],'right').\
    select(countries_df['NAME'],regions_df['NAME'].alias('Region'),countries_df['POPULATION']).display()

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------



# COMMAND ----------

dbutils.fs.rm('/FileStore/bronze')

# COMMAND ----------

