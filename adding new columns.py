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

from pyspark.sql.functions import lit

countries_df.withColumn('Updated By',lit('Bhakti')).display()

# COMMAND ----------

countries_df.groupBy('NATIONALITY').sum('POPULATION').display()

# COMMAND ----------

countries_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import round

countries_df.withColumn('POPULATION_new',round(countries_df['POPULATION']/10000000,1)).display()

# COMMAND ----------

from pyspark.sql.functions import asc_nulls_first,desc_nulls_first
countries_df.sort(desc_nulls_first(countries_df['INTERMDIATE_REGION_ID'])).display()

# COMMAND ----------

countries_df.filter(countries_df['POPULATION'] > 150000000).display()

# COMMAND ----------

from pyspark.sql.functions import length
countries_df.filter((length(countries_df['NAME']) > 15) & (countries_df['REGION_ID']!=10)).display()

countries_df.filter("length(name)>15 and region_id != 10").display()

# COMMAND ----------

from pyspark.sql.functions import when
countries_df.withColumn('name_length',when(countries_df['population']>110000000000,'large').when(countries_df['population']<110000000000,'not large')).display()

# COMMAND ----------

from pyspark.sql.functions import expr
countries_df.select(expr('NAME as country_name')).display()

# COMMAND ----------

countries_df.withColumn('population_class',expr("case when population>100000000 then 'large' when population>50000000 then 'medium' else 'small'end")).display()

# COMMAND ----------

