# Databricks notebook source
container_name = 'gold'
account = 'databrickssaudemy'
mount_point = '/mnt/gold'
secret = 'ZFI8Q~k45HEC89NEOgeNyxx9yiPEmcF~dlyikbgN'
application_id = '18ee97dc-d974-46c4-a762-0d8965706e20'
tenant_id = '94d8fdba-0a19-4af8-bb5e-83b4d171d3f8'


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(source = f"abfss://{container_name}@{account}.dfs.core.windows.net/",mount_point = mount_point,extra_configs = configs)

# COMMAND ----------

#reading files from Bronze 
from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

countries_path = '/mnt/bronze>/countries.csv'

countries_schema = StructType([
StructField("COUNTRY_ID",IntegerType(),False),
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

countries = spark.read.csv(countries_path,header=True,schema = countries_schema)


# COMMAND ----------

countries.display()

# COMMAND ----------

countries = countries.drop("SUB_REGION_ID","INTERMDIATE_REGION_ID","ORGANIZATIONAL_REGION_ID")
countries.display()

# COMMAND ----------

countries.write.parquet("/mnt/silver/countries",mode='overwrite')

# COMMAND ----------

#reading files from Bronze 
from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

country_region_path = '/mnt/bronze>/country_regions.csv'

country_region_schema = StructType([
StructField("ID",IntegerType(),False),
StructField("NAME",StringType(),False)
])

country_region = spark.read.csv(country_region_path,header=True,schema = country_region_schema)


# COMMAND ----------

country_region.display()

# COMMAND ----------

from pyspark.sql.functions import col
country_region = country_region.withColumnRenamed('NAME','REGION_NAME')

# COMMAND ----------

country_region.display()

# COMMAND ----------

country_region.write.parquet('/mnt/silver/country_regions',mode='overwrite')

# COMMAND ----------

#reading data from silver 
#reading files from Bronze

countries = spark.read.parquet('/mnt/silver/countries')
country_region = spark.read.parquet('/mnt/silver/country_regions')

countries.display()
country_region.display()


# COMMAND ----------

country_data = countries.join(country_region,countries['REGION_ID'] == country_region['ID'],'left').drop("ID","REGION_ID","COUNTRY_CODE","ISO_ALPHA2")

# COMMAND ----------

country_data.display()

# COMMAND ----------

country_data.write.parquet('/mnt/gold/country_data',mode='overwrite')

# COMMAND ----------


