# Databricks notebook source
from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

# COMMAND ----------

# Reading order csv file

orders_path = '/FileStore/tables/bronze/orders.csv'

orders_schema = StructType([
    StructField("ORDER_ID",IntegerType(),False),
    StructField("ORDER_DATETIME",StringType(),False),
    StructField("CUSTOMER_ID",IntegerType(),False),
    StructField("ORDER_STATUS",StringType(),False),
    StructField("STORE_ID",IntegerType(),False)])

orders = spark.read.csv(orders_path,header = True, schema = orders_schema)

# COMMAND ----------

orders.display()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
orders = orders.select('ORDER_ID',\
    to_timestamp(orders['ORDER_DATETIME'],"dd-MMM-yy kk.mm.ss.SS").alias('ORDER_TIMESTAMP'),\
    'CUSTOMER_ID', 'ORDER_STATUS','STORE_ID' )

# COMMAND ----------

orders = orders.filter(orders['ORDER_STATUS'] == 'COMPLETE')

# COMMAND ----------

# Reading order csv file

store_path = '/FileStore/tables/bronze/stores.csv'

stores_schema = StructType([
    StructField("STORE_ID",IntegerType(),False),
    StructField("STORE_NAME",StringType(),False),
    StructField("WEB_ADDRESS",StringType(),False),
    StructField("LATITUDE",DoubleType(),False),
    StructField("LONGITUDE",DoubleType(),False)])

stores = spark.read.csv(store_path,header = True, schema = stores_schema)

# COMMAND ----------

stores.display()

# COMMAND ----------

orders = orders.join(stores,orders['store_id'] == stores['store_id'],'left').select('ORDER_ID','ORDER_TIMESTAMP','CUSTOMER_ID','STORE_NAME')

# COMMAND ----------

orders.display()

# COMMAND ----------

orders.write.parquet("/FileStore/tables/silver/orders", mode='overwrite')

# COMMAND ----------

order_items_path = '/FileStore/tables/bronze/order_items.csv'

order_items_schema = StructType([
    StructField("ORDER_ID",IntegerType(),False),
    StructField("LINE_ITEM_ID",StringType(),False),
    StructField("PRODUCT_ID",StringType(),False),
    StructField("UNIT_PRICE",DoubleType(),False),
    StructField("QUANTITY",DoubleType(),False)])

order_items = spark.read.csv(order_items_path,header = True, schema = order_items_schema)

# COMMAND ----------

order_items.display()

# COMMAND ----------

order_items = order_items.drop(order_items['LINE_ITEM_ID'])

# COMMAND ----------

order_items.display()

# COMMAND ----------

order_items.write.parquet("/FileStore/tables/silver/order_items", mode='overwrite')

# COMMAND ----------

products_path = '/FileStore/tables/bronze/order_items.csv'

products_schema = StructType([
    StructField("PRODUCT_ID",IntegerType(),False),
    StructField("PRODUCT_NAME",StringType(),False),
    StructField("UNIT_PRICE",DoubleType(),False)])

products = spark.read.csv(products_path,header = True, schema = products_schema)

# COMMAND ----------

products.display()

# COMMAND ----------

products.write.parquet("/FileStore/tables/silver/products", mode='overwrite')

# COMMAND ----------

customers_path = '/FileStore/tables/bronze/customers.csv'

customers_schema = StructType([
    StructField("CUSTOMER_ID",IntegerType(),False),
    StructField("FULL_NAME",StringType(),False),
    StructField("EMAIL_ADDRESS",StringType(),False)])

customers = spark.read.csv(customers_path,header = True, schema = customers_schema)

# COMMAND ----------

customers.write.parquet("/FileStore/tables/silver/customers", mode='overwrite')