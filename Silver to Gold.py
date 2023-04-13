# Databricks notebook source
from pyspark.sql.functions import *

orders = spark.read.parquet("/FileStore/tables/silver/orders")
products = spark.read.parquet("/FileStore/tables/silver/products")
orders_items = spark.read.parquet("/FileStore/tables/silver/order_items")
customers = spark.read.parquet("/FileStore/tables/silver/customers")

# COMMAND ----------

orders.dtypes

# COMMAND ----------

order_details = orders.select('ORDER_ID',\
    to_date('ORDER_TIMESTAMP').alias('DATE'),\
    'CUSTOMER_ID','STORE_NAME')
    

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = order_details.join(orders_items, order_details['ORDER_ID'] == orders_items['ORDER_ID'],'left').select(order_details['ORDER_ID'],order_details['DATE'],order_details['CUSTOMER_ID'],order_details['STORE_NAME'],orders_items['UNIT_PRICE'],orders_items['quantity'])

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = order_details.withColumn('TOTAL_SALES_AMOUNT',order_details['UNIT_PRICE'] * \
    order_details['QUANTITY'])

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = order_details.groupBy('ORDER_ID','DATE','CUSTOMER_ID','STORE_NAME').\
    sum('TOTAL_SALES_AMOUNT').withColumnRenamed('sum(TOTAL_SALES_AMOUNT)','TOTAL_ORDER_AMOUNT')

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details = order_details.withColumn('TOTAL_ORDER_AMOUNT',round('TOTAL_ORDER_AMOUNT',2))

# COMMAND ----------

order_details.display()

# COMMAND ----------

order_details.write.parquet("/FileStore/tables/gold/order_details", mode='overwrite')

# COMMAND ----------

sales_with_month = order_details.withColumn('MONTH_YEAR', date_format('DATE','yyyy-MM'))

# COMMAND ----------

sales_with_month.display()

# COMMAND ----------

monthly_sales = sales_with_month.groupBy('MONTH_YEAR').sum('TOTAL_ORDER_AMOUNT').\
    withColumn('TOTAL_SALES',round('sum(TOTAL_ORDER_AMOUNT)',2)).sort(sales_with_month['MONTH_YEAR'].desc()).\
    select('MONTH_YEAR','TOTAL_SALES')


# COMMAND ----------

monthly_sales.display()

# COMMAND ----------

monthly_sales.write.parquet("/FileStore/tables/gold/monthly_sales", mode='overwrite')

# COMMAND ----------

store_monthly_sales = sales_with_month.groupBy('MONTH_YEAR','STORE_NAME').sum('TOTAL_ORDER_AMOUNT').\
    withColumn('TOTAL_SALES',round('sum(TOTAL_ORDER_AMOUNT)',2)).sort(sales_with_month['MONTH_YEAR'].desc()).select ('MONTH_YEAR','STORE_NAME','TOTAL_SALES')

# COMMAND ----------

store_monthly_sales.write.parquet('/FileStore/tables/gold/store_monthly_sales', mode= 'overwrite')

# COMMAND ----------

