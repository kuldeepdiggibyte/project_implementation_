# Databricks notebook source
# MAGIC %run ../bronze_to_silver/common_functions

# COMMAND ----------

# MAGIC %run ../bronze_to_silver/on_processing

# COMMAND ----------

from pyspark.sql.functions import col, datediff,sum as _sum, count,explode,when,lit, sum, avg

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vtex_db.order_table1

# COMMAND ----------

orders_df=spark.table('vtex_db.order_table1')


# COMMAND ----------

new_df=orders_df.select("order_id","creation_date","authorized_date","status")
new_df=new_df.withColumn("fulfillment_time",datediff(col('creation_date'),col('authorized_date'))).drop('creation_date',"authorized_date")
new_df.display()

# COMMAND ----------

# status_df=orders_df.select("status","order_id")
status_df=spark.sql('select status,count(status) as order_status_count from vtex_db.order_table1 group by status')
# status_df.display()

# COMMAND ----------

finnal=new_df.join(status_df,new_df.status==status_df.status,'inner').drop(status_df.status)
finnal.display()

# COMMAND ----------

status_df=orders_df.select("sales_channel","order_id","value") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select payment_system_name, count(payment_system_name) from vtex_db.payment_data_table group by payment_system_name
# MAGIC -- select * from vtex_db.payment_data_table

# COMMAND ----------

from pyspark.sql.functions import col,sum as _sum, count

payment_data_table = spark.table('vtex_db.payment_data_table')

orders_data_table=spark.table('vtex_db.order_table1')
combined_data = payment_data_table.join(orders_data_table, on='transaction_id', how='inner')

combined_data1 = combined_data.withColumn('Name_of_the_payment_method_used', col('payment_system_name')).select('Name_of_the_payment_method_used')
combined_data1.display()

combined_data2 = combined_data.groupBy("payment_system_name").agg(
    count("order_id").alias("order_count"),
    _sum("reference_value").alias("total_transaction_value")
)
combined_data2.display()

# COMMAND ----------

# i=spark.table('vtex_db.items_table2')
# c=spark.table('vtex_db.clientprofiledata_table')
# new=i.join(c,i.order_id==c.order_id,'inner').drop(i.order_id)

# COMMAND ----------

from pyspark.sql.functions import col,sum, count

payment_data_table = spark.table('vtex_db.payment_data_table')

orders_data_table=spark.table('vtex_db.order_table1')
combined_data = payment_data_table.join(orders_data_table, on='transaction_id', how='inner')

combined_data1 = combined_data.withColumn('Name_of_the_payment_method_used', col('group')).select('Name_of_the_payment_method_used')
# combined_data1.display()

combined_data2 = combined_data.groupBy("group").agg(
    count("order_id").alias("order_count"),
    sum("reference_value").alias("total_transaction_value")
)
combined_data2.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vtex_db.logisticsinfo_shippingdata_table1

# COMMAND ----------

shipping_price = spark.sql('SELECT AVG(price) as Average_price, sum(price) as Total_price FROM vtex_db.logisticsinfo_shippingdata_table1')
display(shipping_price)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vtex_db.items_table2

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vtex_db.items_table2
# MAGIC

# COMMAND ----------

item_df = spark.sql('SELECT product_id , sum(quantity) as total_quantity_sold , sum(selling_price) as total_sales_value , avg(price) as average_price FROM vtex_db.items_table2 GROUP BY product_id' )
item_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vtex_db.cancellation_data_table
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, count, lit, when

cancellation_data_table = spark.table('vtex_db.cancellation_data_table')

count_by_reason = cancellation_data_table.filter(col("reason").isNotNull()).groupBy("reason").agg(count("order_id").alias("orders_canceled"))



count_by_reason.display()


# COMMAND ----------

from pyspark.sql.functions import col, count, lit, when, expr

item_table = spark.table('vtex_db.items_table2')

joined_df = item_table.join(cancellation_data_table, "order_id", "left")
cancellation_table = cancellation_data_table.withColumn("returned_indicator", when(col("reason").isNotNull(), 1).otherwise(0))
#display(cancellation_table)
joined_df = item_table.join(cancellation_table, "order_id", "left")
returns_per_product = joined_df.filter(col("returned_indicator") == 1).groupBy("product_id", "reason").agg(count("order_id").alias("returns_count"))
display(returns_per_product)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vtex_db.clientprofiledata_table

# COMMAND ----------

customer_df = spark.table('vtex_db.clientprofiledata_table')
orders_per_customer = customer_df.groupBy("email").agg(count("order_id").alias("total_orders"))

orders_per_customer = orders_per_customer.withColumn("repeat_customer", when(col("total_orders") > 1, 1).otherwise(0))

orders_per_customer.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vtex_db.order_table1

# COMMAND ----------

orders_df  = spark.table("vtex_db.order_table1")
orders_per_status = orders_df.groupBy("status").agg(count("order_id").alias("total_orders_per_status"))
orders_per_status.display()

orders_per_channel = orders_df.groupBy("sales_channel").agg(count("order_id").alias("total_orders_per_channel"))
orders_per_channel.display()

sales_value_per_channel = orders_df.groupBy("sales_channel").agg(sum('value').alias("total_sales_value"))

sales_value_per_channel.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###gold_sales_performance

# COMMAND ----------

# MAGIC %sql
# MAGIC USE vtex_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

item_table = spark.read.format("delta").table("vtex_db.items_table2")
item_table.display()

# COMMAND ----------

item_df = item_table.groupBy("product_id").agg(
        sum("quantity").alias("total_quantity_sold"),
        sum("selling_price").alias("total_sales_value"),
        avg("price").alias("average_price")
    )
item_df.display()

# COMMAND ----------

clientprofiledata = spark.read.format("delta").table("vtex_db.clientprofiledata_table")
clientprofiledata.display()

# COMMAND ----------

order_table = spark.read.format("delta").table("vtex_db.order_table1")
order_table.display()

# COMMAND ----------

order_df = order_table.agg(
    count("order_id").alias("total_orders"),
    sum("value").alias("total_order_value"),
    avg("value").alias("average_order_value")
)
order_df.display()

# COMMAND ----------

cancellation_data_table = spark.read.format('delta').table("vtex_db.cancellation_data_table")
cancellation_data_table.display()

# COMMAND ----------

order_cancelled_df = cancellation_data_table.agg(count("order_id").alias("canceled_orders"))
order_cancelled_df.display()

# COMMAND ----------

result_df = order_df.crossJoin(order_cancelled_df)
result_df = result_df.withColumn(
    "completed_orders", 
    result_df["total_orders"] - result_df["canceled_orders"]
)

result_df = result_df.withColumn(
    "fulfillment_rate", 
    (result_df["completed_orders"] / result_df["total_orders"]) * 100
)
result_df = result_df.withColumn(
    "cancellation_rate", 
    (result_df["canceled_orders"] / result_df["total_orders"]) * 100
)



result_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vtex_db.clientprofiledata_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vtex_db.clientprofiledata_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vtex_db.order_table1

# COMMAND ----------

# DBTITLE 1,avg per order
# MAGIC %sql
# MAGIC -- SELECT * FROM vtex_db.items_table2
# MAGIC select avg(quant) as avg from (SELECT order_id,count(product_id) as quant FROM vtex_db.items_table2 GROUP BY order_id)
# MAGIC

# COMMAND ----------

# DBTITLE 1,per customer
# i=spark.table('vtex_db.items_table2')
# c=spark.table('vtex_db.clientprofiledata_table')
# new=i.join(c,i.order_id==c.order_id,'inner').drop(i.order_id)
# temp=new.groupBy('user_profile_id').count()
# display(temp.selectExpr("avg(count)"))

# COMMAND ----------

clienprofiledata_df = spark.sql('SELECT c.user_profile_id  AS customer_id , avg(o.value) AS customer_lifetime_value, count(c.order_id) AS total_orders_per_customer FROM vtex_db.order_table1 as o JOIN vtex_db.clientprofiledata_table  as c on o.order_id = c.order_id GROUP BY c.user_profile_id')
clienprofiledata_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vtex_db.items_table2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.order_id , c.user_profile_id , avg(i.quantity) FROM vtex_db.items_table2 as i JOIN vtex_db.clientprofiledata_table as c on i.order_id = c.order_id GROUP BY c.order_id , c.user_profile_id;