# Databricks notebook source
# MAGIC %run ../bronze_to_silver/vtex_schema

# COMMAND ----------

# MAGIC %run ../bronze_to_silver/common_functions

# COMMAND ----------

dbutils.widgets.text("tablename", "")
dbutils.widgets.text("adls", "")
dbutils.widgets.text("db_name", "")
dbutils.widgets.text("schedule_date", "")
dbutils.widgets.text("primary_key", "")


table_name = dbutils.widgets.get('tablename')
db_name = dbutils.widgets.get('db_name')
sche_val=dbutils.widgets.get('schedule_date')
key=dbutils.widgets.get('primary_key')
adls_path=dbutils.widgets.get('adls')
silver_path=f'{adls_path}{table_name}'

# COMMAND ----------

path='dbfs:/mnt/vtex-data/bronze/vtex_test_data.json'
format='json'
schema=custom_vtex_schema
order_table=read_file_source(path,format)
order_table = order_table.withColumn('orderDetails' , from_json(order_table["orderDetails"], schema))
order_table=order_table.select('orderDetails.*')
# order_table.display()



# COMMAND ----------

# display(order_table)


# COMMAND ----------

# display(order_table.groupBy('orderid').count())

# COMMAND ----------

# order_table.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## total

# COMMAND ----------

# DBTITLE 1,done
totals_table = order_table.select('orderId',explode_outer('totals').alias('totals'))
totals_table = totals_table.select('totals.*', 'orderId')
totals_table = totals_table.groupBy("orderId").pivot('id').sum('value')
totals_table.display()

# COMMAND ----------

# display(totals_table.groupBy('orderid').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Item

# COMMAND ----------

items_table=order_table.select('orderId', explode_outer(order_table['items']).alias('items'))
items_table=items_table.select('orderId',"items.*")
items_table.display()

# COMMAND ----------

display(items_table.groupBy('uniqueId').count())

# COMMAND ----------

# display(items_table.filter('uniqueId=="BE4747D32011429685A09CDB62EE38CF"'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##attachments

# COMMAND ----------

# DBTITLE 1,done
item_attachments_table = items_table.select('uniqueId',explode_outer(items_table.attachments).alias('attachments'))
item_attachments_table = item_attachments_table.select('uniqueId','attachments.*')
item_attachments_table = item_attachments_table.withColumn("garantia_estendida_parsed", from_json(col("content.`garantia-estendida`"), garantia_estendida_schema)).withColumn("takeback_parsed", from_json(col("content.takeback"), takeback_schema)).drop("content", "name")
item_attachments_table = item_attachments_table.select("*", col("garantia_estendida_parsed.name").alias("garantia_estendida_name"),col("garantia_estendida_parsed.refId").alias("garantia_estendida_refid"),col("garantia_estendida_parsed.id").alias("garantia_estendida_id"),
                                                       col("garantia_estendida_parsed.productId").alias("garantia_estendida_productid"),col("garantia_estendida_parsed.productWeight").alias("garantia_estendida_productweight"),col("garantia_estendida_parsed.lineID").alias("garantia_estendida_lineid"),col("garantia_estendida_parsed.model").alias("garantia_estendida_model"), col("takeback_parsed.nome").alias("takeback_parsed_name"),col("takeback_parsed.productName").alias('takeback_parsed_productname'), col('takeback_parsed.valor').alias('takeback_parsed_valor'), col('takeback_parsed.material').alias('takeback_parsed_material')).drop('garantia_estendida_parsed','takeback_parsed')

item_attachments_table = remove_null(item_attachments_table,'uniqueId')
item_attachments_table.display()


# COMMAND ----------

# display(item_attachments_table.groupBy('uniqueId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##priceTags

# COMMAND ----------

# DBTITLE 1,done
item_priceTags_table = items_table.select('uniqueId',explode_outer(items_table.priceTags).alias('priceTags'))

item_priceTags_table = item_priceTags_table.withColumn("priceTags", from_json(col("priceTags"),price_tags_schema))
item_priceTags_table = item_priceTags_table.select('uniqueId','priceTags.*') 
                                                                              
# item_priceTags_table.display()
item_priceTags_table=item_priceTags_table.dropDuplicates()
item_priceTags_table=remove_null(item_priceTags_table,'uniqueId')
item_priceTags_table.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ##Component

# COMMAND ----------

# DBTITLE 1,done
items_table_n = items_table.withColumn('components', explode_outer('components'))
components_table = items_table_n.select(col('uniqueId').alias('item_uniqueid'), 'components.*')
components_table = components_table.select('*',col('itemAttachment.content').alias('itemattachment_content'),col('itemAttachment.name').alias('itemattachment_name')).drop('itemAttachment')
components_table = components_table.select('*','additionalInfo.*','priceDefinition.*').drop('additionalInfo','priceDefinition')
components_table = components_table.withColumn('sellingPrices',explode_outer('sellingPrices'))
components_table = components_table.select('*','dimension.*', col('sellingPrices.value').alias('sellingprices_value') , col('sellingPrices.quantity').alias('sellingprices_quantity') ).drop('dimension', 'sellingPrices','attachments','priceTags','imageUrl','detailUrl','bundleItems','params','offerings','attachmentOfferings','assemblies','itemattachment_content','components')
components_table = remove_null(components_table,'item_uniqueid')
display(components_table)



# COMMAND ----------

# display(components_table.groupBy('uniqueId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## bundle items

# COMMAND ----------

item_bundle_table = items_table.select(col('uniqueId').alias('item_uniqueid'),"bundleItems")
item_bundle_table = item_bundle_table.select('item_uniqueid',explode_outer('bundleItems').alias('bundleItems'))
# item_bundle_table = items_table.withColumn("bundleItems",items_table["bundleItems"]).select("bundleItems")

# item_bundle_table = item_bundle_table.select('item_uniqueid',explode_outer('item_bundle_table.bundleItems').alias('bundleItems'))

item_bundle_table = item_bundle_table.withColumn("bundleItems", from_json(col("bundleItems"),bundle_items_schema))
item_bundle_table = item_bundle_table.select('item_uniqueid','bundleItems.*').drop('itemAttachment','attachments', 'priceTags','components','bundleItems','params','offerings','assemblies','attachmentOfferings','additionalInfo')
item_bundle_table=remove_null(item_bundle_table,'item_uniqueid')
item_bundle_table.display()

# COMMAND ----------

# display(item_bundle_table.groupBy('uniqueId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##AdditonalInfo

# COMMAND ----------

item_additional_info=items_table.select('uniqueId', col('additionalInfo').alias('additional_info'))
item_additional_info=item_additional_info.select('uniqueId','additional_info.*')
item_additional_info=item_additional_info.dropDuplicates()
item_additional_info.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##category

# COMMAND ----------

# DBTITLE 1,done
item_additional_info_categories=item_additional_info.select('uniqueId', explode_outer(col('categories')).alias('categories'))
item_additional_info_categories=item_additional_info_categories.select('uniqueId','categories.*')
item_additional_info_categories=item_additional_info_categories.dropDuplicates()
item_additional_info_categories.display()

# COMMAND ----------

# display(item_additional_info_categories.groupBy('uniqueId','id').count())

# COMMAND ----------

# DBTITLE 1,done
item_additional_info1=item_additional_info.withColumn('categories',item_additional_info['categories.id']).drop('categoriesIds')

# COMMAND ----------

# display(item_additional_info.groupBy('uniqueId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##price def

# COMMAND ----------

# DBTITLE 1,done
price_definition_table = items_table.select('orderid','priceDefinition.*','uniqueId')
price_definition_table = price_definition_table.withColumn('sellingPrices' , explode_outer('sellingPrices'))
price_definition_table = price_definition_table.select('*', 'sellingPrices.*').drop('sellingPrices')
price_definition_table=price_definition_table.dropDuplicates()
# price_definition_table.display()

# COMMAND ----------

display(price_definition_table.groupBy('uniqueId').count())

# COMMAND ----------

# display(price_definition_table.filter('uniqueId=="BE4747D32011429685A09CDB62EE38CF"'))

# COMMAND ----------

itmes_table1 = items_table.withColumn('offerings' , explode_outer('offerings'))
items_table1 = items_table.select('*' , col('offerings.type').alias('offerings_type'), col('offerings.id').alias('offerings_id'),col('offerings.name').alias('offerings_name'),col('offerings.price').alias('offerings_price')).drop('attachments','components','bundleItems','additionalInfo','priceDefinition','attachmentOfferings', 'offerings', 'priceTags')
items_table1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##clientProfileData

# COMMAND ----------

# DBTITLE 1,done
clientProfileData_table=order_table.select("clientProfileData.*", 'orderId').drop('id')
# clientProfileData_table.display()

# COMMAND ----------

# display(clientProfileData_table.groupBy('email').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## marketingData

# COMMAND ----------

# DBTITLE 1,done
marketingdata_table = order_table.select('orderId','marketingData.*')
# display(marketingdata_table)
marketingdata_table = remove_null(marketingdata_table,'orderId')
display(marketingdata_table)

# COMMAND ----------

# display(marketingdata_table.groupBy('orderId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## rates and benifits
# MAGIC

# COMMAND ----------

# DBTITLE 1,done
ratesAndBenefitsData=order_table.select('orderId',order_table['ratesAndBenefitsData']).alias('ratesAndBenefitsData')
ratesAndBenefitsData=ratesAndBenefitsData.select('orderId',"ratesAndBenefitsData.id","ratesAndBenefitsData.rateAndBenefitsIdentifiers")
ratesAndBenefitsData=ratesAndBenefitsData.withColumn("rateAndBenefitsIdentifiers",explode_outer('rateAndBenefitsIdentifiers'))
ratesAndBenefitsData=ratesAndBenefitsData.select('*', 'rateAndBenefitsIdentifiers.*').drop('rateAndBenefitsIdentifiers','id')
ratesAndBenefitsData=remove_null(ratesAndBenefitsData, 'orderId')
ratesAndBenefitsData = ratesAndBenefitsData.drop('matchedParameters','additionalInfo')
# ratesAndBenefitsData.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##shipping table

# COMMAND ----------

shippingData_table=order_table.select('orderId',order_table['shippingData'].alias('shippingdata'))

shippingData_table=shippingData_table.select('orderId',"shippingdata.*")
shippingData_table=shippingData_table.withColumn("selectedAddresses",explode_outer('selectedAddresses'))
shippingData_table=shippingData_table.withColumn("addressId",col('address.addressId'))
shippingData_table=shippingData_table.withColumn("sel_addressId",col('selectedAddresses.addressId'))

# shippingData_table.display()
shippingData_table=shippingData_table.dropDuplicates()
# shippingData_table.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##shippingdata -> logistic info

# COMMAND ----------

logisticsInfo_shippingData_table=shippingData_table.select('orderId',explode_outer(shippingData_table['logisticsInfo']).alias('logisticsInfo'))
logisticsInfo_shippingData_table=logisticsInfo_shippingData_table.select('orderId',"logisticsInfo.*")
logisticsInfo_shippingData_table=logisticsInfo_shippingData_table.withColumn('deliveryIds',explode_outer('deliveryIds'))
logisticsInfo_shippingData_table=logisticsInfo_shippingData_table.withColumn('courierId',col('deliveryIds.courierId'))
# display(logisticsInfo_shippingData_table)
#logisticsInfo_shippingData_table = logisticsInfo_shippingData_table.drop('slas','deliveryIds','versionId','entityId','polygonName')




# COMMAND ----------

# display(logisticsInfo_shippingData_table.filter('orderId == "v100085492elct-01"'))

# COMMAND ----------

logisticsInfo_shippingData_table=logisticsInfo_shippingData_table.dropDuplicates()
#logisticsInfo_shippingData_table = logisticsInfo_shippingData_table.drop('slas','deliveryIds','versionId','entityId','polygonName')
# display(logisticsInfo_shippingData_table)

# COMMAND ----------

# DBTITLE 1,error: unble to find primary key
# display(logisticsInfo_shippingData_table.groupBy('orderId').count())

# COMMAND ----------


deliveryIds_logisticsInfo_shippingData_table=logisticsInfo_shippingData_table.select('orderId',('deliveryIds'))
deliveryIds_logisticsInfo_shippingData_table=deliveryIds_logisticsInfo_shippingData_table.select('orderId',"deliveryIds.*")
#deliveryIds_logisticsInfo_shippingData_table = deliveryIds_logisticsInfo_shippingData_table.drop('accountCarrierName')
# display(deliveryIds_logisticsInfo_shippingData_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ##slas

# COMMAND ----------


slas_logisticsInfo_shippingData_table=logisticsInfo_shippingData_table.select('orderId','courierId',explode_outer('slas').alias('slas'))
slas_logisticsInfo_shippingData_table=slas_logisticsInfo_shippingData_table.select('orderId',"slas.*","courierId")
#slas_logisticsInfo_shippingData_table = slas_logisticsInfo_shippingData_table.drop('polygonName')

# display(slas_logisticsInfo_shippingData_table)

# COMMAND ----------

slas_logisticsInfo_shippingData_table=slas_logisticsInfo_shippingData_table.dropDuplicates()
# display(slas_logisticsInfo_shippingData_table)

# COMMAND ----------

# DBTITLE 1,error : unable to find primary key
# display(slas_logisticsInfo_shippingData_table.groupBy('orderId','courierId','id').count())


# COMMAND ----------

# DBTITLE 1,duplicate value
# display(slas_logisticsInfo_shippingData_table.filter('orderId=="v100085666elct-01"'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##address

# COMMAND ----------

address_shippingData_table=shippingData_table.select('orderId',(shippingData_table['address']).alias('address'))

address_shippingData_table=address_shippingData_table.select('orderId',"address.*")

# COMMAND ----------

address_shippingData_table  = address_shippingData_table.dropDuplicates()
#address_shippingData_table= address_shippingData_table1.drop('versionId','entityId','reference')
display(address_shippingData_table)

# COMMAND ----------

# in address table --> primary key is addressId and orderId
# display(address_shippingData_table1.groupBy('addressId','orderId', ).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##selected address

# COMMAND ----------


selectedAddresses_shippingData_table=shippingData_table.select('orderId',(shippingData_table['selectedAddresses']).alias('selectedAddresses'))
selectedAddresses_shippingData_table=selectedAddresses_shippingData_table.select('orderId',"selectedAddresses.*")


# COMMAND ----------

selectedAddresses_shippingData_table  = selectedAddresses_shippingData_table.dropDuplicates()
display(selectedAddresses_shippingData_table)

# COMMAND ----------

# In selectaddress table addressID and orderId will be primary key
# display(selectedAddresses_shippingData_table.groupBy('addressId','orderId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##paymenttable

# COMMAND ----------

# DBTITLE 1,done
payment_data_table = order_table.select('paymentData.*')
payment_data_table = payment_data_table.withColumn('transactions' , explode_outer(payment_data_table['transactions']))
payment_data_table = payment_data_table.withColumn('giftCards', explode_outer(payment_data_table['giftCards']))
payment_data_table = payment_data_table.select('*','transactions.*', col('giftCards.balance').alias('giftCardbalance'),col('giftCards.inUse').alias('giftCards_inuse'), col('giftCards.isSpecialCard').alias('giftCard_isspecialcard'))
payment_data_table = payment_data_table.withColumn('payments', explode_outer('payments'))
payment_data_table = payment_data_table.select('*',col('payments.id').alias('payment_id'), 'payments.*').drop('payments' , 'id','transactions','giftCards')
# payment_data_table.display()
payment_data_table=payment_data_table.dropDuplicates()
# payment_data_table.display()

# COMMAND ----------

# display(payment_data_table.groupBy('transactionId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## sellers

# COMMAND ----------

sellers_table=order_table.select(explode(order_table['sellers']).alias('sellers'),'orderId')
sellers_table = sellers_table.select('orderId','sellers.*')
# display(sellers_table)

# COMMAND ----------

# seller table primary key is Id and orderId
# display(sellers_table.groupBy('orderId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## invoicedata
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,done
invoice_data =  order_table.select('orderId','invoiceData.*')
invoice_data =  invoice_data.select('*', 'address.*').drop('address')
invoice_data = remove_null(invoice_data , 'orderId')
# invoice_data.display()

# COMMAND ----------

# display(invoice_data.groupBy('orderId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##storePreferencesData

# COMMAND ----------

# DBTITLE 1,done
storePreferencesData_table=order_table.select(order_table['storePreferencesData'].alias('storePreferencesData'))

storePreferencesData_table=storePreferencesData_table.select("storePreferencesData.*")
# storePreferencesData_table.display()
storePreferencesData_table=storePreferencesData_table.dropDuplicates()
# storePreferencesData_table.display()

# COMMAND ----------

# display(storePreferencesData_table.groupBy('countryCode').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## item metadata

# COMMAND ----------

# DBTITLE 1,done
itemMetadata=order_table.select('orderId',order_table['itemMetadata']).alias('itemMetadata')
itemMetadata=itemMetadata.select('orderId',"itemMetadata.Items")
itemMetadata=itemMetadata.withColumn('Items',explode_outer(itemMetadata['Items']))
itemMetadata=itemMetadata.select('orderId',('Items.*'))
# itemMetadata.display()

# COMMAND ----------

# display(itemMetadata.groupBy('orderId','Id').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##cancellationData

# COMMAND ----------

# DBTITLE 1,done
cancellation_data_table = order_table.select('orderId','cancellationData.*')
cancellation_data_table = remove_null(cancellation_data_table , 'orderId')
cancellation_data_table.display()

# COMMAND ----------

# display(cancellation_data_table.groupBy('orderId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Subscription Data
# MAGIC

# COMMAND ----------

# DBTITLE 1,done
subscription_data_table = order_table.select('orderId','subscriptionData.*')
subscription_data_table = subscription_data_table.withColumn('Subscriptions', explode_outer('Subscriptions'))
subscription_data_table = subscription_data_table.select('*', 'Subscriptions.*').drop('Subscriptions')
subscription_data_table = subscription_data_table.select('*' , 'Plan.*')
subscription_data_table = subscription_data_table.select('*', 'frequency.*', 'validity.*').drop('Plan','frequency','validity')
subscription_data_table = remove_null(subscription_data_table , 'orderId')
# subscription_data_table.display()

# COMMAND ----------

# display(subscription_data_table.groupBy('SubscriptionGroupId').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Dropping Column in the order table
# MAGIC

# COMMAND ----------


order_table1 = order_table.withColumn('transactionId' , col('paymentData.transactions.transactionId'))
order_table1 = order_table1.withColumn('transactionId' , explode_outer('transactionId')).drop('totals')
# order_table1.display()

order_table1 = order_table1.withColumn('storePreferencesDataId' , col('storePreferencesData.countryCode')).drop("paymentData")
# order_table1 = order_table1.withColumn('storePreferencesDataId' , col('storePreferencesData.countryCode')).drop("paymentData")
# order_table1.display()
order_table1 = order_table1.select('*',col('clientProfileData.email').alias('client_prodile_email'),col('subscriptionData.SubscriptionGroupId').alias('subscriptiongroup_id')).drop('clientProfileData','marketingData','sellers','subscriptionData','cancellationData','storePreferencesData', 'ratesAndBenefitsData','invoiceData','itemMetadata' ,'items', 'shippingData','packageAttachment','clientPreferencesData')
# order_table1.display()


# order_table1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop column in item table

# COMMAND ----------

itmes_table1 = items_table.withColumn('offerings' , explode_outer('offerings'))
items_table2 = items_table.select('*' , col('offerings.type').alias('offerings_type'), col('offerings.id').alias('offerings_id'),col('offerings.name').alias('offerings_name'),col('offerings.price').alias('offerings_price')).drop('attachments','components','bundleItems','additionalInfo','priceDefinition','attachmentOfferings', 'offerings', 'priceTags')
# items_table2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop column in shipping table

# COMMAND ----------

shippingData_table1=order_table.select('orderId',order_table['shippingData'].alias('shippingdata'))

shippingData_table1=shippingData_table1.select('orderId',"shippingdata.*")
shippingData_table1=shippingData_table1.withColumn("selectedAddresses",explode_outer('selectedAddresses'))


shippingData_table1=shippingData_table1.withColumn("addressId",col('address.addressId'))
shippingData_table1=shippingData_table1.withColumn("sel_addressId",col('selectedAddresses.addressId'))

# shippingData_table.display()
shippingData_table1=shippingData_table1.dropDuplicates()
#shippingData_table1 = shippingData_table1.drop('address','trackingHints')
# shippingData_table1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##logisticsInfo

# COMMAND ----------

logisticsInfo_shippingData_table1=shippingData_table1.select('orderId',explode_outer(shippingData_table1['logisticsInfo']).alias('logisticsInfo'))
logisticsInfo_shippingData_table1=logisticsInfo_shippingData_table1.select('orderId',"logisticsInfo.*")
logisticsInfo_shippingData_table1=logisticsInfo_shippingData_table1.withColumn('deliveryIds',explode_outer('deliveryIds'))
logisticsInfo_shippingData_table1=logisticsInfo_shippingData_table1.withColumn('courierId',col('deliveryIds.courierId'))
#logisticsInfo_shippingData_table1= logisticsInfo_shippingData_table1.drop('slas','deliveryIds','versionId','entityId','polygonName')
# display(logisticsInfo_shippingData_table1)

# COMMAND ----------

address_shippingData_table1=shippingData_table1.select('orderId',(shippingData_table1['address']).alias('address'))
address_shippingData_table1=address_shippingData_table1.select('orderId',"address.*")


address_shippingData_table1  = address_shippingData_table1.dropDuplicates()
address_shippingData_table1 = address_shippingData_table1.drop('versionId','entityId','reference')
# display(address_shippingData_table1)

# COMMAND ----------

deliveryIds_logisticsInfo_shippingData_table1=logisticsInfo_shippingData_table1.select('orderId',('deliveryIds'))
deliveryIds_logisticsInfo_shippingData_table1=deliveryIds_logisticsInfo_shippingData_table1.select('orderId',"deliveryIds.*")
# display(deliveryIds_logisticsInfo_shippingData_table1)
#accountCarrierName

deliveryIds_logisticsInfo_shippingData_table1 = deliveryIds_logisticsInfo_shippingData_table1.dropDuplicates()
deliveryIds_logisticsInfo_shippingData_table1 = deliveryIds_logisticsInfo_shippingData_table1.drop('accountCarrierName')
# display(deliveryIds_logisticsInfo_shippingData_table1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##slas

# COMMAND ----------


slas_logisticsInfo_shippingData_table1=logisticsInfo_shippingData_table1.select('orderId','courierId',explode_outer('slas').alias('slas'))
slas_logisticsInfo_shippingData_table1=slas_logisticsInfo_shippingData_table1.select('orderId',"slas.*","courierId")

slas_logisticsInfo_shippingData_table1=slas_logisticsInfo_shippingData_table1.dropDuplicates()

slas_logisticsInfo_shippingData_table1 = slas_logisticsInfo_shippingData_table1.drop('polygonName')
# display(slas_logisticsInfo_shippingData_table1)

# COMMAND ----------


selectedAddresses_shippingData_table1=shippingData_table1.select('orderId',(shippingData_table1['selectedAddresses']).alias('selectedAddresses'))
selectedAddresses_shippingData_table1=selectedAddresses_shippingData_table1.select('orderId',"selectedAddresses.*")

selectedAddresses_shippingData_table1  = selectedAddresses_shippingData_table1.dropDuplicates()

selectedAddresses_shippingData_table1 = selectedAddresses_shippingData_table1.drop('versionId','entityId','reference')
# display(selectedAddresses_shippingData_table1)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #conveting to snakecase

# COMMAND ----------


for li in table_list:
    # print(li)
    # globals()[li].display()
    globals()[li]=camel_to_snake(globals()[li])



# COMMAND ----------

# MAGIC %md
# MAGIC ## add schedule date

# COMMAND ----------

# DBTITLE 1,olny add column

# sche_val=dbutils.widgets.get('schedule_date')

# for li in table_list:
    # print(li)
    # globals()[li]=add_load_date(globals()[li],sche_val)
    # globals()[li].display()

# # print(len(table_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ## save to silver

# COMMAND ----------

# DBTITLE 1,save with add column as list

# # table_list=['order_table1']
# # # db_name , sche_val getting from widgets

# upsert_col=key.split(',')

# for li in table_list:
#     df=globals()[li]
#     print(li)

#     table_name = li
#     # path=f'dbfs:/mnt/vtex-data/silver/{table_name}'


#     df = add_load_date(df,sche_val)
#     write_func_merge(df,db_name,table_name,upsert_col,path)

# COMMAND ----------

# DBTITLE 1,saving specific table
upsert_col=key.split(',')
df=globals()[table_name]

# print(key)
# print(upsert_col)
# print(silver_path)
# df.display()



# table_name, db_name ,sche_val,key from widgets

# df = add_load_date(df,sche_val)
# write_func_merge(df,db_name,table_name,upsert_col,silver_path)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from vtex_db.items_table2

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *  from vtex_db.order_table1 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL vtex_db.item_bundle_table;