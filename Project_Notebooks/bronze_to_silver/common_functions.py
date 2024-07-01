# Databricks notebook source
# MAGIC %run ../bronze_to_silver/vtex_schema

# COMMAND ----------

def read_file_source(path,format):
    read_df=spark.read.format(format).load(path)
    return read_df

# COMMAND ----------

# def read_file(path,format,schema):
#     read_df=spark.read.format(format).schema(schema).load(path)
#     return read_df



# COMMAND ----------

def write_func_merge(df, db_name, table_name, upsert_col,path):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    full_table_name = f"{db_name}.{table_name}"

    table_exists = spark._jsparkSession.catalog().tableExists(full_table_name)

    if not table_exists:
        df.write.mode("overwrite").format("delta").saveAsTable(full_table_name)


    # df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(path)

    else:
        deltaTable = DeltaTable.forName(spark, full_table_name)
        matchKeys = " AND ".join("old." + col + " = new." + col for col in upsert_col)
        
        deltaTable.alias("old") \
            .merge(df.alias("new"), matchKeys) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()


# COMMAND ----------

def remove_null(df, column):
    columns_to_check = [col for col in df.columns if col != column]
    df_cleaned = df.dropna(subset=columns_to_check)
    condition = " OR ".join([f"{col} IS NOT NULL" for col in columns_to_check])
    df_cleaned = df.filter(expr(condition))
    return df_cleaned



# COMMAND ----------

def camel_to_snake(df):
    for column in df.columns:
        res = ""
        for i in column:
            if i.isupper():
                res += "_" + i.lower()
            else:
                res += i
        df = df.withColumnRenamed(column, res.lstrip("_"))
    return df

# COMMAND ----------

def add_load_date(df, sche_val):
    df = df.withColumn('load_date', to_timestamp(lit(sche_val)))
    # df = df.withColumn('load_date',to_date(col("load_date"),"yyyy-MM-dd"))
    return df