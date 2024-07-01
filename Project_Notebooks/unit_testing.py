# Databricks notebook source
# MAGIC %run ./bronze_to_silver/common_functions

# COMMAND ----------

import unittest
import warnings

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
# print(silver_path)

# COMMAND ----------

# DBTITLE 1,set up df
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

data = [
    (1, "Krishna", 30, "Delhi"),
    (2, "Pratibha", None, "Bangalore"),
    (3, "Kuldeep", 28, None),
    (4, "Basheer", 35, "Hyderabad"),
    (5, None, None, None),
    (6, "Manvi", None, None)
]

df = spark.createDataFrame(data, schema)

# COMMAND ----------


class PTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.filterwarnings("ignore", category=ResourceWarning)

    #testing read_file_source
    def test_read_file_source(self):
        path='dbfs:/mnt/vtex-data/bronze/vtex_test_data.json'
        format='json'
            
        ################
        actual_df=read_file_source(path,format)
        expected_df=spark.read.format(format).load(path)

        self.assertEqual(expected_df.collect(),actual_df.collect())
        print(f"Test for read file of {format}format , passed successfully!")
    
    #testing write_func_merge
    def test_write_func_merge(self):
        upsert_col=key.split(',')
        df=spark.table(f'vtex_db.{table_name}')
        write_func_merge(df,db_name,table_name,upsert_col,silver_path)

        full_table_name = f"{db_name}.{table_name}"

        table_exists = spark._jsparkSession.catalog().tableExists(full_table_name)
        self.assertTrue(table_exists==True)
        print(f"Test for write_func_merge , passed successfully!")
    

    # #testing remove_null
    def test_remove_null(self):
        column='Id'
        actual_df=remove_null(df, column)
        # display(actual_df)
        self.assertNotEqual(df.collect(),actual_df.collect())
        print(f"Test for remove_null , passed successfully!")



    #testing camel_to_snake
    def test_camel_to_snake(self):
        actual_df=camel_to_snake(df)
        val=True
        for colu in actual_df.columns:
            # print(colu)
            t=colu
            for i in t:
                if i.isupper():
                    val=False
        # print(val)
        self.assertTrue(val==True)
        print(f"Test for camel_to_snake , passed successfully!")


    #testing add_load_date
    def test_add_load_date(self):
        actual_df=add_load_date(df, sche_val)
        expected_df=df.withColumn('load_date', to_timestamp(lit(sche_val)))
        self.assertEqual(expected_df.collect(),actual_df.collect())
        print(f"Test for add_load_date , passed successfully!")







if __name__ == "__main__":
    unittest.main(argv=[''], verbosity=2, exit=False)
