# Databricks notebook source
import pyspark.pandas as ps
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, DoubleType, DateType, DecimalType , TimestampType

# COMMAND ----------

storage_account_access_key = "yNWpvxnfTgCHt1mUlj8YPvCHbv4a/c5vmQQ4kcM4I0EC+VQDTxF7ffQ4bS340um02Z+mRYtMVavZ+AStLnlGyw=="

spark.conf.set(
  "fs.azure.account.key.stgbillingpoc.blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE importnextstar CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS importnextstar;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.ACCOUNTPRODUCTBILLINGPREFERENCES;
# MAGIC CREATE TABLE importnextstar.ACCOUNTPRODUCTBILLINGPREFERENCES 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/ACCOUNTPRODUCTBILLINGPREFERENCES";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.ACCOUNTPRODUCTHISTORY;
# MAGIC CREATE TABLE importnextstar.ACCOUNTPRODUCTHISTORY 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/ACCOUNTPRODUCTHISTORY";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.ACCOUNTPRODUCTFEATURE;
# MAGIC CREATE TABLE importnextstar.ACCOUNTPRODUCTFEATURE 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/ACCOUNTPRODUCTFEATURE";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.ACCOUNTPRODUCTATTRIBUTE;
# MAGIC CREATE TABLE importnextstar.ACCOUNTPRODUCTATTRIBUTE 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/ACCOUNTPRODUCTATTRIBUTE";

# COMMAND ----------

ACCOUNTPRODUCTBILLINGPREFERENCES_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.ACCOUNTPRODUCTBILLINGPREFERENCES"))
ACCOUNTPRODUCTBILLINGPREFERENCES_pandas_df_unique = ps.DataFrame(ACCOUNTPRODUCTBILLINGPREFERENCES_pandas_df.nunique())
ACCOUNTPRODUCTBILLINGPREFERENCES_pandas_df_na = ps.DataFrame(ACCOUNTPRODUCTBILLINGPREFERENCES_pandas_df.isna().sum())
ACCOUNTPRODUCTBILLINGPREFERENCES_pandas_df_unique.reset_index(inplace=True)
ACCOUNTPRODUCTBILLINGPREFERENCES_pandas_df_na.reset_index(inplace=True)
ACCOUNTPRODUCTBILLINGPREFERENCES_results = ACCOUNTPRODUCTBILLINGPREFERENCES_pandas_df_unique.merge(ACCOUNTPRODUCTBILLINGPREFERENCES_pandas_df_na, on='index')
ACCOUNTPRODUCTBILLINGPREFERENCES_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

ACCOUNTPRODUCTHISTORY_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.ACCOUNTPRODUCTHISTORY"))
ACCOUNTPRODUCTHISTORY_pandas_df_unique = ps.DataFrame(ACCOUNTPRODUCTHISTORY_pandas_df.nunique())
ACCOUNTPRODUCTHISTORY_pandas_df_na = ps.DataFrame(ACCOUNTPRODUCTHISTORY_pandas_df.isna().sum())
ACCOUNTPRODUCTHISTORY_pandas_df_unique.reset_index(inplace=True)
ACCOUNTPRODUCTHISTORY_pandas_df_na.reset_index(inplace=True)
ACCOUNTPRODUCTHISTORY_results = ACCOUNTPRODUCTHISTORY_pandas_df_unique.merge(ACCOUNTPRODUCTHISTORY_pandas_df_na, on='index')
ACCOUNTPRODUCTHISTORY_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

ACCOUNTPRODUCTFEATURE_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.ACCOUNTPRODUCTFEATURE"))
ACCOUNTPRODUCTFEATURE_pandas_df_unique = ps.DataFrame(ACCOUNTPRODUCTFEATURE_pandas_df.nunique())
ACCOUNTPRODUCTFEATURE_pandas_df_na = ps.DataFrame(ACCOUNTPRODUCTFEATURE_pandas_df.isna().sum())
ACCOUNTPRODUCTFEATURE_pandas_df_unique.reset_index(inplace=True)
ACCOUNTPRODUCTFEATURE_pandas_df_na.reset_index(inplace=True)
ACCOUNTPRODUCTFEATURE_results = ACCOUNTPRODUCTFEATURE_pandas_df_unique.merge(ACCOUNTPRODUCTFEATURE_pandas_df_na, on='index')
ACCOUNTPRODUCTFEATURE_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

ACCOUNTPRODUCTATTRIBUTE_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.ACCOUNTPRODUCTATTRIBUTE"))
ACCOUNTPRODUCTATTRIBUTE_pandas_df_unique = ps.DataFrame(ACCOUNTPRODUCTATTRIBUTE_pandas_df.nunique())
ACCOUNTPRODUCTATTRIBUTE_pandas_df_na = ps.DataFrame(ACCOUNTPRODUCTATTRIBUTE_pandas_df.isna().sum())
ACCOUNTPRODUCTATTRIBUTE_pandas_df_unique.reset_index(inplace=True)
ACCOUNTPRODUCTATTRIBUTE_pandas_df_na.reset_index(inplace=True)
ACCOUNTPRODUCTATTRIBUTE_results = ACCOUNTPRODUCTATTRIBUTE_pandas_df_unique.merge(ACCOUNTPRODUCTATTRIBUTE_pandas_df_na, on='index')
ACCOUNTPRODUCTATTRIBUTE_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

# COMMAND ----------

df_names = ["ACCOUNTPRODUCTBILLINGPREFERENCES"
           ,"ACCOUNTPRODUCTHISTORY"
           ,"ACCOUNTPRODUCTFEATURE"
           ,"ACCOUNTPRODUCTATTRIBUTE"]

df_list = [ACCOUNTPRODUCTBILLINGPREFERENCES_results
           ,ACCOUNTPRODUCTHISTORY_results
           ,ACCOUNTPRODUCTFEATURE_results
           ,ACCOUNTPRODUCTATTRIBUTE_results]

#Create empty DataFrame from empty RDD
df = ps.DataFrame(columns=['Colname', 'Uniquevals', 'Nullvals','Table'])

#function to generate report by consolidating all the separate dataframes and include a column to identify the database table corresponding for each row
def create_tablenamecol(df,dflist,dfnames):
    t=0
    for i in dflist:
        i['Table'] = dfnames[t]
        t += 1
        df = df.append(i)
    return df.to_spark()

# COMMAND ----------

Report = create_tablenamecol(df,df_list,df_names)

Path = "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/report_agreement.csv"
Report.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(Path)
