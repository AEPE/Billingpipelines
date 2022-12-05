# Databricks notebook source
import pyspark.pandas as ps
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, DoubleType, DateType, DecimalType , TimestampType

# COMMAND ----------

storage_account_access_key = "yNWpvxnfTgCHt1mUlj8YPvCHbv4a/c5vmQQ4kcM4I0EC+VQDTxF7ffQ4bS340um02Z+mRYtMVavZ+AStLnlGyw=="

spark.conf.set(
  "fs.azure.account.key.stgbillingpoc.blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE importnextstar CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS importnextstar;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE if exists import.importnextstar;
# MAGIC CREATE TABLE importnextstar.accountproduct 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/accountproduct";

# COMMAND ----------



accountproduct = sqlContext.sql("select * from importnextstar.accountproduct")
accountproduct_pandas_df = ps.DataFrame(accountproduct)
accountproduct_pandas_df_unique = ps.DataFrame(accountproduct_pandas_df.nunique())
accountproduct_pandas_df_na = ps.DataFrame(accountproduct_pandas_df.isna().sum())
accountproduct_pandas_df_unique.reset_index(inplace=True)
accountproduct_pandas_df_na.reset_index(inplace=True)
accountproduct_results = accountproduct_pandas_df_unique.merge(accountproduct_pandas_df_na, on='index')
accountproduct_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

#pending to include a tablename col 

# COMMAND ----------

accountproduct_results = accountproduct_results.to_spark()
accountproduct_results = accountproduct_results.withColumn("table",lit("accountproduct"))

Path = "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/accountproduct_results.csv"
accountproduct_results.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(Path)
