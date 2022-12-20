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
# MAGIC --DROP DATABASE importnextstar CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS importnextstar;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverybalance;
# MAGIC CREATE TABLE importnextstar.deliverybalance 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverybalance";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargeitemtype;
# MAGIC CREATE TABLE importnextstar.deliverychargeitemtype 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargeitemtype";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargestatus;
# MAGIC CREATE TABLE importnextstar.deliverychargestatus 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargestatus";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverymeterdetail;
# MAGIC CREATE TABLE importnextstar.deliverymeterdetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverymeterdetail";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliveryserviceclasscategory;
# MAGIC CREATE TABLE importnextstar.deliveryserviceclasscategory 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliveryserviceclasscategory";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.factorestimateddelivery;
# MAGIC CREATE TABLE importnextstar.factorestimateddelivery 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/factorestimateddelivery";

# COMMAND ----------

deliverybalance_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliverybalance"))
deliverybalance_pandas_df_unique = ps.DataFrame(deliverybalance_pandas_df.nunique())
deliverybalance_pandas_df_na = ps.DataFrame(deliverybalance_pandas_df.isna().sum())
deliverybalance_pandas_df_unique.reset_index(inplace=True)
deliverybalance_pandas_df_na.reset_index(inplace=True)
deliverybalance_results = deliverybalance_pandas_df_unique.merge(deliverybalance_pandas_df_na, on='index')
deliverybalance_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargeitemtype_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliverychargeitemtype"))
deliverychargeitemtype_pandas_df_unique = ps.DataFrame(deliverychargeitemtype_pandas_df.nunique())
deliverychargeitemtype_pandas_df_na = ps.DataFrame(deliverychargeitemtype_pandas_df.isna().sum())
deliverychargeitemtype_pandas_df_unique.reset_index(inplace=True)
deliverychargeitemtype_pandas_df_na.reset_index(inplace=True)
deliverychargeitemtype_results = deliverychargeitemtype_pandas_df_unique.merge(deliverychargeitemtype_pandas_df_na, on='index')
deliverychargeitemtype_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargestatus_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliverychargestatus"))
deliverychargestatus_pandas_df_unique = ps.DataFrame(deliverychargestatus_pandas_df.nunique())
deliverychargestatus_pandas_df_na = ps.DataFrame(deliverychargestatus_pandas_df.isna().sum())
deliverychargestatus_pandas_df_unique.reset_index(inplace=True)
deliverychargestatus_pandas_df_na.reset_index(inplace=True)
deliverychargestatus_results = deliverychargestatus_pandas_df_unique.merge(deliverychargestatus_pandas_df_na, on='index')
deliverychargestatus_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverymeterdetail_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliverymeterdetail"))
deliverymeterdetail_pandas_df_unique = ps.DataFrame(deliverymeterdetail_pandas_df.nunique())
deliverymeterdetail_pandas_df_na = ps.DataFrame(deliverymeterdetail_pandas_df.isna().sum())
deliverymeterdetail_pandas_df_unique.reset_index(inplace=True)
deliverymeterdetail_pandas_df_na.reset_index(inplace=True)
deliverymeterdetail_results = deliverymeterdetail_pandas_df_unique.merge(deliverymeterdetail_pandas_df_na, on='index')
deliverymeterdetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliveryserviceclasscategory_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliveryserviceclasscategory"))
deliveryserviceclasscategory_pandas_df_unique = ps.DataFrame(deliveryserviceclasscategory_pandas_df.nunique())
deliveryserviceclasscategory_pandas_df_na = ps.DataFrame(deliveryserviceclasscategory_pandas_df.isna().sum())
deliveryserviceclasscategory_pandas_df_unique.reset_index(inplace=True)
deliveryserviceclasscategory_pandas_df_na.reset_index(inplace=True)
deliveryserviceclasscategory_results = deliveryserviceclasscategory_pandas_df_unique.merge(deliveryserviceclasscategory_pandas_df_na, on='index')
deliveryserviceclasscategory_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

factorestimateddelivery_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.factorestimateddelivery"))
factorestimateddelivery_pandas_df_unique = ps.DataFrame(factorestimateddelivery_pandas_df.nunique())
factorestimateddelivery_pandas_df_na = ps.DataFrame(factorestimateddelivery_pandas_df.isna().sum())
factorestimateddelivery_pandas_df_unique.reset_index(inplace=True)
factorestimateddelivery_pandas_df_na.reset_index(inplace=True)
factorestimateddelivery_results = factorestimateddelivery_pandas_df_unique.merge(factorestimateddelivery_pandas_df_na, on='index')
factorestimateddelivery_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

#pending to include a tablename col 

# COMMAND ----------

display(deliverybalance_pandas_df_unique)
display(deliverychargeitemtype_pandas_df_unique)
display(deliverychargestatus_pandas_df_unique)
display(deliverymeterdetail_pandas_df_unique)
display(deliveryserviceclasscategory_pandas_df_unique)
display(factorestimateddelivery_pandas_df_unique)

# COMMAND ----------

Path = "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/accountproduct_results_javier.csv"

deliverybalance_results = deliverybalance_results.to_spark()
deliverychargeitemtype_results = deliverychargeitemtype_results.to_spark()
deliverychargestatus_results = deliverychargestatus_results.to_spark()
deliverymeterdetail_results = deliverymeterdetail_results.to_spark()
deliveryserviceclasscategory_results = deliveryserviceclasscategory_results.to_spark()
factorestimateddelivery_results = factorestimateddelivery_results.to_spark()

deliverybalance_results = deliverybalance_results.withColumn("table",lit("deliverybalance"))
deliverychargeitemtype_results = deliverychargeitemtype_results.withColumn("table",lit("deliverychargeitemtype"))
deliverychargestatus_results = deliverychargestatus_results.withColumn("table",lit("deliverychargestatus"))
deliverymeterdetail_results = deliverymeterdetail_results.withColumn("table",lit("deliverymeterdetail"))
deliveryserviceclasscategory_results = deliveryserviceclasscategory_results.withColumn("table",lit("deliveryserviceclasscategory"))
factorestimateddelivery_results = factorestimateddelivery_results.withColumn("table",lit("factorestimateddelivery"))

Report = deliverybalance_results.union(deliverychargeitemtype_results).union(deliverychargestatus_results).union(deliverymeterdetail_results).union(deliveryserviceclasscategory_results).union(factorestimateddelivery_results)

Report.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(Path)
