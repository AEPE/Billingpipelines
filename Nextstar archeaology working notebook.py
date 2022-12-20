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
# MAGIC DROP TABLE if exists importnextstar.deliveryserviceclasscategory;
# MAGIC CREATE TABLE importnextstar.deliveryserviceclasscategory 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliveryserviceclasscategory";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.energymonthlyservicepoint;
# MAGIC CREATE TABLE importnextstar.energymonthlyservicepoint 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/energymonthlyservicepoint";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverycharge;
# MAGIC CREATE TABLE importnextstar.deliverycharge 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverycharge";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargecodedescription;
# MAGIC CREATE TABLE importnextstar.deliverychargecodedescription 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargecodedescription";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargedetail;
# MAGIC CREATE TABLE importnextstar.deliverychargedetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargedetail";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.deliverychargeincludedetail;
# MAGIC CREATE TABLE importnextstar.deliverychargeincludedetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/deliverychargeincludedetail";

# COMMAND ----------

deliveryserviceclasscategory_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliveryserviceclasscategory"))
deliveryserviceclasscategory_pandas_df_unique = ps.DataFrame(deliveryserviceclasscategory_pandas_df.nunique())
deliveryserviceclasscategory_pandas_df_na = ps.DataFrame(deliveryserviceclasscategory_pandas_df.isna().sum())
deliveryserviceclasscategory_pandas_df_unique.reset_index(inplace=True)
deliveryserviceclasscategory_pandas_df_na.reset_index(inplace=True)
deliveryserviceclasscategory_results = deliveryserviceclasscategory_pandas_df_unique.merge(deliveryserviceclasscategory_pandas_df_na, on='index')
deliveryserviceclasscategory_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

energymonthlyservicepoint_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.energymonthlyservicepoint"))
energymonthlyservicepoint_pandas_df_unique = ps.DataFrame(energymonthlyservicepoint_pandas_df.nunique())
energymonthlyservicepoint_pandas_df_na = ps.DataFrame(energymonthlyservicepoint_pandas_df.isna().sum())
energymonthlyservicepoint_pandas_df_unique.reset_index(inplace=True)
energymonthlyservicepoint_pandas_df_na.reset_index(inplace=True)
energymonthlyservicepoint_results = energymonthlyservicepoint_pandas_df_unique.merge(energymonthlyservicepoint_pandas_df_na, on='index')
energymonthlyservicepoint_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverycharge_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliverycharge"))
deliverycharge_pandas_df_unique = ps.DataFrame(deliverycharge_pandas_df.nunique())
deliverycharge_pandas_df_na = ps.DataFrame(deliverycharge_pandas_df.isna().sum())
deliverycharge_pandas_df_unique.reset_index(inplace=True)
deliverycharge_pandas_df_na.reset_index(inplace=True)
deliverycharge_results = deliverycharge_pandas_df_unique.merge(deliverycharge_pandas_df_na, on='index')
deliverycharge_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargecodedescription_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliverychargecodedescription"))
deliverychargecodedescription_pandas_df_unique = ps.DataFrame(deliverychargecodedescription_pandas_df.nunique())
deliverychargecodedescription_pandas_df_na = ps.DataFrame(deliverychargecodedescription_pandas_df.isna().sum())
deliverychargecodedescription_pandas_df_unique.reset_index(inplace=True)
deliverychargecodedescription_pandas_df_na.reset_index(inplace=True)
deliverychargecodedescription_results = deliverychargecodedescription_pandas_df_unique.merge(deliverychargecodedescription_pandas_df_na, on='index')
deliverychargecodedescription_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargedetail_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliverychargedetail"))
deliverychargedetail_pandas_df_unique = ps.DataFrame(deliverychargedetail_pandas_df.nunique())
deliverychargedetail_pandas_df_na = ps.DataFrame(deliverychargedetail_pandas_df.isna().sum())
deliverychargedetail_pandas_df_unique.reset_index(inplace=True)
deliverychargedetail_pandas_df_na.reset_index(inplace=True)
deliverychargedetail_results = deliverychargedetail_pandas_df_unique.merge(deliverychargedetail_pandas_df_na, on='index')
deliverychargedetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

deliverychargeincludedetail_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.deliverychargeincludedetail"))
deliverychargeincludedetail_pandas_df_unique = ps.DataFrame(deliverychargeincludedetail_pandas_df.nunique())
deliverychargeincludedetail_pandas_df_na = ps.DataFrame(deliverychargeincludedetail_pandas_df.isna().sum())
deliverychargeincludedetail_pandas_df_unique.reset_index(inplace=True)
deliverychargeincludedetail_pandas_df_na.reset_index(inplace=True)
deliverychargeincludedetail_results = deliverychargeincludedetail_pandas_df_unique.merge(deliverychargeincludedetail_pandas_df_na, on='index')
deliverychargeincludedetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

# COMMAND ----------

#display(deliveryserviceclasscategory_results)
#display(energymonthlyservicepoint_results)
#display(deliverycharge_results)

# COMMAND ----------

deliveryserviceclasscategory_results = deliveryserviceclasscategory_results.to_spark()
deliveryserviceclasscategory_results = deliveryserviceclasscategory_results.withColumn("table",lit("deliveryserviceclasscategory"))

energymonthlyservicepoint_results = energymonthlyservicepoint_results.to_spark()
energymonthlyservicepoint_results = energymonthlyservicepoint_results.withColumn("table",lit("energymonthlyservicepoint"))

deliverycharge_results = deliverycharge_results.to_spark()
deliverycharge_results = deliverycharge_results.withColumn("table",lit("deliverycharge"))

deliverychargecodedescription_results = deliverychargecodedescription_results.to_spark()
deliverychargecodedescription_results = deliverychargecodedescription_results.withColumn("table",lit("deliverychargecodedescription"))

deliverychargedetail_results = deliverychargedetail_results.to_spark()
deliverychargedetail_results = deliverychargedetail_results.withColumn("table",lit("deliverychargedetail"))

deliverychargeincludedetail_results = deliverychargeincludedetail_results.to_spark()
deliverychargeincludedetail_results = deliverychargeincludedetail_results.withColumn("table",lit("deliverychargeincludedetail"))

Path = "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/report_plopez.csv"

Report = deliveryserviceclasscategory_results.union(energymonthlyservicepoint_results).union(deliverycharge_results).union(deliverychargecodedescription_results).union(deliverychargedetail_results).union(deliverychargeincludedetail_results)
Report.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(Path)


# COMMAND ----------

#display(Report)
