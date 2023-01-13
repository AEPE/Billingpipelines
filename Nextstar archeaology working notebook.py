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
# MAGIC DROP TABLE if exists importnextstar.accountproduct;
# MAGIC CREATE TABLE importnextstar.accountproduct 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/accountproduct";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.daylightsavingtime;
# MAGIC CREATE TABLE importnextstar.daylightsavingtime 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/daylightsavingtime";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.energyhourlyusagedetail;
# MAGIC CREATE TABLE importnextstar.energyhourlyusagedetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/energyhourlyusagedetail";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.basetype;
# MAGIC CREATE TABLE importnextstar.basetype 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/basetype";
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
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.energymonthlyservicepoint;
# MAGIC CREATE TABLE importnextstar.energymonthlyservicepoint 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/energymonthlyservicepoint";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.estimatedusage;
# MAGIC CREATE TABLE importnextstar.estimatedusage 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/estimatedusage";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.meterenergymonthlyusage;
# MAGIC CREATE TABLE importnextstar.meterenergymonthlyusage 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/meterenergymonthlyusage";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.usageallowedthresholdfactor;
# MAGIC CREATE TABLE importnextstar.usageallowedthresholdfactor 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/usageallowedthresholdfactor";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.usagebehaviorrules;
# MAGIC CREATE TABLE importnextstar.usagebehaviorrules 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/usagebehaviorrules";
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
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.accountproductagreement;
# MAGIC CREATE TABLE importnextstar.accountproductagreement 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/accountproductagreement";
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.billinginvoiceparameter;
# MAGIC CREATE TABLE importnextstar.billinginvoiceparameter 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/billinginvoiceparameter";
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.unitofmeasure;
# MAGIC CREATE TABLE importnextstar.unitofmeasure 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/unitofmeasure";
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.energyusagestatus;
# MAGIC CREATE TABLE importnextstar.energyusagestatus 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/energyusagestatus";
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.energyusage;
# MAGIC CREATE TABLE importnextstar.energyusage 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/energyusage";
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.accountproductservicepointdetail;
# MAGIC CREATE TABLE importnextstar.accountproductservicepointdetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/accountproductservicepointdetail";
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.factorestimateddelivery;
# MAGIC CREATE TABLE importnextstar.factorestimateddelivery 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/factorestimateddelivery";

# COMMAND ----------

Accountproduct_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.accountproduct"))
Accountproduct_pandas_df_unique = ps.DataFrame(Accountproduct_pandas_df.nunique())
Accountproduct_pandas_df_na = ps.DataFrame(Accountproduct_pandas_df.isna().sum())
Accountproduct_pandas_df_unique.reset_index(inplace=True)
Accountproduct_pandas_df_na.reset_index(inplace=True)
Accountproduct_results = Accountproduct_pandas_df_unique.merge(Accountproduct_pandas_df_na, on='index')
Accountproduct_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

basetype_pandas_df = ps.DataFrame(sqlContext.sql("select * from importnextstar.basetype"))
basetype_pandas_df_unique = ps.DataFrame(basetype_pandas_df.nunique())
basetype_pandas_df_na = ps.DataFrame(basetype_pandas_df.isna().sum())
basetype_pandas_df_unique.reset_index(inplace=True)
basetype_pandas_df_na.reset_index(inplace=True)
basetype_results = basetype_pandas_df_unique.merge(basetype_pandas_df_na, on='index')
basetype_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

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

daylightsavingtime = sqlContext.sql("select * from importnextstar.daylightsavingtime")
daylightsavingtime_pandas_df = ps.DataFrame(daylightsavingtime)
daylightsavingtime_pandas_df_unique = ps.DataFrame(daylightsavingtime_pandas_df.nunique())
daylightsavingtime_pandas_df_na = ps.DataFrame(daylightsavingtime_pandas_df.isna().sum())
daylightsavingtime_pandas_df_unique.reset_index(inplace=True)
daylightsavingtime_pandas_df_na.reset_index(inplace=True)
daylightsavingtime_results = daylightsavingtime_pandas_df_unique.merge(daylightsavingtime_pandas_df_na, on='index')
daylightsavingtime_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

energyhourlyusagedetail = sqlContext.sql("select * from importnextstar.energyhourlyusagedetail")
energyhourlyusagedetail_pandas_df = ps.DataFrame(energyhourlyusagedetail)
energyhourlyusagedetail_pandas_df_unique = ps.DataFrame(energyhourlyusagedetail_pandas_df.nunique())
energyhourlyusagedetail_pandas_df_na = ps.DataFrame(energyhourlyusagedetail_pandas_df.isna().sum())
energyhourlyusagedetail_pandas_df_unique.reset_index(inplace=True)
energyhourlyusagedetail_pandas_df_na.reset_index(inplace=True)
energyhourlyusagedetail_results = energyhourlyusagedetail_pandas_df_unique.merge(energyhourlyusagedetail_pandas_df_na, on='index')
energyhourlyusagedetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

energymonthlyservicepoint = sqlContext.sql("select * from importnextstar.energymonthlyservicepoint")
energymonthlyservicepoint_pandas_df = ps.DataFrame(energymonthlyservicepoint)
energymonthlyservicepoint_pandas_df_unique = ps.DataFrame(energymonthlyservicepoint_pandas_df.nunique())
energymonthlyservicepoint_pandas_df_na = ps.DataFrame(energymonthlyservicepoint_pandas_df.isna().sum())
energymonthlyservicepoint_pandas_df_unique.reset_index(inplace=True)
energymonthlyservicepoint_pandas_df_na.reset_index(inplace=True)
energymonthlyservicepoint_results = energymonthlyservicepoint_pandas_df_unique.merge(energymonthlyservicepoint_pandas_df_na, on='index')
energymonthlyservicepoint_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

estimatedusage = sqlContext.sql("select * from importnextstar.estimatedusage")
estimatedusage_pandas_df = ps.DataFrame(estimatedusage)
estimatedusage_pandas_df_unique = ps.DataFrame(estimatedusage_pandas_df.nunique())
estimatedusage_pandas_df_na = ps.DataFrame(estimatedusage_pandas_df.isna().sum())
estimatedusage_pandas_df_unique.reset_index(inplace=True)
estimatedusage_pandas_df_na.reset_index(inplace=True)
estimatedusage_results = estimatedusage_pandas_df_unique.merge(estimatedusage_pandas_df_na, on='index')
estimatedusage_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

meterenergymonthlyusage = sqlContext.sql("select * from importnextstar.meterenergymonthlyusage")
meterenergymonthlyusage_pandas_df = ps.DataFrame(meterenergymonthlyusage)
meterenergymonthlyusage_pandas_df_unique = ps.DataFrame(meterenergymonthlyusage_pandas_df.nunique())
meterenergymonthlyusage_pandas_df_na = ps.DataFrame(meterenergymonthlyusage_pandas_df.isna().sum())
meterenergymonthlyusage_pandas_df_unique.reset_index(inplace=True)
meterenergymonthlyusage_pandas_df_na.reset_index(inplace=True)
meterenergymonthlyusage_results = meterenergymonthlyusage_pandas_df_unique.merge(meterenergymonthlyusage_pandas_df_na, on='index')
meterenergymonthlyusage_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

usageallowedthresholdfactor = sqlContext.sql("select * from importnextstar.usageallowedthresholdfactor")
usageallowedthresholdfactor_pandas_df = ps.DataFrame(usageallowedthresholdfactor)
usageallowedthresholdfactor_pandas_df_unique = ps.DataFrame(usageallowedthresholdfactor_pandas_df.nunique())
usageallowedthresholdfactor_pandas_df_na = ps.DataFrame(usageallowedthresholdfactor_pandas_df.isna().sum())
usageallowedthresholdfactor_pandas_df_unique.reset_index(inplace=True)
usageallowedthresholdfactor_pandas_df_na.reset_index(inplace=True)
usageallowedthresholdfactor_results = usageallowedthresholdfactor_pandas_df_unique.merge(usageallowedthresholdfactor_pandas_df_na, on='index')
usageallowedthresholdfactor_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

usagebehaviorrules = sqlContext.sql("select * from importnextstar.usagebehaviorrules")
usagebehaviorrules_pandas_df = ps.DataFrame(usagebehaviorrules)
usagebehaviorrules_pandas_df_unique = ps.DataFrame(usagebehaviorrules_pandas_df.nunique())
usagebehaviorrules_pandas_df_na = ps.DataFrame(usagebehaviorrules_pandas_df.isna().sum())
usagebehaviorrules_pandas_df_unique.reset_index(inplace=True)
usagebehaviorrules_pandas_df_na.reset_index(inplace=True)
usagebehaviorrules_results = usagebehaviorrules_pandas_df_unique.merge(usagebehaviorrules_pandas_df_na, on='index')
usagebehaviorrules_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

accountproductagreement = sqlContext.sql("select * from importnextstar.accountproductagreement")
accountproductagreement_pandas_df = ps.DataFrame(accountproductagreement)
accountproductagreement_pandas_df_unique = ps.DataFrame(accountproductagreement_pandas_df.nunique())
accountproductagreement_pandas_df_na = ps.DataFrame(accountproductagreement_pandas_df.isna().sum())
accountproductagreement_pandas_df_unique.reset_index(inplace=True)
accountproductagreement_pandas_df_na.reset_index(inplace=True)
accountproductagreement_results = accountproductagreement_pandas_df_unique.merge(accountproductagreement_pandas_df_na, on='index')
accountproductagreement_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)


billinginvoiceparameter = sqlContext.sql("select * from importnextstar.billinginvoiceparameter")
billinginvoiceparameter_pandas_df = ps.DataFrame(billinginvoiceparameter)
billinginvoiceparameter_pandas_df_unique = ps.DataFrame(billinginvoiceparameter_pandas_df.nunique())
billinginvoiceparameter_pandas_df_na = ps.DataFrame(billinginvoiceparameter_pandas_df.isna().sum())
billinginvoiceparameter_pandas_df_unique.reset_index(inplace=True)
billinginvoiceparameter_pandas_df_na.reset_index(inplace=True)
billinginvoiceparameter_results = billinginvoiceparameter_pandas_df_unique.merge(billinginvoiceparameter_pandas_df_na, on='index')
billinginvoiceparameter_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

unitofmeasure = sqlContext.sql("select * from importnextstar.unitofmeasure")
unitofmeasure_pandas_df = ps.DataFrame(unitofmeasure)
unitofmeasure_pandas_df_unique = ps.DataFrame(unitofmeasure_pandas_df.nunique())
unitofmeasure_pandas_df_na = ps.DataFrame(unitofmeasure_pandas_df.isna().sum())
unitofmeasure_pandas_df_unique.reset_index(inplace=True)
unitofmeasure_pandas_df_na.reset_index(inplace=True)
unitofmeasure_results = unitofmeasure_pandas_df_unique.merge(unitofmeasure_pandas_df_na, on='index')
unitofmeasure_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

energyusagestatus = sqlContext.sql("select * from importnextstar.energyusagestatus")
energyusagestatus_pandas_df = ps.DataFrame(energyusagestatus)
energyusagestatus_pandas_df_unique = ps.DataFrame(energyusagestatus_pandas_df.nunique())
energyusagestatus_pandas_df_na = ps.DataFrame(energyusagestatus_pandas_df.isna().sum())
energyusagestatus_pandas_df_unique.reset_index(inplace=True)
energyusagestatus_pandas_df_na.reset_index(inplace=True)
energyusagestatus_results = energyusagestatus_pandas_df_unique.merge(energyusagestatus_pandas_df_na, on='index')
energyusagestatus_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

energyusage = sqlContext.sql("select * from importnextstar.energyusage")
energyusage_pandas_df = ps.DataFrame(energyusage)
energyusage_pandas_df_unique = ps.DataFrame(energyusage_pandas_df.nunique())
energyusage_pandas_df_na = ps.DataFrame(energyusage_pandas_df.isna().sum())
energyusage_pandas_df_unique.reset_index(inplace=True)
energyusage_pandas_df_na.reset_index(inplace=True)
energyusage_results = energyusage_pandas_df_unique.merge(energyusage_pandas_df_na, on='index')
energyusage_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

factorestimateddelivery = sqlContext.sql("select * from importnextstar.factorestimateddelivery")
factorestimateddelivery_pandas_df = ps.DataFrame(factorestimateddelivery)
factorestimateddelivery_pandas_df_unique = ps.DataFrame(factorestimateddelivery_pandas_df.nunique())
factorestimateddelivery_pandas_df_na = ps.DataFrame(factorestimateddelivery_pandas_df.isna().sum())
factorestimateddelivery_pandas_df_unique.reset_index(inplace=True)
factorestimateddelivery_pandas_df_na.reset_index(inplace=True)
factorestimateddelivery_results = factorestimateddelivery_pandas_df_unique.merge(factorestimateddelivery_pandas_df_na, on='index')
factorestimateddelivery_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

# COMMAND ----------

df_names = ["Accountproduct"
           ,"basetype"
           ,"deliveryserviceclasscategory"
           ,"energymonthlyservicepoint"
           ,"deliverycharge"
           ,"deliverychargecodedescription"
           ,"deliverychargedetail"
           ,"deliverychargeincludedetail"
           ,"deliverybalance"
           ,"deliverychargeitemtype"
           ,"deliverychargestatus"
           ,"deliverymeterdetail"
           ,"deliveryserviceclasscategory"
           ,"factorestimateddelivery"
           ,"daylightsavingtime"
           ,"energyhourlyusagedetail"
           ,"energymonthlyservicepoint"
           ,"estimatedusage"
           ,"meterenergymonthlyusage"
           ,"usageallowedthresholdfactor"
           ,"usagebehaviorrules"
           ,"accountproductagreement"
           ,"billinginvoiceparameter"
           ,"unitofmeasure"
           ,"energyusagestatus"
           ,"energyusage"
           ,"factorestimateddelivery"]

df_list = [Accountproduct_results
           ,basetype_results
           ,deliveryserviceclasscategory_results
           ,energymonthlyservicepoint_results
           ,deliverycharge_results
           ,deliverychargecodedescription_results
           ,deliverychargedetail_results
           ,deliverychargeincludedetail_results
           ,deliverybalance_results
           ,deliverychargeitemtype_results
           ,deliverychargestatus_results
           ,deliverymeterdetail_results
           ,deliveryserviceclasscategory_results
           ,factorestimateddelivery_results
           ,daylightsavingtime_results
           ,energyhourlyusagedetail_results
           ,energymonthlyservicepoint_results
           ,estimatedusage_results
           ,meterenergymonthlyusage_results
           ,usageallowedthresholdfactor_results
           ,usagebehaviorrules_results
           ,accountproductagreement_results
           ,billinginvoiceparameter_results
           ,unitofmeasure_results
           ,energyusagestatus_results
           ,energyusage_results
           ,factorestimateddelivery_results]

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
