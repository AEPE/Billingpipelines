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
# MAGIC DROP DATABASE importnextstar CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS importnextstar;

# COMMAND ----------

# MAGIC %sql
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

accountproduct = sqlContext.sql("select * from importnextstar.accountproduct")
accountproduct_pandas_df = ps.DataFrame(accountproduct)
accountproduct_pandas_df_unique = ps.DataFrame(accountproduct_pandas_df.nunique())
accountproduct_pandas_df_na = ps.DataFrame(accountproduct_pandas_df.isna().sum())
accountproduct_pandas_df_unique.reset_index(inplace=True)
accountproduct_pandas_df_na.reset_index(inplace=True)
accountproduct_results = accountproduct_pandas_df_unique.merge(accountproduct_pandas_df_na, on='index')
accountproduct_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

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

# COMMAND ----------

accountproduct_results = accountproduct_results.to_spark()
accountproduct_results = accountproduct_results.withColumn("table",lit("accountproduct"))

daylightsavingtime_results = daylightsavingtime_results.to_spark()
daylightsavingtime_results = daylightsavingtime_results.withColumn("table",lit("daylightsavingtime"))

energyhourlyusagedetail_results = energyhourlyusagedetail_results.to_spark()
energyhourlyusagedetail_results = energyhourlyusagedetail_results.withColumn("table",lit("energyhourlyusagedetail"))

energymonthlyservicepoint_results = energymonthlyservicepoint_results.to_spark()
energymonthlyservicepoint_results = energymonthlyservicepoint_results.withColumn("table",lit("energymonthlyservicepoint")))

estimatedusage_results = estimatedusage_results.to_spark()
estimatedusage_results = estimatedusage_results.withColumn("table",lit("estimatedusage"))

meterenergymonthlyusage_results = meterenergymonthlyusage_results.to_spark()
meterenergymonthlyusage_results = meterenergymonthlyusage_results.withColumn("table",lit("meterenergymonthlyusage"))

usageallowedthresholdfactor_results = usageallowedthresholdfactor_results.to_spark()
usageallowedthresholdfactor_results = usageallowedthresholdfactor_results.withColumn("table",lit("usageallowedthresholdfactor"))

usagebehaviorrules_results = usagebehaviorrules_results.to_spark()
usagebehaviorrules_results = usagebehaviorrules_results.withColumn("table",lit("usagebehaviorrules"))

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

#consolidate datasets in one single object
Report = accountproduct_results.union(daylightsavingtime_results).union(energyhourlyusagedetail_results).union(energymonthlyservicepoint_results).union(estimatedusage_results).union(meterenergymonthlyusage_results).union(usageallowedthresholdfactor_results).union(usagebehaviorrules_results)

#write to the csv file and display the report

Path = "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/unified_results.csv"
Report.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(Path)
