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
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.SALESINVOICE;
# MAGIC CREATE TABLE importnextstar.SALESINVOICE 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/SALESINVOICE";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.SALESINVOICEERROR;
# MAGIC CREATE TABLE importnextstar.SALESINVOICEERROR 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/SALESINVOICEERROR";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.SALESINVOICEGROUPINGEMAIL;
# MAGIC CREATE TABLE importnextstar.SALESINVOICEGROUPINGEMAIL 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/SALESINVOICEGROUPINGEMAIL";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.SALESINVOICEITEM;
# MAGIC CREATE TABLE importnextstar.SALESINVOICEITEM 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/SALESINVOICEITEM";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.SALESINVOICEPAYMENT;
# MAGIC CREATE TABLE importnextstar.SALESINVOICEPAYMENT 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/SALESINVOICEPAYMENT";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.INVOICEFILEPATH;
# MAGIC CREATE TABLE importnextstar.INVOICEFILEPATH 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/INVOICEFILEPATH";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.INVOICEITEMTYPE;
# MAGIC CREATE TABLE importnextstar.INVOICEITEMTYPE 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/INVOICEITEMTYPE";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.INVOICENETMETERINFORMATION;
# MAGIC CREATE TABLE importnextstar.INVOICENETMETERINFORMATION 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/INVOICENETMETERINFORMATION";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.INVOICESTATUS;
# MAGIC CREATE TABLE importnextstar.INVOICESTATUS 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/INVOICESTATUS";
# MAGIC 
# MAGIC DROP TABLE if exists importnextstar.MANUALINVOICE;
# MAGIC CREATE TABLE importnextstar.MANUALINVOICE 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://nexstar@stgbillingpoc.blob.core.windows.net/parquet-nextstar-raw/MANUALINVOICE";

# COMMAND ----------

SALESINVOICE = sqlContext.sql("select * from importnextstar.SALESINVOICE")
SALESINVOICE_pandas_df = ps.DataFrame(SALESINVOICE)
SALESINVOICE_pandas_df_unique = ps.DataFrame(SALESINVOICE_pandas_df.nunique())
SALESINVOICE_pandas_df_na = ps.DataFrame(SALESINVOICE_pandas_df.isna().sum())
SALESINVOICE_pandas_df_unique.reset_index(inplace=True)
SALESINVOICE_pandas_df_na.reset_index(inplace=True)
SALESINVOICE_results = SALESINVOICE_pandas_df_unique.merge(SALESINVOICE_pandas_df_na, on='index')
SALESINVOICE_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

SALESINVOICEERROR = sqlContext.sql("select * from importnextstar.SALESINVOICEERROR")
SALESINVOICEERROR_pandas_df = ps.DataFrame(SALESINVOICEERROR)
SALESINVOICEERROR_pandas_df_unique = ps.DataFrame(SALESINVOICEERROR_pandas_df.nunique())
SALESINVOICEERROR_pandas_df_na = ps.DataFrame(SALESINVOICEERROR_pandas_df.isna().sum())
SALESINVOICEERROR_pandas_df_unique.reset_index(inplace=True)
SALESINVOICEERROR_pandas_df_na.reset_index(inplace=True)
SALESINVOICEERROR_results = SALESINVOICEERROR_pandas_df_unique.merge(SALESINVOICEERROR_pandas_df_na, on='index')
SALESINVOICEERROR_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

SALESINVOICEGROUPINGEMAIL = sqlContext.sql("select * from importnextstar.SALESINVOICEGROUPINGEMAIL")
SALESINVOICEGROUPINGEMAIL_pandas_df = ps.DataFrame(SALESINVOICEGROUPINGEMAIL)
SALESINVOICEGROUPINGEMAIL_pandas_df_unique = ps.DataFrame(SALESINVOICEGROUPINGEMAIL_pandas_df.nunique())
SALESINVOICEGROUPINGEMAIL_pandas_df_na = ps.DataFrame(SALESINVOICEGROUPINGEMAIL_pandas_df.isna().sum())
SALESINVOICEGROUPINGEMAIL_pandas_df_unique.reset_index(inplace=True)
SALESINVOICEGROUPINGEMAIL_pandas_df_na.reset_index(inplace=True)
SALESINVOICEGROUPINGEMAIL_results = SALESINVOICEGROUPINGEMAIL_pandas_df_unique.merge(SALESINVOICEGROUPINGEMAIL_pandas_df_na, on='index')
SALESINVOICEGROUPINGEMAIL_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

SALESINVOICEPAYMENT = sqlContext.sql("select * from importnextstar.SALESINVOICEPAYMENT")
SALESINVOICEPAYMENT_pandas_df = ps.DataFrame(SALESINVOICEPAYMENT)
SALESINVOICEPAYMENT_pandas_df_unique = ps.DataFrame(SALESINVOICEPAYMENT_pandas_df.nunique())
SALESINVOICEPAYMENT_pandas_df_na = ps.DataFrame(SALESINVOICEPAYMENT_pandas_df.isna().sum())
SALESINVOICEPAYMENT_pandas_df_unique.reset_index(inplace=True)
SALESINVOICEPAYMENT_pandas_df_na.reset_index(inplace=True)
SALESINVOICEPAYMENT_results = SALESINVOICEPAYMENT_pandas_df_unique.merge(SALESINVOICEPAYMENT_pandas_df_na, on='index')
SALESINVOICEPAYMENT_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

SALESINVOICEITEM = sqlContext.sql("select * from importnextstar.SALESINVOICEITEM")
SALESINVOICEITEM_pandas_df = ps.DataFrame(SALESINVOICEITEM)
SALESINVOICEITEM_pandas_df_unique = ps.DataFrame(SALESINVOICEITEM_pandas_df.nunique())
SALESINVOICEITEM_pandas_df_na = ps.DataFrame(SALESINVOICEITEM_pandas_df.isna().sum())
SALESINVOICEITEM_pandas_df_unique.reset_index(inplace=True)
SALESINVOICEITEM_pandas_df_na.reset_index(inplace=True)
SALESINVOICEITEM_results = SALESINVOICEITEM_pandas_df_unique.merge(SALESINVOICEITEM_pandas_df_na, on='index')
SALESINVOICEITEM_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)



# COMMAND ----------

INVOICEFILEPATH = sqlContext.sql("select * from importnextstar.INVOICEFILEPATH")
INVOICEFILEPATH_pandas_df = ps.DataFrame(INVOICEFILEPATH)
INVOICEFILEPATH_pandas_df_unique = ps.DataFrame(INVOICEFILEPATH_pandas_df.nunique())
INVOICEFILEPATH_pandas_df_na = ps.DataFrame(INVOICEFILEPATH_pandas_df.isna().sum())
INVOICEFILEPATH_pandas_df_unique.reset_index(inplace=True)
INVOICEFILEPATH_pandas_df_na.reset_index(inplace=True)
INVOICEFILEPATH_results = INVOICEFILEPATH_pandas_df_unique.merge(INVOICEFILEPATH_pandas_df_na, on='index')
INVOICEFILEPATH_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

INVOICEITEMTYPE = sqlContext.sql("select * from importnextstar.INVOICEITEMTYPE")
INVOICEFILEPATH_pandas_df = ps.DataFrame(INVOICEITEMTYPE)
INVOICEITEMTYPE_pandas_df_unique = ps.DataFrame(INVOICEFILEPATH_pandas_df.nunique())
INVOICEITEMTYPE_pandas_df_na = ps.DataFrame(INVOICEFILEPATH_pandas_df.isna().sum())
INVOICEITEMTYPE_pandas_df_unique.reset_index(inplace=True)
INVOICEITEMTYPE_pandas_df_na.reset_index(inplace=True)
INVOICEITEMTYPE_results = INVOICEITEMTYPE_pandas_df_unique.merge(INVOICEITEMTYPE_pandas_df_na, on='index')
INVOICEITEMTYPE_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

INVOICENETMETERINFORMATION = sqlContext.sql("select * from importnextstar.INVOICENETMETERINFORMATION")
INVOICENETMETERINFORMATION_pandas_df = ps.DataFrame(INVOICENETMETERINFORMATION)
INVOICENETMETERINFORMATION_pandas_df_unique = ps.DataFrame(INVOICENETMETERINFORMATION_pandas_df.nunique())
INVOICENETMETERINFORMATION_pandas_df_na = ps.DataFrame(INVOICENETMETERINFORMATION_pandas_df.isna().sum())
INVOICENETMETERINFORMATION_pandas_df_unique.reset_index(inplace=True)
INVOICENETMETERINFORMATION_pandas_df_na.reset_index(inplace=True)
INVOICENETMETERINFORMATION_results = INVOICENETMETERINFORMATION_pandas_df_unique.merge(INVOICENETMETERINFORMATION_pandas_df_na, on='index')
INVOICENETMETERINFORMATION_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

INVOICESTATUS = sqlContext.sql("select * from importnextstar.INVOICESTATUS")
INVOICESTATUS_pandas_df = ps.DataFrame(INVOICESTATUS)
INVOICESTATUS_pandas_df_unique = ps.DataFrame(INVOICESTATUS_pandas_df.nunique())
INVOICESTATUS_pandas_df_na = ps.DataFrame(INVOICENETMETERINFORMATION_pandas_df.isna().sum())
INVOICESTATUS_pandas_df_unique.reset_index(inplace=True)
INVOICESTATUS_pandas_df_na.reset_index(inplace=True)
INVOICESTATUS_results = INVOICESTATUS_pandas_df_unique.merge(INVOICESTATUS_pandas_df_na, on='index')
INVOICESTATUS_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

MANUALINVOICE = sqlContext.sql("select * from importnextstar.MANUALINVOICE")
MANUALINVOICE_pandas_df = ps.DataFrame(MANUALINVOICE)
MANUALINVOICE_pandas_df_unique = ps.DataFrame(MANUALINVOICE_pandas_df.nunique())
MANUALINVOICE_pandas_df_na = ps.DataFrame(MANUALINVOICE_pandas_df.isna().sum())
MANUALINVOICE_pandas_df_unique.reset_index(inplace=True)
MANUALINVOICE_pandas_df_na.reset_index(inplace=True)
MANUALINVOICE_results = MANUALINVOICE_pandas_df_unique.merge(MANUALINVOICE_pandas_df_na, on='index')
MANUALINVOICE_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

# COMMAND ----------

df_names = ["SALESINVOICE",
            "SALESINVOICEERROR",
            "SALESINVOICEGROUPINGEMAIL",
            "SALESINVOICEITEM",
            "SALESINVOICEPAYMENT",
            "INVOICEFILEPATH",
            "INVOICEITEMTYPE",
            "INVOICENETMETERINFORMATION",
            "INVOICESTATUS",
            "MANUALINVOICE"]

df_list = [SALESINVOICE_results,
           SALESINVOICEERROR_results,
           SALESINVOICEGROUPINGEMAIL_results,
           SALESINVOICEITEM_results,
           SALESINVOICEPAYMENT_results,
           INVOICEFILEPATH_results,
           INVOICEITEMTYPE_results,
           INVOICENETMETERINFORMATION_results,
           INVOICESTATUS_results,
           MANUALINVOICE_results]

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

Path = "wasbs://nextstar@stgbillingpoc.blob.core.windows.net/report_invoice.csv"
Report.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(Path)
