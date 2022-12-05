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
# MAGIC DROP DATABASE import CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS import;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE if exists import.consumption;
# MAGIC CREATE TABLE import.consumption 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/consumption";
# MAGIC 
# MAGIC DROP TABLE if exists import.consumptiondetail;
# MAGIC CREATE TABLE import.consumptiondetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/consumptiondetail";
# MAGIC 
# MAGIC DROP TABLE if exists import.Invoice;
# MAGIC CREATE TABLE import.Invoice 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Invoice";
# MAGIC 
# MAGIC DROP TABLE if exists import.InvoiceDetail;
# MAGIC CREATE TABLE import.InvoiceDetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/InvoiceDetail";
# MAGIC 
# MAGIC DROP TABLE if exists import.InvoiceTax;
# MAGIC CREATE TABLE import.InvoiceTax 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/InvoiceTax";
# MAGIC 
# MAGIC DROP TABLE if exists import.TDSPCharges;
# MAGIC CREATE TABLE import.TDSPCharges 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/TDSPCharges";
# MAGIC 
# MAGIC DROP TABLE if exists import.InvoiceSpecialCharges;
# MAGIC CREATE TABLE import.InvoiceSpecialCharges 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/InvoiceSpecialCharges";
# MAGIC 
# MAGIC DROP TABLE if exists import.AlertQueue;
# MAGIC CREATE TABLE import.AlertQueue 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/AlertQueue";
# MAGIC 
# MAGIC DROP TABLE if exists import.Customer;
# MAGIC CREATE TABLE import.Customer 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Customer";
# MAGIC 
# MAGIC DROP TABLE if exists import.TDSPInvoice;
# MAGIC CREATE TABLE import.TDSPInvoice 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/TDSPInvoice";
# MAGIC 
# MAGIC DROP TABLE if exists import.InvoiceLog;
# MAGIC CREATE TABLE import.InvoiceLog 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/InvoiceLog";
# MAGIC 
# MAGIC DROP TABLE if exists import.PaymentDetail;
# MAGIC CREATE TABLE import.PaymentDetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/PaymentDetail";
# MAGIC 
# MAGIC DROP TABLE if exists import.SecUser;
# MAGIC CREATE TABLE import.SecUser 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/SecUser";
# MAGIC 
# MAGIC DROP TABLE if exists import.Address;
# MAGIC CREATE TABLE import.Address 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Address";
# MAGIC 
# MAGIC DROP TABLE if exists import.meter;
# MAGIC CREATE TABLE import.meter 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Meter";
# MAGIC 
# MAGIC DROP TABLE if exists import.metertype;
# MAGIC CREATE TABLE import.metertype 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/MeterType";
# MAGIC 
# MAGIC DROP TABLE if exists import.Premise;
# MAGIC CREATE TABLE import.Premise
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Premise";
# MAGIC 
# MAGIC DROP TABLE if exists import.PremiseStatusLog;
# MAGIC CREATE TABLE import.PremiseStatusLog
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/PremiseStatusLog";
# MAGIC 
# MAGIC DROP TABLE if exists import.PremiseStatus;
# MAGIC CREATE TABLE import.PremiseStatus
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/PremiseStatus";
# MAGIC 
# MAGIC DROP TABLE if exists import.Account;
# MAGIC CREATE TABLE import.Account 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Account";
# MAGIC 
# MAGIC DROP TABLE if exists import.Product;
# MAGIC CREATE TABLE import.Product 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Product";
# MAGIC 
# MAGIC DROP TABLE if exists import.Rate;
# MAGIC CREATE TABLE import.Rate 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Rate";
# MAGIC 
# MAGIC DROP TABLE if exists import.Ratedetail;
# MAGIC CREATE TABLE import.Ratedetail 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Ratedetail";
# MAGIC 
# MAGIC DROP TABLE if exists import.Ratedetailhistory;
# MAGIC CREATE TABLE import.Ratedetailhistory 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Ratedetailhistory";
# MAGIC 
# MAGIC DROP TABLE if exists import.Ratetransition;
# MAGIC CREATE TABLE import.Ratetransition 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Ratetransition";
# MAGIC 
# MAGIC DROP TABLE if exists import.Ratetransitionhistory;
# MAGIC CREATE TABLE import.Ratetransitionhistory 
# MAGIC USING parquet
# MAGIC LOCATION "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/parquet-aurea-raw/Ratetransitionhistory";

# COMMAND ----------

consumption = sqlContext.sql("select * from import.consumption")
consumption_pandas_df = ps.DataFrame(consumption)
consumption_pandas_df_unique = ps.DataFrame(consumption_pandas_df.nunique())
consumption_pandas_df_na = ps.DataFrame(consumption_pandas_df.isna().sum())
consumption_pandas_df_unique.reset_index(inplace=True)
consumption_pandas_df_na.reset_index(inplace=True)
consumption_results = consumption_pandas_df_unique.merge(consumption_pandas_df_na, on='index')
consumption_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

consumptiondetail = sqlContext.sql("select * from import.consumptiondetail")
consumptiondetail_pandas_df = ps.DataFrame(consumptiondetail)
consumptiondetail_pandas_df_unique = ps.DataFrame(consumptiondetail_pandas_df.nunique())
consumptiondetail_pandas_df_na = ps.DataFrame(consumptiondetail_pandas_df.isna().sum())
consumptiondetail_pandas_df_unique.reset_index(inplace=True)
consumptiondetail_pandas_df_na.reset_index(inplace=True)
consumptiondetail_results = consumptiondetail_pandas_df_unique.merge(consumptiondetail_pandas_df_na, on='index')
consumptiondetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

Invoice = sqlContext.sql("select * from import.Invoice")
Invoice_pandas_df = ps.DataFrame(Invoice)
Invoice_pandas_df_unique = ps.DataFrame(Invoice_pandas_df.nunique())
Invoice_pandas_df_na = ps.DataFrame(Invoice_pandas_df.isna().sum())
Invoice_pandas_df_unique.reset_index(inplace=True)
Invoice_pandas_df_na.reset_index(inplace=True)
Invoice_results = Invoice_pandas_df_unique.merge(Invoice_pandas_df_na, on='index')
Invoice_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

InvoiceDetail = sqlContext.sql("select * from import.InvoiceDetail")
InvoiceDetail_pandas_df = ps.DataFrame(InvoiceDetail)
InvoiceDetail_pandas_df_unique = ps.DataFrame(InvoiceDetail_pandas_df.nunique())
InvoiceDetail_pandas_df_na = ps.DataFrame(InvoiceDetail_pandas_df.isna().sum())
InvoiceDetail_pandas_df_unique.reset_index(inplace=True)
InvoiceDetail_pandas_df_na.reset_index(inplace=True)
InvoiceDetail_results = InvoiceDetail_pandas_df_unique.merge(InvoiceDetail_pandas_df_na, on='index')
InvoiceDetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

InvoiceTax = sqlContext.sql("select * from import.InvoiceTax")
InvoiceTax_pandas_df = ps.DataFrame(InvoiceTax)
InvoiceTax_pandas_df_unique = ps.DataFrame(InvoiceTax_pandas_df.nunique())
InvoiceTax_pandas_df_na = ps.DataFrame(InvoiceTax_pandas_df.isna().sum())
InvoiceTax_pandas_df_unique.reset_index(inplace=True)
InvoiceTax_pandas_df_na.reset_index(inplace=True)
InvoiceTax_results = InvoiceTax_pandas_df_unique.merge(InvoiceTax_pandas_df_na, on='index')
InvoiceTax_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)


TDSPCharges = sqlContext.sql("select * from import.TDSPCharges")
TDSPCharges_pandas_df = ps.DataFrame(TDSPCharges)
TDSPCharges_pandas_df_unique = ps.DataFrame(TDSPCharges_pandas_df.nunique())
TDSPCharges_pandas_df_na = ps.DataFrame(TDSPCharges_pandas_df.isna().sum())
TDSPCharges_pandas_df_unique.reset_index(inplace=True)
TDSPCharges_pandas_df_na.reset_index(inplace=True)
TDSPCharges_results = TDSPCharges_pandas_df_unique.merge(TDSPCharges_pandas_df_na, on='index')
TDSPCharges_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

InvoiceSpecialCharges = sqlContext.sql("select * from import.InvoiceSpecialCharges")
InvoiceSpecialCharges_pandas_df = ps.DataFrame(InvoiceSpecialCharges)
InvoiceSpecialCharges_pandas_df_unique = ps.DataFrame(InvoiceSpecialCharges_pandas_df.nunique())
InvoiceSpecialCharges_pandas_df_na = ps.DataFrame(InvoiceSpecialCharges_pandas_df.isna().sum())
InvoiceSpecialCharges_pandas_df_unique.reset_index(inplace=True)
InvoiceSpecialCharges_pandas_df_na.reset_index(inplace=True)
InvoiceSpecialCharges_results = InvoiceSpecialCharges_pandas_df_unique.merge(InvoiceSpecialCharges_pandas_df_na, on='index')
InvoiceSpecialCharges_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

AlertQueue = sqlContext.sql("select * from import.AlertQueue")
AlertQueue_pandas_df = ps.DataFrame(AlertQueue)
AlertQueue_pandas_df_unique = ps.DataFrame(AlertQueue_pandas_df.nunique())
AlertQueue_pandas_df_na = ps.DataFrame(AlertQueue_pandas_df.isna().sum())
AlertQueue_pandas_df_unique.reset_index(inplace=True)
AlertQueue_pandas_df_na.reset_index(inplace=True)
AlertQueue_results = AlertQueue_pandas_df_unique.merge(AlertQueue_pandas_df_na, on='index')
AlertQueue_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

Customer = sqlContext.sql("select * from import.Customer")
Customer_pandas_df = ps.DataFrame(Customer)
Customer_pandas_df_unique = ps.DataFrame(Customer_pandas_df.nunique())
Customer_pandas_df_na = ps.DataFrame(Customer_pandas_df.isna().sum())
Customer_pandas_df_unique.reset_index(inplace=True)
Customer_pandas_df_na.reset_index(inplace=True)
Customer_results = Customer_pandas_df_unique.merge(Customer_pandas_df_na, on='index')
Customer_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

TDSPInvoice = sqlContext.sql("select * from import.TDSPInvoice")
TDSPInvoice_pandas_df = ps.DataFrame(TDSPInvoice)
TDSPInvoice_pandas_df_unique = ps.DataFrame(TDSPInvoice_pandas_df.nunique())
TDSPInvoice_pandas_df_na = ps.DataFrame(TDSPInvoice_pandas_df.isna().sum())
TDSPInvoice_pandas_df_unique.reset_index(inplace=True)
TDSPInvoice_pandas_df_na.reset_index(inplace=True)
TDSPInvoice_results = TDSPInvoice_pandas_df_unique.merge(TDSPInvoice_pandas_df_na, on='index')
TDSPInvoice_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

InvoiceLog = sqlContext.sql("select * from import.InvoiceLog")
InvoiceLog_pandas_df = ps.DataFrame(InvoiceLog)
InvoiceLog_pandas_df_unique = ps.DataFrame(InvoiceLog_pandas_df.nunique())
InvoiceLog_pandas_df_na = ps.DataFrame(InvoiceLog_pandas_df.isna().sum())
InvoiceLog_pandas_df_unique.reset_index(inplace=True)
InvoiceLog_pandas_df_na.reset_index(inplace=True)
InvoiceLog_results = InvoiceLog_pandas_df_unique.merge(InvoiceLog_pandas_df_na, on='index')
InvoiceLog_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

PaymentDetail = sqlContext.sql("select * from import.PaymentDetail")
PaymentDetail_pandas_df = ps.DataFrame(PaymentDetail)
PaymentDetail_pandas_df_unique = ps.DataFrame(PaymentDetail_pandas_df.nunique())
PaymentDetail_pandas_df_na = ps.DataFrame(PaymentDetail_pandas_df.isna().sum())
PaymentDetail_pandas_df_unique.reset_index(inplace=True)
PaymentDetail_pandas_df_na.reset_index(inplace=True)
PaymentDetail_results = PaymentDetail_pandas_df_unique.merge(PaymentDetail_pandas_df_na, on='index')
PaymentDetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

SecUser = sqlContext.sql("select * from import.SecUser")
SecUser_pandas_df = ps.DataFrame(SecUser)
SecUser_pandas_df_unique = ps.DataFrame(SecUser_pandas_df.nunique())
SecUser_pandas_df_na = ps.DataFrame(SecUser_pandas_df.isna().sum())
SecUser_pandas_df_unique.reset_index(inplace=True)
SecUser_pandas_df_na.reset_index(inplace=True)
SecUser_results = SecUser_pandas_df_unique.merge(SecUser_pandas_df_na, on='index')
SecUser_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

Address = sqlContext.sql("select * from import.Address")
Address_pandas_df = ps.DataFrame(Address)
Address_pandas_df_unique = ps.DataFrame(Address_pandas_df.nunique())
Address_pandas_df_na = ps.DataFrame(Address_pandas_df.isna().sum())
Address_pandas_df_unique.reset_index(inplace=True)
Address_pandas_df_na.reset_index(inplace=True)
Address_results = Address_pandas_df_unique.merge(Address_pandas_df_na, on='index')
Address_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

meter = sqlContext.sql("select * from import.meter")
meter_pandas_df = ps.DataFrame(meter)
meter_pandas_df_unique = ps.DataFrame(meter_pandas_df.nunique())
meter_pandas_df_na = ps.DataFrame(meter_pandas_df.isna().sum())
meter_pandas_df_unique.reset_index(inplace=True)
meter_pandas_df_na.reset_index(inplace=True)
meter_results = meter_pandas_df_unique.merge(meter_pandas_df_na, on='index')
meter_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

metertype = sqlContext.sql("select * from import.metertype")
metertype_pandas_df = ps.DataFrame(metertype)
metertype_pandas_df_unique = ps.DataFrame(metertype_pandas_df.nunique())
metertype_pandas_df_na = ps.DataFrame(metertype_pandas_df.isna().sum())
metertype_pandas_df_unique.reset_index(inplace=True)
metertype_pandas_df_na.reset_index(inplace=True)
metertype_results = metertype_pandas_df_unique.merge(metertype_pandas_df_na, on='index')
metertype_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

Premise = sqlContext.sql("select * from import.Premise")
Premise_pandas_df = ps.DataFrame(Premise)
Premise_pandas_df_unique = ps.DataFrame(Premise_pandas_df.nunique())
Premise_pandas_df_na = ps.DataFrame(Premise_pandas_df.isna().sum())
Premise_pandas_df_unique.reset_index(inplace=True)
Premise_pandas_df_na.reset_index(inplace=True)
Premise_results = Premise_pandas_df_unique.merge(Premise_pandas_df_na, on='index')
Premise_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

PremiseStatusLog = sqlContext.sql("select * from import.PremiseStatusLog")
PremiseStatusLog_pandas_df = ps.DataFrame(PremiseStatusLog)
PremiseStatusLog_pandas_df_unique = ps.DataFrame(PremiseStatusLog_pandas_df.nunique())
PremiseStatusLog_pandas_df_na = ps.DataFrame(PremiseStatusLog_pandas_df.isna().sum())
PremiseStatusLog_pandas_df_unique.reset_index(inplace=True)
PremiseStatusLog_pandas_df_na.reset_index(inplace=True)
PremiseStatusLog_results = PremiseStatusLog_pandas_df_unique.merge(PremiseStatusLog_pandas_df_na, on='index')
PremiseStatusLog_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

PremiseStatus = sqlContext.sql("select * from import.PremiseStatus")
PremiseStatus_pandas_df = ps.DataFrame(PremiseStatus)
PremiseStatus_pandas_df_unique = ps.DataFrame(PremiseStatus_pandas_df.nunique())
PremiseStatus_pandas_df_na = ps.DataFrame(PremiseStatus_pandas_df.isna().sum())
PremiseStatus_pandas_df_unique.reset_index(inplace=True)
PremiseStatus_pandas_df_na.reset_index(inplace=True)
PremiseStatus_results = PremiseStatus_pandas_df_unique.merge(PremiseStatus_pandas_df_na, on='index')
PremiseStatus_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

Account = sqlContext.sql("select * from import.Account")
Account_pandas_df = ps.DataFrame(Account)
Account_pandas_df_unique = ps.DataFrame(Account_pandas_df.nunique())
Account_pandas_df_na = ps.DataFrame(Account_pandas_df.isna().sum())
Account_pandas_df_unique.reset_index(inplace=True)
Account_pandas_df_na.reset_index(inplace=True)
Account_results = Account_pandas_df_unique.merge(Account_pandas_df_na, on='index')
Account_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

Product = sqlContext.sql("select * from import.Product")
Product_pandas_df = ps.DataFrame(Product)
Product_pandas_df_unique = ps.DataFrame(Product_pandas_df.nunique())
Product_pandas_df_na = ps.DataFrame(Product_pandas_df.isna().sum())
Product_pandas_df_unique.reset_index(inplace=True)
Product_pandas_df_na.reset_index(inplace=True)
Product_results = Product_pandas_df_unique.merge(Product_pandas_df_na, on='index')
Product_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

Rate = sqlContext.sql("select * from import.Rate")
Rate_pandas_df = ps.DataFrame(Rate)
Rate_pandas_df_unique = ps.DataFrame(Rate_pandas_df.nunique())
Rate_pandas_df_na = ps.DataFrame(Rate_pandas_df.isna().sum())
Rate_pandas_df_unique.reset_index(inplace=True)
Rate_pandas_df_na.reset_index(inplace=True)
Rate_results = Rate_pandas_df_unique.merge(Rate_pandas_df_na, on='index')
Rate_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

Ratedetail = sqlContext.sql("select * from import.Ratedetail")
Ratedetail_pandas_df = ps.DataFrame(Ratedetail)
Ratedetail_pandas_df_unique = ps.DataFrame(Ratedetail_pandas_df.nunique())
Ratedetail_pandas_df_na = ps.DataFrame(Ratedetail_pandas_df.isna().sum())
Ratedetail_pandas_df_unique.reset_index(inplace=True)
Ratedetail_pandas_df_na.reset_index(inplace=True)
Ratedetail_results = Ratedetail_pandas_df_unique.merge(Ratedetail_pandas_df_na, on='index')
Ratedetail_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)
 
Ratedetailhistory = sqlContext.sql("select * from import.Ratedetailhistory")
Ratedetailhistory_pandas_df = ps.DataFrame(Ratedetailhistory)
Ratedetailhistory_pandas_df_unique = ps.DataFrame(Ratedetailhistory_pandas_df.nunique())
Ratedetailhistory_pandas_df_na = ps.DataFrame(Ratedetailhistory_pandas_df.isna().sum())
Ratedetailhistory_pandas_df_unique.reset_index(inplace=True)
Ratedetailhistory_pandas_df_na.reset_index(inplace=True)
Ratedetailhistory_results = Ratedetailhistory_pandas_df_unique.merge(Ratedetailhistory_pandas_df_na, on='index')
Ratedetailhistory_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

 
Ratetransition = sqlContext.sql("select * from import.Ratetransition")
Ratetransition_pandas_df = ps.DataFrame(Ratetransition)
Ratetransition_pandas_df_unique = ps.DataFrame(Ratetransition_pandas_df.nunique())
Ratetransition_pandas_df_na = ps.DataFrame(Ratetransition_pandas_df.isna().sum())
Ratetransition_pandas_df_unique.reset_index(inplace=True)
Ratetransition_pandas_df_na.reset_index(inplace=True)
Ratetransition_results = Ratetransition_pandas_df_unique.merge(Ratetransition_pandas_df_na, on='index')
Ratetransition_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)
    
Ratetransitionhistory = sqlContext.sql("select * from import.Ratetransitionhistory")
Ratetransitionhistory_pandas_df = ps.DataFrame(Ratetransitionhistory)    
Ratetransitionhistory_pandas_df_unique = ps.DataFrame(Ratetransitionhistory_pandas_df.nunique())
Ratetransitionhistory_pandas_df_na = ps.DataFrame(Ratetransitionhistory_pandas_df.isna().sum())
Ratetransitionhistory_pandas_df_unique.reset_index(inplace=True)
Ratetransitionhistory_pandas_df_na.reset_index(inplace=True)
Ratetransitionhistory_results = Ratetransitionhistory_pandas_df_unique.merge(Ratetransitionhistory_pandas_df_na, on='index')
Ratetransitionhistory_results.rename({'index': 'Colname', 'None_x': 'Uniquevals', 'None_y': 'Nullvals'}, axis=1, inplace=True)

# COMMAND ----------

display(consumption_results)

Path = "wasbs://aurea-pipelines@stgbillingpoc.blob.core.windows.net/Tablelevel-archaeology/test.csv"
consumption_results.to_csv(Path)

# COMMAND ----------

display(consumptiondetail_results)

# COMMAND ----------

display(Invoice_results)

# COMMAND ----------

display(InvoiceDetail_results)

# COMMAND ----------

display(InvoiceTax_results)

# COMMAND ----------

display(TDSPCharges_results)

# COMMAND ----------

display(TDSPInvoice_results)

# COMMAND ----------

display(InvoiceLog_results)

# COMMAND ----------

display(PaymentDetail_results)

# COMMAND ----------

display(SecUser_results)

# COMMAND ----------

display(Address_results)

# COMMAND ----------

display(meter_results)

# COMMAND ----------

display(metertype_results)

# COMMAND ----------

display(Premise_results)

# COMMAND ----------

display(PremiseStatusLog_results)

# COMMAND ----------

display(PremiseStatus_results)

# COMMAND ----------

display(Account_results)

# COMMAND ----------

display(Product_results)

# COMMAND ----------

display(Rate_results)

# COMMAND ----------

display(Ratedetail_results)

# COMMAND ----------

display(Ratedetailhistory_results)

# COMMAND ----------

display(Ratetransition_results)

# COMMAND ----------

display(Ratetransitionhistory_results)

# COMMAND ----------


