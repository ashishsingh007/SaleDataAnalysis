# Databricks notebook source
'''%run is command to import any other notebook and its avaibale methods'''

# COMMAND ----------

# MAGIC %run "./utility"

# COMMAND ----------

##All required modules/method/function will be declare here
import uuid
from pyspark.sql import functions as f

##Constant variables
job_run_env ='local'
appName ='CreditCardFarud'
location ="/FileStore/tables/invoices.csv"
fileType ="csv"

# COMMAND ----------

job_run_id = uuid.uuid4()
###Initialise Spark Application
print("Initializing " + appName + " Job in " +  str(job_run_env) + " Job Id: " + str(job_run_id))

#createSparkAppSession method is available in utility module and called from here
spark =createSparkAppSession(job_run_env,appName)

#get_invoice_schema method is available in utility module and called from here
schema = get_invoice_schema()

#readDataFromFiles method is available in utility module and called from here
raw_df = readDataFromFiles(spark,fileType,schema,location)

# COMMAND ----------

##Take Total count of records available in raw dataframe
#541909
print("Total available row in raw dataframe - " + str(raw_df.count()))
      
            
#Check schema exploaration
#display(raw_df)
raw_df.printSchema()


# COMMAND ----------

#Perfrom neceassry Data type conversion 
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
df_transf1 = raw_df.withColumn("Invoice_DateTime",f.to_timestamp(raw_df.Invoice_Date,"dd-MM-yyyy HH.mm"))\
                .withColumn("Invoice_Date",f.to_date(raw_df.Invoice_Date,"dd-MM-yyyy"))

#Filter all the record not having invoice number and derive dummy invoice number 
df_Without_InvoiceNo = df_correctDataTypes.filter(df_correctDataTypes.Invoice_No.isNull())\
                                           .withColumn("Invoice_No",f.when(df_correctDataTypes.Invoice_No.isNull(),"DUM-01")
                                               .otherwise(df_correctDataTypes.Invoice_No))

#Filter all the record having invoice number
df_with_InvoiceNo = df_correctDataTypes.filter(df_correctDataTypes.Invoice_No.isNotNull())

#combining dataframes having Dummy invoice number 
df_refined = df_with_InvoiceNo.union(df_Without_InvoiceNo)
#df_refined.filter(df_refined.Invoice_No =='DUM-01').show()
#display(df_refined)

# COMMAND ----------

'''
#raw_df.select('Invoice_No','Stock_Code','Description','quantity',f.col('Invoice_Date').cast('Date'),'Unit_Price','Customer_ID','Country').show()
df_correctDataTypes.filter("quantity is NULL").show()

df_correctDataTypes.fillna(value='0')

'''

# COMMAND ----------

##Analysis performed to indentied columns for partion while saving into data file
#df_refined.select('Invoice_Date','Country').distinct().show()

# COMMAND ----------

###Writing refined dataframe into parquet file format for further transformation via sql process

df_refined.write\
          .partitionBy("Country","Invoice_Date")\
          .parquet("/FileStore/Ashish_SaleAnalysis/")

# COMMAND ----------

#Read parquet file data & create a TempView for further business logic implementation
df_raw_salesdata = spark.read.parquet("/FileStore/Ashish_SaleAnalysis/")
#display(df_raw_salesdata)

# COMMAND ----------

df_raw_salesdata.createOrReplaceTempView("Vw_Salesdata")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Vw_Salesdata limit 100
