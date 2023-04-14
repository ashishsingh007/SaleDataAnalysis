# Databricks notebook source
import logging

# COMMAND ----------

class Log4j(object):
    def __init__(self, spark,appName):
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(appName)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)

# COMMAND ----------

LogProvider = Log4j(spark,"FireCall")
#logger = LogProvider.Log4j(spark,"FireCall")
LogProvider.warn("Test")

# COMMAND ----------

def readDataFromFiles(spark,filetype,schema,location):
    #print(filetype)
    if filetype.lower() =="csv":
        #print('if')
        raw_df = spark.read\
                  .format("csv")\
                  .option("header","true")\
                  .schema(schema)\
                  .load(location)
    elif filetype.lower() == "parquet":
        #print('elif')
        raw_df = spark.read\
                      .format("parquet")\
                      .load(location)

        
    return raw_df

# COMMAND ----------

#readDataFromFiles(spark,"parquet","/databricks-datasets/credit-card-fraud/data/")

# COMMAND ----------

##Create a spark session for your application

from pyspark.sql import SparkSession
def createSparkAppSession(env,apName):
    if env.lower() =='local':
        return spark.builder\
                    .appName(apName)\
                    .master("local[2]")\
                    .enableHiveSupport() \
                    .getOrCreate()
    else:
        return spark.builder\
                    .enableHiveSupport() \
                    .getOrCreate()

# COMMAND ----------

def get_invoice_schema():
    schema ="""Invoice_No int,Stock_Code string, Description string, quantity int, Invoice_Date string,Unit_Price double , Customer_ID int ,Country string"""
    return schema

# COMMAND ----------


