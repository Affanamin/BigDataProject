from pyspark import SparkContext, SparkConf 
from pyspark.conf import SparkConf 
from pyspark.sql import SparkSession, HiveContext,DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType, IntegerType,DoubleType
import pandas as pd
import os
from datetime import datetime,timedelta
import time
from configparser import ConfigParser
import argparse
from datetime import datetime,timedelta
import calendar
from pyarrow import HadoopFileSystem
import json
import requests

parser = argparse.ArgumentParser()
parser.add_argument("--infraenv", help="some useful description.")
args = parser.parse_args()
config = ConfigParser()

config.read("Your Path to conf.ini")
url = "https://storage.googleapis.com/cb_data_test_bucket/transactions.json"

def getHiveSparkSession():
    sparkSessionHive = SparkSession.builder \
                    .appName('example-pyspark-read-and-write-from-hive') \
                    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                    .config("hive.metastore.warehouse.dir", "/user/hive/warehouse") \
                    .enableHiveSupport() \
                    .getOrCreate()
    print(sparkSessionHive.sparkContext.getConf().getAll())
    return sparkSessionHive


def getDateData(spark):
    ## Through this function we extract datedata, which we inserted previously, We have inserted this information into our DB because
    ## we want to maintain uniformity in terms of date in complete pipeline scope.

    df = spark.sql("SELECT todayDay, todayMonth, todayyear, yesterdayDay, yesterDayMonth, yesterDayYear from nayapay_wslog.datetoday")
        
    todayDay = df.withColumn("todayDay", df["todayDay"].cast("string"))
    todayRow = todayDay.head()
    today = todayRow["todayDay"]
    strToday = str(today)

    todayMonth = df.withColumn("todayMonth", df["todayMonth"].cast("string")) #todayDf['todayMonth']
    todayRowMonth = todayMonth.head()
    todayMonth = todayRowMonth["todayMonth"]
    strTodayMonth = str(todayMonth)

    todayYear = df.withColumn("todayyear", df["todayyear"].cast("string")) #todayDf['todayMonth']
    todayRowYear = todayYear.head()
    todayYear = todayRowYear["todayyear"]
    strTodayYear = str(todayYear)

    todayHourObj = datetime.now()
    todayHour = todayHourObj.hour
    todayMin = todayHourObj.minute
    inttodayMin = int(todayMin)
    intTodayHour = int(todayHour)

    yesterdayDf = df.withColumn("yesterdayDay", df["yesterdayDay"].cast("string"))
    yesterdayRow = yesterdayDf.head()
    yesterDay = yesterdayRow["yesterdayDay"]
    strYesterDay = str(yesterDay)

    yesterMonthDf = df.withColumn("yesterDayMonth", df["yesterDayMonth"].cast("string"))
    yesterMonthRow = yesterMonthDf.head()
    yesterMonth = yesterMonthRow["yesterDayMonth"]
    intyesterMonth = int(yesterMonth)
    strYesterMonth = str(yesterMonth)

    yesterYearDf = df.withColumn("yesterDayYear", df["yesterDayYear"].cast("string"))
    yesterYearRow = yesterYearDf.head()
    yesterYear = yesterYearRow["yesterDayYear"]
    strYesterYear = str(yesterYear)


    return strToday, strTodayMonth, strTodayYear, inttodayMin, intTodayHour, strYesterDay, strYesterMonth, strYesterYear

def delete_hadoop_data_directory(fs, path):
    if fs.exists(path):
        fs.delete(path, recursive=True)
        print(f"Data directory {path} deleted successfully.")
    else:
        print(f"Data directory {path} does not exist.")


def transformTransDate(df):

    ## Transforming "created_at" column so that we can have cryear, crmonth, crday.
    
    df = df.withColumn("cryear", f.split(f.col("created_at"), '-').getItem(0).cast(IntegerType())) \
        .withColumn("crmonth", f.split("created_at", '-').getItem(1).cast(IntegerType())) \
        .withColumn("crday", f.split(f.split(f.col("created_at"), "T").getItem(0), "-").getItem(2).cast(IntegerType()))
    
    return df


def save_data_to_hdfs(df):
    """
    Save DataFrame to HDFS in parquet format with snappy compression
    """
    hdfs_path = "hdfs://localhost:9000/creditBookDWH/transactions"
    df.write.mode("append") \
        .format("parquet") \
        .option("path", hdfs_path) \
        .option("compression", "snappy") \
        .partitionBy("cryear", "crmonth", "crday") \
        .save()


def getTransData(spark):
    ## Through this Function we get data which is in json format from the url given by the company. 
    
    ## With this FullLoad arguement we get whole data from url and dump it into our HDFS. 
    if args.infraenv == "FullLoad":
        response = requests.get(url)
        data = response.text.strip().split('\n')

        # Define the schema for the JSON data
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("type", StringType(), True),
            StructField("created_at", StringType(), True)
        ])

        # Convert text to DataFrame
        json_data = [json.loads(line) for line in data]
        df = spark.createDataFrame(json_data, schema=schema)
        return df
    
    ## With this FullLoad arguement we get whole data from url But we filter out only today's data.
    elif args.infraenv == "IncrementalLoad":
        strToday, strTodayMonth, strTodayYear, inttodayMin, intTodayHour, strYesterDay, strYesterMonth, strYesterYear = getDateData(spark)

        ## As according to our logic our data orchestration tool (airflow) run every 30 mins and extract data from url.
        ## For the safe side, we overwrite whole of today's data to our hive database when date gets change. Which means every first
        ## airflow runs first time for today will get whole yesterdays data to be inserted into our hive table.
        if intTodayHour == 0 and inttodayMin <= 28:
            strDir = "hdfs://localhost:9000/creditBookDWH/transactions/CRYEAR=2024/CRMONTH="+strYesterMonth+"/CRDAY="+strYesterDay+""
            data_directory_path = strDir
            fs = HadoopFileSystem()
            
            ## With this delete data directory we remove yesterdays data directory from HDFS, as this block of code 
            ## will run at 12.30 every night and save whole yesterdays data which is in prod DB.

            delete_hadoop_data_directory(fs, data_directory_path)

            response = requests.get(url)
            data = response.text.strip().split('\n')

            # Define the schema for the JSON data
            schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("type", StringType(), True),
            StructField("created_at", StringType(), True)
            ])

            # Convert text to DataFrame
            json_data = [json.loads(line) for line in data]
            df = spark.createDataFrame(json_data, schema=schema)

            df = df.withColumn("cryear", f.split(f.col("created_at"), '-').getItem(0).cast(IntegerType())) \
                    .withColumn("crmonth", f.split("created_at", '-').getItem(1).cast(IntegerType())) \
                    .withColumn("crday", f.split(f.split(f.col("created_at"), "T").getItem(0), "-").getItem(2).cast(IntegerType()))

            yesterday_df = df.filter((f.col("cryear") == strTodayYear) & 
                                    (f.col("crmonth") == strYesterMonth) & 
                                    (f.col("crday") == strToday))

            # Drop the temporary columns
            yesterday_df = yesterday_df.drop('cryear', 'crmonth', 'crday')

            return yesterday_df
    
        ## and after that one time every time airflow will run will insert Todays data only.
        else:
            strToday, strTodayMonth, strTodayYear, inttodayMin, intTodayHour, strYesterDay, strYesterMonth, strYesterYear = getDateData(spark)
            strDir = "hdfs://localhost:9000/creditBookDWH/transactions/CRYEAR=2024/crmonth="+strTodayMonth+"/crday="+strToday+""
            data_directory_path = strDir
            fs = HadoopFileSystem()

            ## With this we delete HDFS directory for today, and every after 30 minutes we will going to do this, we delete the 
            ## directory and then create todays directory again and save all todays data which is in the prod db till that time.
            
            delete_hadoop_data_directory(fs, data_directory_path)
            response = requests.get(url)
            data = response.text.strip().split('\n')

            # Define the schema for the JSON data
            schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("type", StringType(), True),
            StructField("created_at", StringType(), True)])

            # Convert text to DataFrame
            json_data = [json.loads(line) for line in data]
            df = spark.createDataFrame(json_data, schema=schema)

            df = df.withColumn("cryear", f.split(f.col("created_at"), '-').getItem(0).cast(IntegerType())) \
                    .withColumn("crmonth", f.split("created_at", '-').getItem(1).cast(IntegerType())) \
                    .withColumn("crday", f.split(f.split(f.col("created_at"), "T").getItem(0), "-").getItem(2).cast(IntegerType()))

            today_df = df.filter((f.col("cryear") == strTodayYear) & 
                     (f.col("crmonth") == strTodayMonth) & 
                     (f.col("crday") == strToday))

            # Drop the temporary columns
            today_df = today_df.drop('cryear', 'crmonth', 'crday')
            return today_df 

def main():
    ## Get Hive+Spark Session
    session = getHiveSparkSession()

    ## Get Users data through given Urls
    df = getTransData(session)
    
    ## To transform Createdatetime, through this we will get day, month and year.
    df = transformTransDate(df)

    ## Adding processedAt column to the dataframe
    processedAt = time.strftime("%Y-%m-%d %H:%M:%S")
    currentDfFinal = df.withColumn("processed_at", f.lit(processedAt))
    currentDfFinal = currentDfFinal.withColumn("processed_at", f.to_timestamp("processed_at", "yyyy-MM-dd HH:mm:ss"))
    
    ## Redistributing the data across partitions using repartition
    currentDfFinal = currentDfFinal.repartition("cryear","crmonth","crday")
    
    #currentDfFinal.printSchema()
    currentDfFinalCount = int(currentDfFinal.count())
    print("currentDfFinalCount",currentDfFinalCount)

    ## Saving Data to HDFS
    save_data_to_hdfs(currentDfFinal)


if __name__ == '__main__':
    main()
