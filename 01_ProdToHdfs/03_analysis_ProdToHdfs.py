from pyspark import SparkContext, SparkConf 
from pyspark.conf import SparkConf 
from pyspark.sql import SparkSession, HiveContext,DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType, IntegerType,DoubleType, ArrayType, MapType
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
import gc

parser = argparse.ArgumentParser()
parser.add_argument("--infraenv", help="some useful description.")
args = parser.parse_args()
config = ConfigParser()

config.read("Your Path to conf.ini")
url = "https://storage.googleapis.com/cb_data_test_bucket/analysis.json"

def getHiveSparkSession():
    sparkSessionHive = SparkSession.builder \
                    .appName('example-pyspark-read-and-write-from-hive') \
                    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                    .config("hive.metastore.warehouse.dir", "/user/hive/warehouse") \
                    .config("spark.executor.memory", "25g") \
                    .config("spark.driver.memory", "25g") \
                    .config("spark.executor.memoryOverhead", "8g") \
                    .config("spark.executor.instances", "4") \
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


def getAnalyticsData(spark):

    ## Through this Function we get data which is in json format from the url given by the company. 
    
    ## With this FullLoad arguement we get whole data from url and dump it into our HDFS. 
    if args.infraenv == "FullLoad":

        schema = StructType([
        StructField("event_date", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("event_params", ArrayType(StructType([
            StructField("key", StringType(), True),
            StructField("value", MapType(StringType(), StringType()), True)
        ])), True),
        StructField("event_previous_timestamp", StringType(), True),
        StructField("event_bundle_sequence_id", StringType(), True),
        StructField("event_server_timestamp_offset", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_pseudo_id", StringType(), True),
        StructField("user_properties", ArrayType(StructType([
            StructField("key", StringType(), True),
            StructField("value", MapType(StringType(), StringType()), True)
        ])), True),
        StructField("user_first_touch_timestamp", StringType(), True),
        StructField("device", StructType([
            StructField("category", StringType(), True),
            StructField("mobile_brand_name", StringType(), True),
            StructField("mobile_model_name", StringType(), True),
            StructField("mobile_marketing_name", StringType(), True),
            StructField("mobile_os_hardware_model", StringType(), True),
            StructField("operating_system", StringType(), True),
            StructField("operating_system_version", StringType(), True),
            StructField("advertising_id", StringType(), True),
            StructField("language", StringType(), True),
            StructField("is_limited_ad_tracking", StringType(), True),
            StructField("time_zone_offset_seconds", StringType(), True)
        ]), True),
        StructField("geo", StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("continent", StringType(), True),
            StructField("region", StringType(), True),
            StructField("sub_continent", StringType(), True),
            StructField("metro", StringType(), True)
        ]), True),
        StructField("app_info", StructType([
            StructField("id", StringType(), True),
            StructField("version", StringType(), True),
            StructField("firebase_app_id", StringType(), True),
            StructField("install_source", StringType(), True)
        ]), True),
        StructField("traffic_source", StructType([
            StructField("name", StringType(), True),
            StructField("medium", StringType(), True),
            StructField("source", StringType(), True)
        ]), True),
        StructField("stream_id", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("items", ArrayType(StringType()), True)
    ])
        response = requests.get(url)
        data = response.text.strip().split('\n')

        # Convert JSON data to RDD and then create a DataFrame
        rdd = spark.sparkContext.parallelize(data)
        df = spark.read.json(rdd, schema=schema)
        df = df.select("user_id","event_date","device","geo", "app_info")

        df = df.withColumn("device", f.col("device").cast("string"))\
                .withColumn("geo", f.col("geo").cast("string"))\
                .withColumn("app_info", f.col("app_info").cast("string"))

        df = df.withColumn("category", f.split(f.col("device"), ',').getItem(0).cast(StringType())) \
                            .withColumn("category", f.expr("substring(category, 2, length(category))"))\
                            .withColumn("mobile_brand_name", f.split("device", ',').getItem(1).cast(StringType())) \
                            .withColumn("mobile_model_name", f.split("device", ',').getItem(2).cast(StringType())) \
                            .withColumn("mobile_marketing_name", f.split("device", ',').getItem(3).cast(StringType())) \
                            .withColumn("mobile_os_hardware_model", f.split("device", ',').getItem(4).cast(StringType())) \
                            .withColumn("operating_system", f.split("device", ',').getItem(5).cast(StringType())) \
                            .withColumn("operating_system_version", f.split("device", ',').getItem(6).cast(StringType())) \
                            .withColumn("city", f.split("geo", ',').getItem(0).cast(StringType()))\
                            .withColumn("city", f.expr("substring(city, 2, length(city))"))\
                            .withColumn("country", f.split("geo", ',').getItem(1).cast(StringType()))\
                            .withColumn("appversion", f.split("app_info", ',').getItem(1).cast(StringType()))

        #.withColumn("continent", f.split("geo", ',').getItem(2).cast(StringType()))\
        #.withColumn("region", f.split("geo", ',').getItem(3).cast(StringType()))\

        df = df.select("user_id","event_date","category","mobile_brand_name","mobile_model_name","mobile_os_hardware_model","operating_system",\
                    "operating_system_version", "city", "country","appversion")
        
        return df
    
    ## I won't rewrite the logic for the "IncrementalLoad" Block here due to time constraints, 
    ## but it will operate as explained in our previous files. 

    elif args.infraenv == "IncrementalLoad":
        strToday, strTodayMonth, strTodayYear, inttodayMin, intTodayHour, strYesterDay, strYesterMonth, strYesterYear = getDateData(spark)

        ## As according to our logic our data orchestration tool (airflow) run every 30 mins and extract data from url.
        ## For the safe side, we overwrite whole of today's data to our hive database when date gets change. Which means every first
        ## airflow runs first time for today will get whole yesterdays data to be inserted into our hive table.
        

        if intTodayHour == 0 and inttodayMin <= 28:
            return 0
        ## and after that one time every time airflow will run will insert Todays data only.
        else:
            return 0

        
        


def transformTransDate(df):
    df = df.withColumn("cryear", f.substring(f.col("event_date"), 1, 4).cast("int")) \
       .withColumn("crmonth", f.substring(f.col("event_date"), 5, 2).cast("int")) \
       .withColumn("crday", f.substring(f.col("event_date"), 7, 2).cast("int"))
    return df




def save_data_to_hdfs(df):
    """
    Save DataFrame to HDFS in parquet format with snappy compression
    """
    hdfs_path = "hdfs://localhost:9000/creditBookDWH/analytics"
    df.write.mode("append") \
        .format("parquet") \
        .option("path", hdfs_path) \
        .option("compression", "snappy") \
        .partitionBy("cryear", "crmonth", "crday") \
        .save()
    


def main():
    ## Get Hive+Spark Session
    session = getHiveSparkSession()

    ## Get Users data through given Urls
    df = getAnalyticsData(session)
    
    ## To transform Createdatetime, through this we will get day, month and year.
    df = transformTransDate(df)
    
    ## Adding processedAt column to the dataframe
    processedAt = time.strftime("%Y-%m-%d %H:%M:%S")
    currentDfFinal = df.withColumn("processed_at", f.lit(processedAt))
    currentDfFinal = currentDfFinal.withColumn("processed_at", f.to_timestamp("processed_at", "yyyy-MM-dd HH:mm:ss"))
    
    #currentDfFinal = currentDfFinal.filter((f.col("cryear") == 2023) | 
     #                               (f.col("cryear") == 2024))
    
    gc.collect()
    #currentDfFinal.show(10, False)
    #currentDfFinal = currentDfFinal
    ## Redistributing the data across partitions using repartition
    #currentDfFinal = currentDfFinal.repartition("cryear","crmonth","crday")
    currentDfFinal = currentDfFinal.repartition("user_id")

    gc.collect()

    ## Saving Data to HDFS
    #currentDfFinalCount = int(currentDfFinal.count())
    #print("currentDfFinalCount",currentDfFinalCount)
    
    #currentDfFinal.printSchema()
    save_data_to_hdfs(currentDfFinal)


if __name__ == '__main__':
    main()


