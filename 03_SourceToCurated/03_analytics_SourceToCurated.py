from pyspark import SparkContext, SparkConf 
from pyspark.conf import SparkConf 
from pyspark.sql import SparkSession, HiveContext,DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType,DoubleType,IntegerType
import pandas as pd
import time
import os
from datetime import datetime,timedelta
from configparser import ConfigParser
import argparse
from datetime import datetime,timedelta
import calendar

parser = argparse.ArgumentParser()
parser.add_argument("--infraenv", help="some useful description.")
args = parser.parse_args()
config = ConfigParser()

def getHiveSparkSession():
    sparkSessionHive = SparkSession.builder \
                    .appName('example-pyspark-read-and-write-from-hive') \
                    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                    .config("hive.metastore.warehouse.dir", "/user/hive/warehouse") \
                    .config('spark.sql.parquet.binaryAsString', 'true')\
                    .enableHiveSupport() \
                    .getOrCreate()
    print(sparkSessionHive.sparkContext.getConf().getAll())
    return sparkSessionHive

def transformAnalyticsData(session, df):
    df = df.select(
        f.col("user_id").cast(StringType()),
        f.col("event_date").cast(StringType()),
        f.col("category").cast(StringType()),
        f.col("mobile_brand_name").cast(StringType()),
        f.col("mobile_model_name").cast(StringType()),
        f.col("mobile_os_hardware_model").cast(StringType()),
        f.col("operating_system").cast(StringType()),
        f.col("operating_system_version").cast(StringType()),
        f.col("city").cast(StringType()),
        f.col("country").cast(StringType()),
        f.col("appversion").cast(StringType()),
        f.col("processed_at").cast(TimestampType()),
        f.col("cryear").cast(IntegerType()),
        f.col("crmonth").cast(IntegerType()),
        f.col("crday").cast(IntegerType())
    )

    df.write.format("hive").saveAsTable("creditbook_curatedlayer.analytics_details",mode="append")




def main():
    sessionHive = getHiveSparkSession()

    if args.infraenv == "FullLoad":
        df = sessionHive.sql("select * from creditbook_sourcelayer.analytics_details")
        transformAnalyticsData(sessionHive,df)
        #
    else:
        ## Previous Logic (In File user_sourceToCurated.py) can be apply here
        return 0



if __name__ == '__main__':
    main()