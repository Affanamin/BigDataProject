from pyspark import SparkContext, SparkConf 
from pyspark.conf import SparkConf 
from pyspark.sql import SparkSession, HiveContext,DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType
import pandas as pd
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
config.read("Your Path to conf.ini")



def getHiveSparkSession():
    sparkSessionHive = SparkSession.builder \
                    .appName('example-pyspark-read-and-write-from-hive') \
                    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                    .config("hive.metastore.warehouse.dir", "/user/hive/warehouse") \
                    .enableHiveSupport() \
                    .getOrCreate()
    print(sparkSessionHive.sparkContext.getConf().getAll())
    return sparkSessionHive



def main():
    sessionHive = getHiveSparkSession()

    if args.infraenv == "FullLoad":

        yearList = ['2020','2021','2022','2023','2024']
        monthList = [str(i) for i in range(1, 13)]
        dayList = [str(i) for i in range(1, 32)]
        for i in range(5):
            for j in range(12):
                for k in range(31):
                    #ALTER TABLE nayapay_sourcelayer.cust_details ADD PARTITION (CRYEAR='2019',CRMONTH='12',CRDAY='11') LOCATION '/transdata/wslog/data/CRYEAR=2019/CRMONTH=12/CRDAY=11'
                    strToInsertData = "ALTER TABLE creditbook_sourcelayer.trans_details ADD PARTITION (cryear='"+yearList[i]+"',crmonth='"+monthList[j]+"',crday='"+dayList[k]+"') LOCATION '/creditBookDWH/transactions/cryear="+yearList[i]+"/crmonth="+monthList[j]+"/crday="+dayList[k]+"'"
                    #print(strToInsertData)
                    sessionHive.sql(strToInsertData)

        #### Here, we have data for these 5 years only and we are not catering incremental load data. So for time being we are taking 2024 too with all its months, this approach is not right for incremnetal, 
        #### In real world we cater incremental load as well 
        #### So For this I have code too but it will confuse those who are observing this case study. I can explain logic.

        ## Logic will be very straightforward and it will be running in FullLoad, First we Add partition of all legacy data till (present year -1) which is 2023 in our case.
        ## Then we get todaysdate and year (From our hive table :: creditbook_public.dateToday)
        ## Then we Add partition till present year with present month but with (present -1) day. it is because present day will be catered by "incremental Load"


if __name__ == '__main__':
    main()