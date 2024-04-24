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





def getTheDate(sessionHive):
    df = sessionHive.sql("SELECT todayDay, todayMonth, todayyear, yesterdayDay, yesterDayMonth, yesterDayYear from creditbook_public.datetoday")
    todayYear = df.withColumn("todayyear", df["todayyear"].cast("int"))
    todayYearRow = todayYear.head()
    todayYear = todayYearRow["todayyear"]
    intTodayYear = int(todayYear)
    
    todayDf = df.withColumn("todayday", df["todayday"].cast("int"))
    todayRow = todayDf.head()
    today = todayRow["todayday"]
    intToday = int(today)

    todayMonth = df.withColumn("todaymonth", df["todaymonth"].cast("int")) #todayDf['todayMonth']
    todayRowMonth = todayMonth.head()
    todayMonth = todayRowMonth["todaymonth"]
    intTodayMonth = int(todayMonth)

    yesterdayDf = df.withColumn("yesterdayDay", df["yesterdayDay"].cast("string"))
    yesterdayRow = yesterdayDf.head()
    yesterDay = yesterdayRow["yesterdayDay"]
    strYesterDay = str(yesterDay)

    yesterMonthDf = df.withColumn("yesterDayMonth", df["yesterDayMonth"].cast("string"))
    yesterMonthRow = yesterMonthDf.head()
    yesterMonth = yesterMonthRow["yesterDayMonth"]
    strYesterMonth = str(yesterMonth)

    yesterYearDf = df.withColumn("yesterDayYear", df["yesterDayYear"].cast("string"))
    yesterYearRow = yesterYearDf.head()
    yesterYear = yesterYearRow["yesterDayYear"]
    strYesterYear = str(yesterYear)

    return intTodayYear,intTodayMonth,intToday,strYesterDay,strYesterMonth,strYesterYear



def gettransProfile(session,df):
    # Total amount of credits by the user
    # Total amount of debits by the user
    # Sum of amount of total trans
    # count of credits by a user
    # count of debits by a user

    df = df.createOrReplaceTempView("mytempTable")
    TransType = session.sql("""select user_id, count(*) as userTotalTransCount, transaction_type, count(*) as TransTypeCount, SUM(amount) AS totalAmount from mytempTable group by user_id,transaction_type order by userTotalTransCount desc""")
    TransType.createOrReplaceTempView("mytempTable2")
    Resultantdf = session.sql("""select user_id, SUM(CASE WHEN transaction_type = 'debit' THEN TransTypeCount ELSE 0 END) AS debit_count,\
                          SUM(CASE WHEN transaction_type = 'credit' THEN TransTypeCount ELSE 0 END) AS credit_count, \
                          SUM(CASE WHEN transaction_type IN ('debit', 'credit') THEN TransTypeCount ELSE 0 END) AS total_trans_count, \
                          ROUND(SUM(CASE WHEN transaction_type = 'debit' THEN totalAmount ELSE 0 END) / 1000000,2) AS debitamount_inmillion, \
                          ROUND(SUM(CASE WHEN transaction_type = 'credit' THEN totalAmount ELSE 0 END) / 1000000,2) AS creditamount_inmillion, \
                          ROUND(SUM(CASE WHEN transaction_type IN ('debit', 'credit') THEN totalAmount ELSE 0 END) / 1000000,2) AS totaltransamount_inmillion \
                          from mytempTable2 group by user_id order by total_trans_count desc""")
    
    return Resultantdf
    #Resultantdf.printSchema()
    
    #Resultantdf.write.format("hive").saveAsTable("creditbook_curatedlayer.trans_profile",mode="append")
    

def transformTransData(session,df):
    Resultantdf = gettransProfile(session,df)
    joined_df = df.join(Resultantdf, "user_id", "left_outer")

    joined_df = joined_df.withColumn("trans_date", f.concat_ws("-",f.col("cryear"),f.col("crmonth"),f.col("crday")))
    
    joined_df = joined_df.select(
        f.col("user_id").cast(StringType()),
        f.col("transaction_id").cast(StringType()),
        f.col("business_id").cast(StringType()),
        f.col("amount").cast(StringType()),
        f.col("transaction_type").cast(StringType()),
        f.col("type").cast(StringType()),
        f.col("created_at").cast(StringType()),
        f.col("processed_at").cast(TimestampType()),
        f.col("cryear").cast(IntegerType()),
        f.col("crmonth").cast(IntegerType()),
        f.col("crday").cast(IntegerType()),
        f.col("trans_date").cast(StringType()),
        f.col("debit_count").cast(LongType()),
        f.col("credit_count").cast(LongType()),
        f.col("total_trans_count").cast(LongType()),
        f.col("debitamount_inmillion").cast(DoubleType()),
        f.col("creditamount_inmillion").cast(DoubleType()),
        f.col("totaltransamount_inmillion").cast(DoubleType())
    )

    joined_df.write.format("hive").saveAsTable("creditbook_curatedlayer.trans_details",mode="append")


def main():
    sessionHive = getHiveSparkSession()

    if args.infraenv == "FullLoad":
        df = sessionHive.sql("select * from creditbook_sourcelayer.trans_details")
        transformTransData(sessionHive,df)
        #
    else:
        ## Previous Logic (In File user_sourceToCurated.py) can be apply here
        return 0



if __name__ == '__main__':
    main()