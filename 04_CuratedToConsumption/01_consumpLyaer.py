from pyspark import SparkContext, SparkConf 
from pyspark.conf import SparkConf 
from pyspark.sql import SparkSession, HiveContext,DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType,DoubleType
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
config.read("conf.ini")



def getHiveSparkSession():
    sparkSessionHive = SparkSession.builder \
                    .appName('example-pyspark-read-and-write-from-hive') \
                    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                    .config("hive.metastore.warehouse.dir", "/user/hive/warehouse") \
                    .enableHiveSupport() \
                    .getOrCreate()
    print(sparkSessionHive.sparkContext.getConf().getAll())
    return sparkSessionHive




def createDateDimFullLoad(curatedDf, session) -> None:
    
    start_date_dict = curatedDf.agg(f.min("created_at").alias("min_trans_dt")).collect()[0].asDict()
    end_date_dict = curatedDf.agg(f.max("created_at").alias("max_trans_dt")).collect()[0].asDict()

    start_date = start_date_dict["min_trans_dt"]
    #end_date = "2023-01-03 12:34:39"
    end_date = end_date_dict["max_trans_dt"]

    print("min_trans_dt",start_date)
    print("max_trans_dt",end_date)

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    date_dim = pd.DataFrame()
    date_dim['trans_date'] = date_range.date
    date_dim['Year'] = date_range.year
    start_day_of_year = pd.to_datetime(start_date).dayofyear
    date_dim['DayCounter'] = date_range.dayofyear - start_day_of_year + 1

    date_dim['Month'] = date_range.month
    date_dim['Quarter'] = date_range.quarter
    date_dim['Day'] = date_range.day
    date_dim['DayOfWeek'] = date_range.dayofweek
    date_dim['DayName'] = date_range.strftime('%A')
    date_dim['DayOfMonth'] = date_range.day
    date_dim['Weekday'] = date_dim['DayOfWeek'].map({0: 'Weekday', 1: 'Weekday', 2: 'Weekday', 3: 'Weekday', 4: 'Weekday', 5: 'Weekend', 6: 'Weekend'})

    date_dim_df = session.createDataFrame(date_dim)
    date_dim_df = date_dim_df.withColumn("date_id_pk", f.row_number().over(Window.orderBy("trans_date")))
    
    date_dim_df = date_dim_df.selectExpr("date_id_pk","trans_date" ,"Year as trans_year","Month as trans_month", "Quarter as trans_quarter","Day as trans_day","DayOfWeek as trans_dayofweek","DayName as trans_dayname","DayOfMonth as trans_dayofmonth","Weekday as trans_weekday") 
    
    date_dim_df.write.format("hive").saveAsTable('creditbook_consumplayer.date_dim',mode="append")  


def createUserDimFullLoad(curatedDf,session):
    userDimDf = curatedDf.withColumn("user_id_pk", f.row_number().over(Window.orderBy("user_id")))
    userDimDf = userDimDf.withColumn("isActive",f.lit('Y'))
    
    userDimDf = userDimDf.selectExpr("user_id_pk","user_id","business_id","rating",\
                                    "created_at","processed_at","signup_since_days","cryear","crmonth","crday","isActive")
    
    #userDimDf.printSchema()
    userDimDf.write.format("hive").saveAsTable('creditbook_consumplayer.user_dim',mode="append")

def createTransDimFullLoad(curatedDf,session):
    transDimDf = curatedDf.withColumn("trans_id_pk", f.row_number().over(Window.orderBy("user_id")))
    transDimDf = transDimDf.withColumn("isActive",f.lit('Y'))
    transDimDf = transDimDf.selectExpr("trans_id_pk", "user_id", "transaction_id", "business_id", "amount", "transaction_type",\
                                       "type","created_at","processed_at","cryear","crmonth","crday","trans_date","debit_count","credit_count",\
                                        "total_trans_count","debitamount_inmillion","creditamount_inmillion","totaltransamount_inmillion","isActive")
    
    transDimDf.write.format("hive").saveAsTable('creditbook_consumplayer.trans_dim',mode="append")

def createAnalyticsDimFullLoad(curatedDf,session):
    # Define a window specification
    windowSpec = Window.partitionBy("user_id").orderBy(f.col("cryear").desc(), f.col("crmonth").desc())

    # Add a row number column
    analyticsWindow_df = curatedDf.withColumn("row_num", f.row_number().over(windowSpec))

    # Filter rows with row number less than or equal to 3
    analyticsWindow_df = analyticsWindow_df.filter(f.col("row_num") <= 1).drop("row_num")


    analyticsDimDf = analyticsWindow_df.withColumn("analytics_id_pk", f.row_number().over(Window.orderBy("user_id")))
    analyticsDimDf = analyticsDimDf.withColumn("isActive",f.lit('Y'))
    
    analyticsDimDf = analyticsDimDf.selectExpr("analytics_id_pk","user_id","event_date","category",\
                                    "mobile_brand_name","mobile_model_name","mobile_os_hardware_model","operating_system","operating_system_version",\
                                        "city","country","appversion","processed_at","cryear","crmonth","crday","isActive")
    
    #analyticsDimDf.printSchema()
    analyticsDimDf.write.format("hive").saveAsTable('creditbook_consumplayer.analytics_dim',mode="append")



def main():
    sessionHive = getHiveSparkSession()


    if args.infraenv == "FullLoad":
        
        userDetailsDf = sessionHive.sql('SELECT * FROM creditbook_curatedlayer.user_details')
        transDetailsDf = sessionHive.sql('SELECT * FROM creditbook_curatedlayer.trans_details')
        analyticsDetailsDf = sessionHive.sql('SELECT * FROM creditbook_curatedlayer.analytics_details')
        
        createDateDimFullLoad(transDetailsDf,sessionHive) 
        createUserDimFullLoad(userDetailsDf,sessionHive)
        createTransDimFullLoad(transDetailsDf,sessionHive)
        createAnalyticsDimFullLoad(analyticsDetailsDf,sessionHive)

        date_dim_df = sessionHive.sql("select * from creditbook_consumplayer.date_dim")
        user_dim_df = sessionHive.sql("select * from creditbook_consumplayer.user_dim")
        trans_dim_df = sessionHive.sql("select * from creditbook_consumplayer.trans_dim")
        analytics_dim_df = sessionHive.sql("select * from creditbook_consumplayer.analytics_dim")

        trans_dim_df = trans_dim_df.withColumnRenamed("user_id","user_id_trans")
        analytics_dim_df = analytics_dim_df.withColumnRenamed("user_id","user_id_analyst")

        consDf = trans_dim_df.join(analytics_dim_df, trans_dim_df["user_id_trans"] == analytics_dim_df["user_id_analyst"],how='inner')
        
        consDf = consDf.join(user_dim_df, consDf["user_id_trans"] == user_dim_df["user_id"],how='inner')
        consDf = consDf.join(date_dim_df,consDf["trans_date"] == date_dim_df["trans_date"], how='inner')

        ## Like this we can add more calculated Fields, As I have time constraints thatswhy I am just inserting 2 aggregated values here....
        ## 
        gmv_per_month_df = consDf.groupBy("trans_year", "trans_month").agg(f.sum("amount").alias("gmv_per_month"),f.avg("amount").alias("avgtranspermonth")).orderBy("trans_year", "trans_month")
        
        # Join gmv_per_month_df with consDf
        consDf = consDf.join(gmv_per_month_df, 
                            (consDf["trans_year"] == gmv_per_month_df["trans_year"]) & 
                            (consDf["trans_month"] == gmv_per_month_df["trans_month"]), 
                            how='left')

    ### Complete DWH with Star Schmea Data modeling ! 
        consDf = consDf.withColumn("profile_id_pk", f.row_number().over(Window.orderBy("transaction_id")))
        allDatatDf = consDf.selectExpr("profile_id_pk","user_id_pk as user_id_fk","trans_id_pk as trans_id_fk", \
                                       "analytics_id_pk as analytics_id_fk", "date_id_pk as date_id_fk", "gmv_per_month", "avgtranspermonth")
        
        allDatatDf.write.format("hive").saveAsTable('creditbook_consumplayer.profile_fact',mode="append") 
        
        

if __name__ == '__main__':
    main()