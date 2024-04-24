from pyspark import SparkContext, SparkConf 
from pyspark.conf import SparkConf 
from pyspark.sql import SparkSession, HiveContext,DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType,DoubleType,IntegerType
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

def transformUserData(session,sourceUserDf):

    # There are is only one major transformations required for this table, In curated Layer we apply almost all important transactions here usually.
    ## Transformation01 : Days Since Signup

    sourceUserDf = sourceUserDf.withColumn("Signup_at", f.to_date(f.concat(f.col("cryear"), f.lit("-"), f.col("crmonth"), f.lit("-"), f.col("crday"))))\
        .withColumn("Signup_at", f.to_date(f.col("Signup_at")))\
        .withColumn("signup_since_days", f.datediff(f.current_date(), f.col("Signup_at")))
    
    sourceUserDf = sourceUserDf.dropDuplicates(['user_id'])\
        .fillna({'rating': 0})

    sourceUserDf = sourceUserDf.select(
        f.col("user_id").cast(StringType()),
        f.col("business_id").cast(StringType()),
        f.col("rating").cast(StringType()),
        f.col("created_at").cast(StringType()),
        f.col("processed_at").cast(TimestampType()),
        f.col("signup_since_days").cast(IntegerType()),
        f.col("cryear").cast(IntegerType()),
        f.col("crmonth").cast(IntegerType()),
        f.col("crday").cast(IntegerType()),
    )

    sourceUserDf.write.format("hive").saveAsTable("creditbook_curatedlayer.user_details",mode="append")
    

def main():
    sessionHive = getHiveSparkSession()

    if args.infraenv == "FullLoad":
        sourceCustDf = sessionHive.sql("select * from creditbook_sourcelayer.user_details")
        transformUserData(sessionHive,sourceCustDf)
    

    elif args.infraenv == "IncrementalLoad":
        todayYear, todayMonth , todayDay, yesterday, yesterdayMonth, yesterdayYear   = getTheDate(sessionHive)
        todayYear=str(todayYear)
        todayMonth=str(todayMonth)
        todayDay=str(todayDay)
        # Get data where Month == todayMonth && Day == todayDay from creditbook_sourcelayer.user_details
        strSql = "select count(*) from creditbook_curatedlayer.user_details where cryear = "+todayYear+" and crmonth = "+todayMonth+" and crday = "+todayDay+""
        sourceUserDfCount = sessionHive.sql(strSql).collect()[0][0] 

        todayHourObj = datetime.now()
        todayHour = todayHourObj.hour
        todayMin = todayHourObj.minute
        inttodayMin = int(todayMin)
        intTodayHour = int(todayHour)
        print("intTodayHour",intTodayHour,":",inttodayMin)


        if intTodayHour == 0 and inttodayMin <= 28:
        #if sourceUserDfCount == 0:
            # If there is no data in todays day, then we need to get all 
            # data of day -1 (From Source) and get day -1 data (from curated table).
            # Then we will do left anti join, and insert all remaining data

            # Get data -1 from source layer 
            strSql = "select * from creditbook_sourcelayer.user_details where cryear = "+yesterdayYear+" and crmonth = "+yesterdayMonth+" and crday = "+yesterday+""
            sourceCustomerDataDayMinusOne = sessionHive.sql(strSql)
            ## Now Get all day -1 from curated layer
            strSql = "select * from creditbook_curatedlayer.user_details where cryear = "+yesterdayYear+" and crmonth = "+yesterdayMonth+" and crday = "+yesterday+""
            CuratedCustomerDataDayMinusOne = sessionHive.sql(strSql)
        
            ## Apply leftanti join
            resultantDayMinusOneDataForCustomer = sourceCustomerDataDayMinusOne.join(CuratedCustomerDataDayMinusOne,["id_cust","customer_id_cust"],how='leftanti')
            ## Get resultants data count, if it is greater than 0 then insert else no insertion
            
            intsert_cnt = int(resultantDayMinusOneDataForCustomer.count())
            if intsert_cnt>0:
                transformUserData(sessionHive,resultantDayMinusOneDataForCustomer)
                print("save operation ran...")
            else:
                print("No insert ...Opps...") 
            
        elif sourceUserDfCount == 0:
            # Then if count is 0 for nayapay_curatedlayer.cms_custcr then append data we got from nayapay_sourcelayer.cust_details.
            strSql = "select * from nayapay_sourcelayer.cust_details where cryear = "+todayYear+" and crmonth = "+todayMonth+" and crday = "+todayDay+""
            sourceCustDfData = sessionHive.sql(strSql)
            transformUserData(sessionHive,sourceCustDfData)
            
        else:
            # Then Get data where Month == todayMonth && Day == todayDay from nayapay_curatedlayer.cms_custcr
            strSql = "select * from nayapay_sourcelayer.cust_details where cryear = "+todayYear+" and crmonth = "+todayMonth+" and crday = "+todayDay+""
            sourceCustDfData = sessionHive.sql(strSql)
            strSql2 = "select * from nayapay_curatedlayer.cms_custcr where cryear = "+todayYear+" and crmonth = "+todayMonth+" and crday = "+todayDay+""
            ExistingCuratedCustDfData = sessionHive.sql(strSql2)
            # apply anti join and append only Today's data which is not present in currated Layer from Source Layer
            sourceCustDfData = sourceCustDfData.withColumnRenamed("id","id_cust")\
                                                                .withColumnRenamed("customer_id","customer_id_cust")
            onlyNewDataDf = sourceCustDfData.join(ExistingCuratedCustDfData,["id_cust","customer_id_cust"],how='leftanti')
            
            intsert_cnt = int(onlyNewDataDf.count())
            if intsert_cnt>0:
                transformUserData(sessionHive,onlyNewDataDf)
                print("save operation ran...")
            else:
                print("No insert ...Opps...")


if __name__ == '__main__':
    main()
