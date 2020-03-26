from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType

import os
import configparser
import boto3
import datetime
import pandas as pd

DEBUG = False
AIRPORT_RELPATH = "airport-codes_csv.csv"
CITIES_RELPATH = 'us-cities-demographics.csv'
SAS_S3_RELPATH = 'sas_data'
SAS_LOCAL_RELPATH = './sas_data/part-00000-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet' if DEBUG else './sas_data/*'
RAW_RELPATH = 'raw'

config = configparser.ConfigParser()
config.read_file(open('config.cfg'))
AWS_USERNAME = config.get('AWS', 'AWS_USERNAME')
AWS_PASSWORD = config.get('AWS', 'AWS_PASSWORD')
AWS_ACCESS_KEY = config.get('AWS', 'AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']
S3_BUCKET = config.get('AWS', 'S3_BUCKET')
REGION = config.get('AWS', 'REGION')


spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()


# S3 = boto3.resource('s3',
#     region_name=REGION,
#     aws_access_key_id=AWS_ACCESS_KEY,
#     aws_secret_access_key=AWS_SECRET_ACCESS_KEY
# )

# if False:
#     # this config reads the original spark dataset
#     spark = SparkSession.builder.\
#         config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.10")\
#         .enableHiveSupport()\
#         .getOrCreate()
#     df = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')


### HELPERS ################################

def df_overview(df):
    df.printSchema()
    print("Shape: {}".format(get_shape(df)))
    df.show(5)

    
def value_counts(df, col, show=None):
    print(col)
    if show is None:
        df.groupBy(col).count().orderBy(F.desc("count")).show()
    else:
        df.groupBy(col).count().orderBy(F.desc("count")).show(show)

        
def get_null_counts(df):
    """ 
    Assistance from here: https://stackoverflow.com/questions/44627386/how-to-find-count-of-null-and-nan-values-for-each-column-in-a-pyspark-dataframe
    """
    return df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in df.columns])


def get_shape(df):
    """ 
    Assistance from here: https://stackoverflow.com/questions/39652767/pyspark-2-0-the-size-or-shape-of-a-dataframe
    """
    
    return (df.count(), len(df.columns))


def read_raw_airport():
    # Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from here: https://datahub.io/core/airport-codes#data
    return spark.read.csv(AIRPORT_RELPATH, inferSchema=True, header=True)


### END HELPERS ############################        
### START LOAD DATAFRAMES ########################

def load_airport():
    """
    Resulting columns: ident, type, name, elevation_ft, continent, iso_country, iso_region, municipality, gps_code, iata_code, local_code, coordinates, latitude, longitude
    """
    
    print("Loading Airport dataset....")
    airport = read_raw_airport()

    # airport feature engineering
    # separate latitude and longitude columns
    airport = airport.withColumn("latitude", F.split("coordinates", ', ')[0].cast("double"))
    airport = airport.withColumn("longitude", F.split("coordinates", ', ')[1].cast("double"))
    
    df_overview(airport)
    print("Null counts:")
    get_null_counts(airport).show()
    
    
    # understand dataset
    print("latitude null counts: {}".format(airport.where(airport.latitude.isNull()).count()))
    print("longitude null counts: {}".format(airport.where(airport.longitude.isNull()).count()))
    
    value_counts(airport, 'type')
    value_counts(airport, 'continent')
    value_counts(airport, 'iso_country')
    
    airport.describe(['elevation_ft', 'latitude', 'longitude']).show()
    
    return airport


def write_sas_to_s3():
    """ """
    
    assert not DEBUG
    print("Reading raw SAS file")
    df = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')

    .intValue()

    def convert_date(x):
        mDt = datetime.datetime(1899, 12, 30)
        dlt = mDt + datetime.timedelta(days=int(x))
        return dlt.strftime("%Y-%m-%d")

    convert_date_udf = F.udf(lambda z: convert_date(z), StringType())
    df = df.withColumn('arrdate_str', convert_date_udf('arrdate').alias('arrdate'))

    SAS_SCHEMA = StructType([    
        StructField('cicid', DoubleType(), nullable=True), 
        StructField('i94yr', DoubleType(), nullable=True), 
        StructField('i94mon', DoubleType(), nullable=True), 
        StructField('i94cit', DoubleType(), nullable=True), 
        StructField('i94res', DoubleType(), nullable=True), 
        StructField('i94port', StringType(), nullable=True), 
        StructField('arrdate', IntegerType(), nullable=True), 
        StructField('i94mode', DoubleType(), nullable=True), 
        StructField('i94addr', StringType(), nullable=True), 
        StructField('depdate', IntegerType(), nullable=True), 
        StructField('i94bir', DoubleType(), nullable=True), 
        StructField('i94visa', DoubleType(), nullable=True), 
        StructField('count', DoubleType(), nullable=True), 
        StructField('dtadfile', StringType(), nullable=True), 
        StructField('visapost', StringType(), nullable=True), 
        StructField('occup', StringType(), nullable=True), 
        StructField('entdepa', StringType(), nullable=True), 
        StructField('entdepd', StringType(), nullable=True), 
        StructField('entdepu', StringType(), nullable=True), 
        StructField('matflag', StringType(), nullable=True), 
        StructField('biryear', IntegerType(), nullable=True), 
        StructField('dtaddto', StringType(), nullable=True), 
        StructField('gender', StringType(), nullable=True), 
        StructField('insnum', StringType(), nullable=True), 
        StructField('airline', StringType(), nullable=True), 
        StructField('admnum', DoubleType(), nullable=True), 
        StructField('fltno', StringType(), nullable=True), 
        StructField('visatype', StringType(), nullable=True)
    ])
    
    
    
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0), returnType=Tmstp())
    df = df.withColumn("start_time", get_timestamp(df.ts))


    df.arrdate = df.arrdate.apply(datetime.date.fromordinal) # 
    
    get_date = F.udf(lambda x: datetime.date.fromordinal(x), returnType=DateType())
    df = df.withColumn("arrdate_new", get_date(df.arrdate))
    

 
    df = spark.read.format("parquet").schema(SAS_SCHEMA).load(SAS_LOCAL_RELPATH)
    
    
    print("Saving SAS to S3")
    df.write.parquet(SAS_S3_PATH, mode='overwrite')


def load_sas():
    """ """
    
    sas = spark.read.parquet(SAS_S3_PATH)
#     start here - get all parquet files in bucket key


### END LOAD DATAFRAMES ##########################
    
    

    
 
    
