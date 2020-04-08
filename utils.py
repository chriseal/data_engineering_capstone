from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType
import numbers

import os
import configparser
import boto3
import datetime
import pandas as pd
import boto3
from itertools import chain
import json

import maps

DEBUG = False
AIRPORT_RELPATH = "airport-codes_csv.csv"
CITIES_RELPATH = 'us-cities-demographics.csv'
WEATHER_RELFOLDER = 'weather'
SAS_S3_RELPATH = 'sas_data'
# SAS_LOCAL_RELPATH = './sas_data/part-00000-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet' if DEBUG else './sas_data/*'
SAS_LOCAL_RELPATH = './sas_data/*'
RAW_RELPATH = 'raw'
STAGE_RELPATH = 'staging'
WAREHOUSE_RELPATH = 'dwh'


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

S3IO_CATEGORIES = [x.lower().strip() for x in {'airport', 'city', 'sas', 'GlobalLandTemperaturesByCity', 'GlobalLandTemperaturesByCountry', 'GlobalLandTemperaturesByMajorCity', 'GlobalLandTemperaturesByState', 'GlobalTemperatures'}]
DATETIME_INT_ZERO = datetime.datetime(1960, 1, 1)
SAS_ID_TO_FULLTEXT_MAPS_FPATH = './maps/sas_id_to_fullText_maps.json'
try:
    with open(SAS_ID_TO_FULLTEXT_MAPS_FPATH) as fp:
        SAS_ID_TO_FULLTEXT_MAPS = json.load(fp)
except IOError as e:
    print(e)

spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()

S3_CLIENT = boto3.client('s3', 
    aws_access_key_id=AWS_ACCESS_KEY, 
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
S3_RESOURCE = boto3.resource('s3', 
    aws_access_key_id=AWS_ACCESS_KEY, 
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)



### HELPERS ################################

def df_overview(df, show_nulls=True):
    """ Show schema, a few rows, and null counts (optional) of a Spark dataset
    
    Args: 
        df (pyspark.sql.dataframe.DataFrame): Spark dataframe
    """
    df.printSchema()
    print("Shape: {}".format(get_shape(df)))
    df.show(5)
    if show_nulls:
        print("Null counts:")
        get_null_counts(df).show()

    
def value_counts(df, col, show=None):
    """ Prints counts of each category in a column
    
    Args: 
        df (pyspark.sql.dataframe.DataFrame): Spark dataframe
        col (str): column name
        show (int), default None: Maximum number of categories to show
    """
    print(col)
    if show is None:
        df.groupBy(col).count().orderBy(F.desc("count")).show()
    else:
        df.groupBy(col).count().orderBy(F.desc("count")).show(show)

        
def get_null_counts(df):
    """ 
    Args: 
        df (pyspark.sql.dataframe.DataFrame): Spark dataframe
        
    Assistance from here: https://stackoverflow.com/questions/44627386/how-to-find-count-of-null-and-nan-values-for-each-column-in-a-pyspark-dataframe
    """
    try:
        return df.select(
            [F.count(
                F.when(F.isnan(c) | F.isnull(c), 1)
            ).alias(c) for c in df.columns]
        )
    except:
        return df.select(
            [F.count(
                F.when(F.isnull(c), 1)
            ).alias(c) for c in df.columns]
        )

def get_shape(df):
    """ Gets dataframe shape
    
    Args:
        df (pyspark.sql.dataframe.DataFrame): Spark dataframe
    
    Returns: 
        (row count, column count) tuple
    
    Assistance from here: https://stackoverflow.com/questions/39652767/pyspark-2-0-the-size-or-shape-of-a-dataframe
    """
    
    return (df.count(), len(df.columns))


def load_raw_files_to_s3():
    """ Load unaltered raw files to S3. This only needs to be run once """
    
    for category in S3IO_CATEGORIES:
        io = S3IO(category, 'raw')
        io.push(confirm=True)


def list_available_files(bucket, prefix):
    """ List all available files in S3 bucket
    
    Args:
        bucket (str): the name of the root S3 folder
        prefix (str): prefix for S3 filtering
        
    Returns:
        list of filepath strings
    """

    # for debugging
    # bucket = 'udacity-dataeng-capstone'
    # prefix = 'staging/sas_maps/*.csv/*.csv'
    res = S3_RESOURCE.Bucket(bucket)
    s3_fpaths = []
    for obj in res.objects.filter(Prefix=prefix):
        print(obj.key)
        s3_fpaths.append(os.path.join(bucket, prefix, obj.key))
    
    return s3_fpaths


def check_has_data(df, name):
    """ Checks if the data set has any data in it, and raises and exception if it doesn't
    
    Args:
        df (pyspark.sql.dataframe.DataFrame): spark dataframe
        name (str): dataset name to appear in logging
    """
    
    if len(df.columns) > 0 and df.count() > 0:
        print("{} has data!".format(name))
    else:
        raise Exception("Oh no! {} does NOT have data!".format(name))


def check_no_empty_columns(df, name):
    """ Checks to make sure no columns in a data set are empty 
    
    Args:
        df (pyspark.sql.dataframe.DataFrame): spark dataframe
        name (str): dataset name to appear in logging
    """
    
    nulls = get_null_counts(df).toPandas()
    rows = df.count()
    assert not nuls.apply(lambda x: x == rows, axis=1).values.sum(), f"Some columns may contain all missing values in {name}, {nulls}"
    print(f"All columns in {name} have data")

        
### END HELPERS ############################
### START UDF HELPERS ############################        

def strip_text(x):
    """ Strip text for data cleaning """
    
    if type(x) == str:
        return x.strip()
    else:
        return x # nulls
    
udf_strip = udf(strip_text)


def convert_to_null(x):
    """ Convert 'null', '0', 0 to None for data cleaning """
    
    if type(x) == str:
        if x in {'null', '0'}:
            return None
    elif isinstance(x, numbers.Number):
        if x == 0:
            return None
    return x

def integer_to_datetime(x):
    """ Convert integer into a datetime format 
    
    Args:
        x (int): Integer that represents a date
    """
    
    try:
        return DATETIME_INT_ZERO + datetime.timedelta(days=int(x))
    except:
        return None
udf_integer_to_datetime = udf(integer_to_datetime, DateType())

### END UDF HELPERS ############################


class S3IO(object):
    """ Reading/writing helper for immigration data pipeline """
    
    def __init__(self, category, step, from_raw=False):
        """ 
        Args: 
            category (str): name of the relevant dataset
            step (str): raw, staging, or warehouse
            from_raw (bool), default=False: If step is warehouse, whether to transform the dataset from raw files or get from staging
        """

        assert 'spark' in globals(), 'spark object must be instantiated'

        self.category = category if category is None else category.strip().lower()
        self.step = step.strip().lower()
        self.from_raw = from_raw
        assert self.step in {'raw','staging','warehouse'}
        if self.step != 'warehouse':
            assert self.category in S3IO_CATEGORIES or 'sas_maps' in self.category
        assert type(self.from_raw) == bool

        self.sas = None
        self.city = None
        self.airport = None

        
    def get(self, kwargs=None):
        """ Get requisite dataset, based on parameters """
        
        if kwargs is None:
            kwargs = {}
        print(f"Getting: {self.step} - {self.category}")

        if self.step == 'raw':
            if self.category == 'airport':
                return spark.read.csv(os.path.join(os.path.join(S3_BUCKET, RAW_RELPATH, AIRPORT_RELPATH), '*.csv'),  
                                      inferSchema=True, header=True, **kwargs)
            elif self.category in {'city','cities'}:
                return spark.read.csv(os.path.join(os.path.join(S3_BUCKET, RAW_RELPATH, CITIES_RELPATH), '*.csv'), 
                                      inferSchema=True, header=True, sep=';', **kwargs)
            elif self.category == 'sas':
                return spark.read.parquet(os.path.join(S3_BUCKET, RAW_RELPATH, SAS_S3_RELPATH), **kwargs)
            elif self.category.lower() == 'GlobalTemperatures'.lower():
                # max date: datetime.datetime(2015, 12, 1, 0, 0)
                # num rows: (3192, 9)
                return spark.read.csv(os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalTemperatures.csv', '*.csv'),  
                                      inferSchema=True, header=True, **kwargs)
            elif self.category.lower() == 'GlobalLandTemperaturesByCity'.lower():
                return spark.read.csv(os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalLandTemperaturesByCity.csv', '*.csv'),  
                                      inferSchema=True, header=True, **kwargs)
            elif self.category.lower() == 'GlobalLandTemperaturesByCountry'.lower():
                return spark.read.csv(os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalLandTemperaturesByCountry.csv', '*.csv'),  
                                      inferSchema=True, header=True, **kwargs)
            elif self.category.lower() == 'GlobalLandTemperaturesByMajorCity'.lower():
                return spark.read.csv(os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalLandTemperaturesByMajorCity.csv', '*.csv'),  
                                      inferSchema=True, header=True, **kwargs)
            elif self.category.lower() == 'GlobalLandTemperaturesByState'.lower():
                return spark.read.csv(os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalLandTemperaturesByState.csv', '*.csv'),  
                                      inferSchema=True, header=True, **kwargs)
        elif self.step == 'staging':
            if self.category == 'airport':
                return spark.read.csv(os.path.join(S3_BUCKET, STAGE_RELPATH, AIRPORT_RELPATH, '*.csv'),  
                                      inferSchema=True, header=True)
            elif self.category in {'city','cities'}:
                return spark.read.csv(os.path.join(S3_BUCKET, STAGE_RELPATH, CITIES_RELPATH, '*.csv'), 
                                      inferSchema=True, header=True, sep=';')
            elif self.category == 'sas':
                return spark.read.parquet(os.path.join(S3_BUCKET, STAGE_RELPATH, SAS_S3_RELPATH))
            elif 'sas_maps' in self.category:
                return spark.read.csv(os.path.join(S3_BUCKET, STAGE_RELPATH, self.category, '*.csv'),  
                                      inferSchema=True, header=True, **kwargs)
            elif any([self.category.lower() == k.lower() for k in ['GlobalLandTemperaturesByCity', 'GlobalLandTemperaturesByCountry', 'GlobalTemperatures', 'GlobalLandTemperaturesByMajorCity', 'GlobalLandTemperaturesByState']]):
                raise Exception(f"{self.category} not used in warehouse, because it's maximum available date is 2013-09-01, which ends before SAS data starts")
            else:
                raise Exception("category not understood - {}".format(self.category))
        elif self.step == 'warehouse':
            if self.from_raw:
                # self = S3IO(None, 'warehouse', from_raw=True) # for debugging
                trans = Transformer(None, 'warehouse', write=False, verbose=False)
                trans.transform()
                self.sas = trans.sas
                self.city = trans.city
                self.airport = trans.airport
            else:
                if self.category == 'airport':
                    print("Getting airport dataset")
                    self.airport = spark.read.parquet(os.path.join(S3_BUCKET, WAREHOUSE_RELPATH, AIRPORT_RELPATH))
                elif self.category in {'city','cities'}:
                    print("Getting city dataset")
                    self.city = spark.read.parquet(os.path.join(S3_BUCKET, WAREHOUSE_RELPATH, CITIES_RELPATH))
                elif self.category == 'sas':
                    print("Getting SAS dataset")
                    self.sas = spark.read.parquet(os.path.join(S3_BUCKET, WAREHOUSE_RELPATH, SAS_S3_RELPATH))

            print("Applying full-text maps to SAS from maps.py for readability")
            SAS_ARRMODE_MAPPING = F.create_map([F.lit(x) for x in chain(*maps.SAS_ARRMODE.items())])
            self.sas = self.sas.withColumn("arrmode", SAS_ARRMODE_MAPPING.getItem(F.col("arrmode_id")))
            SAS_VISA_MAPPING = F.create_map([F.lit(x) for x in chain(*maps.SAS_VISA.items())])
            self.sas = self.sas.withColumn("visa", SAS_VISA_MAPPING.getItem(F.col("visa_id")))
            SAS_I94CIT_AND_RES_MAPPING = F.create_map([F.lit(x) for x in chain(*maps.SAS_I94CIT_AND_RES.items())])
            self.sas = self.sas.withColumn("cit_country", SAS_I94CIT_AND_RES_MAPPING.getItem(F.col("I94CIT")))
            self.sas = self.sas.withColumn("res_country", SAS_I94CIT_AND_RES_MAPPING.getItem(F.col("I94RES")))
            
            print(f"Applying full-text maps to SAS from {SAS_ID_TO_FULLTEXT_MAPS_FPATH} for readability")
            global SAS_ID_TO_FULLTEXT_MAPS
            with open(SAS_ID_TO_FULLTEXT_MAPS_FPATH) as fp:
                SAS_ID_TO_FULLTEXT_MAPS = json.load(fp)
            for c, col_map in SAS_ID_TO_FULLTEXT_MAPS.items():
                col_map_spark = F.create_map([F.lit(x) for x in chain(*col_map.items())])
                self.sas = self.sas.withColumn(c, col_map_spark.getItem(F.col(f"{c}_id")))
            
            print("Apply city_id to city, state map to SAS")
            self.city.createOrReplaceTempView("city")
            self.sas.createOrReplaceTempView("sas")
            self.sas = spark.sql("""
                SELECT s.*, c.city, c.state
                FROM sas s
                LEFT JOIN city c
                    ON s.city_id=c.id
            """)
            print("Apply city_id to city, state map to AIRPORT")
            self.city.createOrReplaceTempView("city")
            self.airport.createOrReplaceTempView("airport")
            self.airport = spark.sql("""
                SELECT a.*, c.city, c.state
                FROM airport a
                LEFT JOIN city c
                    ON a.city_id=c.id
            """)

                
    def push(self, kwargs=None, confirm=False):
        """ Push files to S3 """
        
        if kwargs is None:
            kwargs = {}
        assert type(confirm) == bool

        if self.step == 'raw':
            # This should be run from Udacity's provided workspace and only needs to be done once per file
            if self.category == 'airport':
                print("Loading raw Airport file from Udacity workspace")
                airport = spark.read.csv(AIRPORT_RELPATH, inferSchema=True, header=True)
                print("Saving raw Airport file to S3")
                airport.write.csv(os.path.join(S3_BUCKET, RAW_RELPATH, AIRPORT_RELPATH), 
                                  header=True, mode='overwrite', **kwargs)
            elif self.category == 'city':
                print("Loading raw city file from Udacity workspace")
                cities = spark.read.csv(CITIES_RELPATH, inferSchema=True, header=True, sep=';')
                print("Saving raw Cities file to S3")
                cities.write.csv(os.path.join(S3_BUCKET, RAW_RELPATH, CITIES_RELPATH), 
                                 header=True, mode='overwrite', sep=';', **kwargs)
            elif self.category == 'sas':
                print("Loading raw sas file from Udacity workspace")
                sas = spark.read.parquet("sas_data")
                print("Saving raw SAS file to S3")
                sas.write.parquet(os.path.join(S3_BUCKET, RAW_RELPATH, SAS_S3_RELPATH), mode='overwrite')
            elif self.category == 'GlobalLandTemperaturesByCity'.lower():
                print("Loading raw GlobalLandTemperaturesByCity file from Udacity workspace")
                df = spark.read.csv(os.path.join(WEATHER_RELFOLDER, 'GlobalLandTemperaturesByCity.csv'), 
                                    inferSchema=True, header=True)
                print("Saving GlobalLandTemperaturesByCity to S3")
                df.write.csv( os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalLandTemperaturesByCity.csv'), 
                             header=True, mode='overwrite', **kwargs)
            elif self.category == 'GlobalLandTemperaturesByCountry'.lower():
                print("Loading raw GlobalLandTemperaturesByCountry file from Udacity workspace")
                df = spark.read.csv(os.path.join(WEATHER_RELFOLDER, 'GlobalLandTemperaturesByCountry.csv'), 
                                    inferSchema=True, header=True)
                print("Saving GlobalLandTemperaturesByCountry to S3")
                df.write.csv( os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalLandTemperaturesByCountry.csv'), 
                             header=True, mode='overwrite', **kwargs)
            elif self.category == 'GlobalLandTemperaturesByMajorCity'.lower():
                print("Loading raw GlobalLandTemperaturesByMajorCity file from Udacity workspace")
                df = spark.read.csv(os.path.join(WEATHER_RELFOLDER, 'GlobalLandTemperaturesByMajorCity.csv'), 
                                    inferSchema=True, header=True)
                print("Saving GlobalLandTemperaturesByCountry to S3")
                df.write.csv( os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalLandTemperaturesByMajorCity.csv'), 
                             header=True, mode='overwrite', **kwargs)
            elif self.category == 'GlobalLandTemperaturesByState'.lower():
                print("Loading raw GlobalLandTemperaturesByState file from Udacity workspace")
                df = spark.read.csv(os.path.join(WEATHER_RELFOLDER, 'GlobalLandTemperaturesByState.csv'), 
                                    inferSchema=True, header=True)
                print("Saving GlobalLandTemperaturesByState to S3")
                df.write.csv( os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalLandTemperaturesByState.csv'), 
                             header=True, mode='overwrite', **kwargs)
            elif self.category == 'GlobalTemperatures'.lower():
                print("Loading raw GlobalTemperatures file from Udacity workspace")
                df = spark.read.csv(os.path.join(WEATHER_RELFOLDER, 'GlobalTemperatures.csv'), 
                                    inferSchema=True, header=True)
                print("Saving GlobalTemperatures to S3")
                df.write.csv( os.path.join(S3_BUCKET, RAW_RELPATH, WEATHER_RELFOLDER, 'GlobalTemperatures.csv'), 
                             header=True, mode='overwrite', **kwargs)
        elif self.step in ['staging','warehouse']:
            raise Exception("This is handled in Transformer class")

        if confirm:
            self.get().show(5)


class Transformer(object):
    """ Perform transformations on a dataset """
    
    def __init__(self, category, step, write=False, verbose=False):
        """ 
        Args: 
            category (str): name of the relevant dataset
            step (str): raw, staging, or warehouse
            write (bool), default False: write file to S3
            verbose (bool), default False: degree to which events are logged
        """
        
        assert 'spark' in globals(), 'spark object must be instantiated'

        self.category = category
        self.step = step
        self.verbose = verbose
        self.write = write
        
        # assertions
        assert self.step in {'staging','warehouse'}
        if self.step in 'staging':
            assert self.category in S3IO_CATEGORIES
        elif self.step == 'warehouse':
            pass # self.category is irrelevant
        assert type(self.verbose) == bool
        assert type(self.write) == bool


    def transform(self):
        """ Convert the dataset from one condition to another, based on parameters """
        
        if self.step == 'staging':
            print(f"Transforming {self.category} to staging as an intermediary step")
            #  all columns WITHIN each dataframe are cleaned
            io = S3IO(self.category, 'raw')
            if self.category == 'airport':
                # io = S3IO('airport', 'raw') # for debugging
                airport = io.get()
                # airport feature engineering
                # separate latitude and longitude columns
                airport = airport.withColumn("latitude", F.split("coordinates", ', ')[0].cast("double"))
                airport = airport.withColumn("longitude", F.split("coordinates", ', ')[1].cast("double"))
                airport = airport.drop(airport.coordinates)
                # clean text
                for column in ['iso_region', 'municipality']:
                    airport = airport.withColumn(column, udf_strip(column))

                if self.verbose:
                    # understand dataset
                    df_overview(airport)

                    print("latitude null counts: {}".format(airport.where(airport.latitude.isNull()).count()))
                    print("longitude null counts: {}".format(airport.where(airport.longitude.isNull()).count()))
                    value_counts(airport, 'type')
                    value_counts(airport, 'continent')
                    value_counts(airport, 'iso_country')
                    airport.describe(['elevation_ft', 'latitude', 'longitude']).show()
                
                # filter on US only airports
                us_only = airport.filter(col('iso_country') == 'US')
                # add additional features
                us_only = us_only.withColumn("state", F.split('iso_region', '-')[1].cast("string"))
                us_only = us_only.withColumnRenamed("municipality", "city")
                us_only = us_only.drop('iso_region','gps_code','iata_code','local_code')

                if self.write:
                    print("Saving transformed Airport file to S3")
                    # saving to csv, because there are only 22K rows
                    us_only.write.csv(os.path.join(S3_BUCKET, STAGE_RELPATH, AIRPORT_RELPATH), header=True, mode='overwrite')
                return us_only
            elif self.category == 'city':
                # io = S3IO('city', 'raw') # for debugging
                cities = io.get()
                cities = cities.withColumnRenamed("City", "city")\
                    .withColumnRenamed("State", "state_full")\
                    .withColumnRenamed("State Code", "state")\
                    .withColumnRenamed('Median Age','median_age')\
                    .withColumnRenamed('Male Population','male_pop')\
                    .withColumnRenamed('Female Population','female_pop')\
                    .withColumnRenamed('Total Population','total_pop')\
                    .withColumnRenamed('Number of Veterans','num_veterans')\
                    .withColumnRenamed('Foreign-born','foreign_born')\
                    .withColumnRenamed('Average Household Size','avg_household_size')
                cities.createOrReplaceTempView("cities_table")
                
                if False:
                    # for debugging
                    # noticed that rows were redundant, and Race was repeated across rows
                    tmp = spark.sql("SELECT state, city, COUNT(*) FROM cities_table GROUP BY state, city").toPandas()
                    spark.sql("SELECT * FROM cities_table WHERE state = 'New York' AND city = 'Yonkers'").toPandas()
                    df = spark.sql("SELECT * FROM cities_table WHERE state = 'New York'").toPandas()

                # recreate dataframe to reduce number of rows and add one column for each race
                new = spark.sql("""
                    SELECT DISTINCT city, state_full, median_age, male_pop, female_pop,
                        total_pop,num_veterans,foreign_born,avg_household_size,state
                    FROM cities_table
                    """
                )
                new.createOrReplaceTempView("new_cities_table")
                distinct_races = spark.sql("SELECT DISTINCT(Race) FROM cities_table").toPandas().values.flatten().tolist()
                for race in distinct_races:
                    race_table = spark.sql("""SELECT state, city, Count AS `{race}`
                            FROM cities_table 
                            WHERE race='{race}'
                        """.format(race=race))
                    race_table.createOrReplaceTempView("race_table")
                    new = spark.sql("""
                        SELECT c.*, `{race}`
                        FROM new_cities_table c 
                        LEFT JOIN race_table r
                            ON c.state=r.state AND c.city=r.city
                    """.format(race=race))
                    new.createOrReplaceTempView("new_cities_table")

                if self.write:
                    print("Saving transformed Cities file to S3")
                    new.write.csv(os.path.join(S3_BUCKET, STAGE_RELPATH, CITIES_RELPATH), header=True, mode='overwrite', sep=';')
                return new
            elif self.category == 'sas':
                # io = S3IO('sas', 'raw') # for debugging
                sas = io.get()
                sas = sas.withColumnRenamed("I94YR", "year")\
                    .withColumnRenamed("I94MON", "month")\
                    .withColumnRenamed("I94CIT", "I94CIT")\
                    .withColumnRenamed("I94RES", "I94RES")\
                    .withColumnRenamed("I94PORT", "port")\
                    .withColumnRenamed("ARRDATE", "arrdate")\
                    .withColumnRenamed("I94MODE", "arrmode")\
                    .withColumnRenamed("I94ADDR", "addr_state")\
                    .withColumnRenamed("DEPDATE", "depdate")\
                    .withColumnRenamed("I94BIR", "age")\
                    .withColumnRenamed("I94VISA", "visa")\
                    .withColumnRenamed("MATFLAG", "match_flag")\
                    .withColumnRenamed("BIRYEAR", "birth_year")\
                    .withColumnRenamed("GENDER", "gender")\
                    .withColumnRenamed("AIRLINE", "airline")\
                    .withColumnRenamed("I94YR", "year")\
                    .withColumnRenamed('admnum', 'admission_num')\
                    .withColumnRenamed('FLTNO', 'flight_num')
                sas = sas.withColumn("birth_year", col("birth_year").cast(IntegerType()))\
                    .withColumn("year", col("year").cast(IntegerType()))\
                    .withColumn("month", col("month").cast(IntegerType()))\
                    .withColumn("age", col("age").cast(IntegerType()))\
                    .withColumn("arrmode", col("arrmode").cast(IntegerType()))\
                    .withColumn("I94CIT", col("I94CIT").cast(IntegerType()))\
                    .withColumn("I94RES", col("I94RES").cast(IntegerType()))\
                    .withColumn("arrdate", col("arrdate").cast(IntegerType()))\
                    .withColumn("depdate", col("depdate").cast(IntegerType()))\
                    .withColumn("visa", col("visa").cast(IntegerType()))\
                    .withColumn("cicid", col("cicid").cast(IntegerType()))

                # "CIC does not use" these columns
                sas = sas.drop('DTADFILE', 'VISAPOST', 'OCCUP', 'ENTDEPA', 'ENTDEPD', 'ENTDEPU', 'DTADDTO', 'COUNT') 

                print("Creating id columns from full-text columns for faster querying...")
                global SAS_ID_TO_FULLTEXT_MAPS
                SAS_ID_TO_FULLTEXT_MAPS = {}
                for c in ['visatype', 'airline', 'gender']:
                    print(f"...creating {c}_id to {c} mapping")
                    col_map_json = sas.select(c).distinct().toPandas().to_dict()
                    SAS_ID_TO_FULLTEXT_MAPS.update(col_map_json)
                    col_map = spark.createDataFrame(pd.DataFrame(col_map_json).reset_index().rename(columns={'index': 'id'}))
                    col_map.createOrReplaceTempView("col_map")
                    sas.createOrReplaceTempView("sas")
                    sas = spark.sql("""
                        SELECT s.*, c.id AS {c}_id
                        FROM sas s 
                        LEFT JOIN col_map c
                            ON s.{c}=c.{c}
                    """.format(c=c))
                    sas = sas.drop(c)

                # save json for mapping upon data warehouse read
                with open(SAS_ID_TO_FULLTEXT_MAPS_FPATH, 'w') as fp:
                    json.dump(SAS_ID_TO_FULLTEXT_MAPS, fp)

                # convert match flag to boolean
                sas = sas.withColumn("match_flag", F.when(col('match_flag') == "M", True).otherwise(False))

                sas = sas.withColumn("arrdate", udf_integer_to_datetime("arrdate"))\
                    .withColumn("depdate", udf_integer_to_datetime("depdate"))
                
                if self.write:
                    print("Saving transformed SAS file to S3")
                    sas.write.parquet(os.path.join(S3_BUCKET, STAGE_RELPATH, SAS_S3_RELPATH), mode='overwrite')

                return sas
            elif self.category == 'GlobalTemperatures'.lower():
                # io = S3IO('GlobalTemperatures', 'raw') # for debugging
                weather = io.get().where(col("LandAverageTemperature").isNotNull())
                weather = weather.withColumn("dt", col('dt').cast("date"))
                if False:
                    # do this once to get the earliest required date
                    # this assumes that new data will come in the future / not the past
                    io = S3IO('sas', 'staging')
                    min_date = io.get().agg({"arrdate": "min"}).collect()[0]['min(arrdate)']
                min_date = datetime.date(2016, 4, 1)
                assert weather.groupby('dt').count().agg({'count': 'max'}).collect()[0]['max(count)'] == 1, "Possible duplicate dates. Should have 1 row per date"
                weather = weather.filter(col('dt') >= min_date)
                raise Exception("Oops, max date is Row(max(dt)=datetime.datetime(2015, 12, 1, 0, 0)), which doesn't overlap with SAS data")
            elif any([self.category == c.lower() for c in ['GlobalLandTemperaturesByMajorCity', 'GlobalLandTemperaturesByCountry', 'GlobalLandTemperaturesByState', 'GlobalTemperatures']]):
                raise Exception(f"{self.category} is not used in data warehouse")
            else:
                raise Exception(f"{self.category} not understood")

        elif self.step == 'warehouse':
            # prepare data to be deployed to production in warehouse
            # get datasets from staging
            print("Preparing all datasets for storage in the data warehouse")
            io = Transformer('city', 'staging', write=False)
            city = io.transform()
            io = Transformer('airport', 'staging', write=False)
            airport = io.transform()
            io = Transformer('sas', 'staging', write=False)
            sas = io.transform()

            sas = sas.withColumnRenamed("arrmode", "arrmode_id")\
                .withColumnRenamed("visa", "visa_id")

            SAS_PORT_TO_CITY_MAPPING = F.create_map([F.lit(x) for x in chain(*maps.SAS_PORT_TO_CITY.items())])
            sas = sas.withColumn("port_city", SAS_PORT_TO_CITY_MAPPING.getItem(F.col("port")))
            SAS_PORT_TO_STATE_MAPPING = F.create_map([F.lit(x) for x in chain(*maps.SAS_PORT_TO_STATE.items())])
            sas = sas.withColumn("port_state", SAS_PORT_TO_STATE_MAPPING.getItem(F.col("port")))

            print("Creating city_id columns...")
            print("...get cities not represented in cities dimension from SAS and append to city table")
            city.createOrReplaceTempView("city")
            sas.createOrReplaceTempView("sas")
            sas_unique_locations = spark.sql("""
                SELECT DISTINCT port_city AS city, port_state AS state
                FROM sas
                WHERE port_city IS NOT NULL 
                    AND port_state IS NOT NULL
            """)
            print("......sas_unique_locations shape: {}".format(get_shape(sas_unique_locations)))
            # downselect to cities not currently in city table
            sas_unique_locations_df = sas_unique_locations.toPandas()
            city_keys_df = spark.sql("""SELECT CONCAT(city,state) AS key FROM city""").toPandas()
            assert city_keys_df['key'].value_counts().max() == 1
            sas_unique_locations_df['key'] = sas_unique_locations_df['city']+sas_unique_locations_df['state']
            sas_unique_locations_df['mask_additional'] = sas_unique_locations_df['key'].apply(lambda x: x not in city_keys_df['key'].values)
            sas_additional_cities = sas_unique_locations_df[sas_unique_locations_df['mask_additional']].loc[:, ['city','state']].copy()
            additional_sas_city_rows = sas_additional_cities.shape[0]
            sas_additional_cities = spark.createDataFrame(sas_additional_cities)
            print("......Additional locations from SAS: {}".format(additional_sas_city_rows))
            before_city_rows = get_shape(city)[0]
            city.createOrReplaceTempView("city")
            sas_additional_cities.createOrReplaceTempView("sas_additional_cities")
            city = spark.sql("""
                SELECT city, state, state_full, median_age, male_pop, female_pop, total_pop, 
                    num_veterans, foreign_born, avg_household_size, `Black or African-American`, 
                    `Hispanic or Latino`, `White`, `Asian`, `American Indian and Alaska Native`
                    FROM city
                UNION
                SELECT city, state, null AS state_full,
                    null AS median_age,
                    null AS male_pop,
                    null AS female_pop,
                    null AS total_pop,
                    null AS num_veterans,
                    null AS foreign_born,
                    null AS avg_household_size,
                    null AS `Black or African-American`,
                    null AS `Hispanic or Latino`,
                    null AS `White`,
                    null AS `Asian`,
                    null AS `American Indian and Alaska Native`
                    FROM sas_additional_cities
            """)
            city_shape = get_shape(city)
            assert city_shape[0] == before_city_rows+additional_sas_city_rows
            print("\tcity table shape: {}".format(city_shape))

            print("...get cities not represented in cities dimension from AIRPORT and append to city table")
            city.createOrReplaceTempView("city")
            airport.createOrReplaceTempView("airport")
            airport_unique_locations = spark.sql("""
                SELECT DISTINCT city, state
                FROM airport
                WHERE city IS NOT NULL 
                    AND state IS NOT NULL
            """)
            print("......airport_unique_locations shape: {}".format(get_shape(airport_unique_locations)))
            city.createOrReplaceTempView("city")
            airport_unique_locations.createOrReplaceTempView("airport_unique_locations")
            # downselect to cities not currently in city table
            airport_additional_cities = spark.sql("""
                SELECT DISTINCT a.city, a.state
                FROM airport_unique_locations a
                WHERE CONCAT(a.city,a.state)
                    NOT IN ( SELECT CONCAT(city,state) AS key FROM city)
            """)
            additional_airport_city_rows = get_shape(airport_additional_cities)[0]
            print("......Additional locations from AIRPORT: {}".format(additional_airport_city_rows))
            before_city_rows = get_shape(city)[0]
            city.createOrReplaceTempView("city")
            airport_additional_cities.createOrReplaceTempView("airport_additional_cities")
            city = spark.sql("""
                SELECT city, state, state_full, median_age, male_pop, female_pop, total_pop, 
                    num_veterans, foreign_born, avg_household_size, `Black or African-American`, 
                    `Hispanic or Latino`, `White`, `Asian`, `American Indian and Alaska Native`
                    FROM city
                UNION
                SELECT city, state, null AS state_full,
                    null AS median_age,
                    null AS male_pop,
                    null AS female_pop,
                    null AS total_pop,
                    null AS num_veterans,
                    null AS foreign_born,
                    null AS avg_household_size,
                    null AS `Black or African-American`,
                    null AS `Hispanic or Latino`,
                    null AS `White`,
                    null AS `Asian`,
                    null AS `American Indian and Alaska Native`
                    FROM airport_additional_cities
            """)
            city_shape = get_shape(city)
            assert city_shape[0] == before_city_rows+additional_airport_city_rows
            print("\tcity table shape: {}".format(city_shape))

            print("Add monotonically increasing city ID to city table")
            city = city.withColumn("id", F.monotonically_increasing_id())
            airport = airport.withColumn("id", F.monotonically_increasing_id())
            sas = sas.withColumn("id", F.monotonically_increasing_id())

            print("Apply city_id to airport")
            city.createOrReplaceTempView("city")
            airport.createOrReplaceTempView("airport")
            airport = spark.sql("""
                SELECT a.*, c.id AS city_id
                FROM airport a
                LEFT JOIN city c
                    ON a.city=c.city AND a.state=c.state
            """)
            airport = airport.drop('city','state')
            print("Apply city_id to sas")
            city.createOrReplaceTempView("city")
            sas.createOrReplaceTempView("sas")
            sas = spark.sql("""
                SELECT s.*, c.id AS city_id
                FROM sas s
                LEFT JOIN city c
                    ON s.port_city=c.city AND s.port_state=c.state
            """)
            sas = sas.drop('port_city','port_state')

            if self.verbose:
                print("city to warehouse"); df_overview(city, show_nulls=True)
                print("airport to warehouse"); df_overview(airport, show_nulls=True)
                print("sas to warehouse"); df_overview(sas, show_nulls=True)

            if self.write:
                print("Transformations finished, saving files...")
                print("...Saving airport to data warehouse")
                airport.write.parquet(os.path.join(S3_BUCKET, WAREHOUSE_RELPATH, AIRPORT_RELPATH), mode='overwrite')
                print("...Saving city to data warehouse")
                city.write.partitionBy('state').parquet(os.path.join(S3_BUCKET, WAREHOUSE_RELPATH, CITIES_RELPATH), mode='overwrite')
                print("...Saving sas to data warehouse")
                sas.write.partitionBy('year', 'month').parquet(os.path.join(S3_BUCKET, WAREHOUSE_RELPATH, SAS_S3_RELPATH), mode='overwrite')
                
            self.sas = sas
            self.city = city
            self.airport = airport