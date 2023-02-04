from pyspark.sql import SparkSession
from ETL_Bike_Data import start_extraction
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *


sc = SparkContext()

spark = SparkSession.builder.getOrCreate()

data = start_extraction()['bikes']

schema = StructType([
    StructField("date_stolen", LongType(), True),
    StructField("description", StringType(), True),
    StructField("frame_colors", ArrayType(StringType()), True),
    StructField("frame_model", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("is_stock_img", BooleanType(), True),
    StructField("large_img", StringType(), True),
    StructField("location_found", StringType(), True),
    StructField("manufacturer_name", StringType(), True),
    StructField("external_id", StringType(), True),
    StructField("registry_name", StringType(), True),
    StructField("registry_url", StringType(), True),
    StructField("serial", StringType(), True),
    StructField("status", StringType(), True),
    StructField("stolen", BooleanType(), True),
    StructField("stolen_coordinates", ArrayType(DoubleType()), True),
    StructField("stolen_location", StringType(), True),
    StructField("thumb", StringType(), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("year", IntegerType(), True)
])

df = spark.createDataFrame(data,schema=schema)

df.show()

# data.createOrReplaceTempView("data")

# split_data = spark.sql("select explode(split(bikes, ', ')) as json_string from data")    

# split_data.show()

spark.stop()
