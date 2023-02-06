from pyspark.sql import SparkSession
from extract_bike_data import start_extraction
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
import sys



postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]
table =sys.argv[4]



def write_new_data(df_new,spark):

    df_saved = spark.read.format("jdbc").option("url", postgres_db).option("dbtable", table).option("user", postgres_user).option("password", postgres_pwd).load()

    df_new.createOrReplaceTempView("new_data")

    df_saved.createOrReplaceTempView("old_data")

    df_new_records = spark.sql('''select distinct date_stolen,description,frame_colors,frame_model,id,is_stock_img,large_img,
     location_found,manufacturer_name,external_id,registry_name,registry_url,serial,status,stolen,stolen_coordinates,stolen_location,
      thumb,title,url,year from (select * from new_data left anti join old_data on new_data.id = old_data.id)''')

    updated_records = spark.sql('''select new_data.date_stolen,new_data.description,new_data.frame_colors,new_data.frame_model,new_data.id,new_data.is_stock_img,new_data.large_img,
     new_data.location_found,new_data.manufacturer_name,new_data.external_id,new_data.registry_name,new_data.registry_url,new_data.serial,new_data.status,new_data.stolen,
      new_data.stolen_coordinates,new_data.stolen_location,new_data.thumb,new_data.title,new_data.url,new_data.year
       from new_data inner join old_data on new_data.id = old_data.id and new_data.status != old_data.status ''')

    df_new_records = df_new_records.union(updated_records)

    df_new_records.write.format("jdbc").option("url", postgres_db).option("dbtable", table).option("user", postgres_user).option("password", postgres_pwd).mode("append").save()

    spark.sql("select count(*) from new_data").show()
    


def display_data(spark):

    df = spark.read.format("jdbc").option("url", postgres_db).option("dbtable", table).option("user", postgres_user).option("password", postgres_pwd).load()
    
    df.createOrReplaceTempView("data")
    
    spark.sql("select count(*) from data ").show()

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

write_new_data(df,spark)

display_data(spark)

spark.stop()
