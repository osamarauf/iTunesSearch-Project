pyspark --jars /root/spark-sql-kafka-0-10_2.11-2.3.1.jar,/usr/hdp/current/kafka-broker/libs/kafka-clients-1.1.1.3.0.1.0-187.jar

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import requests
from time import sleep
from confluent_kafka import Producer
from socket import gethostname
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col,from_json,json_tuple
import pyspark.sql.functions as psf
from pyspark.sql.types import *

schema=StructType([StructField("resultCount",LongType(),True),
StructField("results",ArrayType(StructType([StructField("artistId",LongType(),True),
StructField("artistName",StringType(),True),
StructField("artistViewUrl",StringType(),True),
StructField("artworkUrl100",StringType(),True),
StructField("artworkUrl30",StringType(),True),
StructField("artworkUrl60",StringType(),True),
StructField("collectionArtistId",LongType(),True),
StructField("collectionArtistName",StringType(),True),
StructField("collectionCensoredName",StringType(),True),
StructField("collectionExplicitness",StringType(),True),
StructField("collectionId",LongType(),True),
StructField("collectionName",StringType(),True),
StructField("collectionPrice",DoubleType(),True),
StructField("collectionViewUrl",StringType(),True),
StructField("contentAdvisoryRating",StringType(),True),
StructField("copyright",StringType(),True),
StructField("country",StringType(),True),
StructField("currency",StringType(),True),
StructField("description",StringType(),True),
StructField("discCount",LongType(),True),
StructField("discNumber",LongType(),True),
StructField("isStreamable",BooleanType(),True),
StructField("kind",StringType(),True),
StructField("previewUrl",StringType(),True),
StructField("primaryGenreName",StringType(),True),
StructField("releaseDate",StringType(),True),
StructField("trackCensoredName",StringType(),True),
StructField("trackCount",LongType(),True),
StructField("trackExplicitness",StringType(),True),
StructField("trackId",LongType(),True),
StructField("trackName",StringType(),True),
StructField("trackNumber",LongType(),True),
StructField("trackPrice",DoubleType(),True),
StructField("trackTimeMillis",LongType(),True),
StructField("trackViewUrl",StringType(),True),
StructField("wrapperType",StringType(),True)]),True),True)])

df1 = spark.readStream.format("kafka").option('kafka.bootstrap.servers', 'sandbox-hdp:6667').option('subscribe', 'itunes_search').option('startingOffsets', 'earliest').load()

ds = df1.select(col('value').cast('string')).select(from_json('value',schema).alias('json_data')).select(explode('json_data.results')).writeStream.format("memory").queryName("search").start()

>>> spark.sql("select * from search").show()

record_by_AlanCross = spark.sql("SELECT col.* From search").where(col("artistName") == "Alan Cross")
record_by_AlanCross.show()


record_by_AlanCross.write \
    .option("header", True) \
    .option("delimiter",",") \
    .json("/user/root/record1")
Name = spark.read.json('/user/root/record1/part-00000-be27ad89-b596-403a-b4c9-3d82df2e4b83-c000.json')
                                            OR
hdfs dfs -cat /user/root/record1/part-00000-be27ad89-b596-403a-b4c9-3d82df2e4b83-c000.json



    
record_by_AlanJackson = spark.sql("SELECT col.* From search").where(col("artistName") == "Alan Jackson")
record_by_AlanJackson.show()


record_by_AlanJackson.write \
    .option("header", True) \
    .option("delimiter",",") \
    .json("/user/root/record2")