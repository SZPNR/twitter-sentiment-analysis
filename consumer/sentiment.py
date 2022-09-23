import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import logging
import os

import os

SCALA_VERSION = '2.12'
SPARK_VERSION = '3.3.0'
MONGODB_VERSION = '3.6.8'

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.mongodb.spark:mongo-spark-connector_{SCALA_VERSION}:{MONGODB_VERSION} pyspark-shell'

import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


from pyspark.sql.types import TimestampType, StringType, StructType, StructField
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col


def upload_raw_to_mongodb(df_raw):
    df_raw.write \
    .format("mongo") \
    .option("spark.mongodb.conection.uri", "mongodb://127.0.0.1:27017") \
    .option("spark.mongodb.database", "twitter") \
    .option("spark.mongodb.collection", "streaming") \
    .save()

def preprocessing(lines):
    words = df1.select(explode(split(df1.text, " ")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    print(words)
    return words

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector-2.11-2.3.1") \
        .config("spark.jars", "/usr/local/postgresql-42.2.5.jar") \
        .master("local") \
        .getOrCreate()
    df_raw = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .load()
        

    upload_raw_to_mongodb(df_raw)

    mySchema = StructType([StructField("text", StringType(), True)])
    values = df_raw.select(from_json(df_raw.value.cast("string"), mySchema).alias("test"))
    df1 = values.select("test.*")
    words = preprocessing(df1)
    # text classification to define polarity and subjectivity
    words = text_classification(words)

    words = words.repartition(1)
    query = words.select("word","polarity","subjectivity").write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/twitter_streaming") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "twitter") \
    .option("user", "hduser").option("password", "bigdata").save()

    query.awaitTermination()



