from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Initializing Spark session/application
spark = SparkSession \
       .builder \
       .appName("Kafka-to-local") \
       .getOrCreate()

#Reading Streaming data from de-capstone3 kafka topic
df = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
       .option("startingOffsets", "earliest") \
       .option("subscribe", "de-capstone3") \
       .load()

df= df \
      .withColumn('value_str',df['value'].cast('string').alias('key_str')).drop('value') \
      .drop('key','topic','partition','offset','timestamp','timestampType')

#Writing data from kakfa to Hadoop
df.writeStream \
  .format("json") \
  .outputMode("append") \
  .option("path", "/user/root/clickstream_data_dump") \
  .option("checkpointLocation", "/user/root/clickstream_data_dump_cp") \
  .start() \
  .awaitTermination()
