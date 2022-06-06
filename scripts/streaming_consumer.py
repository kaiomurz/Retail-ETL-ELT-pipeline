# This script consumes data from the SalesTransactions topic and processes it in
# order to serve it back to another kafka topic. It receives messages in a format

# that looks like this:
# ConsumerRecord(topic='ProcessedTransactions', partition=1, offset=30, 
    # timestamp=1654458229256, timestamp_type=0, key=None, value={'transaction_no': 5, 
    # 'store': 'London', 'time': '2022-06-05 20:54:21', 'item_no': 1, 'item': 
    # {'flavour': 'Lemon', 'size': 'medium', 'quantity': 2}}, headers=[], checksum=None,
    # serialized_key_size=-1, serialized_value_size=148, serialized_header_size=-1)

# and outputs data formatted like this: 
# Batch: 1
# -------------------------------------------
# +------+---------+--------+-------+-----+
# |txn_no|  flavour|quantity|variant|price|
# +------+---------+--------+-------+-----+
# |     1|   Coffee|       2| medium|  1.5|
# |     1|Chocolate|       4| medium|  1.5|
# |     1|Chocolate|       2|  large|  2.0|
# +------+---------+--------+-------+-----+


import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

# Submit spark sql kakfa package from Maven repository to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'
# define input and output topics
kafka_input_topic = "SalesTransactions"
kafka_output_topic = "ProcessedTransactions"

# Specify Kafka server to read data from
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .getOrCreate()

# Only display Error messages in the console
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_input_topic) \
        .option("startingOffsets", "latest") \
        .load()

# Spark user-defined functions for processing data
@udf
def transaction(x):
    x = json.loads(x)
    return x['transaction_no']
@udf
def item(x):
    x = json.loads(x)
    return x['item']

@udf
def flavour(x):
    return x['flavour']

@udf
def variant(x):
    return x['size']

@udf
def quantity(x):
    return x['quantity']

@udf
def price(x):
    price_dict = {
    'large': 2.0,
    'medium': 1.5,
    'small': 1.0
    }
    return price_dict[x]

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)") 

# process message and populate output dataframe

# extract the 'transaction_no' field and  'item' dict 
stream_df = stream_df\
    .withColumn("txn_no", transaction("value"))\
    .withColumn("item", item("value")) 

# extract various features from 'item' dict
stream_df = stream_df\
    .withColumn("flavour", flavour("item"))\
    .withColumn("quantity", quantity("item"))\
    .withColumn("variant", variant("item"))

# add price column
stream_df = stream_df.withColumn("price", price('variant'))

stream_df = stream_df\
    .drop("item")

   
# write prepared data frame to the output topic which can be viewed 
# by running the script 'processed_stream_consumer.py'
stream_df.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("checkpointLocation", os.getcwd())\
    .option("topic", kafka_output_topic) \
    .start() \
    .awaitTermination()

# # outputting the messages to the console 
# stream_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start() \
#     .awaitTermination()