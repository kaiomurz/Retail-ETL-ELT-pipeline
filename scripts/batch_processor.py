# This script ingests data from an S3 data lake in the form of json files with 
# this format: 
# {'transaction_no': 12, 'store': 'London', 'time': '2022-06-05 21:44:51', 'item_no': 1,\
#  'item': {'flavour': 'Lemon', 'size': 'large', 'quantity': 4}}

# It processes the data to create a dataframe that looks like this, and one which has a 
# schema that matches the fact table in the PostgreSQL data warehouse, and upserts it into 
# the fact table.

    # |transaction_no|item_no|store_key|datetime_key|product_key|quantity|price|
    # +--------------+-------+---------+------------+-----------+--------+-----+
    # |            10|      1|        1|  2022060315|          7|       2|    2|
    # |            10|      2|        1|  2022060315|          4|       2|    2|
    # |             1|      1|        1|  2022060521|          5|       4|    1|
    # |             1|      2|        1|  2022060521|          7|       1|    2|
    # |             1|      3|        1|  2022060521|          6|       3|    1|
    # +--------------+-------+---------+------------+-----------+--------+-----+

# the data warehouse contains the necessary dim tables that connect to the various keys.
# the ERD of the data warehouse is available in the README.md file. 

import json

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from sqlalchemy import create_engine

import boto3

import spark_schemas as sch
from secrets import postgres_user_name, postgres_password, aws_access_key_id, aws_secret_access_key

####### Functions #######

def get_postgres_table(server: str,
                       table: str,
                       postgres_user_name: str, 
                       postgres_password: str) -> SparkDataFrame:
    """
    Retrieves table from PostgreSQL server and returns it as a 
    PySpark Dataframe
    """
    
    return spark.read.format("jdbc")\
    .option("driver", "org.postgresql.Driver")\
    .option("url", f"jdbc:postgresql://localhost:5432/{server}")\
    .option("dbtable", table)\
    .option("user", postgres_user_name).option("password", postgres_password).load()

def ingest_s3_json_to_df(
    spark: SparkSession,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    bucket: str,
    prefix: str,
    encoding: str = 'utf-8'
) -> SparkDataFrame:
    """
    ingests json files from a specified bucket and folder and returns 
    a PySpark dataframe created with the list of json files.    

    """

    s3 = boto3.resource(
            's3', 
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
            )

    bucket = s3.Bucket(bucket)
    transactions_list = []

    for obj in bucket.objects.filter(Prefix=prefix):
        response = bucket.Object(obj.key)
        response_json = response.get()['Body'].read().decode(encoding)

        transactions_list.append(json.loads(response_json))

    return spark.createDataFrame(transactions_list)


def get_dicts():
    pg_products_df = get_postgres_table('Cupcakes', 'products', postgres_user_name, postgres_password)
    product_dict =  {(p["flavour"], p["variant"].strip()):p["product_key"]  for p in pg_products_df.collect()}

    price_dict = {
    'large': 2.0,
    'medium': 1.5,
    'small': 1.0
    }

    pg_stores_df = get_postgres_table('Cupcakes', 'stores', postgres_user_name, postgres_password)
    stores_dict = {s["city"]:s["store_key"] for s in pg_stores_df.collect()}

    return product_dict, price_dict, stores_dict





# datetime table
# fact table data frame

#### Main ####
# create Spark session
spark = SparkSession.builder\
.config("spark.jars", "postgresql-42.3.6.jar")\
.getOrCreate()



# ingest unprocessed data from s3 into dataframe for a specific date

transactions_df = ingest_s3_json_to_df(
    spark,
    aws_access_key_id,
    aws_secret_access_key,
    bucket='retail-data-lake',
    prefix='unprocessed/',
)


# extract values from item map into separate columns
transactions_df = transactions_df\
.withColumn("flavour", transactions_df.item.getItem("flavour"))\
.withColumn("variant", transactions_df.item.getItem("size"))\
.withColumn("quantity", transactions_df.item.getItem("quantity"))


# create date_time_key column
transactions_df.createOrReplaceTempView("transactionsTable")

transactions_df = spark.sql("""

select *, 
    concat(
    substring(cast(time as string),1,4),
    substring(cast(time as string),6,2),
    substring(cast(time as string),9,2),
    substring(cast(time as string),12,2)
    ) 
    as datetime_key
from transactionsTable
""")


product_dict, price_dict, stores_dict = get_dicts()

@udf
def get_product_key(x,y):    
    return product_dict[(x.lower(), y.lower())]
@udf
def get_price(x):    
    return price_dict[x]
@udf
def get_store_key(x):
    return stores_dict[x]

# add key columns to and delete redunt columns from df 
transactions_df = \
    transactions_df\
    .withColumn('product_key', get_product_key("flavour", "variant"))\
    .withColumn('price', get_price("variant"))\
    .withColumn('store_key', get_store_key("store"))\
    .drop(
        "row_id",
        "item",
        "store",
        "time",
        "flavour",
        "variant"
        )

transactions_df = transactions_df\
    .withColumn("store_key", transactions_df["store_key"].cast(IntegerType()))\
    .withColumn("product_key", transactions_df["product_key"].cast(IntegerType()))\
    .withColumn("price", transactions_df["price"].cast(IntegerType()))\
    .withColumn("quantity", transactions_df["quantity"].cast(IntegerType()))
    


# reorder columns according to PostgreSQL schema
transactions_df = transactions_df.select("transaction_no", "item_no","store_key", "datetime_key", "product_key", "quantity", "price")

# update fact table on PostgreSQL

server = "Cupcakes"
table = "temp_transactions"

engine = create_engine(f'postgresql://{postgres_user_name}:{postgres_password}@localhost:5432/Cupcakes')
con = engine.connect() 

con.execute("""
    drop table if exists temp_transactions
""")

transactions_df\
    .write\
    .format("jdbc")\
    .option("driver", "org.postgresql.Driver")\
    .option("url", f"jdbc:postgresql://localhost:5432/{server}")\
    .option("dbtable", "temp_transactions")\
    .option("user", postgres_user_name).option("password", postgres_password).save()



con.execute("""
    insert into transactions
    select * from temp_transactions
""")

#drop temp_transactions
con.execute("""
    drop table temp_transactions
""")

transactions_df.show(5)

# create date_time data frame using imported schema
# populate it with all the hours of that date
# upsert into date_time dim table

# move unprocessed files to processed
