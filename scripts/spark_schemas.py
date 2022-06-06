# This is not a working script. It just contains the schema to be used by 
# batch_pricessing.py for ingesting the transaction json files from the 
# S3 data lake.

from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType, DateType


# transaction ingestion schema
transactionSchema = StructType([
    StructField("transaction_no", IntegerType(), True),
    StructField("store", StringType(), True),
    StructField("time", DateType(), True),
    StructField("item_no", IntegerType(), True),
    StructField("item", MapType(StringType(), StringType()), True),
])