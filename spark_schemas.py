
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType, DateType


# transaction ingestion schema
transactionSchema = StructType([
    StructField("transaction_no", IntegerType(), True),
    StructField("store", StringType(), True),
    StructField("time", DateType(), True),
    StructField("item_no", IntegerType(), True),
    StructField("item", MapType(StringType(), StringType()), True),
])