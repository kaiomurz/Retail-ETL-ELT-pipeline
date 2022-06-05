# this script receives messages from the 'SalesTransactions' kafka topic, 
# prepares the json for the transaction and uploads it to the S3 data lake.

from kafka import KafkaConsumer
import json
from secrets import aws_access_key_id, aws_secret_access_key
import boto3

#connect to s3
s3 = boto3.resource(
        's3', 
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
        )


# create consumer and subscribe to topic
transactions_batch_consumer = KafkaConsumer(### change consumer name
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message:json.loads(message),
    auto_offset_reset='latest'
)


transactions_batch_consumer.subscribe(topics=['SalesTransactions']) 
print('consumer created')

count = 0
for message in transactions_batch_consumer:
    count+=1

    message_value = message.value
    file_name = f"unprocessed/{message_value['transaction_no']}_{message_value['item_no']}_txn.json"
    # file_name = str(count)+'_txn.json'
    s3object = s3.Object('retail-data-lake',file_name)
    s3object.put(
        Body=(bytes(json.dumps(message.value).encode('UTF-8')))
    )
    print('uploaded to s3')
    print(type(message.value))
    print(message.value)
