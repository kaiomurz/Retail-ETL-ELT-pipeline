# this script merely for checking whether the messages from the script
# streaming_consumer_processor.py are being correctly sent into the 
# 'ProcessedTransactions' topic in kafka.

from kafka import KafkaConsumer
import json
transactions_consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message:json.loads(message),
    auto_offset_reset='latest'
)

transactions_consumer.subscribe(topics=['ProcessedTransactions'])
print('consumer created')

for message in transactions_consumer:
    print(message)