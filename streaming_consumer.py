from kafka import KafkaConsumer
import json
faust_transactions_consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda message:json.loads(message),
    auto_offset_reset='latest'
)

faust_transactions_consumer.subscribe(topics=['SalesTransactions'])
print('consumer created')

for message in faust_transactions_consumer:
    print(message)