
from cmath import log
from random import choice, choices
from functools import reduce
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
import time
from store import Store#, transaction_time
import logging

# logging.basicConfig(
#     # filename=log_log,
#     # filemode='w',
#     level=logging.DEBUG,
#     format='%(levelname)s: %(message)s'
#     )


  

### Creating the producer and the receiver

def increment_time(transaction_time):
    increment = choice(range(60,300))
    return transaction_time + timedelta(seconds=increment)

# transaction_message = "test"
transactions_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    client_id='Sales Transactions Producer',
    value_serializer=lambda transactions_message:json.dumps(transactions_message).encode('utf-8')
)


# logging.info('producer created')
print('producer created')


# logging.info('consumer created')

#### Driver code that calls the Class
locations = ['London', 'Paris', 'Rome']
locations_weights = [0.5, 0.3, 0.2 ]

transaction_time = datetime.now()
print(transaction_time)

#instantiate stores_dict
stores_dict = {}
for location in locations:
    stores_dict[location] = Store(location, transactions_producer)


for _ in range(10):
    location = choices(locations, locations_weights)[0]
#     transaction = stores_dict[location].generate_transaction()# change to send_transaction_to_topic
    stores_dict[location].publish_transaction(transaction_time)
    
#     print(transaction_time, transaction, "\n")
    transaction_time = increment_time(transaction_time)
#     print(transaction, "\n")

   