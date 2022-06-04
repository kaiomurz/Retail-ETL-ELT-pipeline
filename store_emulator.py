
from random import choice, choices
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer
import time



#### Choices and weights
locations = ['London', 'Manchester', 'Edinburgh']
locations_weights = [0.5, 0.3, 0.2 ]

number_of_items = [1,2,3,4,5]
number_of_items_weights = [0.1, 0.4, 0.2, 0.2, 0.1]

flavours = ['Banana','Coffee','Chocolate','Lemon']
flavours_weights = [0.1, 0.3, 0.4, 0.2]

sizes = ['small', 'medium', 'large']
sizes_weights = [0.2, 0.6, 0.2]

quantities = [1,2,3,4,5]
quantities_weights = [0.1, 0.4, 0.2, 0.2, 0.1]

SLEEP_TIME = 1


def increment_time(transaction_time):
    """
    function takes last transaction time, adds a random interval  
    and returns next transaction time
    """
    increment = choice(range(60,300))
    return transaction_time + timedelta(seconds=increment)  

### Creating the producer and the receiver



transactions_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    client_id='Sales Transactions Producer',
    value_serializer=lambda transactions_message:json.dumps(transactions_message).encode('utf-8')
)

print('producer created')

transaction_time = datetime.now() #beginning transaction time




if __name__ == "__main__":
    for i in range(10):
            location = choices(locations, locations_weights)[0]
            
            items_number = choices(number_of_items, number_of_items_weights)

            items = []
            for j in range(items_number[0]):
                item = {
                        'flavour':choices(flavours, flavours_weights)[0],
                        'size': choices(sizes, sizes_weights)[0],
                        'quantity': choices(quantities, quantities_weights)[0]
                    }

                transaction_item = {
                    'transaction_no': i+1,            
                    'store': location,
                    'time': str(transaction_time)[:-7],
                    'item_no':j+1,
                    'item':item}
            
                transactions_producer.send(topic='SalesTransactions', value=transaction_item)
            
                print(transaction_item, type(transaction_item))
            transaction_time = increment_time(transaction_time)    

            
            time.sleep(SLEEP_TIME)

    
