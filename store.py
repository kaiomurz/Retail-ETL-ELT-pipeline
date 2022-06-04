from random import choices
from datetime import datetime, timedelta
import json

# transaction_time = 'x'

class Store:
    def __init__(self, location, transactions_producer):
        self.location = location
        self.transactions_producer = transactions_producer
    
        self.flavours = ['Banana','Coffee','Chocolate','Lemon']
        self.flavours_weights = [0.1, 0.3, 0.4, 0.2]

        self.sizes = ['small', 'medium', 'large']
        self.sizes_weights = [0.2, 0.6, 0.2]

        self.number_of_items = [1,2,3,4,5]
        self.number_of_items_weights = [0.1, 0.4, 0.2, 0.2, 0.1]

        self.quantities = [1,2,3,4,5]
        self.quantities_weights = [0.1, 0.4, 0.2, 0.2, 0.1]

    def generate_transaction(self, transaction_time):
        print("xxxx", transaction_time)
        items_number = choices(self.number_of_items, self.number_of_items_weights)

        items = []
        for i in range(items_number[0]):    
            item = {
                'flavour':choices(self.flavours, self.flavours_weights)[0],
                'size': choices(self.sizes, self.sizes_weights)[0],
                'quantity': choices(self.quantities, self.quantities_weights)[0]
            }
            items.append(item)

        self.transaction = {
            'store': self.location,
            'time': str(transaction_time)[:-7],
            'items':items}
        return self.transaction


    def publish_transaction(self, transaction_time):
        transaction = self.generate_transaction(transaction_time)
        # print(transaction_time)
        # transaction = "test 2"
        print(transaction, type(transaction))
        
        self.transactions_producer.send(topic='SalesTransactions', value={transaction})
        print(self.transactions_producer)
  
        
#instantiate stores as dict of {location:store}
#instantiate producer instances as dict of {location:producer_instance}




# store has methods
#   generate transaction
#   send to topic
# pick time delta and store by ran



#infer price in stream
