# import mongodb tool for python
import pymongo
import os
import time

# create a connection with the client
client = pymongo.MongoClient('mongodb://localhost:27017')

# create a connection with the database
db = client.huwebshop

# create an object for the collection products
products = db.products

pipeline = [
    {"$match": {"color": {"$ne": None}}}
]

# Execute the query pipeline and print the results
results = products.aggregate(pipeline)
for doc in results:
    print(doc)

input('nextL')

for i in products.find():
    os.system('CLS')
    for j in i:
        print(j)
    input('next: ')
print('end')