# import mongodb tool for python
import pymongo

# create a connection with the client
client = pymongo.MongoClient('mongodb://localhost:27017')

# create a connection with the database
db = client.huwebshop

# create an object for the collection products
products = db.products

# get the first record of the collection
product_zero = products.find_one()

# print the name and the sellingprice of the product
print(f"Het eerste product is {product_zero['name']} en kost {product_zero['price']['selling_price']} cent")

# get a group of products where the name starts with 'R'
cursor = products.find({'name': {'$regex': '^R'}})

# print the name of the first product in the group
print(f"Het eerste product dat begint met R is {cursor[0]['name']}")

# create a group query for the aggregation
query = [
    {'$group': 
        {'_id': None, 
         'avg_price': 
            {'$avg': '$price.selling_price'}
        }
    }]

# use aggregate to gather the data
result = products.aggregate(query)

# calculate the price in euros
average_price = round(next(result)['avg_price'] / 100, 2)

print(f"De gemiddelde prijs is {average_price} euro")