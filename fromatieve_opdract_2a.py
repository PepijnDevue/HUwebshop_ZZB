import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017')

db = client.huwebshop

products = db.products

product_zero = products.find_one()

print(f"Het eerste product is {product_zero['name']} en kost {product_zero['price']['selling_price']} cent")

cursor = products.find({'name': {'$regex': '^R'}})

print(f"Het eerste product dat begint met R is {cursor[0]['name']}")

total = 0
n = 0
for product in products.find():
    if 'price' in product:
        total += product['price']['selling_price']
    n += 1

print(f"De gemiddelde prijs is {round(total/n/100, 2)} euro")