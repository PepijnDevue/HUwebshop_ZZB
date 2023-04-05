from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os, psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv

app = Flask(__name__)
api = Api(app)

# We define these variables to (optionally) connect to an external MongoDB
# instance.
envvals = ["MONGODBUSER","MONGODBPASSWORD","MONGODBSERVER"]
dbstring = 'mongodb+srv://{0}:{1}@{2}/test?retryWrites=true&w=majority'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the mongo connection to the database outside of the 
# Recom_mongo class.
load_dotenv()
if os.getenv(envvals[0]) is not None:
    envvals = list(map(lambda x: str(os.getenv(x)), envvals))
    client = MongoClient(dbstring.format(*envvals))
else:
    client = MongoClient()
database = client.huwebshop 

# Since we are asked to pass a Class rather than an instance of the class to the
# add_resource method, we open the postgre connection to the database outside of the
# Random_postgre class
password_file = open('password.txt')
password = password_file.readline()
db_name = 'huwebshop'
connection = psycopg2.connect(f'dbname={db_name} user=postgres password={password}')
cursor = connection.cursor()

class Recom_mongo(Resource):
    """ This class represents the REST API that provides the recommendations for
    the web shop. At the moment, the API simply returns a random set of products taken
    out of mongoDB to recommend."""

    def get(self, profile_id, count):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        rand_cursor = database.products.aggregate([{ '$sample': { 'size': count } }])
        prod_ids = list(map(lambda x: x['_id'], list(rand_cursor)))
        return prod_ids, 200
    
class Random_postgre(Resource):
    """This class represents the API that provides random recommendations from
    Postgre."""

    def get(self):
        """
        Take 4 random products from postgre
        and return a list of product_id's 

        Returns:
            tuple: returns a list of strings and an OK status code
        """
        # select the _id of 4 random products
        cursor.execute('select _id from product order by random() limit 4')
        # put the _id's in a list
        prod_ids = [i[0] for i in cursor.fetchall()]
        return prod_ids, 200
    
class Recom_product_page(Resource):
    """This class represents the API that provides a recommendations for the
    product page based on target_group"""

    def get(self, product_id):
        """
        get 4 recommendable products based on a given product id

        Args:
            product_id (str): The product on which the recommendation has to be based
        Return:
            A list of 4 product ids to recommend and status code OK
        """
        # list to return
        prod_ids = []

        # the tables to look at in order
        tables = ['series_products', 'sscat_products', 'brand_products', 'category_products', 'group_products']
        table_index = 0

        # the values of these columns for the given product
        cursor.execute('select series, sub_sub_category, brand, category, target_group from product where _id = %s', (product_id,))
        trait_vals = cursor.fetchall()[0]

        # the column names to retrieve recommendable products from
        traits = ['series', 'sub_sub_category', 'brand', 'category', 'target_group']

        # while not enough recommendations have been retrieved yet, keep repeating
        while len(prod_ids) < 4:
            # if the given product has the current trait
            if trait_vals[table_index] != None:
                # get the recommendable products that share the same trait
                cursor.execute(f"select product_ids from {tables[table_index]} where {traits[table_index]} = '{trait_vals[table_index]}'")
                fetch_list = cursor.fetchall()[0][0].split(', ')
                # dont recommend the product that is given
                if product_id in fetch_list:
                    fetch_list.remove(product_id)
                prod_ids.extend(fetch_list)
            # look for the next trait
            table_index += 1
            # if all traits are None, get products that are generally recommendable
            if table_index > 4:
                # get 5 products that are recommendable and on discount
                cursor.execute('select _id from product where recommendable = true and discount = true order by random() limit 5')
                fetch_list = [i[0] for i in cursor.fetchall()]

                # dont recommend the product that is given
                if product_id in fetch_list:
                    fetch_list.remove(product_id)
                prod_ids.extend(fetch_list)

        # return the first 4 products
        print(prod_ids)
        return(prod_ids[:4], 200)

    

# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom_mongo, "/<string:profile_id>/<int:count>")

# resources added by ZZB
api.add_resource(Random_postgre, '/zzb/rand_pg')
api.add_resource(Recom_product_page, '/zzb/product/<string:product_id>')