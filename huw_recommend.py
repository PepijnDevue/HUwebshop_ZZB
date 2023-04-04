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
    the webshop. At the moment, the API simply returns a random set of products taken
    out of mongoDB to recommend."""

    def get(self, profileid, count):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        randcursor = database.products.aggregate([{ '$sample': { 'size': count } }])
        prodids = list(map(lambda x: x['_id'], list(randcursor)))
        return prodids, 200
    
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
        Get 4 products from postgre to recommend based on the
        target_group of a given product

        Args:
            product_id (str): The given product to base recommendation on
        """
        # get the product group
        cursor.execute('select target_group from product where _id = %s', (product_id,))
        group = cursor.fetchall()[0][0]

        # if no group is know, recommend the generally most recommended products
        if group == None:
            cursor.execute('select product_id from most_recommended order by frequency desc limit 5')
            products = [i[0] for i in cursor.fetchall()]
        else:
            cursor.execute('select * from top_group_product where target_group = %s', (group,))
            products = list(cursor.fetchall()[0][1:6])

        # dont recommend the same product that is inputted
        if product_id in products:
            products.remove(product_id)

        prod_ids = products[:4]

        # return the first 4 products
        return(prod_ids, 200)

class Recom_subcategory(Resource):
    """This class represents the API that provides a recommendations for the
    shopping cart based on the profile"""

    def get(self, subcategory):
        """
        Get 4 products from postgre to recommend based on the
        profile id

        Args:
            profile_id (str): The given product to base recommendation on profile id

        Return: (Dave lees dit aub)
            Tuple with product id's and API response code
                example: return(prod_ids, 200)
        """
        """*** IN TE VULLEN DOOR DAVE***"""

class Recom_category(Resource):
    """This class represents the API that provides a recommendations for the
    shopping cart based on the profile"""

    def get(self, category):
        """
        Get 4 products from postgre to recommend based on the
        profile id

        Args:
            profile_id (str): The given product to base recommendation on profile id

        Return: (Dave lees dit aub)
            Tuple with product id's and API response code
                example: return(prod_ids, 200)
        """
        """*** IN TE VULLEN DOOR DAVE***"""

class Recom_shopping_cart(Resource):
    """This class represents the API that provides a recommendations for the
    shopping cart based on the profile"""

    def get(self, profile_id):
        """
        Get 4 products from postgre to recommend based on the
        profile id

        Args:
            subcategory (str): The given profile_id to base recommendation on profile_id

        Return: (Dave lees dit aub)
            Tuple with product id's and API response code
                example: return(prod_ids, 200)
        """
        """*** IN TE VULLEN DOOR DAVE***"""

    

# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom_mongo, "/<string:profileid>/<int:count>")

# resources added by ZZB
api.add_resource(Random_postgre, '/zzb/rand_pg')
api.add_resource(Recom_product_page, '/zzb/product/<string:product_id>')
api.add_resource(Recom_shopping_cart, '/zzb/winkelmand/<string:profile_id>')
api.add_resource(Recom_subcategory, '/zzb/winkelmand/<string:subcategory>')
api.add_resource(Recom_category, '/zzb/winkelmand/<string:category>')
