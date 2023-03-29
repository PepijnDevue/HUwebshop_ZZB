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

    def get(self, count):
        """
        Take an amount (equal to count) of random products from postgre
        and return a list of product_id's 

        Args:
            count (int): The amount of product id's returned

        Returns:
            tuple: returns a list of strings and an OK statuscode
        """
        # select the _id of an amount of random products where amount is equal to count
        cursor.execute('select _id from product order by random() limit %s', (count,))
        # put the _id's in a list
        prod_ids = [i[0] for i in cursor.fetchall()]
        return prod_ids, 200

    

# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom_mongo, "/<string:profileid>/<int:count>")
api.add_resource(Random_postgre, '/zzb/rand_pg/<int:count>')