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
    """This class represents the API that provides recommendations for the
    product page"""

    def get(self, product_id):
        """
        Get 4 products from the PostgreSQL database to recommend based on the
        product page using content filtering.
        Look at the "Adviesrapport" for further explanation for this filter.

        Args:
            product_id (str): the product id to get recommendations for.

        Returns:
            Tuple with product id's and API response code
                example: (['32522', '6910', '2537', '5844'], 200)
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
                # remove duplicates
                prod_ids = list(dict.fromkeys(prod_ids))
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
                # remove duplicates
                prod_ids = list(dict.fromkeys(prod_ids))

        # return the first 4 products
        return(prod_ids[:4], 200)

class Recom_subcategory(Resource):
    """This class represents the API that provides a recommendations for the
    subcategory page based on the category"""
    encode_dict = {
        'lichaamsverzorging':'Lichaamsverzorging',
        'persoonlijke-hygiene':'Persoonlijke hygiene',
        'mini-reisverpakkingen':'Mini reisverpakkingen',
        'haarverzorging':'Haarverzorging',
        'mondverzorging':'Mondverzorging',
        'scheren-en-ontharen':'Scheren & ontharen',
        'gezichtsverzorging-man':'Gezichtsverzorging man',
        'gezichtsverzorging-vrouw':'Gezichstverzorging vrouw',
        'optiek':'Optiek',
        'wondverzorging':'Wondverzorging',
        'seksualiteit':'Seksualiteit',
        'vitaminen-en-supplementen':'Vitaminen en supplementen',
        'geneesmiddelen':'Geneesmiddelen',
        'afslanken':'Afslanken',
        'voetverzorging':'Voetverzorging',
        'sportvoeding':'Sportvoeding',
        'zwangerschap':'Zwangerschap',
        'haaraccessoires':'Haaraccessoires',
        'gehoorbescherming':'Gehoorbescherming',
        'toilet-en-keuken':'Toilet en keuken',
        'wassen-en-schoonmaken':'Wassen en schoonmaken',
        'overig-huishoudelijk':'Overig huishoudelijk',
        'dierverzorging':'Dierverzorging',
        'outdoor-en-vrij-tijd':'Outdoor en vrije tijd',
        'woonaccessoires':'Woonaccessoires',
        'feestartikelen':'Feestartikelen',
        'seizoenen':'Seizoenen',
        'tuinartikelen':'Tuinartikelen',
        'boeken-en-tijdschriften':'Boeken & tijdschriften',
        'kantoor-benodigdheden':'Kantoor benodigdheden',
        'knutselen-en-hobby':'Knutselen en hobby',
        'muziek':'Muziek',
        'films':'Films',
        'wonen':'Wonen',
        'gamen':'Gamen',
        'dames':'Dames',
        'heren':'Heren',
        'kleding-accessoires':'Kleding accessoires',
        'babys-en-kinderen':"Baby's en kinderen",
        'sieraden-en-bijoux':'Sieraden & bijoux',
        'make-up-accessoires':'Make-up accessoires',
        'geuren-en-geschenkset':'Geuren en geschenkset',
        'make-up':'Make-up',
        'luiers-en-verschonen':'Luiers en verschonen',
        'babyverzorging':'Babyverzorging',
        'baby-accessoires':'Baby accessoires',
        'speelgoed':'Speelgoed',
        'babyvoeding':'Babyvoeding',
        'koude-dranken':'Koude dranken',
        'snacks-en-snoep':'Snacks en snoep',
        'koffie-en-thee':'Koffie en thee',
        'elektonica-en-media':'Elektronica & media'
    }

    def get(self, subcategory):
        """
        Get 4 products from the PostgreSQL database to recommend based on the
        subcategory using content filtering.
        Look at the "Adviesrapport" for further explanation for this filter.

        Args:
            subcategory (str): the subcategory name to get recommendations for.

        Returns:
            Tuple with product id's and API response code
                example: (['32522', '6910', '2537', '5844'], 200)
        """
        # Here we make the query
        subcategory = self.encode_dict[subcategory]
        query = """SELECT rec1_product_id,rec2_product_id,rec3_product_id,rec4_product_id 
                   FROM sub_category_recommendation 
                   WHERE sub_category = %s;
                """
        
        # Here we fetch the result from the query
        cursor.execute(query,(subcategory,))

        # Here we fetch the result from the above cursor execute.
        result = cursor.fetchall()

        # Products ids are being put in a list instead of the tuple they come in when you use fetch
        product_ids = [product_id for product_id in result[0]]

        
        # Returns the product_ids and a api response code inside a tuple
        return(product_ids,200)

class Recom_category(Resource):
    """This class represents the API that provides a recommendations for the
    category page based on the category"""
    encode_dict = {
        'gezond-en-verzorging':"Gezond & verzorging",
        'huishouden':"Huishouden",
        'wonen-en-vrije-tijd':"Wonen & vrije tijd",
        'kleding-en-sieraden':"Kleding & sieraden",
        'make-up-en-geuren':"Make-up & geuren",
        'baby-en-kind':"Baby & kind",
        'eten-en-drinken':"Eten & drinken",
        'elektronica-en-media':"Elektronica & media",
        'opruiming':"Opruiming",
        'black-friday':"Black Friday",
        'cadeau-ideeen':"Cadeau ideeën",
        'op-is-opruiming':"op=opruiming",
        '50-procent-korting':"50% korting",
        'nieuw':"Nieuw",
        'extra-deals':"Extra Deals",
        'folder-artikelen':"Folder artikelen"
    }

    def get(self, category):
        """
        Get 4 products from the PostgreSQL database to recommend based on the
        category using content filtering.
        Look at the "Adviesrapport" for further explanation for this filter.

        Args:
            category (str): the category name to get recommendations for.

        Returns:
            Tuple with product id's and API response code
                example: (['32522', '6910', '2537', '5844'], 200)
        """

        # Here we make the query
        category = self.encode_dict[category]
        query = """SELECT rec1_product_id,rec2_product_id,rec3_product_id,rec4_product_id 
                   FROM category_recommendation 
                   WHERE category = %s;
                """
        
        # Here we fetch the result from the query
        cursor.execute(query,(category,))

        # Here we fetch the result from the above cursor execute.
        result = cursor.fetchall()

        # Products ids are being put in a list instead of the tuple they come in when you use fetch
        product_ids = [product_id for product_id in result[0]]

        
        # Returns the product_ids and a api response code inside a tuple
        return(product_ids,200)

class Recom_shopping_cart(Resource):
    """This class represents the API that provides a recommendations for the
    shopping cart based on the profile"""

    def get(self, profile_id):
        """
        Get 4 products from the PostgreSQL database to recommend based on the
        profile id using collaborative filtering.
        Look at the "Adviesrapport" for further explanation for this filter.

        Args:
            profile_id (str): the profile id to get recommendations for

        Returns:
            Tuple with product id's and API response code
                example: (['32522', '6910', '2537', '5844'], 200)
        """
        # Query that gets 4 product_ids from the data base out of the table profile_recommendation based on the profile_id
        query = """SELECT rec1_product_id,rec2_product_id,rec3_product_id,rec4_product_id 
                    FROM profile_recommendation 
                    WHERE profile_id = %s;
                """
        # Here we execute the query with the value of the profile_id
        cursor.execute(query,(profile_id,))
        # Here we fetch the result from the above cursor execute.
        result = cursor.fetchall()
        
        # Here we check if the result is empty.
        # If its not we continue with the product ids that have been fetched from the query.
        if len(result) > 0:  
        # Products ids are being put in a list instead of the tuple they come in when you use fetch 
            product_ids = [product_id for product_id in result[0]]

        # If the list is empty we recommend 4 random products
        else:
            cursor.execute("SELECT _id FROM product WHERE recommendable = true AND discount = true ORDER BY random() limit 4")
            result = cursor.fetchall()
            # Products ids are being put in a list instead of the tuple they come in when you use fetch 
            product_ids = [product_id[0] for product_id in result]
        
        # Returns the product_ids and a api response code inside a tuple
        return(product_ids,200)

# resources added by ZZB
api.add_resource(Random_postgre, '/zzb/rand_pg')
api.add_resource(Recom_product_page, '/zzb/product/<string:product_id>')
api.add_resource(Recom_shopping_cart, '/zzb/winkelmand/<string:profile_id>')
api.add_resource(Recom_subcategory, '/zzb/subcategory/<string:subcategory>')
api.add_resource(Recom_category, '/zzb/category/<string:category>')
