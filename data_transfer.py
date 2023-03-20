# imports
import random
import postgre_functions
import mongo_functions
import price_functions
import fit_data_functions

def handle_2c_2(mongo_db):
    """
    Calculate the average price of all products

    mongo_db: the mongodb object to use for the database
    returns: None
    """
    products = mongo_functions.collect_product_data(mongo_db)
    price_avg = price_functions.get_avg_price(products)
    print(f"The average price of the products is {price_avg} euros")

def handle_2c_3(mongo_db):
    """
    Choose a random product, get it price and find the product which price deviates the most from it

    mongo_db:the mongodb object to use for the database
    returns: None
    """
    products = mongo_functions.collect_product_data(mongo_db)
    random_product = random.choice(products)
    max_deviated_product = price_functions.find_max_price_deviation_product(random_product, products)
    print(f"The product which price deviates the most from the product {random_product['name']} with the price {round(int(random_product['selling_price'])/100, 2)} is {max_deviated_product['name']} with a price of {round(int(max_deviated_product['selling_price'])/100, 2)} euros")
    

def handle_products(mongo_db, postgre_cursor, postgre_connection):
    """
    Get all useful product info and transfer it to the relational database

    mongo_db: the mongodb object to use for the database
    postgre_cursor: the cursor for the relational database
    postgre_connection: the connection object for the relational database
    returns: none
    """
    # get the mongodb cursor to retrieve data
    mongo_cursor = mongo_functions.get_products(mongo_db)

    done = False

    # transfer the products in batches to increase performance
    batch_size = 3000

    while not done:
        # get a batch of information from mongodb
        item_dicts, done = mongo_functions.batch_handler_products(batch_size, mongo_cursor)
        # manipulate the data so it fits in the relational database
        item_dicts = fit_data_functions.fit_product_data(item_dicts)
        # transfer the data to the relational database
        postgre_functions.products_to_postgre(postgre_cursor, item_dicts, postgre_connection)

def handle_profiles(mongo_db, postgre_cursor, postgre_connection):
    """
    Get all useful profile info and transfer it to the relational database

    mongo_db: the mongodb object to use for the database
    postgre_cursor: the cursor for the relational database
    postgre_connection: the connection object for the relational database
    returns: none
    """
    # get the mongodb cursor to retrieve data
    mongo_cursor = mongo_functions.get_profiles(mongo_db)

    done = False

    # transfer the profiles in batches to increase performance
    batch_size = 20000

    while not done:
        # get a batch of information from mongodb
        item_dicts, done = mongo_functions.batch_handler_profiles(batch_size, mongo_cursor)
        # transfer the data to the relational database
        postgre_functions.profiles_to_postgre(postgre_cursor, item_dicts, postgre_connection)

def handle_sessions(mongo_db, postgre_cursor, postgre_connection):
    """
    Get all useful session info and transfer it to the relational database

    mongo_db: the mongodb object to use for the database
    postgre_cursor: the cursor for the relational database
    postgre_connection: the connection object for the relational database
    returns: none
    """
    # get the mongodb cursor to retrieve data
    mongo_cursor = mongo_functions.get_sessions(mongo_db)

    done = False

    # transfer the sessions in batches to increase performance
    batch_size = 50000

    while not done:
        # get a batch of information from mongodb
        item_dicts, done = mongo_functions.batch_handler_sessions(batch_size, mongo_cursor)
        # manipulate the data so it fits in the relational database
        item_dicts = fit_data_functions.fit_session_data(item_dicts)
        # transfer the data to the relational database
        postgre_functions.sessions_to_postgre(postgre_cursor, item_dicts, postgre_connection)

if __name__ == "__main__":
    # Connect to mongoDB
    mongo_db = mongo_functions.open_mongodb()

    # Connect to postgreDB
    postgre_cursor, postgre_connection = postgre_functions.open_postgre()

    # Calculate average price
    # handle_2c_2(mongo_db)

    # Find product which price deviates most from random given product
    # handle_2c_3(mongo_db)

    # Transfer products
    # handle_products(mongo_db, postgre_cursor, postgre_connection)

    # Transfer profiles
    handle_profiles(mongo_db, postgre_cursor, postgre_connection)

    # Transfer sessions
    # handle_sessions(mongo_db, postgre_cursor, postgre_connection)

    # Save data manipulation in postgre and close postgreDB
    postgre_functions.close_postgre(postgre_cursor, postgre_connection)  