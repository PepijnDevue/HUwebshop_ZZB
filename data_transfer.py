# imports
import random
import postgre_functions
import mongo_functions
import price_functions
import fit_data_functions

if __name__ == "__main__":
    # Connect to mongoDB
    mongo_db = mongo_functions.open_mongodb()

    # Get product data
    # products = mongo_functions.collect_product_data(mongo_db)

    # Get profile data
    # profiles = mongo_functions.collect_profile_data(mongo_db)

    # Get session data

    # Calculate average price (2c.2)
    # price_avg = price_functions.get_avg_price(products)
    # print(f"The average price of the products is {price_avg} euros")

    # Find product which price deviates most from random given product (2c.3)
    # random_product = random.choice(products)
    # max_deviated_product = price_functions.find_max_price_deviation_product(random_product, products)
    # print(f"The product which price deviates the most from the product {random_product['name']} with the price {round(int(random_product['selling_price'])/100, 2)} is {max_deviated_product['name']} with a price of {round(int(max_deviated_product['selling_price'])/100, 2)}")
    
    # Connect to postgreDB
    prostgre_cursor, postgre_connection = postgre_functions.open_postgre()

    # Transfer product data to postgreDB (2c. 1)
    # products = fit_data_functions.fit_product_data(products)
    # postgre_functions.products_to_postgre(prostgre_cursor, products)

    # Transfer profile data to postgreDB (profile data already fit for relational database)
    # postgre_functions.profiles_to_postgre(prostgre_cursor, profiles)

    # Transfer session data to postgreDB

    # Save data manipulation in postgre and close postgreDB
    postgre_functions.close_postgre(prostgre_cursor, postgre_connection)