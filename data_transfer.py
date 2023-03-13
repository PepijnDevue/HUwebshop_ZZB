# imports
import random
import postgre_functions
import mongo_functions
import price_functions
import fit_data_functions

if __name__ == "__main__":
    # get products
    mongo_db = mongo_functions.open_mongodb()
    products = mongo_functions.collect_product_data(mongo_db)

    # 2c.2
    price_avg = price_functions.get_avg_price(products)
    print(f"The average price of the products is {price_avg} euros")

    # 2c.3
    random_product = random.choice(products)
    max_deviated_product = price_functions.find_max_price_deviation_product(random_product, products)
    print(f"The product which price deviates the most from the product {random_product['name']} with the price {round(int(random_product['selling_price'])/100, 2)} is {max_deviated_product['name']} with a price of {round(int(max_deviated_product['selling_price'])/100, 2)}")
    
    # 2c. 1
    prostgre_cursor, postgre_connection = postgre_functions.open_postgre()
    products = fit_data_functions.fit_product_data(products)
    postgre_functions.products_to_postgre(prostgre_cursor, products)
    postgre_functions.close_postgre(prostgre_cursor, postgre_connection)