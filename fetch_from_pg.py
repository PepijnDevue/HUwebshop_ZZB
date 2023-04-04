# imports
import postgre_functions
import fetch_product_functions

if __name__ == '__main__':
    # Connect to postgreDB
    cursor, connection = postgre_functions.open_postgre()

    input_product = '7225'
    content_filter_products = fetch_product_functions.content_filter(input_product, cursor)

    input_profile = '5a393d68ed295900010384ca'
    collaborative_filter_products = fetch_product_functions.collaborative_filter(input_profile, cursor)

    # save changes and close postgre connection safely
    postgre_functions.close_postgre(cursor, connection)