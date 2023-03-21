# imports
import handle_functions
import postgre_functions
import mongo_functions

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
    handle_functions.handle_products(mongo_db, postgre_cursor, postgre_connection)

    # Transfer profiles
    handle_functions.handle_profiles(mongo_db, postgre_cursor)

    # Transfer sessions
    handle_functions.handle_sessions(mongo_db, postgre_cursor)

    # Save data manipulation in postgre and close postgreDB
    postgre_functions.close_postgre(postgre_cursor, postgre_connection)  