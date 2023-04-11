# imports
import handle_functions
import postgre_functions
import mongo_functions

if __name__ == "__main__":
    # Connect to mongoDB
    mongo_db = mongo_functions.open_mongodb()

    # Connect to postgreDB
    postgre_cursor, postgre_connection = postgre_functions.open_postgre()

    # Transfer products
    handle_functions.handle_products(mongo_db, postgre_cursor)
    print("Product data is transferred")

    # Transfer profiles
    handle_functions.handle_profiles(mongo_db, postgre_cursor)
    print("Profile data is transferred")

    # Transfer sessions
    handle_functions.handle_sessions(mongo_db, postgre_cursor)
    print("Session data is transferred")

    # Save data manipulation in postgre and close postgreDB
    postgre_functions.close_postgre(postgre_cursor, postgre_connection)  