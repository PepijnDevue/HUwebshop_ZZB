# imports
import postgre_functions
import mongo_functions
import fit_data_functions
import get_batch_functions

def handle_products(mongo_db, postgre_cursor):
    """
    Retrieves useful product information from MongoDB and transfers it to a relational database.

    Args:
        mongo_db: A MongoDB object used to connect to the MongoDB database.
        postgre_cursor: A cursor object used to execute PostgreSQL commands.

    Returns:
        None
    """
    # get the mongodb cursor to retrieve data
    mongo_cursor = mongo_functions.get_products(mongo_db)

    done = False

    # transfer the products in batches to increase performance
    batch_size = 3000

    while not done:
        # get a batch of information from mongodb
        item_dicts, done = get_batch_functions.get_product_batch(batch_size, mongo_cursor)
        # manipulate the data so it fits in the relational database
        item_dicts = fit_data_functions.fit_product_data(item_dicts)
        # transfer the data to the relational database
        postgre_functions.products_to_postgre(postgre_cursor, item_dicts)

def handle_profiles(mongo_db, postgre_cursor):
    """
    Transfer all useful profile information from the MongoDB object to the
    relational database using the provided cursor and connection objects.

    Args:
        mongo_db (pymongo.database.Database): The MongoDB database object to
            retrieve the profile information from.
        postgre_cursor (psycopg2.extensions.cursor): The cursor for the
            PostgreSQL relational database to transfer the profile information
            to.

    Returns:
        None. The function transfers the profile information to the PostgreSQL
        database but does not return anything.
    """
    # get the mongodb cursor to retrieve data
    mongo_cursor = mongo_functions.get_profiles(mongo_db)

    done = False

    # transfer the profiles in batches to increase performance
    batch_size = 50000

    while not done:
        # get a batch of information from mongodb
        item_dicts, done = get_batch_functions.get_profile_batch(batch_size, mongo_cursor)
        # transfer the data to the relational database
        postgre_functions.profiles_to_postgre(postgre_cursor, item_dicts)

def handle_sessions(mongo_db, postgre_cursor):
    """
    Transfer all useful session information from the MongoDB object to the
    relational database using the provided cursor and connection objects.

    Args:
        mongo_db (pymongo.database.Database): The MongoDB database object to
            retrieve the session information from.
        postgre_cursor (psycopg2.extensions.cursor): The cursor for the
            PostgreSQL relational database to transfer the session information
            to.
        postgre_connection (psycopg2.extensions.connection): The connection
            object for the PostgreSQL relational database to transfer the
            session information to.

    Returns:
        None. The function transfers the session information to the PostgreSQL
        database but does not return anything.
    """
    # get the mongodb cursor to retrieve data
    mongo_cursor = mongo_functions.get_sessions(mongo_db)

    done = False

    # transfer the sessions in batches to increase performance
    batch_size = 50000

    while not done:
        # get a batch of information from mongodb
        item_dicts, done = get_batch_functions.get_session_batch(batch_size, mongo_cursor)
        # manipulate the data so it fits in the relational database
        item_dicts = fit_data_functions.fit_session_data(item_dicts)
        # transfer the data to the relational database
        postgre_functions.sessions_to_postgre(postgre_cursor, item_dicts)