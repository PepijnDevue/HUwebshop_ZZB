import pymongo

def open_mongodb():
    """
    Creates a connection to the MongoDB database.

    Returns:
        The `Database` object representing the connected database.

    Raises:
        `pymongo.errors.ConnectionFailure` if the connection attempt fails.
    """
    # create a connection with the client
    client = pymongo.MongoClient('mongodb://localhost:27017')

    # create a connection with the database
    db = client.huwebshop
    return db

def get_products(db):
    """
    Retrieves all data from the 'products' collection in the given database.

    Args:
        db: The `Database` object representing the database to retrieve data from.

    Returns:
        A `Cursor` object representing the retrieved data.

    Raises:
        `pymongo.errors.InvalidOperation` if an invalid operation is performed on the cursor.
    """
    # create an object for the collection products
    products = db.products

    # create a cursor including all records from the collection products
    cursor = products.find()

    return cursor

def get_profiles(db):
    """
     Opens a connection to the 'profiles' collection and retrieves all data from it.

     Args:
         db: The `Database` object representing the MongoDB database.

     Returns:
         A MongoDB `Cursor` object representing the 'profiles' collection.

     Raises:
         `pymongo.errors.OperationFailure`: If the user does not have permission to read the 'profiles' collection.
     """
    # create an object for the collection products
    profiles = db.profiles

    # create a cursor including all records from the collection products
    cursor = profiles.find()

    return cursor

def get_sessions(db):
    """
    Open a connection to the sessions collection and retrieve all data.

    Args:
        db: The db object used to get the data.

    Returns:
        A cursor including all records from the sessions collection.
    """
    # create an object for the collection sessions
    sessions = db.sessions

    # create a cursor including all records from the collection products
    cursor = sessions.find()

    return cursor