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

def batch_handler_products(batch_size, cursor):
    """
    Retrieves a batch of useful product information from the provided `cursor` object.

    Args:
        batch_size: The number of product records to retrieve.
        cursor: The MongoDB `Cursor` object representing the 'products' collection.

    Returns:
        A tuple containing two items:
            1. A list of dictionaries containing the retrieved product information.
            2. A boolean value indicating if the end of the cursor has been reached.

    Notes:
        - The list of keys used to retrieve data from the records is hardcoded in the function.
        - Only products with a name are included in the returned list.
    """
    # a list of keys wanted for the data transfer
    keys = ['_id',
            'brand', 
            'category', 
            'color', 
            'fast_mover', 
            'gender', 
            'name', 
            'price.selling_price', 
            'properties.discount', 
            'properties.doelgroep', 
            'size', 
            'properties.leeftijd', 
            'properties.serie', 
            'properties.type', 
            'sub_category', 
            'sub_sub_category', 
            'sub_sub_sub_category']

    # a list to fill with the useful data
    item_dicts = []

    # keep track of the amount of records retrieved
    count = 0

    # loop through the data from mongoDB
    for record in cursor:
        # create a dict with only the necessary info per record
        item_dict = {}
        # check if the current record includes the wanted info
        for key in keys:
            # some info is an additional layer deep
            if '.' in key:
                sub_keys = key.split('.')
                key0 = sub_keys[0]
                key1 = sub_keys[1]
                # use if statement to prevent error if the info is absent
                if key0 in record and key1 in record[key0]:
                    item_dict[key1] = record[key0][key1]
            else:
                if key in record:
                    item_dict[key] = record[key]
        
        # only take products that have a name
        if 'name' in item_dict:
            item_dicts.append(item_dict)
            count += 1
            if count > batch_size:
                return item_dicts, False
    return item_dicts, True

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

def batch_handler_profiles(batch_size, cursor):
    """
    Gets a batch of all useful profile information from the 'profiles' collection cursor.

    Args:
        batch_size: The number of profile records to get.
        cursor: The MongoDB `Cursor` object from the 'profiles' collection.

    Returns:
        A tuple containing:
            - A list of dictionaries representing the batch of data, each dictionary containing the following keys:
                - '_id': The profile's MongoDB ID as a string.
                - 'previously_recommended' (optional): A list of strings representing previously recommended products.
                - 'buids' (optional): A list of strings representing BUIDs (Browser Unique Identifiers).
            - A boolean indicating whether there are more records to retrieve.

    Raises:
        `ValueError`: If `batch_size` is less than or equal to 0.
    """
    # a list to fill with the useful data
    item_dicts = []

    # keep track of the amount of records retrieved
    count = 0

    # loop through the data from mongoDB
    for record in cursor:
        # create a dict with only the necessary info per record
        item_dict = {}

        # save _id
        item_dict['_id'] = str(record['_id'])
        # save previously recommended if exists and not empty
        if 'previously_recommended' in record and len(record['previously_recommended']) > 0:
            item_dict['previously_recommended'] = record['previously_recommended']

        # save buids
        if 'buids' in record and len(record['buids']) != 0:
            item_dict['buids'] = record['buids']

        # add record to the list
        item_dicts.append(item_dict)
        count+=1
        if count > batch_size:
            return item_dicts, False

    return item_dicts, True

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

def batch_handler_sessions(batch_size, cursor):
    """
    Retrieve a batch of session information from the MongoDB session collection cursor.

    Args:
        batch_size (int): The number of session records to retrieve.
        cursor: The MongoDB cursor from the session collection.

    Returns:
        A tuple containing:
            - A batch of session data as a list of dictionaries, each containing the following keys:
                - '_id' (str): The session ID.
                - 'preferences.brand' (str): The brand preference of the user.
                - 'preferences.category' (str): The category preference of the user.
                - 'preferences.gender' (str): The gender preference of the user.
                - 'preferences.sub_category' (str): The sub-category preference of the user.
                - 'preferences.sub_sub_category' (str): The sub-sub-category preference of the user.
                - 'preferences.promos' (bool): Whether the user has opted-in to receive promotional messages.
                - 'preferences.product_type' (str): The product type preference of the user.
                - 'preferences.product_size' (str): The product size preference of the user.
                - 'products' (List[Dict[str, Any]]): A list of dictionaries representing the ordered products.
                - 'buid' (str): The browser unique identifier (BUID).
            - A boolean indicating whether there are more batches to retrieve (True) or not (False).
    """
    # a list to fill with the useful data
    item_dicts = []

    # a list of keys wanted for the data transfer
    keys = ['_id',
            'preferences.brand',
            'preferences.category',
            'preferences.gender',
            'preferences.sub_category',
            'preferences.sub_sub_category',
            'preferences.promos',
            'preferences.product_type',
            'preferences.product_size']

    # keep track of the amount of records retrieved
    count = 0

    # loop through the data from mongoDB
    for record in cursor:
        # only save session from humans
        if 'user_agent' in record and record['user_agent']['flags']['is_bot'] == False:
            # create a dict with only the necessary info per record
            item_dict = {}
            # check if the current record includes the wanted info
            for key in keys:
                # some info is an additional layer deep
                if '.' in key:
                    sub_keys = key.split('.')
                    key0 = sub_keys[0]
                    key1 = sub_keys[1]
                    # use if statement to prevent error if the info is absent
                    if key0 in record and key1 in record[key0]:
                        item_dict[key1] = record[key0][key1]
                else:
                    if key in record:
                        item_dict[key] = record[key]

            # add ordered products
            if 'order' in record and record['order'] != None:
                item_dict['products'] = record['order']['products']

            # add buid
            if 'buid' in record and len(record['buid']) == 1:
                item_dict['buid'] = record['buid'][0]

            item_dicts.append(item_dict)
            if count > batch_size:
                return item_dicts, False

    return item_dicts, True