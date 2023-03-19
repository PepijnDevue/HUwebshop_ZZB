import pymongo

def open_mongodb():
    """
    Create a connection with the database in mongoDB
    
    returns: the db object
    """
    # create a connection with the client
    client = pymongo.MongoClient('mongodb://localhost:27017')

    # create a connection with the database
    db = client.huwebshop
    return db

def get_products(db):
    """
    Open a connection to the product collection and retrieve all data from it

    db: the db object used to get the data
    returns: all data from the products collection
    """
    # create an object for the collection products
    products = db.products

    # create a cursor including all records from the collection products
    cursor = products.find()

    return cursor

def batch_handler_products(batch_size, cursor):
    """
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
        if 'name' in item_dict:
            item_dicts.append(item_dict)
            count += 1
            if count > batch_size:
                return item_dicts, False
    return item_dicts, True

def get_profiles(db):
    """
    Open a connection to the profiles collection and retrieve all data from it

    db: the db object used to get the data
    returns: all data from the profiles collection
    """
    # create an object for the collection products
    profiles = db.profiles

    # create a cursor including all records from the collection products
    cursor = profiles.find()

    return cursor

def batch_handler_profiles(batch_size, cursor):
    """
    """
     # a list to fill with the useful data
    item_dicts = []

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
        
        # add buids if exist and not empty
        if 'buids' in record and record['buids'] != None:
            item_dict['buids'] = record['buids']

        # add record to the list
        item_dicts.append(item_dict)
        count+=1
        # if count%10000 == 0:
            # print(count)
        if count > batch_size:
            return item_dicts, False

    return item_dicts, True

def get_sessions(db):
    """
    Open a connection to the session collection and retrieve all data from it

    db: the db object used to get the data
    returns: all data from the sessions collection
    """
    # create an object for the collection sessions
    sessions = db.sessions

    # create a cursor including all records from the collection products
    cursor = sessions.find()

    return cursor

def get_session_buids(batch_size, cursor):
    item_lists = []

    count = 0
    for record in cursor:
        # only save session from humans
        if 'user_agent' in record and record['user_agent']['flags']['is_bot'] == False:
            if '_id' in record and 'buid' in record:
                item_list = [record['_id'], record['buid']]

                item_lists.append(item_list)
                count+=1
                if count%1000==0:
                    print(count)
                if count > batch_size:
                    return item_lists, False

    return item_lists, True

def batch_handler_sessions(batch_size, cursor):
    # a list to fill with the useful data
    item_dicts = []

    # a list of keys wanted for the data transfer
    keys = ['_id',
            'buid',
            'preferences.brand',
            'preferences.category',
            'preferences.gender',
            'preferences.sub_category',
            'preferences.sub_sub_category',
            'preferences.promos',
            'preferences.product_type',
            'preferences.product_size']

    # loop through the data from mongoDB
    count = 0
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

            item_dicts.append(item_dict)
            count+=1
            if count%1000==0:
                print(count)
            if count > batch_size:
                return item_dicts, False

    return item_dicts, True