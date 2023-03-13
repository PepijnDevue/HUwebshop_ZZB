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

def collect_product_data(db):
    """
    Get all product data from mongoDB and save only the important and useful data and record

    returns: a list of dicts, each dict resembles one product record
    """
    cursor = get_products(db)

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

    # a list to fill with the usefull data
    item_dicts = []

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
        # only save the info of the record if it has the necessary name and id
        if 'name' in item_dict and '_id' in item_dict and 'selling_price' in item_dict:
            item_dicts.append(item_dict)
    return item_dicts