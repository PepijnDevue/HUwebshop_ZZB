def content_filter(product_id, cursor):
    """
    Use a product to recommend 4 other products based on the target_group

    Args:
        product_id (str): the identification string for a product
        cursor (postgres-cursor): The psycopg2 cursor to access the database
    """
    # get the product group
    cursor.execute('select target_group from product where _id = %s', (product_id,))
    group = cursor.fetchall()[0][0]

    # if no group is know, recommend the generally most recommended products
    if group == None:
        cursor.execute('select product_id from most_recommended order by frequency desc limit 5')
        products = [i[0] for i in cursor.fetchall()]
    else:
        cursor.execute('select * from top_group_product where target_group = %s', (group,))
        products = cursor.fetchall()[0][1:6]

    # dont recommend the same product that is inputted
    if product_id in products:
        products.pop(product_id)

    # return the first 4 products
    return(products[:4])

def collaborative_filter(profile_id, cursor):
    """
    Use a profile to recommend 4 products based their most preferred category

    Args:
        profile_id (str): the identification string for a profile
        cursor (postgres-cursor): The psycopg2 cursor to access the database
    """
    # get the profile most preferred category
    cursor.execute('select top_category from user_profile where _id = %s', (profile_id,))
    category = cursor.fetchall()[0][0]

    # if there is no most preferred category known, recommend the generally most recommended products
    if category == None:
        cursor.execute('select product_id from most_recommended order by frequency desc limit 5')
        products = [i[0] for i in cursor.fetchall()]
    else:
        cursor.execute('select * from top_category_product where target_group = %s', (category,))
        products = cursor.fetchall()[0][1:6]

    # return the first 4 products
    return(products[:4])