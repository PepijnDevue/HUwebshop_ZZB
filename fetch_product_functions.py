def content_filter(product_id, cursor):
    cursor.execute('select target_group from product where _id = %s', (product_id,))
    group = cursor.fetchall()[0][0]

    if group == None:
        cursor.execute('select product_id from most_recommended order by frequency desc limit 5')
        products = [i[0] for i in cursor.fetchall()]
    else:
        cursor.execute('select * from top_group_product where target_group = %s', (group,))
        products = cursor.fetchall()[0][1:6]

    if product_id in products:
        products.pop(product_id)

    return(products[:4])

def collaborative_filter(profile_id, cursor):
    cursor.execute('select top_category from user_profile where _id = %s', (profile_id,))
    category = cursor.fetchall()[0][0]

    if category == None:
        cursor.execute('select product_id from most_recommended order by frequency desc limit 5')
        products = [i[0] for i in cursor.fetchall()]
    else:
        cursor.execute('select * from top_category_product where target_group = %s', (category,))
        products = cursor.fetchall()[0][1:6]

    return(products[:4])