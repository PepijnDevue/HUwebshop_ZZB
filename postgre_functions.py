# imports
import psycopg2
from psycopg2.extras import execute_batch

def open_postgre():
    """
    create a connection to the postgre database

    returns: a cursor to use the database and the connection object
    """
    # get my secret password securely from a git_ignored file
    password_file = open('password.txt')
    password = password_file.readline()

    # write your db name here
    db_name = 'huwebshop'
    # password = 'admin'

    # create a connection
    connection = psycopg2.connect(f'dbname={db_name} user=postgres password={password}')

    # create a cursor
    cursor = connection.cursor()
    return cursor, connection

def products_to_postgre(cursor, products, connection):
    """
    saves all fitted product information to the postgre database

    cursor: the postgre cursor
    products: a list of dicts containing the fitted product info
    """
    for product in products:
        # get the list of rows
        row_list = list(product.keys())

        # get the list of values
        val_list = [product[key] for key in row_list]

        key_string = ", ".join(row_list)
        # use %s to prevent errors with names like ' L 'Oreal '
        value_string = ', '.join(['%s' for _ in val_list])
        cursor.execute(f"INSERT INTO product ({key_string}) VALUES ({value_string})", val_list)
    
    connection.commit()

def profiles_to_postgre(cursor, profiles, connection):
    """
    saves all fitted profile information to the postgre database

    cursor: the postgre cursor
    products: a list of dicts containing the fitted profile info
    """
    # first create a row in the profile, then create rows for every previously recommended (see ERD.png)
    # count = 0
    for profile in profiles:
        cursor.execute("INSERT INTO user_profile (_id) VALUES (%s)", (profile['_id'],))

        if 'previously_recommended' in profile:
            for item in profile['previously_recommended']:
                # check if product is recommendable
                cursor.execute(f"SELECT * FROM product WHERE _id = '{item}'")
                if len(cursor.fetchall()) > 0:
                    cursor.execute("INSERT INTO prev_recommended (user_profile_id, product_id) VALUES (%s, %s)", (profile['_id'], item))
        
        if 'buids' in profile:
            for item in profile['buids']:
                cursor.execute("INSERT INTO buid (_id, user_profile_id) VALUES (%s, %s)", (item, profile['_id']))
                
    connection.commit()

def sessions_to_postgre(cursor, sessions, connection):
    """
    saves all fitted session information to the postgre database

    cursor: the postgre cursor
    products: a list of dicts containing the fitted session info
    """
    # count = 0
    session_values = []
    order_values = []
    # We need all the product id's to only append order values that are still valid within our database. -Dave
    cursor.execute("SELECT _id FROM product")
    products = cursor.fetchall()
    # Here we make a list from a big list with tupels
    product_id_values = [product[0] for product in products]

    # buid_values = []
    # first create a row in the session, then create rows for every order (see ERD.png)
    for session in sessions:
        # create lists for keys and values
        session_keys = ['_id', 
                        'preference_brand', 
                        'preference_category', 
                        'preference_gender', 
                        'preference_sub_category', 
                        'preference_sub_sub_category', 
                        'preference_promos', 
                        'preference_product_type', 
                        'preference_product_size']
        session_row = []

        for key in session_keys:
            if key in session:
                session_row.append(session[key])
            else:

                session_row.append(None)

        session_values.append(tuple(session_row))

        # add to session_order
        if 'products' in session:
            for product in session['products']:
                # Here we check if the product actually exists in the database. -Dave
                if product in product_id_values:
                    order_values.append((product, session['_id']))

    print('execute0')
    # print(session_values)
    # print(buid_values)
    execute_batch(cursor, "INSERT INTO user_session (_id, preference_brand, preference_category, preference_gender, preference_sub_category, preference_sub_sub_category, preference_promos, preference_product_type, preference_product_size) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", session_values)
    print('execute1')
    execute_batch(cursor, "INSERT INTO session_order (product_id, session_id) VALUES (%s, %s)", order_values)

    print('finish execute')
    input('PRESS ENTER TO COMMIT')
    connection.commit()
    
def close_postgre(cursor, connection):
    """
    close the connection to the postgre database safely

    cursor: the cursor object for postgre
    connection: the connection object for postgre
    """
    connection.commit()
    cursor.close()
    connection.close()