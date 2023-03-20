# imports
import psycopg2
from psycopg2.extras import execute_batch

def open_postgre():
    """
    The function open_postgre() opens up a simple connection to the database from huwebshop.

    Parameters:
        None
    
    Return:
        cursor : The curser is used to execute sql queries to the postgres database
        connection : The connections is used to create a curser and or commit multiple sql queries stored in the cursor.
    """
    # get my secret password securely from a git_ignored file
    password_file = open('password.txt')
    password = password_file.readline()

    # write your db name here.
    # This is the postgres database Name and password
    db_name = 'huwebshop'

    # create a connection
    connection = psycopg2.connect(f'dbname={db_name} user=postgres password={password}')

    # create a cursor
    cursor = connection.cursor()

    # Here we return the values
    return cursor, connection

def products_to_postgre(cursor, products, connection):
    """
    The function products_to_postgre() will transfer all the fitted data collected from mongodb and insert it in to the postgres database.

    Parameters:
        cursor: The psycopg2 cursor is used to execute sql queries.
        products: This is a list full of dictionaries with all the information about the products that we want to insert into the postgres database.
        connection: The connection is used to commit the changes add the end of all the queries being made in the cursor.
    
    Return:
        None

    """
    # Loops trough all the products.
    for product in products:
        # get the list of rows
        row_list = list(product.keys())

        # get the list of values
        val_list = [product[key] for key in row_list]

        key_string = ", ".join(row_list)
        # use %s to prevent errors with names like ' L 'Oreal '
        value_string = ', '.join(['%s' for _ in val_list])
        cursor.execute(f"INSERT INTO product ({key_string}) VALUES ({value_string})", val_list)
    
    # Commit all the changes to the database.
    connection.commit()

def profiles_to_postgre(cursor, profiles, connection):
    """
    The function profiles_to_postgre() inserts all the data collected from profiles in mongo an transfers it in to the postgres database.


    Parameters:
        cursor: The psycopg2 cursor is used to execute sql queries.
        profiles: This is a list full of dictionaries with all the information about the profiles that we want to insert into the postgres database.
        connection: The connection is used to commit the changes add the end of all the queries being made in the cursor.

    Return:
        None:

    """
    # first create a row in the profile, then create rows for every previously recommended (see ERD.png)
    for profile in profiles:
        cursor.execute("INSERT INTO user_profile (_id) VALUES (%s)", (profile['_id'],))

        if 'previously_recommended' in profile:
            for item in profile['previously_recommended']:
                # check if product is recommendable
                cursor.execute(f"SELECT * FROM product WHERE _id = '{item}'")
                # Checks if the cursor fetch is bigger then 0.
                if len(cursor.fetchall()) > 0:
                    cursor.execute("INSERT INTO prev_recommended (user_profile_id, product_id) VALUES (%s, %s)", (profile['_id'], item))

        for buid in profile['buids']:
            cursor.execute('INSERT INTO buid (_id, user_profile_id) VALUES (%s, %s)', (buid, profile['_id']))

    # Commit all the changes to the database. 
    connection.commit()

def sessions_to_postgre(cursor, sessions, connection):
    """
    The function sessions_to_postgre() is here to insert all the session information gathered from mongo into the postgres database.

    Parameters:
        cursor: The psycopg2 cursor is used to execute sql queries.
        sessions: This is a list full of dictionaries with all the information about the sessions that we want to insert into the postgres database.
        connection: The connection is used to commit the changes add the end of all the queries being made in the cursor.

    Return:
        None:
    """
    # Create a empty list for the session_values
    session_values = []
    # Create a empty list for the order_values
    order_values = []

    # We need all the product id's to only append order values that are still valid within our database.
    cursor.execute("SELECT _id FROM product")
    products = cursor.fetchall()
    # Here we make a list from a big list with tuples
    product_id_values = [product[0] for product in products]

    # first create a row in the session, then create rows for every order (see ERD.png)
    for session in sessions:
        # create lists for keys and values
        session_keys = ['_id',
                        'buid',
                        'preference_brand', 
                        'preference_category', 
                        'preference_gender', 
                        'preference_sub_category', 
                        'preference_sub_sub_category', 
                        'preference_promos', 
                        'preference_product_type', 
                        'preference_product_size']
        # Create a empty list for the session_rows.
        session_row = []

        # Loop trough the session keys.
        for key in session_keys:
            # If key is present, add it to the session_row
            if key in session:
                session_row.append(session[key])
            # Otherwise fill that place with None to prevent errors with execute_batch
            else:
                session_row.append(None)

        session_values.append(tuple(session_row))

        # add to session_order
        if 'products' in session:
            for product in session['products']:
                # Here we check if the product actually exists in the database.
                if product in product_id_values:
                    order_values.append((product, session['_id']))

    # Execute the first batch
    execute_batch(cursor, "INSERT INTO user_session (_id, buid, preference_brand, preference_category, preference_gender, preference_sub_category, preference_sub_sub_category, preference_promos, preference_product_type, preference_product_size) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", session_values)
    # Execute the second batch
    execute_batch(cursor, "INSERT INTO session_order (product_id, session_id) VALUES (%s, %s)", order_values)

    connection.commit()
    
def close_postgre(cursor, connection):
    """
    The function close_postgre() is used to safely close the connection to the database

    Parameters:
        cursor: The psycopg2 cursor is used to execute sql queries.
        connection: The connection is used to commit the changes add the end of all the queries being made in the cursor.
    
    Return:
        None:

    cursor: the cursor object for postgre
    connection: the connection object for postgre
    """

    # Connection commits queries that may have been missed.
    connection.commit()
    # we Close the cursor.
    cursor.close()
    # Connection gets closed.
    connection.close()