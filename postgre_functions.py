# imports
import psycopg2

def open_postgre():
    """
    create a connection to the postgre database

    returns: a cursor to use the database and the connection object
    """
    # get my secret password securely from a git_ignored file
    password_file = open('password.txt')
    password = password_file.readline()

    # write your db name here
    db_name = 'HUwebshop_Pepijn'

    # create a connection
    connection = psycopg2.connect(f'dbname={db_name} user=postgres password={password}')

    # create a cursor
    cursor = connection.cursor()
    return cursor, connection

def products_to_postgre(cursor, products):
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

def profiles_to_postgre(cursor, profiles):
    """
    saves all fitted profile information to the postgre database

    cursor: the postgre cursor
    products: a list of dicts containing the fitted profile info
    """
    # first create a row in the profile, then create rows for every previously recommended (see ERD.png)
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

def sessions_to_postgre(cursor, sessions):
    """
    saves all fitted session information to the postgre database

    cursor: the postgre cursor
    products: a list of dicts containing the fitted session info
    """
    # first create a row in the session, then create rows for every order (see ERD.png)
    for session in sessions:
        # create lists for keys and values
        keys = []
        values = []

        # Loop over the dictionary and append key-value pairs to the lists
        for key, value in session.items():
            if key.startswith('preferences_') or key == '_id':
                keys.append(key)
                values.append(value)

        # add to user_session
        cursor.execute(f"INSERT INTO user_session ({keys}) VALUES ({values})")

        # add to session_order
        if 'products' in session:
            for product in session['products']:
                cursor.execute('INSERT INTO session_order (product_id, session_id) VALUES (%s, %s)', (product, session['_id']))

        # FILL EXISTING BUID ROWS WITH SESSION_ID

def close_postgre(cursor, connection):
    """
    close the connection to the postgre database safely

    cursor: the cursor object for postgre
    connection: the connection object for postgre
    """
    connection.commit()
    cursor.close()
    connection.close()