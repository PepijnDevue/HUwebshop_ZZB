# imports
import psycopg2
from psycopg2.extras import execute_batch

def open_postgre():
    """
    Open a connection to the Postgres database from huwebshop.

    Returns:
        A tuple containing:
            - The cursor to execute SQL queries to the Postgres database.
            - The connection to the database to create a cursor and/or commit multiple SQL queries stored in the cursor.
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
    Transfer all the fitted data collected from MongoDB and insert it into the Postgres database.

    Parameters:
        cursor: The cursor to execute SQL queries.
        products: A list full of dictionaries with all the information about the products that we want to insert into the Postgres database.
        connection: The connection to the database to commit the changes add the end of all the queries being made in the cursor.

    Returns:
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

def profiles_to_postgre(cursor, profiles):
    """
    Inserts data collected from profiles in MongoDB into the PostgreSQL database.

    Parameters:
        cursor (psycopg2.extensions.cursor): Cursor used to execute SQL queries.
        profiles (list[dict]): List of dictionaries with information about profiles to be inserted into the database.

    Returns:
        None.

    The function inserts data into the following tables:
    - user_profile
    - prev_recommended
    - buid

    For each profile in the list, it creates a row in the user_profile table with the _id field value. If the profile has
    a 'previously_recommended' field, the function creates a row for every product in the list with the profile _id and
    product _id in the prev_recommended table. If the profile has a 'buids' field, the function creates a row for every
    buid in the list in the buid table, with the _id and user_profile_id fields.
    """
    # We need all the buids to only append buid values that are still valid within our database.
    cursor.execute("SELECT _id FROM buid")
    buids = cursor.fetchall()
    # Here we make a list from a big list with tuples
    buid_values = [bu_id[0] for bu_id in buids]

    # Here we make 2 list's which we will fill up with tuples with the values we need to execute the batch
    profile_batch_values = []
    prev_recommended_batch_values = []
    buid_batch_values = []

    # first create a row in the profile, then create rows for every previously recommended (see ERD.png)
    for profile in profiles:
        profile_batch_values.append((profile['_id'],))

        if 'previously_recommended' in profile:
            for item in profile['previously_recommended']:
                # check if product is recommendable
                cursor.execute(f"SELECT * FROM product WHERE _id = '{item}'")
                # Checks if the cursor fetch is bigger than 0.
                if len(cursor.fetchall()) > 0:
                    # Then append the values to be used in the batch insert
                    prev_recommended_batch_values.append((profile['_id'], item))

        if 'buids' in profile:
            for buid in profile['buids']:
                if buid not in buid_values:
                    # Here we append the values we need to use to execute a batch add the end of the function.
                    buid_batch_values.append((buid, profile['_id']))
                    # cursor.execute('INSERT INTO buid (_id, user_profile_id) VALUES (%s, %s)', (buid, profile['_id']))
                    buid_values.append(buid)

    execute_batch(cursor, "INSERT INTO user_profile (_id) VALUES (%s)", profile_batch_values)

    execute_batch(cursor, "INSERT INTO prev_recommended (user_profile_id, product_id) VALUES (%s, %s)", prev_recommended_batch_values)

    execute_batch(cursor, "INSERT INTO buid (_id, user_profile_id) VALUES (%s, %s)", buid_batch_values)


def sessions_to_postgre(cursor, sessions):
    """
    Insert all session information gathered from MongoDB into the PostgreSQL database.

    Args:
        cursor: psycopg2 cursor object used to execute SQL queries.
        sessions: list of dictionaries containing information about the sessions to be inserted into the database.

    Returns:
        None

    Raises:
        None
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
        session_keys = ['preference_brand', 
                        'preference_category', 
                        'preference_gender', 
                        'preference_sub_category', 
                        'preference_sub_sub_category', 
                        'preference_promos', 
                        'preference_product_type', 
                        'preference_product_size']
        # Create a empty list for the session_rows.
        session_row = []

        session_row.append(session['_id'])

        if 'buid' in session:
            cursor.execute(f"""SELECT _id FROM buid WHERE _id = '{session["buid"]}'""")
            if len(cursor.fetchall()) > 0:
                session_row.append(session['buid'])
            else:
                session_row.append(None)
        else:
            session_row.append(None)

        # Loop trough the rest of the session keys.
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
    
def close_postgre(cursor, connection):
    """
    Safely close the connection to the PostgreSQL database.

    Args:
        cursor: psycopg2 cursor object used to execute SQL queries.
        connection: psycopg2 connection object used to commit changes made to the database.

    Returns:
        None

    Raises:
        None
    """

    # Connection commits queries that may have been missed.
    print("Commit final queries")
    connection.commit()
    # we Close the cursor.
    print("Close the cursor")
    cursor.close()
    # Connection gets closed.
    print("Connections gets closed")
    connection.close()
    print('Transfer complete')