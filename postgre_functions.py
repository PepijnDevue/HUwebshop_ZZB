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

    # create a connection
    connection = psycopg2.connect(f'dbname=HUwebshop_Pepijn user=postgres password={password}')

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

def close_postgre(cursor, connection):
    """
    close the connection to the postgre database safely

    cursor: the cursor object for postgre
    connection: the connection object for postgre
    """
    connection.commit()
    cursor.close()
    connection.close()