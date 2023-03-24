# imports
from psycopg2.extras import execute_batch

def create_table_most_recommended(cursor):
    """
    Create and fill the new table most_recommended from the information of prev_recommended

    Args:
        cursor (postgres-cursor): The psycopg2 cursor to access the database
    """
    # create the table
    cursor.execute('create table most_recommended(product_id varchar(255) primary key, frequency integer)')

    # fetch all previously recommended products
    cursor.execute('select product_id from prev_recommended')
    prev_recommended_products = cursor.fetchall()

    # create frequency dict on how many times a product has been recommended
    freq_dict = {}
    for product in prev_recommended_products:
        if product in freq_dict:
            freq_dict[product] += 1
        else:
            freq_dict[product] = 1

    # make the info fit the table (from dict to tuple for execute_batch)
    most_recommended_vals = []
    for product_id in freq_dict:
        most_recommended_vals.append((product_id, freq_dict[product_id]))

    # insert all rows into the table
    execute_batch(cursor, 'insert into most_recommended (product_id, frequency) values (%s, %s)', most_recommended_vals)

def create_row_top_category(cursor):
    return

def create_table_top_category_product(cursor):
    return

def create_table_top_group_product(cursor):
    return