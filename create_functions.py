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
    # add row to user_profile
    cursor.execute('alter table user_profile add top_category varchar(255)')

    # fetch buid and preference_category from all user_sessions
    cursor.execute('select buid, preference_category from user_session where buid is not null')
    sessions = cursor.fetchall()

    # TODO: TEST THIS IN PGADMIN
    freq_dict = {}
    for session in sessions:
        cursor.execute('select user_profile_id form buid where buid._id = %s', (session[0]))
        user_profile_id = cursor.fetchall()
        if user_profile_id not in freq_dict:
            freq_dict[user_profile_id] = {}
        if session[1] in freq_dict[user_profile_id]:
            freq_dict[user_profile_id][session[1]] += 1
        else:
            freq_dict[user_profile_id][session[1]] = 1


    

def create_table_top_category_product(cursor):
    return

def create_table_top_group_product(cursor):
    return