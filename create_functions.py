# imports
from psycopg2.extras import execute_batch

def create_table_most_recommended(cursor):
    """
    Create and fill the new table most_recommended from the information of prev_recommended

    Args:
        cursor (postgres-cursor): The psycopg2 cursor to access the database
    """
    # create the table
    cursor.execute('create table if not exists most_recommended(product_id varchar(255) primary key, frequency integer)')

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
    cursor.execute('ALTER TABLE user_profile ADD COLUMN IF NOT EXISTS top_category VARCHAR(255)')

    # fetch buid and preference_category from all user_sessions
    cursor.execute('select buid, preference_category from user_session where buid is not null')
    sessions = cursor.fetchall()

    freq_dict = {}
    for session in sessions:
        cursor.execute('select user_profile_id from buid where buid._id = %s', (session[0],))
        user_profile_id = cursor.fetchall()[0][0]
        if user_profile_id not in freq_dict:
            freq_dict[user_profile_id] = {}
        if session[1] in freq_dict[user_profile_id]:
            freq_dict[user_profile_id][session[1]] += 1
        else:
            freq_dict[user_profile_id][session[1]] = 1

    for profile_id in freq_dict:
        top_preference = ''
        top_preference_frequency = -1
        for preference in freq_dict[profile_id]:
            if freq_dict[profile_id][preference] > top_preference_frequency:
                top_preference = preference
                top_preference_frequency = freq_dict[profile_id][preference]
        freq_dict[profile_id] = top_preference

    cursor.execute('select _id from user_profile')
    user_profiles = [i[0] for i in cursor.fetchall()]

    top_preference_vals = []
    for profile_id in user_profiles:
        if profile_id in freq_dict:
            top_preference_vals.append((freq_dict[profile_id], profile_id))
        else:
            top_preference_vals.append((None, profile_id))

    execute_batch(cursor, 'update user_profile set top_category = %s where _id = %s', top_preference_vals) 

def create_table_top_category_product(cursor):
    cursor.execute('create table if not exists top_category_product (category varchar(255) primary key, product_1 varchar(255), product_2 varchar(255), product_3 varchar(255), product_4 varchar(255), product_5 varchar(255))')

    cursor.execute('select distinct category from product where category is not null')
    categories = [i[0] for i in cursor.fetchall()]

    cursor.execute('select product_id from most_recommended order by frequency desc limit 5')
    general_most_recommended = [i[0] for i in cursor.fetchall()]

    top_cat_vals = []
    for category in categories:
        cursor.execute('select most_recommended.product_id from most_recommended inner join product on most_recommended.product_id = product._id where product.category = %s order by most_recommended.frequency desc limit 5;', (category,))
        products = [i[0] for i in cursor.fetchall()]
        top_cat_vals.append(tuple([category] + products + general_most_recommended[len(products):5]))

    execute_batch(cursor, 'insert into top_category_product (category, product_1, product_2, product_3, product_4, product_5) values (%s, %s, %s, %s, %s, %s)', top_cat_vals)

def create_table_top_group_product(cursor):
    cursor.execute('create table if not exists top_group_product (target_group varchar(255) primary key, product_1 varchar(255), product_2 varchar(255), product_3 varchar(255), product_4 varchar(255), product_5 varchar(255))')

    cursor.execute('select distinct target_group from product where target_group is not null')
    groups = [i[0] for i in cursor.fetchall()]

    cursor.execute('select product_id from most_recommended order by frequency desc limit 5')
    general_most_recommended = [i[0] for i in cursor.fetchall()]

    top_group_vals = []
    for group in groups:
        cursor.execute('select most_recommended.product_id from most_recommended inner join product on most_recommended.product_id = product._id where product.target_group = %s order by most_recommended.frequency desc limit 5;', (group,))
        products = [i[0] for i in cursor.fetchall()]
        top_group_vals.append(tuple([group] + products + general_most_recommended[len(products):5]))

    execute_batch(cursor, 'insert into top_group_product (target_group, product_1, product_2, product_3, product_4, product_5) values (%s, %s, %s, %s, %s, %s)', top_group_vals)
