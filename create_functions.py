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

    # insert all products the have not been recommended with a frequency of zero
    cursor.execute('insert into most_recommended (product_id, frequency) select _id, 0 from product where not exists (select 1 from most_recommended where most_recommended.product_id = product._id)')

def create_row_top_category(cursor):
    """
    Add the column top_category to the table user_profile
    Fill it with the category most preferred by that user

    Args:
        cursor (postgres-cursor): The psycopg2 cursor to access the database
    """
    # add row to user_profile
    cursor.execute('ALTER TABLE user_profile ADD COLUMN IF NOT EXISTS top_category VARCHAR(255)')

    # fetch buid and preference_category from all user_sessions that have a buid
    cursor.execute('select buid, preference_category from user_session')
    sessions = cursor.fetchall()

    # create a frequency dict that will look like freq_dict[user][category] = n
    freq_dict = {}

    # count every time a category has been recommended to a user, per user, per category (for example user x has had category y as preference_category n times)
    for session in sessions:
        # get user profile that is connected to the session
        cursor.execute('select user_profile_id from buid where buid._id = %s', (session[0],))
        result = cursor.fetchall()
        if len(result) == 2:
            user_profile_id = result[0][0]

            # count the category
            if user_profile_id not in freq_dict:
                freq_dict[user_profile_id] = {}
            if session[1] in freq_dict[user_profile_id]:
                freq_dict[user_profile_id][session[1]] += 1
            else:
                freq_dict[user_profile_id][session[1]] = 1

    # take the category that is most preferred by each user
    for profile_id in freq_dict:
        top_preference = ''
        top_preference_frequency = -1

        for preference in freq_dict[profile_id]:
            if freq_dict[profile_id][preference] > top_preference_frequency:
                top_preference = preference
                top_preference_frequency = freq_dict[profile_id][preference]
        freq_dict[profile_id] = top_preference

    # get a list of all profile_id's
    cursor.execute('select _id from user_profile')
    user_profiles = [i[0] for i in cursor.fetchall()]

    # create a list of tuples so all top_category rows can be filled
    top_preference_vals = []
    for profile_id in user_profiles:
        # if a preferred category is known for this user, use it
        if profile_id in freq_dict:
            top_preference_vals.append((freq_dict[profile_id], profile_id))
        # otherwise fill None
        else:
            top_preference_vals.append((None, profile_id))

    # update the rows altogether
    execute_batch(cursor, 'update user_profile set top_category = %s where _id = %s', top_preference_vals) 

def create_table_top_category_product(cursor):
    """
    Add the table top_category_product if it does not exist yet
    Fill it with every unique category and the 5 most recommended products from that category

    Args:
        cursor (postgres-cursor): The psycopg2 cursor to access the database
    """
    # create the table top_category product if it does not exist yet
    cursor.execute('create table if not exists top_category_product (category varchar(255) primary key, product_1 varchar(255), product_2 varchar(255), product_3 varchar(255), product_4 varchar(255), product_5 varchar(255))')

    # get all different categories
    cursor.execute('select distinct category from product where category is not null')
    categories = [i[0] for i in cursor.fetchall()]

    # get the top 5 generally most recommended in case there are not enough recommended in the wanted category
    cursor.execute('select product_id from most_recommended order by frequency desc limit 5')
    general_most_recommended = [i[0] for i in cursor.fetchall()]

    # create a list of tuples so all top_category_product rows can be filled
    top_cat_vals = []
    for category in categories:
        # get the 5 products from this category that have been recommended most
        cursor.execute('select most_recommended.product_id from most_recommended inner join product on most_recommended.product_id = product._id where product.category = %s order by most_recommended.frequency desc limit 5;', (category,))
        products = [i[0] for i in cursor.fetchall()]

        # if less than 5 products from this category where present, add the next most-generally-recommended
        top_cat_vals.append(tuple([category] + products + general_most_recommended[len(products):5]))

    # insert all rows altogether
    execute_batch(cursor, 'insert into top_category_product (category, product_1, product_2, product_3, product_4, product_5) values (%s, %s, %s, %s, %s, %s)', top_cat_vals)

def create_table_top_group_product(cursor):
    """
    Add the table top_group_product if it does not exist yet
    Fill it with every unique category and the 5 most recommended products from that group

    Args:
        cursor (postgres-cursor): The psycopg2 cursor to access the database
    """
    # create the table top_group_product if it does not exist yet
    cursor.execute('create table if not exists top_group_product (target_group varchar(255) primary key, product_1 varchar(255), product_2 varchar(255), product_3 varchar(255), product_4 varchar(255), product_5 varchar(255))')

    # get all unique groups
    cursor.execute('select distinct target_group from product where target_group is not null')
    groups = [i[0] for i in cursor.fetchall()]

    # get the top 5 generally most recommended in case there are not enough recommended in the wanted group
    cursor.execute('select product_id from most_recommended order by frequency desc limit 5')
    general_most_recommended = [i[0] for i in cursor.fetchall()]

    # create a list of tuples so all top_group_product rows can be filled
    top_group_vals = []
    for group in groups:
        # get the 5 products from this group that were recommended most
        cursor.execute('select most_recommended.product_id from most_recommended inner join product on most_recommended.product_id = product._id where product.target_group = %s order by most_recommended.frequency desc limit 5;', (group,))
        products = [i[0] for i in cursor.fetchall()]

        # if less than 5 products from this group where present, add the next most-generally-recommended
        top_group_vals.append(tuple([group] + products + general_most_recommended[len(products):5]))

    # insert all rows altogether
    execute_batch(cursor, 'insert into top_group_product (target_group, product_1, product_2, product_3, product_4, product_5) values (%s, %s, %s, %s, %s, %s)', top_group_vals)
