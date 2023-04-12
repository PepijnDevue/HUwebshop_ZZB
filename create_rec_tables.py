def create_table_series_products(cursor):
    """
    Create a table with all unique series and the first 5 recommendable of that series

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists series_products (series varchar(255) primary key, product_ids varchar(255))')
    cursor.execute('truncate table series_products')
    cursor.execute("insert into series_products (series, product_ids) select series, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where series is not null and recommendable is true group by series having count(*) > 1 order by series;")

def create_table_group_products(cursor):
    """
    Create a table with all unique target_groups and the first 5 recommendable of that target_group

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists group_products (target_group varchar(255) primary key, product_ids varchar(255))')
    cursor.execute('truncate table group_products')
    cursor.execute("insert into group_products (target_group, product_ids) select target_group, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where target_group is not null and recommendable is true group by target_group having count(*) > 1 order by target_group;")

def create_table_sscat_products(cursor):
    """
    Create a table with all unique sub_sub_categories and the first 5 recommendable of that sub_sub_category

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists sscat_products (sub_sub_category varchar(255) primary key, product_ids varchar(255))')
    cursor.execute('truncate table sscat_products')
    cursor.execute("insert into sscat_products (sub_sub_category, product_ids) select sub_sub_category, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where sub_sub_category is not null and recommendable is true group by sub_sub_category having count(*) > 1 order by sub_sub_category;")

def create_table_brand_products(cursor):
    """
    Create a table with all unique brands and the first 5 recommendable of that brand

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists brand_products (brand varchar(255) primary key, product_ids varchar(255))')
    cursor.execute('truncate table brand_products')
    cursor.execute("insert into brand_products (brand, product_ids) select brand, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where brand is not null and recommendable is true group by brand having count(*) > 1 order by brand;")

def create_table_category_products(cursor):
    """
    Create a table with all unique categories and the first 5 recommendable of that category

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists category_products (category varchar(255) primary key, product_ids varchar(255))')
    cursor.execute('truncate table category_products')
    cursor.execute("insert into category_products (category, product_ids) select category, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where category is not null and recommendable is true group by category having count(*) > 1 order by category;")

def get_All_categories(cursor):
    """
    The function get_all_categories() wil gather all the unique categories from the table products.

    Parameters:
        Cursor: The cursor will be used to execute the query's
    Return:
        category_values: A list with all the unique categories
    """

    # Here we make the query that we will use in the cursor
    query = f"SELECT DISTINCT category FROM product WHERE category IS not null"

    # Here we use the cursor to execute
    cursor.execute(query)

    category_values = [category[0] for category in cursor.fetchall()]

    return category_values

def get_Five_products_From_Category(cursor,categories):
    """
    The Function get_Five_Products_From_Category() will get 5 products if possible from the database that are discounted.

    Parameters:
        Cursor: The cursor will be used to execute the query's
        categories: 
    
    Return
        category_Product_Values: a dictionary with a key 
    
    """

    # here we create a empty dictionary
    category_Product_Values = {}

    for category in categories:

        # Here we make a query and execute it
        query = f"SELECT _id FROM product WHERE category = '{category}' AND discount = True AND recommendable = True LIMIT 5"
        cursor.execute(query)

        # fetches all the result the cursor have gotten from the query
        result = cursor.fetchall()
        
        # Appends none to the result when the select does not return a total of 5 products to recommend.
        for x in range(len(result),6):
            result.append((None,))

        # Here make a empty list to append the new tuples
        product_id_list = []
        for products in result:
            for product in products:
                # print(product)
                product_id_list.append(product)

        # here we append the tuple full of product ids connected to the key that is the category
        category_Product_Values[category] = tuple(product_id_list)
            
    # Returns the dictionary with the keys and the values that belongs to the key
    return category_Product_Values

def make_Table_Category_Recommendation(cursor):
    """
    The function make_Table_Category_Recommendation() is used to create a table in the database called category_recommendation,
    This table wil be filled with new data in the function insert_Into_Category_recommendation().

    Parameters:
        cursor: The cursor will be used to execute the query's 
        Connection: the connection is used to commit the executes the cursor has done
    Return:
        None
    """

    # Here we write the query to create table
    query = f"""
            DROP TABLE IF EXISTS category_recommendation;
            CREATE TABLE IF NOT EXISTS category_recommendation(
                category varchar(255),
                rec1_product_id varchar(255),
                rec2_product_id varchar(255),
                rec3_product_id varchar(255),
                rec4_product_id varchar(255),
                rec5_product_id varchar(255),
                PRIMARY KEY (category));"""


    # Here we use the cursor to execute the query.
    cursor.execute(query)

def insert_Into_Category_recommendation(cursor,category_product_values):
    """
    The function  insert_Into_Category_recommendation() wil insert the values into the category_recommendation table
    
    Parameters:
        cursor: The cursor will be used to execute the insert statements
        connection: the connection is used to commit the executes the cursor has done
        category_product_values: 
    return:
        None:
    """

    # Here we loop trough all the values and keys to make a query.
    for k,v in category_product_values.items():

        # Here we create the query that we wil use in the cursor execute
        query = f"""
                INSERT INTO category_recommendation (
                    category, 
                    rec1_product_id, 
                    rec2_product_id, 
                    rec3_product_id, 
                    rec4_product_id, 
                    rec5_product_id) 
                    VALUES ('{k}','{v[0]}','{v[1]}','{v[2]}','{v[3]}','{v[4]}')"""
        
        # Here we execute the query
        cursor.execute(query)


def get_Most_Recommended_Sub_Category_For_User_id(cursor):
    """
    The function get_Most_Recommended_Sub_Category_For_User_id() gets for every user_id a specific sub_category that has been recommended to them the most.


    Parameters:
        cursor: The cursor that is connected to the database used for queries

    Return:
        result : a List full of tuples that contains the profile_id and the most preferred sub_category and the amount they have looked ad said sub_category
    """

    # The query gets the highest prev recommended sub_category for each specific user id.
    query =  """SELECT temp.user_profile_id, temp.sub_category, temp.total_recommendations
                FROM (
                    SELECT prev_recommended.user_profile_id, product.sub_category, COUNT(*) as total_recommendations,
                    ROW_NUMBER() OVER (PARTITION BY prev_recommended.user_profile_id ORDER BY COUNT(*) DESC) as rn
                    FROM prev_recommended
                    INNER JOIN product ON prev_recommended.product_id = product._id
                    GROUP BY prev_recommended.user_profile_id, product.sub_category
                ) AS temp
                WHERE temp.rn = 1
                ORDER BY temp.user_profile_id, temp.total_recommendations DESC;"""
    
    # Here we execute the query.
    cursor.execute(query)

    # Fetches all the results 
    result = cursor.fetchall()

    #Returns the result
    return result

def get_all_subcategories(cursor):
    """
    The function get_all_subcategories() wil gather all the existing sub_categories and put them in a list.

    Parameters:
        cursor:The cursor that is connected to the database used for queries
    return:
        get_all_sub_categories : A List full of unique sub categories
    """

    # Here we make the query to execute the cursor to get all the sub_category's 
    query =  """SELECT DISTINCT sub_category FROM product WHERE sub_category is not null;"""

    # Here we execute the query 
    cursor.execute(query)

    get_all_sub_categories = [str(sub_category[0]) for sub_category in cursor.fetchall()]

    # This catches the error for the future in the query the little ' in baby's messes up the query to get the products from the sub_category
    for i in range(len(get_all_sub_categories)):
        if get_all_sub_categories[i] == "Baby's en kinderen":
            # Replaces the string
            get_all_sub_categories[i] = 'Baby''s en kinderen'

    return get_all_sub_categories


def get_5Products_From_subcategory(cursor, sub_categories):
    """
    The function get_5Products_From_Subcategory() Gets 5 products or less for each sub category and appends them in a dictionary

    Parameters:
        cursor:The cursor that is connected to the database used for queries
        sub_categories: this is a list with all the existing sub_category's in the database

    return:
        sub_Category_Product_Values: dictionary wit the sub_category as key and the 5 products as values
    """
    # here we create a empty dictionary
    sub_category_product_values = {}

    # Here we loop over all the sub_categories
    for sub_category in sub_categories:

        # Here we make the query
        query = f"SELECT _id FROM product WHERE recommendable = True AND sub_category = '{sub_category}' LIMIT 5"
        # print(query)
        cursor.execute(query,sub_category)

        # fetches all the result the cursor have gotten from the query
        result = cursor.fetchall()
        
        # Appends none to the result when the select does not return a total of 5 products to recommend.
        for x in range(len(result),5):
            result.append(("None",))

        # Here make a empty list to append the new tuples
        product_id_list = []
        for products in result:
            for product in products:
                # print(product)
                product_id_list.append(product)

        # here we append the tuple full of product ids connected to the key that is the category
        sub_category_product_values[sub_category] = tuple(product_id_list)
            
    # Returns the dictionary with the keys and the values that belongs to the key
    return sub_category_product_values


def link_Profile_Id_To_Products(sub_category_product_values,most_recommended_subcategory_profileid):
    """
    The function link_Profile_Id_To_Products() wil link te products that need to be recommended to a specific profile id.
    
    Parameters:
        cursor:The cursor that is connected to the database used for queries
        sub_category_product_values: dict with the sub_category as key and 5 values appended to the key
        most_recommended_subcategory_profileid: list with tuples that contain the profile_id and the sub_category that should be recommended

    return
        linked_profiles: a list with tuples that now contain the profile id linked to products based on the sub_category
    
    """

    # Make a empty dictionary where we wil add all the new values.
    Linked_Profiles = {}

    # print(most_recommended_subcategory_profileid)
    # Loops trough all the profile information and then just looks add the key in the most_recommended_sub_category_profileid and just appends the value found there.
    for profile_info in most_recommended_subcategory_profileid:
        # print(profile_info)

        if profile_info[1] == None:
            # checks if the category is None for a user if so we append a string
            Linked_Profiles[profile_info[0]] = "None"
        # Replaces the string that wil actually go trough a query the little ' hinders the execute queries
        elif profile_info[1] == "Baby's en kinderen":
             Linked_Profiles[profile_info[0]] = sub_category_product_values['Baby''s en kinderen']

        else:
            Linked_Profiles[profile_info[0]] = sub_category_product_values[profile_info[1]]


    # Here we return the linked profiles dictionary
    return Linked_Profiles


def create_profile_recommendation_table(cursor):
    """
    The function create_profile_recommendation_table() creates a table for all the linked profiles to their recommended products to be inserted later.


    Parameters:
        cursor: The cursor will be used to execute the query's 
        Connection: the connection is used to commit the executes the cursor has done
    return
        None:
    """

    query = f"""DROP TABLE IF EXISTS profile_recommendation;
            CREATE TABLE IF NOT EXISTS profile_recommendation(
                profile_id varchar(255),
                rec1_product_id varchar(255),
                rec2_product_id varchar(255),
                rec3_product_id varchar(255),
                rec4_product_id varchar(255),
                rec5_product_id varchar(255),
                PRIMARY KEY (profile_id));"""
    
    # Execute the query
    cursor.execute(query)

def insert_into_profile_recommendation(cursor,linked_profiles):
    """
    The function insert_into_profile_recommendation() is used to insert all the profiles linked to their products into the database table called profile_recommendations


    Parameters:
        cursor: The cursor will be used to execute the query's 
        Connection: the connection is used to commit the executes the cursor has done
        linked_profiles: a dict that has the profile_id linked 
    return
        None:
    
    
    """
        # Here we loop trough all the values and keys to make a query.
    for k,v in linked_profiles.items():
        
        #print(f"Key: {k} Values: {v}")
        # print(v)
        if v == "None":
            v = ("None","None","None","None","None")

    
        query = f"""INSERT INTO profile_recommendation (
                    profile_id, 
                    rec1_product_id, 
                    rec2_product_id, 
                    rec3_product_id, 
                    rec4_product_id, 
                    rec5_product_id) 
                    VALUES ('{k}','{v[0]}','{v[1]}','{v[2]}','{v[3]}','{v[4]}')"""
            
            # Here we execute the query
        cursor.execute(query)

def get_two_users_for_every_SubCategory(cursor,sub_categorys):
    """
    This function will get 2 users for every category this way we dont have to query alot since it already takes along time.
    We wil use this collection of users connect to category 
    
    Parameters:
        cursor: The cursor that is connected to the database used for querys
        sub_categorys: A list full of sub_categorys.
    return:
        users_subCategory: a dict with the subCategory as key and the 2 profile ids as values linked to that sub_category 

    """

    subCategory_Users = {}

    for subCategory in sub_categorys:
        print(subCategory)
        query = f"""SELECT temp.user_profile_id
                    FROM (
                        SELECT prev_recommended.user_profile_id, product.sub_category, COUNT(*) as total_recommendations,
                        ROW_NUMBER() OVER (PARTITION BY prev_recommended.user_profile_id ORDER BY COUNT(*) DESC) as rn
                        FROM prev_recommended
                        INNER JOIN product ON prev_recommended.product_id = product._id
                        GROUP BY prev_recommended.user_profile_id, product.sub_category
                    ) AS temp
                    WHERE temp.rn = 1 and temp.sub_category = '{subCategory}'
				    ORDER BY temp.total_recommendations DESC LIMIT 2;
                """
        
        cursor.execute(query)

        subCategory_Users[subCategory] = cursor.fetchall()

    return subCategory_Users



def create_top_subCategory_Users_Table(cursor):
    """
    This function wil create a table for the 2 users per sub_category.

    Parameters:
        cursor: The cursor will be used to execute the query's 
        Connection: the connection is used to commit the executes the cursor has done
    return:
        None
    """

    query = f"""DROP TABLE IF EXISTS top_subCategory_users;
            CREATE TABLE top_subCategory_users(
                sub_category varchar(255),
                rec1_profile_id varchar(255),
                rec2_profile_id varchar(255),
                PRIMARY KEY (sub_category));"""


    # Execute the query
    cursor.execute(query)

def insert_top_subCategory_Users_Table(cursor,DictOfsubcategoryUsers):

    for k,v in DictOfsubcategoryUsers.items():
        
        print(f"Key: {k} Values: {v}")
        # print(v)
        if len(v) == 0:
            v = [(None,),(None,)]
        elif len(v) == 1:
            v.append((None,))

        query = f"""INSERT INTO top_subCategory_users (
                    sub_category, 
                    rec1_profile_id, 
                    rec2_profile_id ) 
                    VALUES ('{k}','{v[0][0]}','{v[1][0]}')"""
            
            # Here we execute the query
        cursor.execute(query)
        

def content_filtering(cursor):
    """
    The function content_filtering() will run all the necessary functions to be create 1 table in the database with the use of content filtering.

    
    Parameters:
        cursor: The cursor will be used to execute the query's 
        connection: the connection is used to commit the executes the cursor has done
    return:
        None 
    
    """
    # First we get all the unique categories out of the table products
    All_categories = get_All_categories(cursor)
    print('Got cats')
    
    # Next is to get 5 products if they exist in that category based on if they are on sale.
    five_Product_Category_Dict = get_Five_products_From_Category(cursor,All_categories)
    print('Got 5 cats')

    # Next we make the table to inserts the values later on.
    make_Table_Category_Recommendation(cursor)
    print('Made table')
    
    # Here we actually insert all the values in to the table created above
    insert_Into_Category_recommendation(cursor,five_Product_Category_Dict)
    print('Inserted cats')

def collaborative_filtering(cursor):
    """
    The function collaborative_filtering() will run all the functions needed to create a new table based on collaborative_filtering.
    
    Parameters:
        cursor: The cursor will be used to execute the query's 
        connection: the connection is used to commit the executes the cursor has done
    return:
        None
    """

    # first we get all the recommended sub_categories for each unique user and store it in a variable to pass it on to a other function later on.
    recommended_sub_category_for_profile_ids = get_Most_Recommended_Sub_Category_For_User_id(cursor)
    print('Got pref_sub_cat')

    # Here we get all the sub_categories that exists in the database and store it in a variable to pass it to a other function later on.
    sub_categories = get_all_subcategories(cursor)
    print('Got all sub_cats')

    # Here we link all the categories to 5 product ids that can be recommended to a user later on.
    five_products_linked_to_sub_category = get_5Products_From_subcategory(cursor,sub_categories)
    print('Got 5 sub_cats')
    
    # Here we link the  user_ids to there own 5 products depended on the sub_category they have seen the most.
    linked_profiles = link_Profile_Id_To_Products(five_products_linked_to_sub_category,recommended_sub_category_for_profile_ids)
    print('Linked profiles')

    # Here we call a function to create the table for the profile recommendation in the database
    create_profile_recommendation_table(cursor)
    print('Created table')

    # We insert everything in to the profiles
    insert_into_profile_recommendation(cursor,linked_profiles)
    print('Inserted subcats')

    # Here we get 2 users for each sub category to use for comparing.
    twoUsers_For_each_subCategory = get_two_users_for_every_SubCategory(cursor)
    print("2 users for every subCategory collected")

    # Here we create the table to insert all the sub categorys with their 2 user ids
    create_top_subCategory_Users_Table(cursor)
    print("Table Top subcategory users made")

    # Here we insert the actual information in to the table.
    insert_top_subCategory_Users_Table(cursor,twoUsers_For_each_subCategory)
    print("Top sub category users inserted")

