# Imports
import psycopg2
from psycopg2.extras import execute_batch

def make_Connection_To_DB():
    """
    Opens up a connection to the database

    Parameters:
        None

    Returns:
        connection: the connection varible to the database
    """
    # Save the database name in a varible
    db_name = 'huwebshop'

    # Gets the password from a gitignore file
    password_file = open('password.txt')
    password = password_file.readline()

    # create a connection
    connection = psycopg2.connect(f'dbname={db_name} user=postgres password={password}')

    # Here we return the connection
    return connection

def make_Cursor(db_Connection):
    """
    The function make_cursor() is used to make a cursor connected to a data base and is used to execute query's

    Parameters:
        db_connection: Connection to the database that is needed to make the cursor

    Returns:
        cursor: the cursor wil be used to execute query's with the database
    """

    # Creates a cursor to be used in querry_handler.py
    cursor = db_Connection.cursor()
    
    # Returns the cursor
    return cursor

def get_All_Categorys(cursor):
    """
    The function get_all_Categorys() wil gather all the unique categorys from the table products.

    Parameters:
        Cursor: The cursor will be used to execute the query's
    Return:
        category_values: A list with all the unique categorys
    """

    # Here we make the query that we will use in the cursor
    query = f"SELECT DISTINCT category FROM product WHERE category IS not null"

    # Here we use the cursor to execute
    cursor.execute(query)

    category_values = [category[0] for category in cursor.fetchall()]

    return category_values

def get_Five_Producst_From_Category(cursor,categorys):
    """
    The Function get_Five_Products_From_Category() will get 5 products if possible from the database that are discounted.

    Parameters:
        Cursor: The cursor will be used to execute the query's
        categorys: 
    
    Return
        category_Product_Values: a dictonary with a key 
    
    """

    # here we create a empty dictonary
    category_Product_Values = {}

    for category in categorys:

        # Here we make a querry and execute it
        query = f"SELECT _id FROM product WHERE category = '{category}' AND discount = True LIMIT 5"
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
            
    # Returns the dictonary with the keys and the values that belongs to the key
    return category_Product_Values

def make_Table_Category_Recommendation(cursor,connection):
    """
    The function make_Table_Category_Recommendation() is used to creat a table in the database called category_recommendation,
    This table wil be filled with new data in the function insert_Into_Category_recomendation().

    Parameters:
        cursor: The cursor will be used to execute the query's 
        Connection: the connection is used to commit the executes the cursor has done
    Return:
        None
    """

    # Here we write the query to create table
    query = f"""
            DROP TABLE IF EXISTS category_recomendation;
            CREATE TABLE category_recomendation(
                category varchar(255),
                rec1_product_id varchar(255),
                rec2_product_id varchar(255),
                rec3_product_id varchar(255),
                rec4_product_id varchar(255),
                rec5_product_id varchar(255),
                PRIMARY KEY (category));"""


    # Here we use the cursor to execute the query.
    cursor.execute(query)

    # Here we make sure to commit to the connection so that the executed query actually gets put into the database
    connection.commit()

def insert_Into_Category_recomendation(cursor,connection,category_product_values):
    """
    The function  insert_Into_Category_recomendation() wil insert the values into the category_recemendation tabel
    
    Parameters:
        cursor: The cursor will be used to execute the insert statments
        connection: the connection is used to commit the executes the cursor has done
        category_product_values: 
    return:
        None:
    """

    # Here we loop trough all the values and keys to make a query.
    for k,v in category_product_values.items():

        # Here we create the query that we wil use in the cursor execute
        query = f"""
                INSERT INTO category_recomendation (
                    category, 
                    rec1_product_id, 
                    rec2_product_id, 
                    rec3_product_id, 
                    rec4_product_id, 
                    rec5_product_id) 
                    VALUES ('{k}','{v[0]}','{v[1]}','{v[2]}','{v[3]}','{v[4]}')"""
        
        # Here we execute the query
        cursor.execute(query)

    # Commit  the executes made by the cursor.
    connection.commit()



def get_Most_Recommended_Sub_Category_For_User_id(cursor):
    """
    The function get_Most_Recommended_Sub_Category_For_User_id() gets for every user_id a specific sub_categroy that has been recommended to them the most.


    Paremeters:
        cursor: The cursor that is connected to the database used for querys

    Return:
        result : a List full of tuples that containt the profile_id there preferenced sub_category and the amount they have looked ad said sub_category
    """

    # query source : Alot of help from ChatGpt
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
    
    # Here we execute the querry.
    cursor.execute(query)

    # Fetches all the results 
    result = cursor.fetchall()

    #Returns the result
    return result

def get_all_subcategorys(cursor):
    """
    The function get_all_subcategorys() wil gather all the existing sub_categorys and put them in a list.

    Paremeters:
        cursor:The cursor that is connected to the database used for querys
    return:
        get_all_sub_categorys : A List full of unique sub categorys
    """

    # Here we make the querry to execute the cursor to get all the sub_category's 
    query =  """SELECT DISTINCT sub_category FROM product WHERE sub_category is not null;"""

    # Here we execute the query 
    cursor.execute(query)

    get_all_sub_categorys = [str(sub_category[0]) for sub_category in cursor.fetchall()]

    # This catches the error for the future in the query the littel ' in baby's messes up the query to get the prodcuts from the sub_category
    for i in range(len(get_all_sub_categorys)):
        if get_all_sub_categorys[i] == "Baby's en kinderen":
            # Replaces the string
            get_all_sub_categorys[i] = 'Baby''s en kinderen'

    return get_all_sub_categorys

def get_5Products_From_subcategory(cursor, sub_Categorys):
    """
    The function get_5Products_From_Subcategory() Gets 5 products or less for each sub category and appends them in a dictonary

    Paremeters:
        cursor:The cursor that is connected to the database used for querys
        sub_Categorys: this is a list with all the existing sub_category's in the database

    return:
        sub_Category_Product_Values: dictonary wit the sub_category as key and the 5 products as values
    """
    # here we create a empty dictonary
    sub_Category_Product_Values = {}

    # Here we loop over all the sub_categorys
    for sub_category in sub_Categorys:

        # Here we make the query
        query = f"SELECT _id FROM product WHERE sub_category = '{sub_category}' LIMIT 5"
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
        sub_Category_Product_Values[sub_category] = tuple(product_id_list)
            
    # Returns the dictonary with the keys and the values that belongs to the key
    return sub_Category_Product_Values


def link_Profile_Id_To_Products(sub_category_product_values,most_recommended_subcategory_profileid):
    """
    The function link_Profile_Id_To_Products() wil link te products that need to be recommended to a specific profile id.
    
    Parameters:
        cursor:The cursor that is connected to the database used for querys
        sub_category_product_values: dict with the sub_category as key and 5 values appended to the key
        most_recommended_subcategory_profileid: list with tupels that contain the profile_id and the sub_category that should be recommended

    return
        linked_profiles: a list with tupels that now contain the profile id linked to products based on the sub_category
    
    """

    # Make a empty dictonary where we wil add all the new values.
    Linked_Profiles = {}

    # print(most_recommended_subcategory_profileid)
    # Loopes trough all the profile information and then just looks add the key in the most_recommended_sub_category_profileid and just appends the value found there.
    for profile_info in most_recommended_subcategory_profileid:
        # print(profile_info)

        if profile_info[1] == None:
            # checks if the category is None for a user if so we append a string
            Linked_Profiles[profile_info[0]] = "None"
        # Replaces the string that wil actually go trough a querry the littel ' hinders the execute querys
        elif profile_info[1] == "Baby's en kinderen":
             Linked_Profiles[profile_info[0]] = sub_category_product_values['Baby''s en kinderen']

        else:
            Linked_Profiles[profile_info[0]] = sub_category_product_values[profile_info[1]]


    # Here we return the linked profiles dictonary
    return Linked_Profiles


def create_profile_recommendation_table(cursor,connection):
    """
    The function create_profile_recommendation_table() creates a table for all the linked profiles to there recomended products to be inserte later.


    Parameters:
        cursor: The cursor will be used to execute the query's 
        Connection: the connection is used to commit the executes the cursor has done
    return
        None:
    """

    query = f"""DROP TABLE IF EXISTS profile_recommendation;
            CREATE TABLE profile_recommendation(
                profile_id varchar(255),
                rec1_product_id varchar(255),
                rec2_product_id varchar(255),
                rec3_product_id varchar(255),
                rec4_product_id varchar(255),
                rec5_product_id varchar(255),
                PRIMARY KEY (profile_id));"""
    
    # Execute the query
    cursor.execute(query)
    # Commit the cursor query
    connection.commit()

def insert_into_profile_recommendation(cursor,connection,linked_profiles):
    """
    The function insert_into_profile_recommendation() is used to insert all the profiles linked to their products into the database table called profile_recomendations


    Parameters:
        cursor: The cursor will be used to execute the query's 
        Connection: the connection is used to commit the executes the cursor has done
        linked_profiles: a dict that has the profile_id linked 
    return
        None:
    
    
    """
        # Here we loop trough all the values and keys to make a query.
    for k,v in linked_profiles.items():
        
        print(f"Key: {k} Values: {v}")
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
        
        # print(query)
        # Here we execute the query

    # Commit  the executes made by the cursor.
    connection.commit()
