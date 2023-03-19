# imports
import random
import postgre_functions
import mongo_functions
import price_functions
import fit_data_functions
import csv

def handle_2c_2(mongo_db):
    products = mongo_functions.collect_product_data(mongo_db)
    price_avg = price_functions.get_avg_price(products)
    print(f"The average price of the products is {price_avg} euros")

def handle_2c_3(mongo_db):
    products = mongo_functions.collect_product_data(mongo_db)
    random_product = random.choice(products)
    max_deviated_product = price_functions.find_max_price_deviation_product(random_product, products)
    print(f"The product which price deviates the most from the product {random_product['name']} with the price {round(int(random_product['selling_price'])/100, 2)} is {max_deviated_product['name']} with a price of {round(int(max_deviated_product['selling_price'])/100, 2)} euros")
    

def handle_products(mongo_db, postgre_cursor, postgre_connection):
    mongo_cursor = mongo_functions.get_products(mongo_db)
    done = False
    batch_size = 3000
    while not done:
        item_dicts, done = mongo_functions.batch_handler_products(batch_size, mongo_cursor)
        item_dicts = fit_data_functions.fit_product_data(item_dicts)
        postgre_functions.products_to_postgre(postgre_cursor, item_dicts, postgre_connection)

def handle_profiles(mongo_db, postgre_cursor, postgre_connection):
    mongo_cursor = mongo_functions.get_profiles(mongo_db)
    done = False
    batch_size = 20000
    while not done:
        # print('batch')
        item_dicts, done = mongo_functions.batch_handler_profiles(batch_size, mongo_cursor)
        # print('mid_batch')
        postgre_functions.profiles_to_postgre(postgre_cursor, item_dicts, postgre_connection)

def handle_sessions(mongo_db, postgre_cursor, postgre_connection):
    """
    """
    mongo_cursor = mongo_functions.get_sessions(mongo_db)
    done = False
    batch_size = 10000
    counter =0
    while not done:
        counter += 1
        print(f'start batch {counter}')
        item_dicts, done = mongo_functions.batch_handler_sessions(batch_size, mongo_cursor)
        print('pre_fit batch')
        item_dicts = fit_data_functions.fit_session_data(item_dicts)
        print('post_fit batch')
        postgre_functions.sessions_to_postgre(postgre_cursor, item_dicts, postgre_connection)

def buids_to_csv(cursor):
    """
    """
    with open('buids.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        for record in cursor:
            write_list = []
            write_list.append(str(record['_id']))

            # add buids if exist and not empty
            if 'buids' in record and record['buids'] != None:
                write_list.append(record['buids'])
                writer.writerow(write_list)

def gather_buids(sessions):
    item_lists = []
    print(len(sessions))
    for record in sessions:
        print(record)
        session_id = record[0]
        # MAYBE JUST PUT IT IN RAM SEE WHATSAPP OR SMT
        for buid in record[1]:
            with open('buids.csv') as file:
                reader = csv.reader(file)
                for row in reader:
                    if buid in row[1]:
                        item_lists.append([buid, row[0], session_id])
    print(len(item_lists))

def handle_fill_buid(mongo_db, postgre_cursor, postgre_connection):
    """
    """
    profile_cursor = mongo_functions.get_profiles(mongo_db)
    print('start fill csv')
    # buids_to_csv(profile_cursor)
    print('end fill csv')
    session_cursor = mongo_functions.get_sessions(mongo_db)
    done = False
    batch_size = 10000
    counter = 0
    while not done:
        counter += 1
        print(f'start batch {counter}')
        # GET SESSION_buid BATCH
        item_lists, done = mongo_functions.get_session_buids(batch_size, session_cursor)
        print('pre_gather batch')
        gather_buids(item_lists)
        print('post_gather batch')
        # PUSH BATCH

if __name__ == "__main__":
    # Connect to mongoDB
    mongo_db = mongo_functions.open_mongodb()

    # Connect to postgreDB
    postgre_cursor, postgre_connection = postgre_functions.open_postgre()

    # Calculate average price
    # handle_2c_2(mongo_db)

    # Find product which price deviates most from random given product
    # handle_2c_3(mongo_db)

    # Transfer products
    # handle_products(mongo_db, postgre_cursor, postgre_connection)

    # Transfer profiles
    # handle_profiles(mongo_db, postgre_cursor, postgre_connection)

    # Transfer sessions
    # handle_sessions(mongo_db, postgre_cursor, postgre_connection)

    # Transfer buids
    handle_fill_buid(mongo_db, postgre_cursor, postgre_connection)

    # Save data manipulation in postgre and close postgreDB
    postgre_functions.close_postgre(postgre_cursor, postgre_connection)


    # Get profile data
    # profiles = mongo_functions.collect_profile_data(mongo_db)

    # Get session data
    # sessions = mongo_functions.collect_session_data(mongo_db)    

    # Transfer profile data to postgreDB (profile data already fit for relational database)
    # postgre_functions.profiles_to_postgre(prostgre_cursor, profiles, postgre_connection)

    # Transfer session data to postgreDB
    # sessions = fit_data_functions.fit_session_data(sessions)
    # postgre_functions.sessions_to_postgre(prostgre_cursor, sessions, postgre_connection)