from mongo_functions import *
from postgre_functions import *

def create_table(postgre_db_cur, postgre_db_conn):
    """
    Creates the "is_recommendable" table in the PostgreSQL database if it doesn't exist.

    Args:
        - postgre_db_cur: the cursor for the PostgreSQL database
        - postgre_db_conn: the connection for the PostgreSQL database

    Returns:
        Nothing
    """
    query = "create table if not exists is_recommendable(product_id varchar(255),	foreign key (product_id) references product(_id));"
    postgre_db_cur.execute(query)
    postgre_db_conn.commit()

def insert_into_table(postgre_db_cur, product):
    """
    Inserts the product into the "is_recommendable" table.

    Args:
        - postgre_db_cur: the cursor for the PostgreSQL database
        - product: the MongoDB product data

    Returns:
        Nothing
    """
    query = f"insert into is_recommendable (product_id) values ('{product['_id']}')"
    postgre_db_cur.execute(query)


if __name__ == "__main__":
    mongo_db = open_mongodb()
    postgre_db_cur, postgre_db_conn = open_postgre()
    create_table(postgre_db_cur, postgre_db_conn)
    products = get_products(mongo_db)
    # Count the amount of products in the MongoDB collection
    product_len = mongo_db['products'].count_documents({})
    # This will be the in-loop percentage of the insertion progress
    done_percentage = None
    # This counts the amount of inserted recommendable products in the PostgreSQL database
    recommendable_counter = 0

    for idx, product in enumerate(products):
        # Since some products do not have a "recommendable" key, they will be skipped using a try/exception block
        try:
            if product['recommendable'] is True:
                insert_into_table(postgre_db_cur, product)
                recommendable_counter += 1
        except:
            print("Product data invalid, skipping to next product")
            continue
        done_percentage = (idx / product_len) * 100
        print(f"Handled product {idx + 1} of {product_len} ({done_percentage:.2f}%)")
    print(f"Inserts complete, inserted {recommendable_counter} products")
    # Commit the changes
    postgre_db_conn.commit()
