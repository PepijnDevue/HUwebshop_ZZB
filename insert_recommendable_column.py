from mongo_functions import *
from postgre_functions import *


def add_escapechar(product_id):
    """
    Adds an escape character to a single quote character in a product id to avoid errors.

    Args:
        - product_id: the product id

    Returns:
        product_id: string with an escape character added to it
    """
    idx = product_id.find("'")
    product_id = product_id[:idx] + "'" + product_id[idx:]
    return product_id
def create_new_column(postgre_db_conn, postgre_db_cur):
    """
        Creates a new boolean column named 'recommendable' in the 'product' table in the specified PostgreSQL database.

        Args:
            - postgre_db_conn: connection to the PostgreSQL database
            - postgre_db_cur: cursor object for executing SQL statements

        Returns:
            None
    """
    query = "ALTER TABLE product ADD COLUMN IF NOT EXISTS recommendable boolean;"
    postgre_db_cur.execute(query)
    postgre_db_conn.commit()

def update_table(postgre_db_cur, product_id, recommendable):
    """
    Inserts the product into the "is_recommendable" table.

    Args:
        - postgre_db_cur: the cursor for the PostgreSQL database
        - product: the MongoDB product data
        - recommendable: whether the product is recommendable or not

    Returns:
        Nothing
    """
    # SQL will throw an error because of the ' in an id, so we have to alter the _id by adding an escape character to it
    if "'" in product_id:
        product_id = add_escapechar(product_id)
    query = f"UPDATE product SET recommendable = {recommendable} WHERE _id = '{product_id}';"
    postgre_db_cur.execute(query)


if __name__ == "__main__":
    mongo_db = open_mongodb()
    postgre_db_cur, postgre_db_conn = open_postgre()
    products = get_products(mongo_db)
    # Count the amount of products in the MongoDB collection
    product_len = mongo_db['products'].count_documents({})
    # This will be the in-loop percentage of the insertion progress
    done_percentage = None
    # This counts the amount of inserted recommendable products in the PostgreSQL database
    recommendable_counter = 0

    # Creates the recommendable column to the product table if it doesn't exist
    create_new_column(postgre_db_conn, postgre_db_cur)

    for idx, product in enumerate(products):
        if 'recommendable' not in product:
            update_table(postgre_db_cur, product['_id'], False)
            continue
        recommendable = product['recommendable']
        update_table(postgre_db_cur, product['_id'], recommendable)
        done_percentage = (idx / product_len) * 100
        print(f"Inserted product {idx + 1} of {product_len} ({done_percentage:.2f}%)")
    print("Success")
    # Commit the changes
    postgre_db_conn.commit()
