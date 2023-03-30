# imports
import create_functions
import postgre_functions

# only run the following if this script is directly ran
if __name__ == '__main__':
    # Connect to postgreDB
    cursor, connection = postgre_functions.open_postgre()

    # create and fill table most_recommended
    create_functions.create_table_most_recommended(cursor)

    # create and fill row top_category in table user_profile
    # create_functions.create_row_top_category(cursor)

    # create and fill table top_category_product
    # create_functions.create_table_top_category_product(cursor)

    # create and fill table top_group_product
    create_functions.create_table_top_group_product(cursor)

    # save changes and close postgre connection safely
    postgre_functions.close_postgre(cursor, connection)