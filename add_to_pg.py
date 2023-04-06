# imports
import create_rec_tables
import postgre_functions

# only run the following if this script is directly ran
if __name__ == '__main__':
    # Connect to postgreDB
    cursor, connection = postgre_functions.open_postgre()

    create_rec_tables.create_table_brand_products(cursor)
    create_rec_tables.create_table_group_products(cursor)
    create_rec_tables.create_table_series_products(cursor)
    create_rec_tables.create_table_sscat_products(cursor)
    create_rec_tables.create_table_category_products(cursor)
    print('Done with content filter Pepijn')
    create_rec_tables.content_filtering(cursor)
    print('Done with content filter Dave')
    create_rec_tables.collaborative_filtering(cursor)
    print('Done with collab filter Dave')

    # save changes and close postgre connection safely
    postgre_functions.close_postgre(cursor, connection)