def create_table_series_products(cursor):
    """
    Create a table with all unique series and the first 5 recommendable of that series

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists series_products (series varchar(255) primary key, product_ids varchar(1200))')
    cursor.execute("insert into series_products (series, product_ids) select series, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where series is not null and recommendable is true group by series having count(*) > 1 order by series;")

def create_table_group_products(cursor):
    """
    Create a table with all unique target_groups and the first 5 recommendable of that target_group

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists group_products (target_group varchar(255) primary key, product_ids varchar(1200))')
    cursor.execute("insert into group_products (target_group, product_ids) select target_group, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where target_group is not null and recommendable is true group by target_group having count(*) > 1 order by target_group;")

def create_table_sscat_products(cursor):
    """
    Create a table with all unique sub_sub_categories and the first 5 recommendable of that sub_sub_category

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists sscat_products (sub_sub_category varchar(255) primary key, product_ids varchar(1200))')
    cursor.execute("insert into sscat_products (sub_sub_category, product_ids) select sub_sub_category, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where sub_sub_category is not null and recommendable is true group by sub_sub_category having count(*) > 1 order by sub_sub_category;")

def create_table_brand_products(cursor):
    """
    Create a table with all unique brands and the first 5 recommendable of that brand

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists brand_products (brand varchar(255) primary key, product_ids varchar(1200))')
    cursor.execute("insert into brand_products (brand, product_ids) select brand, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where brand is not null and recommendable is true group by brand having count(*) > 1 order by brand;")

def create_table_category_products(cursor):
    """
    Create a table with all unique categories and the first 5 recommendable of that category

    Args:
        cursor (psycopg2 cursor): The postgreSQL cursor for the database
    """
    cursor.execute('create table if not exists category_products (category varchar(255) primary key, product_ids varchar(1200))')
    cursor.execute("insert into category_products (category, product_ids) select category, array_to_string(array(SELECT unnest(array_agg(_id)) LIMIT 5), ', ') as product_ids from product where category is not null and recommendable is true group by category having count(*) > 1 order by category;")