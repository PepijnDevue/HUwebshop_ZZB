def get_avg_price(products):
    """
    calculates the average price from a list of products

    products: a list of dicts, every dict is one product record
    returns: the average price from the list in euros
    """
    total = 0
    n = 0
    for record in products:
        total += int(record['selling_price'])
        n += 1
    # calculate result in euros, total refers to amount of cents
    result = round(total / n / 100, 2)
    return result


def find_max_price_deviation_product(given_product, products):
    """
    finds the product which price deviates the most from the price of a given product
    
    product: the given product, dict
    products: the given list of products, list of dicts
    returns: the product that deviates most from the given product according to price, dict
    """
    compare_price = given_product['selling_price']
    max_deviated_price = 0
    # check what the deviations is for each product
    for product in products:
        product_price = product['selling_price']
        # if the current deviation is higher than those before, save it
        if abs(compare_price - product_price) > max_deviated_price:
            max_deviated_product = product
            max_deviated_price = product_price
    return max_deviated_product