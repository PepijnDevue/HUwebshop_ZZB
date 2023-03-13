def fit_product_data(products):
    """
    changes product data so it fits in the relational database

    products: list of dicts, each dict is one product record
    returns: list of dicts, fitted product data
    """
    for product in products:
        # change key names to unify them (remove dutch or american names eg.) only if keys present
        if 'color' in product:
            product['colour'] = product.pop('color')
        if 'leeftijd' in product:
            product['age'] = product.pop('leeftijd')
        if 'serie' in product:
            product['series'] = product.pop('serie')
        if 'type' in product:
            product['product_type'] = product.pop('type')

        # change more key names and value types
        product['product_name'] = product.pop('name')
        product['price'] = int(product.pop('selling_price'))
        product['target_group'] = product.pop('doelgroep', None)
        product['fast_mover'] = bool(product.get('fast_mover', False))
        product['discount'] = bool(product.get('discount', False))

    return products   