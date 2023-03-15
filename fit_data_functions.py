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

def fit_session_data(sessions):
    """
    changes session data so it fits in the relational database

    products: list of dicts, each dict is one session record
    returns: list of dicts, fitted session data
    """
    for session in sessions:
        # change keys to correct name
        if 'brand' in session:
            session['preferences_brand'] = list(session.pop('brand').keys())[0]
        if 'category' in session:
            session['preferences_category'] = list(session.pop('category').keys())[0]
        if 'gender' in session:
            session['preferences_gender'] = list(session.pop('gender').keys())[0]
        if 'sub_category' in session:
            session['preferences_sub_category'] = list(session.pop('sub_category').keys())[0]
        if 'sub_sub_category' in session:
            session['preferences_sub_sub_category'] = list(session.pop('sub_sub_category').keys())[0]
        if 'promos' in session:
            session['preferences_promos'] = list(session.pop('promos').keys())[0]
        if 'product_type' in session:
            session['preferences_product_type'] = list(session.pop('product_type').keys())[0]
        if 'product_size' in session:
            session['preferences_product_size'] = list(session.pop('product_size').keys())[0]
        
        if 'products' in session:
            product_lst = []
            for product in session['products']:
                product_lst.append(product['id'])
            session['products'] = product_lst

        for i in session:
                print(i, session[i])

