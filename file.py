import pymongo

# create a connection with the client
client = pymongo.MongoClient('mongodb://localhost:27017')

# create a connection with the database
db = client.huwebshop

# create an object for the collection products
profiles_col = db.profiles

# create an object for the collection sessions
sessions_col = db.sessions

profile_cursor = profiles_col.find()

count = 0
for record in profile_cursor:
    # save _id
    profile_id = str(record['_id'])
    
    # save_buids
    if 'buids' in record and record['buids'] != None:
        profile_buids = record['buids']

    for buid in profile_buids:
        session_cursor = sessions_col.find_one({'buid':{"$in": [buid]}})
        # print(session_cursor['buid'][0] == buid, session_cursor['_id'], profile_id)
    count += 1
    if count % 100 == 0:
        print(count)