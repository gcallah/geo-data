# """
# All interaction with MongoDB should be through this file!
# We may be required to use a new database at any point.
# """
# import os

# import pymongo as pm

# LOCAL = "0"
# CLOUD = "1"

# SE_DB = 'seDB'

# client = None

# MONGO_ID = '_id'


# def connect_db():
#     """
#     This provides a uniform way to connect to the DB across all uses.
#     Returns a mongo client object... maybe we shouldn't?
#     Also set global client variable.
#     We should probably either return a client OR set a
#     client global.
#     """
#     global client
#     if client is None:  # not connected yet!
#         print('Setting client because it is None.')
#         if os.environ.get('CLOUD_MONGO', LOCAL) == CLOUD:
#             password = os.environ.get('MONGO_PASSWD')
#             if not password:
#                 raise ValueError('You must set your password '
#                                  + 'to use Mongo in the cloud.')
#             print('Connecting to Mongo in the cloud.')
#             client = pm.MongoClient(f'mongodb+srv://gcallah:{password}'
#                                     + '@koukoumongo1.yud9b.mongodb.net/'
#                                     + '?retryWrites=true&w=majority')
#         else:
#             print("Connecting to Mongo locally.")
#             client = pm.MongoClient()
#     return client


# def convert_mongo_id(doc: dict):
#     if MONGO_ID in doc:
#         # Convert mongo ID to a string so it works as JSON
#         doc[MONGO_ID] = str(doc[MONGO_ID])


# def create(collection, doc, db=SE_DB):
#     """
#     Insert a single doc into collection.
#     """
#     print(f'{db=}')
#     return client[db][collection].insert_one(doc)


# def read_one(collection, filt, db=SE_DB):
#     """
#     Find with a filter and return on the first doc found.
#     Return None if not found.
#     """
#     for doc in client[db][collection].find(filt):
#         convert_mongo_id(doc)
#         return doc


# def delete(collection: str, filt: dict, db=SE_DB):
#     """
#     Find with a filter and return on the first doc found.
#     """
#     print(f'{filt=}')
#     del_result = client[db][collection].delete_one(filt)
#     return del_result.deleted_count


# def update(collection, filters, update_dict, db=SE_DB):
#     return client[db][collection].update_one(filters, {'$set': update_dict})


# def read(collection, db=SE_DB, no_id=True) -> list:
#     """
#     Returns a list from the db.
#     """
#     ret = []
#     for doc in client[db][collection].find():
#         if no_id:
#             del doc[MONGO_ID]
#         else:
#             convert_mongo_id(doc)
#         ret.append(doc)
#     return ret


# def read_dict(collection, key, db=SE_DB, no_id=True) -> dict:
#     recs = read(collection, db=db, no_id=no_id)
#     recs_as_dict = {}
#     for rec in recs:
#         recs_as_dict[rec[key]] = rec
#     return recs_as_dict


# def fetch_all_as_dict(key, collection, db=SE_DB):
#     ret = {}
#     for doc in client[db][collection].find():
#         del doc[MONGO_ID]
#         ret[doc[key]] = doc
#     return ret


import os
import pymongo as pm
import urllib.parse

from dotenv import load_dotenv
load_dotenv()

CLOUD = "1"
LOCAL = "0"
DB_NAME = "US_census_county_data"
COL_COUNTIES = "counties"
COL_STATES = "states"

client = None

def connect_db():
    global client
    if client is None:
        if os.environ.get("CLOUD_MONGO", LOCAL) == CLOUD:
            user = os.environ.get("MONGO_USER", "pb2882")
            raw_password = os.environ.get("MONGO_PASSWD")
            if not raw_password:
                raise ValueError("You must set MONGO_PASSWD in env.")
            password = urllib.parse.quote_plus(raw_password)
            uri = f"mongodb+srv://{user}:{password}@cluster0.xjc8gra.mongodb.net/{DB_NAME}?retryWrites=true&w=majority"
            print("Connecting to cloud MongoDB...")
            client = pm.MongoClient(uri, tls=True, serverSelectionTimeoutMS=10000)
        else:
            print("Connecting to local MongoDB...")
            client = pm.MongoClient()
    print("Connected to mongo db")
    return client

def get_collection(col_name):
    return connect_db()[DB_NAME][col_name]

def read_all(col_name):
    return list(get_collection(col_name).find({}, {"_id": 0}))

def read_one(col_name, filt):
    return get_collection(col_name).find_one(filt, {"_id": 0})

def read_as_dict(col_name, key_field="_id"):
    return {doc[key_field]: doc for doc in read_all(col_name)}
