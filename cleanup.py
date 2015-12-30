import pymongo
import hashlib
import os
from pymongo import MongoClient
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("nickname", help="the nickname of the location")
parser.add_argument("--collection", help="the name of the mongodb collection")

args = parser.parse_args()

client = MongoClient()
db = client['pydup']

collection_name = args.collection if args.collection else 'test'
collection = db[collection_name]

def clean():

    loc_name = args.nickname
    file_count = 0

    records = list(collection.find({'loc': loc_name, 'to_delete': True}))

    for record in records:
        print('Removing %s' % record['path'])
        os.remove(record['path'])

    result = collection.delete_many({'loc': loc_name, 'to_delete': True})
    print('Removed %s files.' % result.deleted_count);

if __name__ == '__main__':
    clean()
