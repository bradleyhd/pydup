import pymongo
import argparse
import csv
import sys
from pymongo import MongoClient
from bson.objectid import ObjectId

parser = argparse.ArgumentParser()
parser.add_argument("--infile", help="the name of the input file")
parser.add_argument("--collection", help="the name of the mongodb collection")
args = parser.parse_args()

client = MongoClient()
db = client['pydup']

# set the name of the output file
in_file = args.infile if args.infile else 'review_me.csv'

# set collection name
collection_name = args.collection if args.collection else 'test'
collection = db[collection_name]

def mark():

    # create list for ObjectIds to delete
    ids = []

    # read csv file
    try:
        with open(in_file, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in reader:
                if not row[0] == 'ObjectId':
                    ids.append(ObjectId(row[0]))
    except EnvironmentError as e:
        print('Could not open review file, exiting.')
        print(e)
        sys.exit()

    # run query
    bulk = collection.initialize_ordered_bulk_op()
    bulk.find({'_id': {'$in': ids}}).update({'$set': {'to_delete': True}})
    result = bulk.execute()

    print('%s file(s) marked for deletion.' % result['nMatched'])

if __name__ == '__main__':
    mark()
