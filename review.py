import pymongo
import argparse
import csv
from pymongo import MongoClient


parser = argparse.ArgumentParser()
parser.add_argument("--outfile", help="the name of the output file")
parser.add_argument("--minsize", help="exclude files smaller than minsize bytes", type=int)
parser.add_argument("--collection", help="the name of the mongodb collection")
args = parser.parse_args()

client = MongoClient()
db = client['pydup']

# set minimum filesize for inspection
min_file_size = args.minsize if args.minsize else 1000

# set the name of the output file
outfile = args.outfile if args.outfile else 'review_me.csv'

# set collection name
collection_name = args.collection if args.collection else 'test'
collection = db[collection_name]

def review():

    # create aggegation pipeline
    pipeline = [
        {"$group": {
            "_id": "$md5",
            "uniqueIds": {
                "$addToSet": {
                    "_id": "$_id",
                    "name": "$name",
                    "loc": "$loc",
                    "size": "$size",
                    "path": "$path"
                }
            },
            "count": {
                "$sum": 1
            }
        }},
        {"$match": {
            "count" : {
                "$gte": 2
            },
            "uniqueIds": {
                "$elemMatch": {
                    "size": {
                        "$gte": min_file_size
                    }
                }
            }
        }},
        {"$sort": {"count": -1}}
    ]

    # run query
    results = list(collection.aggregate(pipeline, allowDiskUse=True))

    # write to file
    with open(outfile, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['ObjectId', 'Location', 'Name', 'Size (b)', 'Path'])

        for result in results:
            for item in result['uniqueIds']:
                writer.writerow([item.get('_id'), item.get('loc', ''), item.get('name', ''), item.get('size', ''), item.get('path', '')])

    print('Review file created (%s). Please open it and remove the lines corresponding to files you wish to keep.' % outfile)

if __name__ == '__main__':
    review()
