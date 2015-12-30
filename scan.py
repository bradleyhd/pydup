import pymongo
import hashlib
from pymongo import MongoClient
from os import walk, remove, stat
from os.path import join as joinpath
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("rootdir", help="the root directory to scan")
parser.add_argument("nickname", help="the nickname of the location")
parser.add_argument("--minsize", help="skip indexing of all files smaller minsize bytes", type=int)
parser.add_argument("--bulksize", help="bulk insert records every bulksize files", type=int)
parser.add_argument("--collection", help="the name of the mongodb collection")

args = parser.parse_args()

def scan():

    client = MongoClient()
    db = client['pydup']

    collection_name = args.collection if args.collection else 'test'
    collection = db[collection_name]

    loc_name = args.nickname
    rootdir = args.rootdir
    file_count = 0

    # set minimum filesize for inspection
    min_file_size = args.minsize if args.minsize is not None else 1000

    # set bulk insert size
    bulk_size = args.bulksize if args.bulksize else 10000

    # load filename blacklist
    try:
        with open('ignore_files', 'r') as f:
            filename_blacklist = [line.rstrip('\n') for line in f]
    except EnvironmentError:
        print('No filename blacklist found, skipping.')
        filename_blacklist = []

    # load dirname blacklist
    try:
        with open('ignore_dirs', 'r') as f:
            dirname_blacklist = [line.rstrip('\n') for line in f]
    except EnvironmentError:
        print('No directory blacklist found, skipping.')
        dirname_blacklist = []

    # create record list for bulk inserts
    records = []

    # walk the directory
    for path, dirs, files in walk(rootdir):

        # filter out hidden and blacklisted files and directories
        files = [f for f in files if (not f[0] == '.') and (f not in filename_blacklist)]
        dirs[:] = [d for d in dirs if (not d[0] == '.') and (d not in dirname_blacklist)]

        # for each file
        for filename in files:

            # concatenate the full filepath
            filepath = joinpath(path, filename)

            # attempt to get the size of the file
            try:
                filesize = stat(filepath).st_size
            except FileNotFoundError:
                continue

            # check if filesize is above threshold
            if filesize <= min_file_size:
                continue

            # compute md5 of any matching characteristics
            hashstring = (filename + str(filesize)).encode('utf-8')
            m = hashlib.md5()
            m.update(hashstring)

            # construct the db recrod
            obj = {
                "loc": loc_name,
                "name": filename,
                "size": filesize,
                "path": filepath,
                "md5": m.hexdigest()
            }
            records.append(obj)

            if file_count > 0 and file_count % bulk_size == 0:

                # bulk insert records
                bulk_insert(collection, records)
                records = []

                print('Files scanned %s' % file_count)

            file_count += 1

    bulk_insert(collection, records)

def bulk_insert(collection, records):

    if records:
        collection.insert_many(records)

if __name__ == '__main__':
    scan()
