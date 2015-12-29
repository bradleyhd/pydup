import pymongo
import hashlib
from pymongo import MongoClient
from os import walk, remove, stat
from os.path import join as joinpath

client = MongoClient()
db = client['pydup']
collection = db['test1']

rootdir = '/Users/bradley/Github'
file_count = 0

for path, dirs, files in walk(rootdir):

    # filter out hidden files and directories
    files = [f for f in files if not f[0] == '.']
    dirs[:] = [d for d in dirs if not d[0] == '.']

    # create record container for bulk insert
    records = []

    # for each file
    for filename in files:

        filepath = joinpath(path, filename)
        filesize = stat(filepath).st_size
        hashstring = filename + str(filesize)
        hashstring = hashstring.encode('utf-8')
        m = hashlib.md5()
        m.update(hashstring)
        obj = {
            "path": filepath,
            "size": filesize,
            "md5": m.hexdigest()
        }
        records.append(obj)

        if file_count % 1000 == 0:
            print('Files scanned %s' % file_count)
            
        file_count += 1

    if records:
        collection.insert_many(records)
