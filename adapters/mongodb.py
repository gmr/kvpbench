"""
MongoDB Benchmark Module
"""

import core.data
import hashlib
import logging
import pymongo
import random
import time


def connect(host, port):

    if not host:
        host = 'localhost'
    if not port:
        port = 27017

    connection = pymongo.Connection(host, port)
    database = connection.kvpbench
    return database

def bench(host, port, keys):

    database = connect(host, port)
    entries = database.entries

    logging.info('Starting Random Workload')
    bid = core.bench.start('Random Workload')
    for key in keys:

        try:
            x = random.randint(0,100)
            if x < 50:
                record = entries.find_one({'pkey': key})
            elif x > 49 and x < 91:
                record = entries.find_one({'pkey': key})
                if record:
                    record['dateDecision'] = '%s KVPUPDATE' % record['dateDecision']
                    entries.update({'pkey': key}, record)
            elif x > 90:
                record = entries.remove({'pkey': key})
        except Exception, e:
            logging.error('Exception Received: %s' % e)
        
    core.bench.end(bid)
    return core.bench.get()

def load(host, port, csvfile):

    # Try and load the datafile
    csv = core.data.load_csv(csvfile)
    
    # Connect to the database
    logging.info('Connecting to the database')
    database = connect(host, port)
    entries = database.entries
    database.entries.create_index("pkey", unique=True)

    # Loop through and load the data
    try:
        for row in csv:
            row['pkey'] = core.data.make_key(csv.fieldnames, row)
            entries.insert(row)
            
    except Exception,e:
        logging.error('Load failed: %s' % e)
        return False
    
    logging.info('Load complete')
    return True