"""
MongoDB Benchmark Module
"""

import core.data
import hashlib
import logging
import multiprocessing
import pymongo
import random
import time

class Benchmark(multiprocessing.Process):

    def __init__(self, host, port, keys = None, q = None):

        super(self.__class__, self).__init__()    

        self.keys = keys
        self.q = q

        # Database connection settings
        if host:
            self.host =  host
        else:
            self.host = 'localhost'

        if port:
            self.port = port
        else:
            self.port = 27017         
        
        # Connect to the database
        self.connect()
        
    def connect(self):
        logging.info('Connecting to MongoDB')
        self.connection = pymongo.Connection(self.host, self.port)
        self.database = self.connection.kvpbench
        self.entries = self.database.entries
    
    def run(self):

        logging.info('Starting Random Workload')
        bid = core.bench.start('Random Workload')
        failed = 0
        for key in self.keys:
    
            try:
                x = random.randint(0,100)
                if x < 75:
                    record = self.entries.find_one({'pkey': key})
                    if not record:
                        failed += 1
                elif x > 74:
                    record = self.entries.find_one({'pkey': key})
                    if record:
                        record['dateDecision'] = '%s KVPUPDATE' % record['dateDecision']
                        self.entries.update({'pkey': key}, record)
                    else:
                        failed += 1
                    pass
            except Exception, e:
                logging.error('Exception Received: %s' % e)
        core.bench.end(bid)
        logging.info('Random Workload Failed Queries: %i' % failed)
        self.q.put(core.bench.get())

    def load(self, csvfile):
    
        # Try and load the datafile
        csv = core.data.load_csv(csvfile)
        
        self.database.entries.ensure_index("pkey", unique=True)
        logging.info('Loading the database from the CSV file')
    
        # Loop through and load the data
        x = 0
        try:
            for row in csv:
                row['pkey'] = core.data.make_key(csv.fieldnames, row)
                self.entries.insert(row)
                x += 1
        except Exception,e:
            logging.error('Load failed: %s' % e)
            return False
    
        logging.info('Loaded %i rows into the database' % x)
        return True