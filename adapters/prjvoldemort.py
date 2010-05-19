"""
Voldemort Benchmark Module
"""

import core.data
import hashlib
import json
import logging
import multiprocessing
import random
import time
import socket
from voldemort import StoreClient

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
            self.port = int(port)
        else:
            self.port = 6666

        self.store = StoreClient('test', [(self.host, self.port)])

    def run(self):

        logging.info('Starting Random Workload')
        bid = core.bench.start('Random Workload')
        failed = 0
        for key in self.keys:
            try:
                x = random.randint(0,100)
                if x < 75:
                    content = self.store.get(key)
                    if not content:
                        failed += 1
                elif x > 74:
                    content = self.store.get(key)
                    if  content:
                        record = json.loads(content)
                        record['dateDecision'] = '%s KVPUPDATE' % record['dateDecision']
                        self.store.put(key, json.dumps(record))
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

        logging.info('Loading the database from the CSV file')

        # Loop through and load the data
        x = 0
        try:
            for row in csv:
                pkey = core.data.make_key(csv.fieldnames, row)
                self.store.put(pkey, json.dumps(row))
                x += 1
        except Exception, e:
            logging.error('Exception Received: %s' % e)
            return False

        logging.info('Loaded %i rows into the database' % x)
        return True