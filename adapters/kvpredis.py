"""
Redis Benchmark Module
"""

import core.data

import json
import logging
import multiprocessing
import random
import redis
import time

import traceback
import sys

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
            self.port = 6379
        
        host = '%s:%i' % (self.host, self.port)

        logging.info('Connecting to Redis')
        self.connection = redis.Redis(host=self.host, port=self.port)
        
    def run(self):

        logging.info('Starting Random Workload')
        bid = core.bench.start('Random Workload')
        failed = 0
        for key in self.keys:
            try:
                x = random.randint(0,100)
                if x < 75:
                    data = self.connection.get(key)
                    if not data:
                        failed += 1
                elif x > 74:
                    data = self.connection.get(key)
                    if data:
                        row = json.loads(data)
                        row['dateDecision'] = '%s KVPUPDATE' % row['dateDecision']
                        self.connection.set(key, json.dumps(row))               
                    else:
                        failed += 1
            except Exception, e:
                logging.error('Exception Received: %s' % e)
        core.bench.end(bid)
        logging.info('Random Workload Failed Queries: %i' % failed)
        self.q.put(core.bench.get())

    def load(self, csvfile):

        # Try and load the datafile
        logging.info('Loading the database from the CSV file')
        csv = core.data.load_csv(csvfile)

        # Loop through and load the data
        x = 0
        try:
            for row in csv:
                pkey = core.data.make_key(csv.fieldnames, row)
                self.connection.set(pkey, json.dumps(row))               
                x += 1
        except Exception, e:
            traceback.print_exc(file=sys.stdout)
            logging.error('Exception Received: %s' % e)
            return False

        logging.info('Loaded %i rows into the database' % x)
        return True