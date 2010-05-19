"""
Cassandra Benchmark Module
"""

import core.data

import json
import lazyboy
import logging
import multiprocessing
import random
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
            self.port = 9160
        
        host = '%s:%i' % (self.host, self.port)

        logging.info('Connecting to Cassandra')
        lazyboy.connection.add_pool('Keyspace1', [host])
        
    def run(self):

        logging.info('Starting Random Workload')
        bid = core.bench.start('Random Workload')
        failed = 0
        for key in self.keys:
            try:
                x = random.randint(0,100)
                if x < 75:
                    kvp = lazyboy.record.Record()
                    kvp.load(kvp.make_key(keyspace="Keyspace1", column_family="Standard2", key=key))
                    if not kvp['data']:
                        failed += 1
                elif x > 74:
                    kvp = lazyboy.record.Record()
                    kvp.load(kvp.make_key(keyspace="Keyspace1", column_family="Standard2", key=key))
                    if kvp['data']:
                        row = json.loads(kvp['data'])
                        
                        row['dateDecision'] = '%s KVPUPDATE' % row['dateDecision']
                        kvp.update({'data': json.dumps(row)})
                        kvp.save()
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
                
                # Create our cassandra key
                # Create our cassandra record
                kvp = lazyboy.record.Record()
                kvp.key = kvp.make_key(keyspace="Keyspace1", column_family="Standard2", key=pkey)
                # Update the record
                kvp.update({'data': json.dumps(row)})
                # Save the record
                kvp.save()
                x += 1
        except Exception, e:
            traceback.print_exc(file=sys.stdout)
            logging.error('Exception Received: %s' % e)
            return False

        logging.info('Loaded %i rows into the database' % x)
        return True