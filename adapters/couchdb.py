"""
CouchDB Benchmark Module
"""

import core.data
import hashlib
import httplib2
import json
import logging
import multiprocessing
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
            self.port = 5984

        self.base_url = 'http://%s:%i/kvpbench/' % (self.host, int(self.port))

    def run(self):

        logging.info('Starting Random Workload')
        http = httplib2.Http()
        bid = core.bench.start('Random Workload')
        failed = 0
        for key in self.keys:
            try:
                x = random.randint(0,100)
                if x < 75:
                    url = '%s%s' % (self.base_url, key)
                    response, content = http.request(url, 'GET')
                    if response['status'] == '404':
                        print response
                        x += 1
                elif x > 74:
                    url = '%s%s' % (self.base_url, key)
                    response, content = http.request(url, 'GET')
                    if response['status'] == '200':
                        record = json.loads(content)
                        record['dateDecision'] = '%s KVPUPDATE' % record['dateDecision']
                        response, content = http.request(url, 'PUT', body=json.dumps(record))
                        if response['status'] not in ['201', '409']:
                            failed += 1
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

        logging.info('Making sure we have a database defined')
        http = httplib2.Http()
        response, content = http.request(self.base_url, 'PUT')
        if response['status'] != '201':
            e = json.loads(content)
            if e['error'] != 'file_exists':
                logging.error('Load failed: %s' % content)
                return False

        logging.info('Loading the database from the CSV file')

        # Loop through and load the data
        x = 0
        try:
            for row in csv:
                pkey = core.data.make_key(csv.fieldnames, row)
                url = '%s%s' % (self.base_url, pkey)
                response, content = http.request(url, 'PUT', body=json.dumps(row))
                if response['status'] != '201':
                    x += 1
        except Exception, e:
            logging.error('Exception Received: %s' % e)
            return False

        logging.info('Loaded %i rows into the database' % x)
        return True