"""
PostgreSQL Benchmark Module
"""

import core.data
import hashlib
import json
import logging
import psycopg2
import psycopg2.extensions
import random
import multiprocessing
import time

class Benchmark(multiprocessing.Process):

    def __init__(self, host, port, user, name, password, keys = None, q = None):

        super(self.__class__, self).__init__()
        self.keys = keys

        # Database connection settings
        if host:
            self.host = host
        else:
            self.host = 'localhost'

        if port:
            self.port = port
        else:
            self.port = 5432

        self.user = user
        self.dbname = name
        self.password = password
        self.cursor = None

        # Connect to the database
        self.connect()

        # Define our work queue
        self.q = q

    def connect(self):

        dsn = "host='%s' port=%i user='%s' dbname='%s'" % (self.host, int(self.port), self.user, self.dbname)
        if self.password:
            dsn = "%s password='%s'" % (dsn, self.password)

        logging.info('Connecting to the database')
        self.pgsql = psycopg2.connect(dsn)
        self.pgsql.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.pgsql.cursor()

    def run(self):

        select = "SELECT value FROM kvpbench_kv WHERE pkey = '%s'"
        update = "UPDATE kvpbench_kv SET value = %(value)s WHERE pkey = %(pkey)s"

        logging.info('Starting Random Workload')
        bid = core.bench.start('Random Workload')
        failed = 0
        for key in self.keys:
            try:
                x = random.randint(0,100)
                if x < 75:
                    sql = select % key
                    self.cursor.execute(sql)
                    if not self.cursor.rowcount:
                        failed += 1
                        break
                elif x > 74:
                    self.cursor.execute(select % key)
                    record = self.cursor.fetchone()
                    if not record:
                        failed += 1
                    else:
                        value = json.loads(record[0])
                        value['dateDecision'] = '%s KVPUPDATE' % value['dateDecision']
                        self.cursor.execute(update, {'pkey': key, 'value': json.dumps(value)})
                        if not self.cursor.rowcount:
                            failed += 1
            except Exception, e:
                logging.error('Exception Received: %s' % e)
        core.bench.end(bid)
        logging.info('Random Workload Failed Queries: %i' % failed)
        self.q.put(core.bench.get())

    def load(self, csvfile):

        # Try and load the datafile
        csv = core.data.load_csv(csvfile)

        try:
            self.cursor.execute('SELECT * FROM kvpbench_kv LIMIT 1')
            if self.cursor.rowcount:
                logging.info('Truncating previous data')
                self.cursor.execute('TRUNCATE kvpbench_kv')
        except psycopg2.ProgrammingError:

            # Create the table
            logging.info('Creating table from scratch')
            sql = 'CREATE TABLE kvpbench_kv ( pkey text primary key, value text );'
            logging.debug(sql)
            self.cursor.execute(sql)

        # Define our SQL statement
        sql = "INSERT INTO kvpbench_kv VALUES(%(pkey)s, %(value)s)"
        logging.info('Loading data')

        # Loop through and load the data
        x = 0
        try:
            for row in csv:
                pkey = core.data.make_key(csv.fieldnames, row)
                self.cursor.execute(sql, {'pkey': pkey, 'value': json.dumps(row)})
                x += 1
        except Exception,e:
            logging.error('Load failed: %s' % e)
            return False

        logging.info('Loaded %i rows into the database' % x)
        return True