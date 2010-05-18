"""
PostgreSQL Benchmark Module
"""

import core.data
import hashlib
import logging
import psycopg2
import random
import time

def connect(host, port, user, name, password):

    if not host:
        host = 'localhost'
    if not port:
        port = 5432

    dsn = "host='%s' port=%i user='%s' dbname='%s'" % (host, int(port), user, name)
    if password:
        dsn = "%s password='%s'" % (dsn, password)

    pgsql = psycopg2.connect(dsn)
    pgsql.set_isolation_level(0)
    cursor = pgsql.cursor()
    return cursor

def bench(host, port, user, name, password, keys):

    cursor = connect(host, port, user, name, password)

    queries = ["SELECT * FROM kvpbench WHERE pkey = '%s'",
               "UPDATE kvpbench SET datedecision = datedecision || ' KVPUPDATE' WHERE pkey = '%s'",
               "DELETE FROM kvpbench WHERE pkey = '%s'"]

    logging.info('Starting Random Workload')
    bid = core.bench.start('Random Workload')
    for key in keys:
        sql = queries[random.randint(0, 2)]
        cursor.execute(sql % key)
    core.bench.end(bid)

    return core.bench.get()


def load(host, port, user, name, password, csvfile):

    # Try and load the datafile
    csv = core.data.load_csv(csvfile)
    
    # Connect to the database
    logging.info('Connecting to the database')
    cursor = connect(host, port, user, name, password)
    try:
        cursor.execute('SELECT * FROM kvpbench LIMIT 1')
        if cursor.rowcount:
            logging.info('Truncating previous data')
            cursor.execute('TRUNCATE kvpbench')
    except psycopg2.ProgrammingError:
        
        # Create the table
        logging.info('Creating table from scratch')
        sql = 'CREATE TABLE kvpbench (pkey TEXT PRIMARY KEY, %s TEXT);' % ( ' TEXT, '.join(csv.fieldnames) )
        logging.debug(sql)
        cursor.execute(sql)    

    # Define our SQL statement
    sql = "INSERT INTO kvpbench VALUES(%%(pkey)s, %%(%s)s)" % ')s,%('.join(csv.fieldnames)
    logging.info('Loading data')

    # Loop through and load the data
    try:
        for row in csv:
            row['pkey'] = core.data.make_key(csv.fieldnames, row)
            cursor.execute(sql, row)
    except Exception,e:
        logging.error('Load failed: %s' % e)
        return False
    
    logging.info('Load complete')
    return True