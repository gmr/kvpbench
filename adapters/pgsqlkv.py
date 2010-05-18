"""
PostgreSQL Benchmark Module
"""

import core.data
import hashlib
import json
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

    select = "SELECT * FROM kvpbench_kv WHERE pkey = %%(pkey)s"
    update = "UPDATE kvpbench_kv SET value = %%(value)s WHERE pkey = %%(pkey)s"
    delete = "DELETE FROM kvpbench_kv WHERE pkey = %%(pkey)s"

    logging.info('Starting Random Workload')
    bid = core.bench.start('Random Workload')
    for key in keys:
        try:
            x = random.randint(0,100)
            if x < 50:
                record = cursor.execute(select, {'pkey': key})
            elif x > 49 and x < 91:
                record = cursor.execute(select, {'pkey': key})
                value = json.loads(record['value'])
                record['dateDecision'] = '%s KVPUPDATE' % record['dateDecision']
                cursor.execute(update % {'pkey': key, 'value': json.dumps(record)})
            elif x > 90:
                record = cursor.execute(delete, {'pkey': key})
        except Exception, e:
            logging.error('Exception Received: %s' % e)
    core.bench.end(bid)
    return core.bench.get()


def load(host, port, user, name, password, csvfile):

    # Try and load the datafile
    csv = core.data.load_csv(csvfile)
    
    # Connect to the database
    logging.info('Connecting to the database')
    cursor = connect(host, port, user, name, password)
    try:
        cursor.execute('SELECT * FROM kvpbench_kv LIMIT 1')
        if cursor.rowcount:
            logging.info('Truncating previous data')
            cursor.execute('TRUNCATE kvpbench_kv')
    except psycopg2.ProgrammingError:
        
        # Create the table
        logging.info('Creating table from scratch')
        sql = 'CREATE TABLE kvpbench_kv ( pkey text primary key, value text );'
        logging.debug(sql)
        cursor.execute(sql)    

    # Define our SQL statement
    sql = "INSERT INTO kvpbench_kv VALUES(%(pkey)s, %(value)s)"
    logging.info('Loading data')

    # Loop through and load the data
    try:
        for row in csv:
            pkey = core.data.make_key(csv.fieldnames, row)
            cursor.execute(sql, {'pkey': pkey, 'value': json.dumps(row)})
    except Exception,e:
        logging.error('Load failed: %s' % e)
        return False
    
    logging.info('Load complete')
    return True