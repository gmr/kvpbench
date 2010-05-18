"""
PostgreSQL Benchmark Module
"""

import psycopg2
import time

def bench(host, port, user, name, password, q):
    q.put({'time': time.time()})

def load(host, port, user, name, password, csvfile):
    print csvfile
    pass