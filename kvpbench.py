#!/usr/bin/env python
"""
KVPBench

A simplistic benchmark utility to compare various NoSQL and RDBMS systems

Copyright (c) 2010, Insider Guides, Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
Neither the name of the Insider Guides, Inc. nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

__author__  = "Gavin M. Roy"
__email__   = "gmr@myyearbook.com"
__date__    = "2010-05-17"
__appname__ = 'kvpbench.py'
__version__ = '0.1'

import core.bench
import getpass
import logging
import optparse
import sys
import multiprocessing
import time

databases = ['cassandra', 'couchdb', 'mongodb', 'pgsql', 'pgsqlkv', 'redis', 'tokyotyrant']
timings = []

def bench_results(results):
    global timings
    timings.append(results)

def main():

    usage = "usage: %s -d datafile [options]" % __appname__
    version_string = "%%prog %s" % __version__
    description = "Database Benchmark Utility"
    
    # Create our parser and setup our command line options
    parser = optparse.OptionParser(usage=usage, version=version_string,
                                   description=description)

    parser.add_option("-t", "--type", action="store", type="string", dest="dbtype", 
                      help="Type of database to benchmark: [%s]" % ', '.join(databases))
    
    parser.add_option("-p", "--processes", action="store", type="int", dest="threads", 
                      default=1, help="Number of concurrent processes to benchmark with")
                      
    parser.add_option("-b", "--bench", action="store_true", dest="bench", default=False,
                      help="Perform the data read/write/update test")                  
    
    parser.add_option("-l", "--load", action="store_true", dest="load", default=False,
                      help="Perform the data load test")                  

    parser.add_option("-o", "--operations", action="store", dest="operations", type="int",
                      default=10000, help="Number of operations to test")

    parser.add_option("-d", "--data", action="store", dest="data",
                      help="Specify the CSV file to load")                  
    
    # Parse our options and arguments
    options, args = parser.parse_args()

    if not options.dbtype or options.dbtype not in databases:
        print "\nError: you must specify a database type\n"
        parser.print_help()
        sys.exit()
        
    if not options.bench and not options.load:
        print "\nError: you must perform a load test or bench test\n"
        parser.print_help()
        sys.exit()
        
    if not options.data:
        print "\nError: you must specify a data file to load (hint: look in data dir)\n"
        parser.print_help()
        sys.exit()

    log_format = "%(asctime)-15s %(message)s"
    logging.basicConfig(format=log_format, level=logging.DEBUG)

    # Get our parameters for mongodb
    if options.dbtype == 'mongodb':
        username = getpass.getuser()
        print 'MongoDB Connection Information (Empty defaults to localhost/27017)'
        
        args = {}
        args['host'] = raw_input('Host: ')
        args['port'] = raw_input('Port: ')
        
        # Import our mongo adapter
        import adapters.mongodb as adapter
        
    # Get our parameters for pgsql
    if options.dbtype == 'pgsql':
        username = getpass.getuser()
        print 'PostgreSQL Connection Information (Empty defaults to localhost/5432/%s/%s)' % (username,username)
        
        args = {}
        args['host'] = raw_input('Host: ')
        args['port'] = raw_input('Port: ')
        args['name'] = raw_input('Database: ')
        if not args['name']:
            args['name'] = username
        args['user'] = raw_input('User: ')
        if not args['user']:
            args['user'] = username
        args['password'] = getpass.getpass('Password: ')
    
        for arg in args:
            if not args[arg]:
                args[arg] = None

        # Import our pgsql adapter
        import adapters.pgsql as adapter

    # Get our parameters for pgsql
    if options.dbtype == 'pgsqlkv':
        username = getpass.getuser()
        print 'PostgreSQL Connection Information (Empty defaults to localhost/5432/%s/%s)' % (username,username)
        
        args = {}
        args['host'] = raw_input('Host: ')
        args['port'] = raw_input('Port: ')
        args['name'] = raw_input('Database: ')
        if not args['name']:
            args['name'] = username
        args['user'] = raw_input('User: ')
        if not args['user']:
            args['user'] = username
        args['password'] = getpass.getpass('Password: ')
    
        for arg in args:
            if not args[arg]:
                args[arg] = None

        # Import our pgsql adapter
        import adapters.pgsqlkv as adapter

    if options.load:
        bid = core.bench.start('Load Test')
        benchmark = adapter.Benchmark(**args)
        if benchmark.load(options.data):
            core.bench.end(bid)
            print 'Load successful:'
            result = core.bench.get()
            print core.bench.aggregate([result])
            
    # Create our sub-processes
    if options.bench:
        
        # Get a queue for the stack of items
        q = multiprocessing.Queue()
        args['q'] = q
        
        # Add our queue and keys
        args['keys'] = core.data.get_keys(options.data, options.operations)

        # Loop through and spawn threads to do the actual benching
        logging.debug('Starting a benchmark pool with %i members' % options.threads)

        # Loop through and run the threads
        threads = []
        for x in xrange(0, options.threads):
            thread = adapter.Benchmark(**args)
            thread.start()
            threads.append(thread)
            
        # Wait for the threads to finish
        while len(threads) > 0:
            x = 0
            remove_thread = []
            for thread in threads:
                if not thread.is_alive():
                    remove_thread.append(x)
            for thread in remove_thread:
                threads.pop(thread)
            
            # Sleep for a second
            time.sleep(1)

        # Get the timing data from the queue
        while not q.empty():
            timings.append(q.get())
                
        # Print the results
        print core.bench.aggregate(timings)

if __name__ == '__main__':
    main()