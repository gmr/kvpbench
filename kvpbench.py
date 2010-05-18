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

import csv
import getpass
import logging
import multiprocessing
import optparse
import sys
import time

databases = ['cassandra', 'couchdb', 'mongodb', 'pgsql', 'redis', 'tokyotyrant']


def main():
    
    usage = "usage: %s -c <configfile> [options]" % __appname__
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
        
    if options.load and not options.data:
        print "\nError: you must specify a data file to load (hint: look in data dir)\n"
        parser.print_help()
        sys.exit()

    # Get our parameters for pgsql
    if options.dbtype == 'pgsql':
        print 'PostgreSQL Connection Information'
        
        args = {}
        args['host'] = raw_input('Host: ')
        args['port'] = raw_input('Port: ')
        args['name'] = raw_input('Database: ')
        args['user'] = raw_input('User: ')
        args['password'] = getpass.getpass('Password: ')

        # Import our pgsql adapter
        import adapters.pgsql as adapter

    if options.load:
        args['csvfile'] = options.data
        adapter.load(**args)


    # Create our sub-processes
    if options.bench:

        # Queue for sharing data
        q = multiprocessing.Queue()

        # Loop through and spawn sub-processes to do the actual benching
        for p in xrange(0, options.threads):
            args['q'] = q
            p = multiprocessing.Process(target=adapter.bench, kwargs=args)
            p.start()
            p.join()

        while not q.empty():
            print q.get()

if __name__ == '__main__':
    main()