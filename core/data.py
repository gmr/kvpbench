import csv
import hashlib
import logging
import random

def load_csv(csvfile):

    f = csv.DictReader(open(csvfile, "rb"), dialect='excel')
    return f
    
def make_key(fields, data):

    h = hashlib.sha1()
    h.update('%s,%s,%s,%s' % (data[fields[0]],data[fields[1]],data[fields[2]],data[fields[3]]))
    return h.hexdigest()
    
def get_keys(csvfile, size):

    logging.info('Loading key list')
    
    # Load the CSV file
    f = load_csv(csvfile)

    # Loop through the CSV file and append the key value to the list
    key_list = []
    for row in f:
        key_list.append(make_key(f.fieldnames, row))
    
    logging.info('Randomizing key list')

    # Build our list randomly from the original list    
    keys = []
    for x in xrange(0, size):
        keys.append(key_list.pop(random.randint(0, len(key_list) - 1)))

    # Return the list
    return keys