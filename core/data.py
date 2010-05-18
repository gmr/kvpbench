import csv
import hashlib

def load_csv(csvfile):

    f = csv.DictReader(open(csvfile, "rb"), dialect='excel')
    return f
    
def make_key(fields, data):

    h = hashlib.sha1()
    h.update('%s,%s,%s,%s' % (data[fields[0]],data[fields[1]],data[fields[2]],data[fields[3]]))
    return h.hexdigest()