import time
import uuid

_timings = {} 

def start(bid, description = 'Benchmark'):
   
    # Use /dev/random because uuid.uuid4 is broken on the mac using multiprocessing
    f = open('/dev/random', 'rb')
    b = f.read(16)
    f.close()
    bid = str(uuid.UUID(bytes=b))

    # Build our initial dictionary
    _timings[bid] = {'description': description, 'start': time.time(), 'end': -1}
    return bid

def end(bid):
    _timings[bid]['end'] = time.time()
    _timings[bid]['duration'] = _timings[bid]['end'] - _timings[bid]['start']

def get():
    return _timings
    
def aggregate(timings):
    
    output = {}
    
    for timing in timings:
        for bid in timing:
            key = timing[bid]['description']
            if not output.has_key(key):
                output[key] = {'samples': 0, 'min': 10000000, 'max': 0, 'avg': 0, 'durations': []}
        output[key]['samples'] += 1
        output[key]['durations'].append(timing[bid]['duration'])
        if timing[bid]['duration'] < output[key]['min']:
            output[key]['min'] = timing[bid]['duration']
        if timing[bid]['duration'] > output[key]['max']:
            output[key]['max'] = timing[bid]['duration']
        
    for category in output:
        t = sum(output[category]['durations'])
        a = t / len(output[category]['durations'])
        output[category]['avg'] = '%.5f' % a
        output[category]['max'] = '%.5f' % output[category]['max']
        output[category]['min'] = '%.5f' % output[category]['min']
        del(output[category]['durations'])
        
    return output    
    