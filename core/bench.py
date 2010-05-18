import time
import uuid

_timings = {} 

def start(description = 'Benchmark'):
    bid = uuid.uuid4().hex
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
        key = timings[timing]['description']
        if not output.has_key(key):
            output[key] = {'samples': 0, 'min': 10000000, 'max': 0, 'avg': 0, 'durations': []}
        output[key]['samples'] += 1
        output[key]['durations'].append(timings[timing]['duration'])
        if timings[timing]['duration'] < output[key]['min']:
            output[key]['min'] = timings[timing]['duration']
        if timings[timing]['duration'] > output[key]['max']:
            output[key]['max'] = timings[timing]['duration']
        
    for category in output:
        t = sum(output[category]['durations'])
        a = t / len(output[category]['durations'])
        output[category]['avg'] = '%.5f' % a
        output[category]['max'] = '%.5f' % output[category]['max']
        output[category]['min'] = '%.5f' % output[category]['min']
        del(output[category]['durations'])
        
    return output    
    