'''
Created on Oct 11, 2012

@author: jluker
'''

import sys
import select
from datetime import datetime

from adsdata.session import get_session

import logging
log = logging.getLogger(__name__)
        
def init_logging(logfile, verbose=False, debug=False):
    logfile = logfile + "." + datetime.now().strftime("%Y%m%d-%H%M%S")
    logging.basicConfig(
        filename = logfile, 
        level = logging.INFO,
        format = '%(asctime)s %(levelname)s %(message)s'
    )
    log = logging.getLogger()
    log.debug("logging to %s" % logfile)
    if verbose:
        log.addHandler(logging.StreamHandler(sys.stdout))
        log.debug("logging to stdout")
    if debug:
        log.setLevel(logging.DEBUG)
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(thread)d %(filename)s %(lineno)d %(message)s')
        for h in log.handlers:
            h.setFormatter(fmt)
        log.debug("debug level logging enabled")
    return log

def has_stdin():
    return select.select([sys.stdin],[],[],0.0)[0] and True or False

def map_reduce_listify(source, target_collection_name, key_field, value_field):
    from bson.code import Code

    session = get_session()
    
    map_func = Code("function(){ " \
                + "emit( this.%s, { '%s': [this.%s] } ); " % (key_field, value_field, value_field) \
            + "};")

    reduce_func = Code("function( key , values ){ " \
                + "var ret = { '%s': [] }; " % value_field \
                + "for ( var i = 0; i < values.length; i++ ) " \
                    + "ret['%s'].push.apply(ret['%s'],values[i]['%s']); " % (value_field, value_field, value_field) \
                + " return ret;" \
            + "};")

    log.info("running map-reduce on %s" % source.name)
    source.map_reduce(map_func, reduce_func, target_collection_name)
    
    target = session.get_collection(target_collection_name)
    log.info("cleaning up target collection: %s" % target_collection_name)
    target.update({}, {'$rename': {('value.' + value_field) : value_field}}, multi=True)
    target.update({}, {'$unset': { 'value': 1 }}, multi=True)
    source.drop()
    
