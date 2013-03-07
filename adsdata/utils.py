'''
Created on Oct 11, 2012

@author: jluker
'''

import sys
import select
import hashlib
import simplejson
from datetime import datetime

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

def mongo_uri(host, port, db=None, user=None, passwd=None):
    if user and passwd:
        uri = "mongodb://%s:%s@%s:%d" % (user, passwd, host, port)
    else:
        uri = "mongodb://%s:%d" % (host, port)
    if db:
        uri += "/%s" % db
    return uri

def get_session():
    from config import config
    from session import DataSession
    uri = mongo_uri(config.MONGO_HOST, config.MONGO_PORT, user=config.MONGO_USER, passwd=config.MONGO_PASSWORD)
    return DataSession(config.MONGO_DATABASE, uri) 

def has_stdin():
    return select.select([sys.stdin],[],[],0.0)[0] and True or False

def doc_digest(doc, hashtype='sha1'):
    """
    generate a digest hash from a 'docs' dictionary
    """
    # remove any 'meta' values
    digest_doc = doc.copy()
    for k in digest_doc.keys():
        if k.startswith('_'):
            del digest_doc[k]
            
    h = hashlib.new(hashtype)
    # sort_keys=True should make this deterministic?
    json = simplejson.dumps(digest_doc, sort_keys=True)
    h.update(json)
    return h.hexdigest()
    
def map_reduce_listify(session, source, target_collection_name, key_field, value_field):
    from bson.code import Code

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
    
