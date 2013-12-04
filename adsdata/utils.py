'''
Created on Oct 11, 2012

@author: jluker
'''

import os
import sys
from jinja2 import Template
from datetime import datetime
import ConfigParser


import logging
log = logging.getLogger(__name__)
        
def init_logging(base_dir, calling_script, verbose=False, debug=False):

    log_dir = os.path.exists(base_dir + "/logs") and base_dir + "/logs" or "."
    logfile = "%s/%s" % (log_dir, os.path.basename(calling_script)) + "." + datetime.now().strftime("%Y%m%d-%H%M%S")

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

def load_config(config_file):
    """
    NOTE: since this returns a dict of config values, it's conceivable that code
    could change these config values. DO NOT DO THIS!
    """
    config = ConfigParser.ConfigParser()
    # otherwise the config parser lowercases setting names
    config.optionxform = str
    config.read(config_file)
    items = []
    for k, v in config.items('adsdata'):
        if isinstance(v, str) and v.isdigit():
            items.append((k,int(v)))
        else:
            items.append((k,v))
    items.append(('collections', dict(config.items('collections'))))
    return dict(items)

def commandList():
    """
    decorator that allows scripts to register functions to be used as script commands
    """
    registry = {}
    def registrar(func):
        registry[func.__name__] = func
        return func
    registrar.map = registry
    return registrar

def mongo_uri(host, port, db=None, user=None, passwd=None):
    if user and passwd:
        uri = "mongodb://%s:%s@%s:%d/%s" % (user, passwd, host, port, db)
    else:
        uri = "mongodb://%s:%d" % (host, port)
    return uri

def get_session(config, **kwargs):
    from session import DataSession
    uri = mongo_uri(config['ADSDATA_MONGO_HOST'], config['ADSDATA_MONGO_PORT'], 
                    db=config['ADSDATA_MONGO_DATABASE'], 
                    user=config['ADSDATA_MONGO_USER'], 
                    passwd=config['ADSDATA_MONGO_PASSWORD'])
    return DataSession(config['ADSDATA_MONGO_DATABASE'], uri) 

def map_reduce_listify(session, source, target_collection_name, group_key, value_field):
    """
    Will take a 1:many key:value collection and aggregate values in a list by unique key
    """
    from bson.code import Code

    map_func = Code("function(){ " \
                + "emit( this.%s, { '%s': [this.%s] } ); " % (group_key, value_field, value_field) \
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

def map_reduce_dictify(session, source, target_collection_name, group_key, value_fields, output_key=None):
    """
    Will take a 1:many key:multiple-values collection and aggregate values in a list of dicts
    by unique key
    """
    from bson.code import Code
    if not output_key:
        output_key = target_collection_name
    
    emit_vals = ','.join(["'%s': this.%s" % (x,x) for x in value_fields])
    map_func = Template("""
        function() {
            emit( this.{{ group_key }}, { '{{ output_key }}': [{ {{ emit_vals }} }] });
        };
    """).render(group_key=group_key, emit_vals=emit_vals, output_key=output_key)
    
    reduce_func = Template("""
        function(key, values) {
            var ret = { '{{ output_key }}': [] };
            for ( var i = 0; i < values.length; i++ ) {
                ret['{{ output_key }}'].push.apply(ret['{{ output_key }}'], values[i]['{{ output_key }}']);
            }
            return ret;
        };
    """).render(output_key=output_key)
    
    log.info("running map-reduce on %s" % source.name)
    source.map_reduce(map_func, reduce_func, target_collection_name)
    
    target = session.get_collection(target_collection_name)
    log.info("cleaning up target collection: %s" % target_collection_name)
    target.update({}, {'$rename': {('value.' + output_key) : output_key}}, multi=True)
    target.update({}, {'$unset': { 'value': 1 }}, multi=True)
    source.drop()
    
